package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

// KafkaProducer interface for sending messages to Kafka
type KafkaProducer interface {
	SendMessage(topic, key string, value []byte, headers map[string]string) error
	Close() error
}

// SaramaKafkaProducer is a real Kafka producer using Sarama
type SaramaKafkaProducer struct {
	producer sarama.SyncProducer
}

// NewSaramaKafkaProducer creates a new Sarama-based Kafka producer
func NewSaramaKafkaProducer(brokers []string) (*SaramaKafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas
	config.Producer.Retry.Max = 2                    // Retry up to 2 times
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true                // Ensure idempotence
	config.Net.MaxOpenRequests = 1                   // Required for idempotence
	
	// Timeout configurations
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second
	
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &SaramaKafkaProducer{producer: producer}, nil
}

// SendMessage sends a message to Kafka
func (s *SaramaKafkaProducer) SendMessage(topic, key string, value []byte, headers map[string]string) error {
	// Convert headers to Sarama format
	saramaHeaders := make([]sarama.RecordHeader, 0, len(headers))
	for k, v := range headers {
		saramaHeaders = append(saramaHeaders, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.StringEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: saramaHeaders,
	}

	_, _, err := s.producer.SendMessage(msg)
	return err
}

// Close closes the Kafka producer
func (s *SaramaKafkaProducer) Close() error {
	return s.producer.Close()
}

// sendToKafka sends a news request to Kafka with proper schema
func sendToKafka(producer KafkaProducer, req *NewsRequest) error {
	// Generate unique news ID
	newsID := uuid.New().String()
	
	// Create Kafka message matching the schema
	kafkaMsg := KafkaMessage{
		NewsID:    newsID,
		Headline:  req.Headline,
		URL:       req.URL,
		Published: req.Published,
		FullText:  req.FullText,
	}
	
	// Marshal to JSON
	msgBytes, err := json.Marshal(kafkaMsg)
	if err != nil {
		return err
	}
	
	// Send to Kafka
	// Key is the news_id for partitioning
	return producer.SendMessage("raw_news_fulltext", newsID, msgBytes, nil)
}