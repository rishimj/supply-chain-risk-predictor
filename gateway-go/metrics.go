package main

import (
	"net/http"
	
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for the gateway
type Metrics struct {
	newsReceived     prometheus.Counter
	kafkaProduceLatency prometheus.Histogram
	kafkaProduceSuccess prometheus.Counter
	kafkaProduceFailed  prometheus.Counter
}

// NewMetrics creates and registers all metrics
func NewMetrics() *Metrics {
	m := &Metrics{
		newsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "news_received_total",
			Help: "Total number of news requests received",
		}),
		kafkaProduceLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kafka_produce_latency_seconds",
			Help:    "Time taken to produce message to Kafka in seconds",
			Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0},
		}),
		kafkaProduceSuccess: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_produce_success_total",
			Help: "Total number of successful Kafka message productions",
		}),
		kafkaProduceFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kafka_produce_failed_total",
			Help: "Total number of failed Kafka message productions",
		}),
	}

	// Register all metrics
	prometheus.MustRegister(
		m.newsReceived,
		m.kafkaProduceLatency,
		m.kafkaProduceSuccess,
		m.kafkaProduceFailed,
	)

	return m
}

// IncNewsReceived increments the news received counter
func (m *Metrics) IncNewsReceived() {
	m.newsReceived.Inc()
}

// ObserveKafkaProduceLatency records Kafka produce latency
func (m *Metrics) ObserveKafkaProduceLatency(seconds float64) {
	m.kafkaProduceLatency.Observe(seconds)
}

// IncKafkaProduceSuccess increments successful Kafka produce counter
func (m *Metrics) IncKafkaProduceSuccess() {
	m.kafkaProduceSuccess.Inc()
}

// IncKafkaProduceFailed increments failed Kafka produce counter
func (m *Metrics) IncKafkaProduceFailed() {
	m.kafkaProduceFailed.Inc()
}

// MetricsHandler returns an HTTP handler for Prometheus metrics
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}