package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"github.com/sirupsen/logrus"
)


// TestValidateNewsRequest tests request validation
func TestValidateNewsRequest(t *testing.T) {
	tests := []struct {
		name         string
		request      NewsRequest
		expectError  bool
		errorMessage string
	}{
		{
			name: "valid_request_with_all_fields",
			request: NewsRequest{
				Headline:  "Test headline",
				URL:       "https://example.com",
				Published: "2024-01-01T10:00:00Z",
				FullText:  "Full article text",
			},
			expectError: false,
		},
		{
			name: "valid_request_without_fulltext",
			request: NewsRequest{
				Headline:  "Test headline",
				URL:       "https://example.com",
				Published: "2024-01-01T10:00:00Z",
			},
			expectError: false,
		},
		{
			name: "missing_headline",
			request: NewsRequest{
				URL:       "https://example.com",
				Published: "2024-01-01T10:00:00Z",
			},
			expectError:  true,
			errorMessage: "headline is required",
		},
		{
			name: "missing_url",
			request: NewsRequest{
				Headline:  "Test headline",
				Published: "2024-01-01T10:00:00Z",
			},
			expectError:  true,
			errorMessage: "url is required",
		},
		{
			name: "missing_published",
			request: NewsRequest{
				Headline: "Test headline",
				URL:      "https://example.com",
			},
			expectError:  true,
			errorMessage: "published is required",
		},
		{
			name: "invalid_published_format",
			request: NewsRequest{
				Headline:  "Test headline",
				URL:       "https://example.com",
				Published: "invalid-date",
			},
			expectError:  true,
			errorMessage: "published must be valid ISO-8601 format",
		},
		{
			name: "empty_headline",
			request: NewsRequest{
				Headline:  "",
				URL:       "https://example.com",
				Published: "2024-01-01T10:00:00Z",
			},
			expectError:  true,
			errorMessage: "headline is required",
		},
		{
			name: "empty_url",
			request: NewsRequest{
				Headline:  "Test headline",
				URL:       "",
				Published: "2024-01-01T10:00:00Z",
			},
			expectError:  true,
			errorMessage: "url is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNewsRequest(&tt.request)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if err.Error() != tt.errorMessage {
					t.Errorf("expected error message %q but got %q", tt.errorMessage, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got %v", err)
				}
			}
		})
	}
}

// TestNewsEndpointValidation tests the HTTP endpoint with invalid requests
func TestNewsEndpointValidation(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel) // Suppress logs during tests
	
	// Create metrics once for the entire test function
	metrics := NewMetrics()
	
	tests := []struct {
		name           string
		method         string
		requestBody    string
		expectedStatus int
		expectedError  string
	}{
		{
			name:           "invalid_json",
			method:         "POST",
			requestBody:    `{"headline": "test"`,
			expectedStatus: http.StatusBadRequest,
			expectedError:  "invalid JSON",
		},
		{
			name:           "missing_headline",
			method:         "POST",
			requestBody:    `{"url": "https://example.com", "published": "2024-01-01T10:00:00Z"}`,
			expectedStatus: http.StatusBadRequest,
			expectedError:  "headline is required",
		},
		{
			name:           "missing_url",
			method:         "POST",
			requestBody:    `{"headline": "test", "published": "2024-01-01T10:00:00Z"}`,
			expectedStatus: http.StatusBadRequest,
			expectedError:  "url is required",
		},
		{
			name:           "missing_published",
			method:         "POST",
			requestBody:    `{"headline": "test", "url": "https://example.com"}`,
			expectedStatus: http.StatusBadRequest,
			expectedError:  "published is required",
		},
		{
			name:           "method_not_allowed",
			method:         "GET",
			requestBody:    `{}`,
			expectedStatus: http.StatusMethodNotAllowed,
			expectedError:  "method not allowed",
		},
		{
			name:           "valid_request",
			method:         "POST",
			requestBody:    `{"headline": "test", "url": "https://example.com", "published": "2024-01-01T10:00:00Z"}`,
			expectedStatus: http.StatusOK,
			expectedError:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockProducer := &MockKafkaProducer{}
			handler := NewNewsHandler(mockProducer, logger, metrics)

			req := httptest.NewRequest(tt.method, "/v1/news", bytes.NewBufferString(tt.requestBody))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			var response map[string]interface{}
			if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
				t.Errorf("failed to decode response: %v", err)
				return
			}

			if tt.expectedError != "" {
				if errorMsg, ok := response["error"].(string); !ok || errorMsg != tt.expectedError {
					t.Errorf("expected error %q, got %q", tt.expectedError, errorMsg)
				}
			} else {
				// Valid request should have news_id and status
				if newsID, ok := response["news_id"].(string); !ok || newsID == "" {
					t.Error("expected news_id in response")
				}
				if status, ok := response["status"].(string); !ok || status != "queued" {
					t.Error("expected status 'queued' in response")
				}
			}
		})
	}
}

// TestHealthEndpoint tests the health check endpoint
func TestHealthEndpoint(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel) // Suppress logs during tests
	
	handler := NewHealthHandler(logger)

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Errorf("failed to decode response: %v", err)
		return
	}

	if status, ok := response["status"].(string); !ok || status != "ok" {
		t.Error("expected status 'ok' in health response")
	}

	if service, ok := response["service"].(string); !ok || service != "gateway" {
		t.Error("expected service 'gateway' in health response")
	}
}

// TestHealthEndpointMethodNotAllowed tests health endpoint with wrong method
func TestHealthEndpointMethodNotAllowed(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	
	handler := NewHealthHandler(logger)

	req := httptest.NewRequest("POST", "/healthz", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

// Mock Kafka producer for testing
type MockKafkaProducer struct {
	messages    []MockKafkaMessage
	shouldError bool
	errorMsg    string
}

type MockKafkaMessage struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

func (m *MockKafkaProducer) SendMessage(topic, key string, value []byte, headers map[string]string) error {
	if m.shouldError {
		return fmt.Errorf("%s", m.errorMsg)
	}
	
	m.messages = append(m.messages, MockKafkaMessage{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
	})
	return nil
}

func (m *MockKafkaProducer) Close() error {
	return nil
}

// TestKafkaProducerIntegration tests that messages are sent to Kafka correctly
func TestKafkaProducerIntegration(t *testing.T) {
	tests := []struct {
		name         string
		request      NewsRequest
		expectError  bool
		expectedTopic string
	}{
		{
			name: "successful_kafka_send",
			request: NewsRequest{
				Headline:  "Test headline",
				URL:       "https://example.com",
				Published: "2024-01-01T10:00:00Z",
				FullText:  "Test content",
			},
			expectError:   false,
			expectedTopic: "raw_news_fulltext",
		},
		{
			name: "successful_kafka_send_without_fulltext",
			request: NewsRequest{
				Headline:  "Test headline 2",
				URL:       "https://example2.com",
				Published: "2024-01-02T11:00:00Z",
			},
			expectError:   false,
			expectedTopic: "raw_news_fulltext",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockProducer := &MockKafkaProducer{}
			
			// Test the message creation and sending
			err := sendToKafka(mockProducer, &tt.request)
			
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			
			// Verify message was sent
			if len(mockProducer.messages) != 1 {
				t.Errorf("expected 1 message, got %d", len(mockProducer.messages))
				return
			}
			
			msg := mockProducer.messages[0]
			if msg.Topic != tt.expectedTopic {
				t.Errorf("expected topic %q, got %q", tt.expectedTopic, msg.Topic)
			}
			
			// Verify message content matches schema
			var kafkaMsg KafkaMessage
			if err := json.Unmarshal(msg.Value, &kafkaMsg); err != nil {
				t.Errorf("failed to unmarshal message: %v", err)
				return
			}
			
			if kafkaMsg.NewsID == "" {
				t.Error("news_id should not be empty")
			}
			if kafkaMsg.Headline != tt.request.Headline {
				t.Errorf("expected headline %q, got %q", tt.request.Headline, kafkaMsg.Headline)
			}
			if kafkaMsg.URL != tt.request.URL {
				t.Errorf("expected url %q, got %q", tt.request.URL, kafkaMsg.URL)
			}
			if kafkaMsg.Published != tt.request.Published {
				t.Errorf("expected published %q, got %q", tt.request.Published, kafkaMsg.Published)
			}
			if kafkaMsg.FullText != tt.request.FullText {
				t.Errorf("expected full_text %q, got %q", tt.request.FullText, kafkaMsg.FullText)
			}
		})
	}
}

// TestKafkaProducerFailure tests error handling when Kafka fails
func TestKafkaProducerFailure(t *testing.T) {
	request := NewsRequest{
		Headline:  "Test headline",
		URL:       "https://example.com",
		Published: "2024-01-01T10:00:00Z",
	}

	mockProducer := &MockKafkaProducer{
		shouldError: true,
		errorMsg:    "kafka connection failed",
	}

	err := sendToKafka(mockProducer, &request)
	if err == nil {
		t.Error("expected error but got none")
	}

	if err.Error() != "kafka connection failed" {
		t.Errorf("expected error message 'kafka connection failed', got %q", err.Error())
	}
}

// TestKafkaMessageSchema tests that the Kafka message matches expected schema
func TestKafkaMessageSchema(t *testing.T) {
	// Skip for now - will implement after message creation logic
	t.Skip("Message creation logic not yet implemented")
}