package main

import (
	"encoding/json"
	"net/http"
	"time"
	"github.com/sirupsen/logrus"
	"github.com/google/uuid"
)

// NewsHandler handles POST /v1/news requests
type NewsHandler struct {
	producer KafkaProducer
	logger   *logrus.Logger
	metrics  *Metrics
}

// NewNewsHandler creates a new news handler
func NewNewsHandler(producer KafkaProducer, logger *logrus.Logger, metrics *Metrics) *NewsHandler {
	return &NewsHandler{
		producer: producer,
		logger:   logger,
		metrics:  metrics,
	}
}

// ServeHTTP handles the POST /v1/news endpoint
func (h *NewsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	
	// Generate trace ID for request tracking
	traceID := uuid.New().String()
	
	// Increment news received metric
	h.metrics.IncNewsReceived()
	
	// Log request start
	h.logger.WithFields(logrus.Fields{
		"trace_id": traceID,
		"method":   r.Method,
		"path":     r.URL.Path,
	}).Info("news_request_received")

	// Only allow POST
	if r.Method != http.MethodPost {
		h.writeErrorResponse(w, traceID, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Parse request body
	var req NewsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.WithFields(logrus.Fields{
			"trace_id": traceID,
			"error":    err.Error(),
		}).Warn("invalid_json")
		h.writeErrorResponse(w, traceID, http.StatusBadRequest, "invalid JSON")
		return
	}

	// Validate request
	if err := validateNewsRequest(&req); err != nil {
		h.logger.WithFields(logrus.Fields{
			"trace_id": traceID,
			"error":    err.Error(),
		}).Warn("validation_failed")
		h.writeErrorResponse(w, traceID, http.StatusBadRequest, err.Error())
		return
	}

	// Send to Kafka with timing
	kafkaStart := time.Now()
	if err := sendToKafka(h.producer, &req); err != nil {
		h.logger.WithFields(logrus.Fields{
			"trace_id": traceID,
			"error":    err.Error(),
		}).Error("kafka_produce_fail")
		h.metrics.IncKafkaProduceFailed()
		h.writeErrorResponse(w, traceID, http.StatusInternalServerError, "internal server error")
		return
	}
	
	// Record successful Kafka produce
	kafkaLatency := time.Since(kafkaStart).Seconds()
	h.metrics.ObserveKafkaProduceLatency(kafkaLatency)
	h.metrics.IncKafkaProduceSuccess()

	// Generate response
	newsID := uuid.New().String()
	response := NewsResponse{
		NewsID: newsID,
		Status: "queued",
	}

	h.logger.WithFields(logrus.Fields{
		"trace_id": traceID,
		"news_id":  newsID,
		"latency_ms": time.Since(startTime).Milliseconds(),
	}).Info("kafka_produce_ok")

	// Write success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// writeErrorResponse writes a JSON error response
func (h *NewsHandler) writeErrorResponse(w http.ResponseWriter, traceID string, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	errorResp := map[string]interface{}{
		"error":    message,
		"trace_id": traceID,
	}
	
	json.NewEncoder(w).Encode(errorResp)
}

// HealthHandler handles GET /healthz requests
type HealthHandler struct {
	logger *logrus.Logger
}

// NewHealthHandler creates a new health handler
func NewHealthHandler(logger *logrus.Logger) *HealthHandler {
	return &HealthHandler{logger: logger}
}

// ServeHTTP handles the GET /healthz endpoint
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	response := map[string]string{
		"status": "ok",
		"service": "gateway",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}