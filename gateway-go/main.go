package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// Config holds application configuration
type Config struct {
	KafkaBrokers    []string
	ServiceEnv      string
	Port            string
	PrometheusPort  string
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() *Config {
	return &Config{
		KafkaBrokers:   strings.Split(getEnv("KAFKA_BOOTSTRAP", "localhost:9092"), ","),
		ServiceEnv:     getEnv("SERVICE_ENV", "dev"),
		Port:           getEnv("PORT", "8080"),
		PrometheusPort: getEnv("PROM_PORT", "9100"),
	}
}

// getEnv gets environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	// Load configuration
	config := LoadConfig()

	// Setup logger with JSON output
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})
	
	// Set log level based on environment
	if config.ServiceEnv == "dev" {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	logger.WithFields(logrus.Fields{
		"service": "gateway",
		"env":     config.ServiceEnv,
	}).Info("starting_gateway_service")

	// Initialize metrics
	metrics := NewMetrics()

	// Create Kafka producer
	producer, err := NewSaramaKafkaProducer(config.KafkaBrokers)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error":   err.Error(),
			"brokers": config.KafkaBrokers,
		}).Fatal("failed_to_create_kafka_producer")
	}
	defer producer.Close()

	// Create handlers
	newsHandler := NewNewsHandler(producer, logger, metrics)
	healthHandler := NewHealthHandler(logger)

	// Setup main HTTP server
	mainRouter := mux.NewRouter()
	mainRouter.Handle("/v1/news", newsHandler).Methods("POST")
	mainRouter.Handle("/healthz", healthHandler).Methods("GET")

	mainServer := &http.Server{
		Addr:         ":" + config.Port,
		Handler:      mainRouter,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Setup metrics server
	metricsRouter := mux.NewRouter()
	metricsRouter.Handle("/metrics", MetricsHandler())

	metricsServer := &http.Server{
		Addr:         ":" + config.PrometheusPort,
		Handler:      metricsRouter,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start servers
	go func() {
		logger.WithFields(logrus.Fields{
			"port": config.Port,
		}).Info("starting_main_server")
		
		if err := mainServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithFields(logrus.Fields{
				"error": err.Error(),
			}).Fatal("main_server_failed")
		}
	}()

	go func() {
		logger.WithFields(logrus.Fields{
			"port": config.PrometheusPort,
		}).Info("starting_metrics_server")
		
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithFields(logrus.Fields{
				"error": err.Error(),
			}).Fatal("metrics_server_failed")
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting_down_servers")

	// Shutdown servers gracefully
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := mainServer.Shutdown(ctx); err != nil {
		logger.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Error("main_server_shutdown_error")
	}

	if err := metricsServer.Shutdown(ctx); err != nil {
		logger.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Error("metrics_server_shutdown_error")
	}

	logger.Info("servers_stopped")
}