.PHONY: help start stop restart logs logs-gateway logs-kafka logs-flink build test test-gateway test-kafka send-news clean flink-status flink-ui verify-flink

# Default target
help: ## Show this help message
	@echo "Supply Chain Risk Predictor - Docker Services"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

start: ## Start all services
	@echo "🚀 Starting all services..."
	docker-compose up -d
	@echo "✅ Services started!"
	@echo "🔗 Gateway API: http://localhost:8080"
	@echo "📊 Metrics: http://localhost:9100/metrics"
	@echo "🖥️  Kafka UI: http://localhost:8090"
	@echo "🔴 Redis UI: http://localhost:8084 (admin/admin)"
	@echo "⚡ Flink Web UI: http://localhost:8081"
	@echo "🐘 PostgreSQL: localhost:5432 (supply_chain_user/changeme)"

stop: ## Stop all services
	@echo "🛑 Stopping all services..."
	docker-compose down
	@echo "✅ Services stopped!"

restart: stop start ## Restart all services

logs: ## Show logs for all services
	docker-compose logs -f

logs-gateway: ## Show logs for gateway service only
	docker-compose logs -f gateway

logs-kafka: ## Show logs for kafka service only
	docker-compose logs -f kafka

logs-kafka-ui: ## Show logs for kafka-ui service only
	docker-compose logs -f kafka-ui

logs-flink: ## Show logs for Flink cluster
	docker-compose logs -f flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 flink-processor

logs-flink-job: ## Show logs for Flink job processor
	docker-compose logs -f flink-processor

build: ## Build all Docker images
	@echo "🔨 Building services..."
	docker-compose build
	@echo "✅ Build complete!"

rebuild: ## Rebuild images without cache
	@echo "🔨 Rebuilding services (no cache)..."
	docker-compose build --no-cache
	@echo "✅ Rebuild complete!"

test: test-gateway test-kafka ## Test all services

test-gateway: ## Test gateway service health
	@echo "🧪 Testing gateway health..."
	@curl -s http://localhost:8080/healthz | jq . || echo "❌ Gateway not responding"
	@echo "🧪 Testing metrics endpoint..."
	@curl -s http://localhost:9100/metrics | head -5 || echo "❌ Metrics not responding"

test-kafka: ## Test kafka service
	@echo "🧪 Testing Kafka..."
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list || echo "❌ Kafka not responding"

send-news: ## Send a test news article
	@echo "📰 Sending test news..."
	@curl -X POST http://localhost:8080/v1/news \
		-H "Content-Type: application/json" \
		-d '{ \
			"headline": "Test Supply Chain Disruption Alert", \
			"url": "https://example.com/supply-chain-news", \
			"published": "'$(shell date -u +%Y-%m-%dT%H:%M:%SZ)'", \
			"full_text": "Major semiconductor shortage affecting automotive supply chains across Asia-Pacific region." \
		}' | jq . || echo "❌ Failed to send news"

status: ## Show status of all services
	@echo "📊 Service Status:"
	@docker-compose ps

flink-status: ## Show detailed Flink cluster status
	@./scripts/monitor_flink.sh

flink-ui: ## Open Flink Web UI in browser
	@echo "⚡ Opening Flink Web UI..."
	@open http://localhost:8081 || xdg-open http://localhost:8081 || echo "Open http://localhost:8081 in your browser"

verify-flink: ## Verify Flink cluster setup and health
	@./scripts/verify_flink_cluster.sh

verify-lb: ## Verify load balancer setup
	@./scripts/verify_load_balancer.sh

clean: ## Clean up Docker resources
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	docker system prune -f
	@echo "✅ Cleanup complete!"

# Development targets
dev-gateway: ## Run gateway in development mode (outside Docker)
	@echo "🔧 Starting gateway in development mode..."
	cd gateway-go && KAFKA_BOOTSTRAP=localhost:9092 SERVICE_ENV=dev go run .

debug-gateway: ## Debug gateway with Delve (outside Docker)
	@echo "🐛 Starting gateway in debug mode..."
	cd gateway-go && KAFKA_BOOTSTRAP=localhost:9092 SERVICE_ENV=dev dlv debug .

test-unit: ## Run unit tests
	@echo "🧪 Running unit tests..."
	cd gateway-go && go test -v -cover

# Quick development workflow
dev: start dev-gateway ## Start Kafka in Docker, run gateway locally