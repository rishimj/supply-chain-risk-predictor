# Supply Chain Risk Predictor

A real-time supply chain risk monitoring system that analyzes news articles to detect and alert on potential supply chain disruptions.

## Architecture

The system consists of several components:

- **Gateway (Go)**: HTTP API for ingesting news articles
- **Load-Balanced Enrichment Cluster**:
  - **3x Enrichment Service Instances (Python)**: NLP-based company extraction and sentiment analysis
  - **Nginx Load Balancer**: Distributes requests across enrichment instances using `least_conn` algorithm
  - **Automatic Failover**: Routes around unhealthy instances (max_fails=3, fail_timeout=30s)
- **Flink Processor**: Stream processing for feature aggregation and alerting
- **Redis**: Feature storage for online serving and alert cooldown tracking
- **Kafka**: Message queue for reliable event streaming

### Load-Balanced Architecture Diagram

```
Gateway (8080)
    ↓
Kafka
    ↓
Flink Processor (8083)
    ↓
Nginx Load Balancer (8085)
    ├─→ Enrichment-1 (8082)
    ├─→ Enrichment-2 (8086)
    └─→ Enrichment-3 (8087)
    ↓
Redis (6379) ← Feature Storage & Alert Cooldowns
```

**Benefits of Load Balancing:**

- **3x Throughput**: Parallel processing across 3 enrichment instances
- **High Availability**: Automatic failover if an instance fails
- **Better Resource Utilization**: Distributed CPU/memory load
- **Zero Downtime Deploys**: Can restart instances individually

## Features

### Real-Time Risk Scoring

The system computes supply chain risk scores for companies based on:

- Sentiment analysis of news mentions
- Volume of negative vs. positive news
- Temporal patterns and trends

### Slack Alerting

Automated Slack notifications for high-risk events with:

- **Universal Risk Threshold**: 0.7 (configurable)
- **Redis-Backed Cooldown**: 30-minute cooldown per company to prevent alert spam
- **Graceful Degradation**: System continues operating if Slack/Redis fail
- **Parallel Processing**: Alerts run alongside feature storage without blocking

## Configuration

### Slack Alerts

Configure Slack notifications via environment variables in `docker-compose.yml`:

```yaml
environment:
  SLACK_WEBHOOK_URL: "disabled" # Set to your Slack webhook URL to enable
  SLACK_CHANNEL: "#supply-chain-alerts"
  ALERT_COOLDOWN_MINUTES: "30"
```

#### Setting Up Slack Webhook

1. Go to your Slack workspace settings
2. Navigate to "Apps" → "Incoming Webhooks"
3. Click "Add to Slack" and select a channel
4. Copy the webhook URL
5. Update `SLACK_WEBHOOK_URL` in `docker-compose.yml`
6. Restart Flink processor: `docker-compose restart flink-processor`

#### Alert Behavior

- **Trigger Threshold**: Risk score ≥ 0.7
- **Cooldown Period**: 30 minutes per company
- **Cooldown Storage**: Redis (persists across restarts)
- **Fail-Safe**: If Redis fails, alerts are still sent (no missed critical alerts)
- **Alert Format**: Rich Slack attachments with severity colors and metadata

Example alert:

```
🚨 Supply Chain Risk Alert
CRITICAL Risk Alert: AAPL
Risk Score: 0.850
Threshold: 0.700
Window End: 2025-11-15 23:45:00 UTC
```

### Other Configuration

```yaml
environment:
  KAFKA_BOOTSTRAP: kafka:9092
  REDIS_URL: redis://redis:6379/0
  USE_MOCK_ENRICHMENT: "true" # Set to false for real NLP processing
  ENRICHMENT_ENDPOINT: http://enrichment:8082
  FLINK_PARALLELISM: 2
```

## Running the System

### Start All Services

```bash
docker-compose up -d
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f flink-processor
```

### Check Alert Status

View Redis cooldown keys:

```bash
docker exec -it supply-chain-redis redis-cli
> KEYS "alert:last_sent:*"
> TTL alert:last_sent:AAPL
> GET alert:last_sent:AAPL
```

### Send Test News

```bash
curl -X POST http://localhost:8080/api/v1/news \
  -H "Content-Type: application/json" \
  -d '{
    "news_id": "test123",
    "content": "AAPL supply chain disrupted by severe factory shutdown",
    "pub_time": "2025-11-15T23:45:00Z",
    "source": "test"
  }'
```

## Testing

### Verify Load Balancer Setup

```bash
./verify_load_balancer.sh
```

This script verifies:

- All 3 enrichment instances are running
- Nginx load balancer is healthy
- Requests are distributed across instances
- Failover capabilities

### Run Unit Tests

```bash
cd flink-processor
pytest tests/ -v
```

### Run Load Balancing Tests

```bash
# Load balancer health and distribution tests
pytest tests/test_load_balancing.py -v

# End-to-end integration with load balancer
pytest tests/test_e2e_load_balanced.py -v
```

### Run Specific Test Suites

```bash
# Redis cooldown tests
pytest tests/test_alerting_redis.py -v

# Integration tests
pytest tests/test_alerting_integration.py -v

# Original alerting tests
pytest tests/test_alerting.py -v
```

## Monitoring

- **Kafka UI**: http://localhost:8090
- **Redis UI**: http://localhost:8081 (admin/admin)
- **Gateway Health**: http://localhost:8080/healthz
- **Load Balancer Health**: http://localhost:8085/healthz
- **Load Balancer Stats**: http://localhost:8085/nginx_status
- **Enrichment-1 Health**: http://localhost:8082/healthz
- **Enrichment-2 Health**: http://localhost:8086/healthz
- **Enrichment-3 Health**: http://localhost:8087/healthz

## Development

### Project Structure

```
.
├── gateway-go/           # Go API gateway
├── enrichment-py/        # Python NLP service
├── nginx/                # Load balancer configuration
│   ├── nginx.conf        # Nginx load balancing config
│   └── Dockerfile        # Nginx container build
├── flink-processor/      # Stream processing
│   ├── src/
│   │   ├── alerting.py           # Alert management
│   │   ├── news_processing_job.py # Main Flink job
│   │   ├── models.py             # Data models
│   │   └── ...
│   └── tests/
│       ├── test_load_balancing.py      # Load balancer tests
│       ├── test_e2e_load_balanced.py   # E2E with load balancer
│       ├── test_alerting_redis.py      # Redis cooldown tests
│       ├── test_alerting_integration.py # E2E alert tests
│       └── test_alerting.py            # Core alert tests
├── tests/
│   ├── test_load_balancing.py    # Load balancer health & distribution
│   └── test_e2e_load_balanced.py # Full pipeline integration
├── verify_load_balancer.sh       # Load balancer verification script
└── docker-compose.yml
```

## Troubleshooting

### Alerts Not Sending

1. Check Slack webhook URL is set: `docker-compose config | grep SLACK_WEBHOOK_URL`
2. View Flink logs: `docker-compose logs flink-processor | grep -i alert`
3. Verify Redis connection: `docker exec -it supply-chain-redis redis-cli ping`

### Duplicate Alerts

- Cooldown is enforced via Redis with 30-min TTL
- Check Redis keys: `redis-cli KEYS "alert:last_sent:*"`
- Verify TTL: `redis-cli TTL alert:last_sent:AAPL`

### Redis Connection Issues

- Flink will log errors but continue processing
- Alerts will still send (fail-safe behavior)
- Check Redis health: `docker-compose ps redis`

## License

MIT
