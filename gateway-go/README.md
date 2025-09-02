# News Gateway Service

A high-performance Go service that ingests news articles and publishes them to Kafka for downstream processing.

## Features

✅ **TDD-First Development** - All code written test-first
✅ **Input Validation** - Strict validation of required fields and formats  
✅ **Kafka Integration** - Publishes messages to `raw_news_fulltext` topic
✅ **Full Observability** - Prometheus metrics, structured JSON logging, trace IDs
✅ **Error Handling** - Never crashes, safe defaults, proper HTTP status codes
✅ **Production Ready** - Idempotent Kafka producer, retries, timeouts

## API Endpoints

### POST /v1/news
Ingests news articles and queues them for processing.

**Request:**
```json
{
  "headline": "required string",
  "url": "required string", 
  "published": "required ISO-8601 timestamp",
  "full_text": "optional string"
}
```

**Response:**
```json
{
  "news_id": "UUID",
  "status": "queued"
}
```

### GET /healthz
Returns service health status.

### GET /metrics (port 9100)
Prometheus metrics endpoint.

## Environment Variables

```bash
KAFKA_BOOTSTRAP=localhost:9092    # Kafka broker addresses
SERVICE_ENV=dev                   # Environment (dev/prod)
PORT=8080                         # Main HTTP port
PROM_PORT=9100                   # Prometheus metrics port
```

## Running the Service

```bash
# Build
go build -o gateway .

# Run with default config
./gateway

# Run with custom config
KAFKA_BOOTSTRAP=broker1:9092,broker2:9092 ./gateway
```

## Testing

```bash
# Run all tests
go test -v

# Run with coverage
go test -cover -v

# Test specific functionality
go test -v -run TestValidateNewsRequest
```

## Metrics Exposed

- `news_received_total` - Total news requests received
- `kafka_produce_latency_seconds` - Kafka produce latency histogram
- `kafka_produce_success_total` - Successful Kafka productions
- `kafka_produce_failed_total` - Failed Kafka productions

## Message Schema

Messages published to Kafka match this exact schema:
```json
{
  "news_id": "UUID",
  "headline": "string",
  "url": "string",
  "published": "ISO-8601",
  "full_text": "string"
}
```

## Error Handling

- Invalid JSON → 400 with error details
- Missing required fields → 400 with field name
- Kafka failures → 500 (retries automatically)
- All errors include trace_id for debugging