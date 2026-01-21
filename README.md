# Supply Chain Risk Predictor

Real-time supply chain risk monitoring using NLP and stream processing.

## What It Does

Ingests news articles → Extracts company mentions → Analyzes sentiment → Calculates risk scores → Sends alerts

## Architecture

```
┌──────────┐    ┌───────┐    ┌───────────────┐    ┌────────────┐
│  Gateway │───▶│ Kafka │───▶│ Flink Cluster │───▶│ Enrichment │
│   (Go)   │    │       │    │  (PyFlink)    │    │  (Python)  │
└──────────┘    └───────┘    └───────────────┘    └────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
              ┌──────────┐                   ┌────────────┐
              │  Redis   │                   │ PostgreSQL │
              │(features)│                   │  (alerts)  │
              └──────────┘                   └────────────┘
```

## Quick Start

### Docker Compose (Development)

```bash
# Start everything
docker-compose up -d

# Send a test article
curl -X POST http://localhost:8080/api/v1/news \
  -H "Content-Type: application/json" \
  -d '{"id": "1", "title": "Tesla supply chain disruption", "content": "...", "published": "2026-01-20T10:00:00Z", "source": "reuters"}'

# View Flink dashboard
open http://localhost:8081
```

### Kubernetes (Production)

```bash
cd k8s
./deploy.sh dev
./monitor.sh --port-forward
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Gateway | 8080 | REST API for news ingestion |
| Flink UI | 8081 | Stream processing dashboard |
| Kafka UI | 8090 | Message queue monitoring |
| Redis | 6379 | Feature cache |
| PostgreSQL | 5432 | Alert storage |


## Key Features

- **Real-time Processing**: Sub-second latency with Apache Flink
- **Fault Tolerance**: Checkpointing with exactly-once semantics
- **Auto-scaling**: HPA-based scaling in Kubernetes
- **Alerting**: Slack notifications with cooldown periods
- **Monitoring**: Prometheus + Grafana integration


## Common Commands

```bash
# Start/stop
make start
make stop

# Logs
make logs
make logs-flink

# Status
make status
make flink-status

# Test
make test
```

## License

MIT
