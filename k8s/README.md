# Kubernetes Deployment

Production-ready Kubernetes deployment for Supply Chain Risk Predictor.

## Quick Start

### Prerequisites

- Kubernetes cluster (minikube, kind, GKE, EKS, AKS)
- kubectl installed
- Docker installed (for building images)

### Deploy

```bash
# Deploy to development
./deploy.sh dev

# Deploy to production
./deploy.sh prod

# Skip building images (use existing)
./deploy.sh dev --skip-build

# Preview changes without applying
./deploy.sh dev --dry-run
```

### Monitor

```bash
# View all status
./monitor.sh --all

# View logs
./monitor.sh --logs gateway

# Setup port forwards
./monitor.sh --port-forward
```

### Access UIs

After running `./monitor.sh --port-forward`:

- **Flink Web UI**: http://localhost:8081
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9090
- **Gateway API**: http://localhost:8080

### Undeploy

```bash
# Remove deployment (keep data)
./undeploy.sh dev

# Remove deployment and data
./undeploy.sh dev --delete-pvc

# Remove entire namespace
./undeploy.sh dev --delete-namespace
```

## Directory Structure

```
k8s/
├── base/                          # Base Kubernetes manifests
│   ├── namespace.yaml            # Namespace definition
│   ├── secrets.yaml              # Secrets (gitignored in prod)
│   ├── kafka.yaml                # Kafka + Zookeeper StatefulSets
│   ├── redis.yaml                # Redis StatefulSet
│   ├── postgres.yaml             # PostgreSQL StatefulSet
│   ├── gateway.yaml              # Gateway Deployment
│   ├── enrichment.yaml           # Enrichment Deployment + HPA
│   ├── flink.yaml                # Flink cluster (JobManager + TaskManager)
│   ├── prometheus.yaml           # Prometheus Deployment
│   ├── grafana.yaml              # Grafana Deployment
│   ├── ingress.yaml              # Ingress for external access
│   └── kustomization.yaml        # Base kustomization
│
├── overlays/                      # Environment-specific overrides
│   ├── dev/
│   │   └── kustomization.yaml    # Dev configuration
│   └── prod/
│       └── kustomization.yaml    # Production configuration
│
├── configs/                       # ConfigMaps
│   ├── flink-conf-configmap.yaml
│   └── app-config-configmap.yaml
│
├── deploy.sh                      # Deployment script
├── undeploy.sh                    # Cleanup script
├── monitor.sh                     # Monitoring utilities
└── README.md                      # This file
```

## Architecture

```
┌──────────────────────────────────────────────────────┐
│              Kubernetes Cluster                       │
│                                                        │
│  Ingress (nginx)                                      │
│      ↓                                                │
│  Gateway (2 pods) → Kafka → Flink Cluster            │
│      ↓                           ↓                    │
│  Enrichment (3-10 pods)          ↓                   │
│      ↓                           ↓                    │
│  Redis + PostgreSQL ← ← ← ← ← ← ←                    │
│                                                        │
│  Prometheus + Grafana (monitoring)                    │
└──────────────────────────────────────────────────────┘
```

## Components

| Component | Type | Replicas | Description |
|-----------|------|----------|-------------|
| Gateway | Deployment | 2 | REST API for news ingestion |
| Enrichment | Deployment + HPA | 3-10 | NLP processing service |
| Flink JobManager | Deployment | 1 | Flink cluster coordinator |
| Flink TaskManager | Deployment | 3 | Flink worker nodes |
| Kafka | StatefulSet | 1 | Message broker |
| Zookeeper | StatefulSet | 1 | Kafka coordination |
| Redis | StatefulSet | 1 | Feature caching |
| PostgreSQL | StatefulSet | 1 | Alert storage |
| Prometheus | Deployment | 1 | Metrics collection |
| Grafana | Deployment | 1 | Visualization |

## Configuration

### Environment Variables

Edit `k8s/configs/app-config-configmap.yaml`:

```yaml
data:
  KAFKA_BOOTSTRAP_SERVERS: "kafka-service:9092"
  ENRICHMENT_BATCH_SIZE: "10"
  FLINK_JOB_MODE: "shock"  # or "normal"
  SHOCK_SENTIMENT_THRESHOLD: "-0.3"
```

### Secrets

**Never commit secrets to git!** 

Create secrets manually:

```bash
kubectl create secret generic app-secrets \
  --from-literal=POSTGRES_PASSWORD=your-password \
  --from-literal=SLACK_WEBHOOK_URL=your-webhook \
  -n supply-chain-risk
```

Or use:
- Sealed Secrets
- External Secrets Operator
- HashiCorp Vault

### Scaling

```bash
# Manual scaling
kubectl scale deployment enrichment --replicas=5 -n supply-chain-risk

# View HPA status
kubectl get hpa -n supply-chain-risk
```

## Monitoring

### Metrics

```bash
# Pod resource usage
kubectl top pods -n supply-chain-risk

# Node usage
kubectl top nodes
```

### Logs

```bash
# View logs
kubectl logs -n supply-chain-risk -l app=enrichment --tail=100 -f

# Previous container (if crashed)
kubectl logs -n supply-chain-risk <pod-name> --previous
```

### Events

```bash
kubectl get events -n supply-chain-risk --sort-by='.lastTimestamp'
```

## Troubleshooting

### Pod Issues

```bash
# Describe pod
kubectl describe pod <pod-name> -n supply-chain-risk

# Check logs
kubectl logs <pod-name> -n supply-chain-risk

# Exec into pod
kubectl exec -it <pod-name> -n supply-chain-risk -- /bin/bash
```

### Common Problems

1. **ImagePullBackOff**: Image not found or not loaded
   ```bash
   minikube image load supply-chain-gateway:dev
   ```

2. **CrashLoopBackOff**: Check logs for errors
   ```bash
   kubectl logs <pod-name> -n supply-chain-risk --previous
   ```

3. **Pending**: Resource constraints or PVC issues
   ```bash
   kubectl describe pod <pod-name> -n supply-chain-risk
   kubectl get pvc -n supply-chain-risk
   ```

### Debug Network

```bash
# Create debug pod
kubectl run debug --rm -it --image=nicolaka/netshoot \
  -n supply-chain-risk -- bash

# Test connectivity
nslookup kafka-service
curl http://gateway-service:8080/health
```

## Production Checklist

- [ ] Use managed Kubernetes (GKE, EKS, AKS)
- [ ] Setup proper secrets management
- [ ] Configure network policies
- [ ] Enable RBAC
- [ ] Setup backups for StatefulSets
- [ ] Configure monitoring and alerting
- [ ] Use persistent storage with snapshots
- [ ] Setup CI/CD pipeline
- [ ] Configure pod disruption budgets
- [ ] Enable pod security policies
- [ ] Setup ingress with TLS
- [ ] Configure resource quotas
- [ ] Test disaster recovery

## Documentation

For detailed documentation, see:
- [Full Kubernetes Deployment Guide](../documents/KUBERNETES_DEPLOYMENT.md)
- [Flink Cluster Documentation](../documents/FLINK_CLUSTER.md)
- [Monitoring Setup](../documents/MONITORING.md)

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review pod logs and events
3. Consult the detailed documentation
4. Check Kubernetes cluster health
