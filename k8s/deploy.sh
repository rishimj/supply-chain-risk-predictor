#!/bin/bash

# Supply Chain Risk Predictor - Kubernetes Deployment Script
# Usage: ./deploy.sh [dev|prod] [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT="dev"
SKIP_BUILD=false
DRY_RUN=false
NAMESPACE="supply-chain-risk"

# Print colored messages
print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Print usage
usage() {
    cat << EOF
Usage: $0 [ENVIRONMENT] [OPTIONS]

ENVIRONMENT:
    dev         Deploy to development environment (default)
    prod        Deploy to production environment

OPTIONS:
    --skip-build        Skip Docker image building
    --dry-run          Show what would be deployed without applying
    --namespace NAME   Use custom namespace (default: supply-chain-risk)
    -h, --help         Show this help message

EXAMPLES:
    $0 dev                      # Deploy to dev
    $0 prod --skip-build        # Deploy to prod without rebuilding images
    $0 dev --dry-run            # Preview dev deployment

EOF
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        dev|prod)
            ENVIRONMENT=$1
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --namespace)
            NAMESPACE=$2
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            ;;
    esac
done

print_info "==================================================="
print_info "Supply Chain Risk Predictor - Kubernetes Deployment"
print_info "==================================================="
print_info "Environment: $ENVIRONMENT"
print_info "Namespace: $NAMESPACE"
print_info "Skip Build: $SKIP_BUILD"
print_info "Dry Run: $DRY_RUN"
print_info "==================================================="

# Check prerequisites
print_info "Checking prerequisites..."

if ! command -v kubectl &> /dev/null; then
    print_error "kubectl not found. Please install kubectl."
    exit 1
fi

if ! command -v kustomize &> /dev/null; then
    print_warning "kustomize not found. Using kubectl kustomize instead."
    KUSTOMIZE_CMD="kubectl kustomize"
else
    KUSTOMIZE_CMD="kustomize"
fi

if [ "$SKIP_BUILD" = false ] && ! command -v docker &> /dev/null; then
    print_error "Docker not found. Please install Docker or use --skip-build."
    exit 1
fi

print_success "Prerequisites check passed"

# Check cluster connection
print_info "Checking Kubernetes cluster connection..."
if ! kubectl cluster-info &> /dev/null; then
    print_error "Cannot connect to Kubernetes cluster. Please check your kubeconfig."
    exit 1
fi
print_success "Connected to cluster: $(kubectl config current-context)"

# Build Docker images
if [ "$SKIP_BUILD" = false ]; then
    print_info "Building Docker images..."
    
    cd "$(dirname "$0")/.."
    
    # Build Gateway
    print_info "Building Gateway image..."
    docker build -t supply-chain-gateway:${ENVIRONMENT} -f gateway/Dockerfile gateway/
    print_success "Gateway image built"
    
    # Build Enrichment Service
    print_info "Building Enrichment service image..."
    docker build -t supply-chain-enrichment:${ENVIRONMENT} -f enrichment-py/Dockerfile enrichment-py/
    print_success "Enrichment service image built"
    
    # Build Flink Processor
    print_info "Building Flink processor image..."
    docker build -t supply-chain-flink-processor:${ENVIRONMENT} -f flink-processor/Dockerfile flink-processor/
    print_success "Flink processor image built"
    
    print_success "All images built successfully"
    
    # If using minikube or kind, load images
    if kubectl config current-context | grep -q "minikube"; then
        print_info "Detected minikube. Loading images..."
        minikube image load supply-chain-gateway:${ENVIRONMENT}
        minikube image load supply-chain-enrichment:${ENVIRONMENT}
        minikube image load supply-chain-flink-processor:${ENVIRONMENT}
        print_success "Images loaded to minikube"
    elif kubectl config current-context | grep -q "kind"; then
        print_info "Detected kind. Loading images..."
        kind load docker-image supply-chain-gateway:${ENVIRONMENT}
        kind load docker-image supply-chain-enrichment:${ENVIRONMENT}
        kind load docker-image supply-chain-flink-processor:${ENVIRONMENT}
        print_success "Images loaded to kind"
    fi
else
    print_warning "Skipping Docker image build"
fi

# Deploy with Kustomize
print_info "Deploying to $ENVIRONMENT environment..."

cd "$(dirname "$0")"

if [ "$DRY_RUN" = true ]; then
    print_info "Dry run - showing what would be deployed:"
    $KUSTOMIZE_CMD overlays/$ENVIRONMENT
    exit 0
fi

# Apply the configuration
$KUSTOMIZE_CMD overlays/$ENVIRONMENT | kubectl apply -f -

print_success "Deployment completed!"

# Wait for pods to be ready
print_info "Waiting for pods to be ready..."

kubectl wait --for=condition=ready pod \
    -l app.kubernetes.io/part-of=supply-chain-risk \
    -n $NAMESPACE \
    --timeout=300s || print_warning "Some pods may not be ready yet"

# Display status
print_info "Deployment status:"
kubectl get pods -n $NAMESPACE

print_info ""
print_info "Service endpoints:"
kubectl get svc -n $NAMESPACE

print_info ""
print_success "==================================================="
print_success "Deployment completed successfully!"
print_success "==================================================="
print_info ""
print_info "Next steps:"
print_info "  1. Check pod status: kubectl get pods -n $NAMESPACE"
print_info "  2. View logs: kubectl logs -n $NAMESPACE -l app=<service-name>"
print_info "  3. Access Flink UI: kubectl port-forward -n $NAMESPACE svc/flink-jobmanager 8081:8081"
print_info "  4. Access Grafana: kubectl port-forward -n $NAMESPACE svc/grafana 3000:3000"
print_info "  5. Access Prometheus: kubectl port-forward -n $NAMESPACE svc/prometheus 9090:9090"
print_info ""
print_info "For troubleshooting, run: kubectl describe pod -n $NAMESPACE <pod-name>"
