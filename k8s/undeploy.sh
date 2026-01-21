#!/bin/bash

# Supply Chain Risk Predictor - Kubernetes Undeployment Script
# Usage: ./undeploy.sh [dev|prod] [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT="dev"
DELETE_PVC=false
DELETE_NAMESPACE=false
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
    dev         Undeploy from development environment (default)
    prod        Undeploy from production environment

OPTIONS:
    --delete-pvc          Delete PersistentVolumeClaims (data will be lost!)
    --delete-namespace    Delete entire namespace
    --namespace NAME      Use custom namespace (default: supply-chain-risk)
    -h, --help           Show this help message

EXAMPLES:
    $0 dev                           # Remove dev deployment
    $0 prod --delete-pvc             # Remove prod and delete data
    $0 dev --delete-namespace        # Delete entire dev namespace

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
        --delete-pvc)
            DELETE_PVC=true
            shift
            ;;
        --delete-namespace)
            DELETE_NAMESPACE=true
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

print_warning "==================================================="
print_warning "Supply Chain Risk Predictor - Kubernetes Undeployment"
print_warning "==================================================="
print_warning "Environment: $ENVIRONMENT"
print_warning "Namespace: $NAMESPACE"
print_warning "Delete PVCs: $DELETE_PVC"
print_warning "Delete Namespace: $DELETE_NAMESPACE"
print_warning "==================================================="

# Confirmation prompt
read -p "Are you sure you want to continue? (yes/no): " -r
echo
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    print_info "Undeployment cancelled."
    exit 0
fi

# Check kubectl
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

# Check cluster connection
print_info "Checking Kubernetes cluster connection..."
if ! kubectl cluster-info &> /dev/null; then
    print_error "Cannot connect to Kubernetes cluster."
    exit 1
fi
print_success "Connected to cluster: $(kubectl config current-context)"

cd "$(dirname "$0")"

if [ "$DELETE_NAMESPACE" = true ]; then
    print_warning "Deleting entire namespace: $NAMESPACE"
    kubectl delete namespace $NAMESPACE --ignore-not-found=true
    print_success "Namespace deleted"
    exit 0
fi

# Delete resources
print_info "Removing $ENVIRONMENT deployment..."
$KUSTOMIZE_CMD overlays/$ENVIRONMENT | kubectl delete -f - --ignore-not-found=true

print_success "Deployment removed"

# Delete PVCs if requested
if [ "$DELETE_PVC" = true ]; then
    print_warning "Deleting PersistentVolumeClaims (data will be lost)..."
    kubectl delete pvc -n $NAMESPACE --all
    print_success "PVCs deleted"
else
    print_info "PVCs retained. Use --delete-pvc to remove data."
fi

print_success "==================================================="
print_success "Undeployment completed!"
print_success "==================================================="
