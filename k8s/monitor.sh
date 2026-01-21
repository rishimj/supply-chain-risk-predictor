#!/bin/bash

# Supply Chain Risk Predictor - Kubernetes Monitoring Script
# Usage: ./monitor.sh [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

NAMESPACE="supply-chain-risk"

print_info() {
    echo -e "${BLUE}$1${NC}"
}

print_success() {
    echo -e "${GREEN}$1${NC}"
}

print_warning() {
    echo -e "${YELLOW}$1${NC}"
}

print_error() {
    echo -e "${RED}$1${NC}"
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

OPTIONS:
    --pods              Show pod status
    --logs SERVICE      Show logs for a service
    --describe POD      Describe a specific pod
    --port-forward      Setup port forwards for all UIs
    --metrics           Show resource metrics
    --events            Show recent events
    --all               Show comprehensive status
    -h, --help          Show this help message

EXAMPLES:
    $0 --all                        # Show everything
    $0 --logs gateway               # Show gateway logs
    $0 --port-forward               # Access all UIs locally

EOF
    exit 1
}

show_pods() {
    print_info "================================"
    print_info "Pod Status"
    print_info "================================"
    kubectl get pods -n $NAMESPACE -o wide
}

show_services() {
    print_info ""
    print_info "================================"
    print_info "Services"
    print_info "================================"
    kubectl get svc -n $NAMESPACE
}

show_logs() {
    local service=$1
    print_info "================================"
    print_info "Logs for $service"
    print_info "================================"
    kubectl logs -n $NAMESPACE -l app=$service --tail=100 --follow
}

describe_pod() {
    local pod=$1
    kubectl describe pod -n $NAMESPACE $pod
}

show_metrics() {
    print_info "================================"
    print_info "Resource Metrics"
    print_info "================================"
    kubectl top pods -n $NAMESPACE 2>/dev/null || print_warning "Metrics server not available"
}

show_events() {
    print_info ""
    print_info "================================"
    print_info "Recent Events"
    print_info "================================"
    kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp' | tail -20
}

setup_port_forwards() {
    print_info "================================"
    print_info "Setting up port forwards..."
    print_info "================================"
    
    print_info "Flink Web UI: http://localhost:8081"
    kubectl port-forward -n $NAMESPACE svc/flink-jobmanager 8081:8081 &
    
    print_info "Grafana: http://localhost:3000 (admin/admin123)"
    kubectl port-forward -n $NAMESPACE svc/grafana 3000:3000 &
    
    print_info "Prometheus: http://localhost:9090"
    kubectl port-forward -n $NAMESPACE svc/prometheus 9090:9090 &
    
    print_info "Gateway API: http://localhost:8080"
    kubectl port-forward -n $NAMESPACE svc/gateway-service 8080:8080 &
    
    print_success ""
    print_success "All port forwards established!"
    print_info "Press Ctrl+C to stop all port forwards"
    
    wait
}

show_all() {
    show_pods
    show_services
    show_metrics
    show_events
}

# Parse arguments
if [ $# -eq 0 ]; then
    usage
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --pods)
            show_pods
            shift
            ;;
        --logs)
            show_logs $2
            shift 2
            ;;
        --describe)
            describe_pod $2
            shift 2
            ;;
        --port-forward)
            setup_port_forwards
            shift
            ;;
        --metrics)
            show_metrics
            shift
            ;;
        --events)
            show_events
            shift
            ;;
        --all)
            show_all
            shift
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
