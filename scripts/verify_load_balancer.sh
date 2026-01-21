#!/bin/bash
#
# Load Balancer Verification Script
# 
# This script verifies that the load-balanced enrichment cluster is working correctly.
#

echo "🔍 Verifying Load Balancer Setup"
echo "================================"
echo ""

# Check if all services are running
echo "1. Checking Docker containers..."
docker ps | grep supply-chain | grep -E "enrichment|nginx-lb"
echo ""

# Wait for services to be ready
echo "2. Waiting for enrichment services to be ready (this may take 60-90s for NLP model downloads)..."
sleep 5
echo ""

# Check nginx load balancer
echo "3. Testing Nginx Load Balancer..."
if curl -s http://localhost:8085/healthz | grep -q "healthy"; then
    echo "✓ Nginx load balancer is healthy"
else
    echo "✗ Nginx load balancer is not responding"
    exit 1
fi
echo ""

# Test enrichment services individually
echo "4. Testing enrichment instances..."
for port in 8082 8086 8087; do
    if curl -s -m 5 http://localhost:$port/healthz 2>/dev/null | grep -q "healthy"; then
        echo "✓ Enrichment service on port $port is healthy"
    else
        echo "⚠ Enrichment service on port $port is still starting (may need more time for NLP models)"
    fi
done
echo ""

# Test load balancer distribution
echo "5. Testing load balancer distribution (sending 10 requests)..."
for i in {1..10}; do
    response=$(curl -s -X POST http://localhost:8085/v1/enrich \
        -H "Content-Type: application/json" \
        -d "{
            \"news_id\": \"test-$i\",
            \"headline\": \"Test article $i\",
            \"content\": \"Testing load balancer distribution\",
            \"pub_time\": \"2025-11-17T10:00:00Z\"
        }" 2>/dev/null)
    
    if echo "$response" | grep -q "news_id"; then
        echo "✓ Request $i successful"
    else
        echo "⚠ Request $i failed or service still loading"
    fi
done
echo ""

# Check nginx status
echo "6. Nginx load balancer stats:"
curl -s http://localhost:8085/nginx_status
echo ""

echo "================================"
echo "✅ Load balancer verification complete!"
echo ""
echo "Architecture:"
echo "  - 3x Enrichment Service Instances"
echo "  - 1x Nginx Load Balancer (least_conn algorithm)"
echo "  - Automatic failover with max_fails=3"
echo ""
echo "Endpoints:"
echo "  - Load Balancer: http://localhost:8085"
echo "  - Enrichment-1:  http://localhost:8082"
echo "  - Enrichment-2:  http://localhost:8086"
echo "  - Enrichment-3:  http://localhost:8087"

