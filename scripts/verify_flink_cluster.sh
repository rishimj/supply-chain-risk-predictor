#!/bin/bash
# Verification script for Flink cluster setup

set -e

echo "==================== FLINK CLUSTER VERIFICATION ===================="
echo

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_step() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $1"
    else
        echo -e "${RED}✗${NC} $1"
        return 1
    fi
}

echo "1. Checking Docker services..."
docker-compose ps | grep -q "flink-jobmanager.*Up" && check_step "JobManager is running" || echo -e "${YELLOW}⚠${NC} JobManager not running"
docker-compose ps | grep -q "flink-taskmanager-1.*Up" && check_step "TaskManager-1 is running" || echo -e "${YELLOW}⚠${NC} TaskManager-1 not running"
docker-compose ps | grep -q "flink-taskmanager-2.*Up" && check_step "TaskManager-2 is running" || echo -e "${YELLOW}⚠${NC} TaskManager-2 not running"
docker-compose ps | grep -q "flink-processor.*Up" && check_step "Flink processor is running" || echo -e "${YELLOW}⚠${NC} Flink processor not running"
echo

echo "2. Checking Flink Web UI..."
curl -sf http://localhost:8081/overview > /dev/null && check_step "Web UI is accessible" || echo -e "${YELLOW}⚠${NC} Web UI not accessible"
echo

echo "3. Checking TaskManager registration..."
TASK_MANAGERS=$(curl -s http://localhost:8081/taskmanagers 2>/dev/null | jq -r '.taskmanagers | length' 2>/dev/null)
if [ "$TASK_MANAGERS" = "2" ]; then
    check_step "2 TaskManagers registered"
else
    echo -e "${YELLOW}⚠${NC} Found $TASK_MANAGERS TaskManagers (expected 2)"
fi
echo

echo "4. Checking task slots..."
TOTAL_SLOTS=$(curl -s http://localhost:8081/taskmanagers 2>/dev/null | jq -r '[.taskmanagers[].slotsNumber] | add' 2>/dev/null)
if [ "$TOTAL_SLOTS" = "8" ]; then
    check_step "8 task slots available"
else
    echo -e "${YELLOW}⚠${NC} Found $TOTAL_SLOTS task slots (expected 8)"
fi
echo

echo "5. Checking running jobs..."
JOB_COUNT=$(curl -s http://localhost:8081/jobs 2>/dev/null | jq -r '.jobs | length' 2>/dev/null)
if [ "$JOB_COUNT" -gt 0 ]; then
    check_step "$JOB_COUNT job(s) running"
    JOB_ID=$(curl -s http://localhost:8081/jobs 2>/dev/null | jq -r '.jobs[0].id' 2>/dev/null)
    JOB_STATUS=$(curl -s http://localhost:8081/jobs 2>/dev/null | jq -r '.jobs[0].status' 2>/dev/null)
    echo "   Job ID: $JOB_ID"
    echo "   Status: $JOB_STATUS"
else
    echo -e "${YELLOW}⚠${NC} No jobs running"
fi
echo

echo "6. Checking checkpoint configuration..."
if [ -n "$JOB_ID" ] && [ "$JOB_ID" != "null" ]; then
    CHECKPOINT_INFO=$(curl -s http://localhost:8081/jobs/$JOB_ID/checkpoints 2>/dev/null)
    CHECKPOINT_COUNT=$(echo "$CHECKPOINT_INFO" | jq -r '.counts.total // 0' 2>/dev/null)
    if [ "$CHECKPOINT_COUNT" -gt 0 ]; then
        check_step "Checkpointing is active ($CHECKPOINT_COUNT checkpoints)"
        LATEST_CHECKPOINT=$(echo "$CHECKPOINT_INFO" | jq -r '.latest.completed.external_path // "N/A"' 2>/dev/null)
        echo "   Latest checkpoint: $LATEST_CHECKPOINT"
    else
        echo -e "${YELLOW}⚠${NC} No checkpoints yet (may be starting up)"
    fi
else
    echo -e "${YELLOW}⚠${NC} Cannot check checkpoints (no running job)"
fi
echo

echo "7. Checking volumes..."
docker volume ls | grep -q "flink-checkpoints" && check_step "Checkpoint volume exists" || echo -e "${RED}✗${NC} Checkpoint volume missing"
docker volume ls | grep -q "flink-savepoints" && check_step "Savepoint volume exists" || echo -e "${RED}✗${NC} Savepoint volume missing"
echo

echo "8. Checking checkpoint directory..."
docker exec supply-chain-flink-taskmanager-1 ls -la /tmp/flink-checkpoints > /dev/null 2>&1 && \
    check_step "Checkpoint directory is accessible" || \
    echo -e "${YELLOW}⚠${NC} Checkpoint directory not accessible"
echo

echo "9. Checking dependent services..."
docker-compose ps | grep -q "kafka.*Up" && check_step "Kafka is running" || echo -e "${RED}✗${NC} Kafka not running"
docker-compose ps | grep -q "redis.*Up" && check_step "Redis is running" || echo -e "${RED}✗${NC} Redis not running"
docker-compose ps | grep -q "postgres.*Up" && check_step "PostgreSQL is running" || echo -e "${RED}✗${NC} PostgreSQL not running"
echo

echo "==================== SUMMARY ===================="
echo
echo "Flink Web UI: http://localhost:8081"
echo "Redis UI: http://localhost:8084 (admin/admin)"
echo "Kafka UI: http://localhost:8090"
echo "Gateway API: http://localhost:8080"
echo
echo "To view logs:"
echo "  make logs-flink       # All Flink services"
echo "  make logs-flink-job   # Job processor only"
echo
echo "To monitor cluster:"
echo "  ./monitor_flink.sh"
echo "  make flink-status"
echo
echo "==============================================="
