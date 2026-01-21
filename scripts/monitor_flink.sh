#!/bin/bash
# Script to monitor Flink cluster status

echo "==================== FLINK CLUSTER STATUS ===================="
echo

# Check if JobManager is running
echo "1. JobManager Status:"
curl -s http://localhost:8081/overview | jq '.' 2>/dev/null || echo "JobManager not accessible"
echo

# Check TaskManagers
echo "2. TaskManagers:"
curl -s http://localhost:8081/taskmanagers | jq '.taskmanagers[] | {id: .id, slots: .slotsNumber, freeSlots: .freeSlots}' 2>/dev/null || echo "Cannot fetch TaskManagers"
echo

# Check running jobs
echo "3. Running Jobs:"
curl -s http://localhost:8081/jobs | jq '.jobs[] | {id: .id, status: .status}' 2>/dev/null || echo "Cannot fetch jobs"
echo

# Check job details if any job is running
JOB_ID=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].id' 2>/dev/null)
if [ "$JOB_ID" != "null" ] && [ -n "$JOB_ID" ]; then
    echo "4. Job Details for $JOB_ID:"
    curl -s http://localhost:8081/jobs/$JOB_ID | jq '{
        name: .name,
        state: .state,
        "start-time": .["start-time"],
        duration: .duration,
        vertices: .vertices | length
    }' 2>/dev/null
    echo
    
    echo "5. Checkpoints:"
    curl -s http://localhost:8081/jobs/$JOB_ID/checkpoints | jq '{
        latest: .latest,
        count: .counts
    }' 2>/dev/null
    echo
fi

echo "==================== DOCKER SERVICES ===================="
docker-compose ps flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 flink-processor 2>/dev/null

echo
echo "==================== FLINK WEB UI ===================="
echo "Access at: http://localhost:8081"
echo "============================================================"
