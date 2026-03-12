#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
KAFKA_SCRIPT="$PROJECT_DIR/kafka/create_kafka_topic.sh"
PRODUCER_SCRIPT="$SCRIPT_DIR/run_kafka_producer.sh"
CONSUMER_SCRIPT="$SCRIPT_DIR/run_kafka_consumer.sh"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_step() { echo -e "\n${YELLOW}[STEP $1]${NC} $2"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Cleanup on exit
cleanup() {
    if [ -n "${CONSUMER_PID:-}" ]; then
        kill "$CONSUMER_PID" 2>/dev/null || true
        wait "$CONSUMER_PID" 2>/dev/null || true
        echo -e "${GREEN}✓${NC} Consumer stopped"
    fi
}
trap cleanup EXIT

# Load environment
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a; source "$PROJECT_DIR/.env"; set +a
    echo -e "${GREEN}✓${NC} Loaded environment from .env"
else
    print_error ".env file not found"
    exit 1
fi

echo "══════════════════════════════════════════════════════════╗"
echo "           Atlas Streaming Pipeline                       "
echo "══════════════════════════════════════════════════════════╝"

# Step 1: Kafka topic
print_step "1" "Creating Kafka topic..."
bash "$KAFKA_SCRIPT" || { print_error "Kafka topic creation failed"; exit 1; }
echo -e "${GREEN}✓${NC} Kafka topic created"

# Step 2: Start consumer in background (Docker exec)
print_step "2" "Starting Flink consumer..."
 
bash "$CONSUMER_SCRIPT" > >(while read line; do
    if [[ $line == *"Message sent"* ]]; then
         
        partition=$(echo "$line" | grep -oP 'partition \K[0-9]+')
        offset=$(echo "$line" | grep -oP 'offset \K[0-9]+')
        echo -e "${YELLOW}[CONSUMER]${NC} Partition $partition processed offset $offset"
    fi
done) &
CONSUMER_PID=$!
echo -e "${GREEN}✓${NC} Consumer started with PID: $CONSUMER_PID"
sleep 5

# Step 3: Run producer
print_step "3" "Starting Kafka producer..."
bash "$PRODUCER_SCRIPT" || { print_error "Producer failed"; exit 1; }
echo -e "${GREEN}✓${NC} Producer finished sending messages"

# Step 4: Wait for consumer
print_step "4" "Waiting for consumer to process..."
sleep 10

# Step 5: Stop consumer handled by trap

# Summary
echo ""
echo "══════════════════════════════════════════════════════════╗"
echo -e "${GREEN}           Pipeline Completed Successfully                ${NC}"
echo "══════════════════════════════════════════════════════════╝"
echo ""
echo "Summary:"
echo "   ├─ Kafka Topic: atlas_measurements"
echo "   ├─ Iceberg Table: atlas_db.measurements"
echo "   ├─ MinIO Bucket: atlasevents"
echo "   └─ Data Location: s3a://atlasevents/warehouse/"
echo ""
echo "Links:"
echo "   ├─ Flink UI: http://localhost:${FLINK_UI_PORT:-8081}"
echo "   ├─ Kafka UI: http://localhost:${KAFKA_UI_EXTERNAL_PORT:-8088}"
echo "   ├─ MinIO Console: http://localhost:${MINIO_CONSOLE_PORT:-9001}"
echo "   └─ Airflow: http://localhost:${AIRFLOW_WEBSERVER_EXTERNAL_PORT:-8087}"
 