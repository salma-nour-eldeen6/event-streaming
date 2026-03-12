#!/bin/bash

# Strict error handling
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON_SCRIPT="/opt/flink/jobs/consumer.py"  # Path inside Flink container
FLINK_CONTAINER="flink-jobmanager"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_step() {
    echo -e "${YELLOW}[STEP]${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_jars() {
    REQUIRED_JARS=(
        "flink-sql-connector-kafka-3.1.0-1.18.jar"
        "iceberg-flink-runtime-1.18-1.9.2.jar"
        "aws-java-sdk-bundle-1.12.262.jar"
        "hadoop-aws-3.3.4.jar"
        "hadoop-common-3.3.4.jar"
        "hadoop-client-3.3.4.jar"
        "hadoop-hdfs-3.3.4.jar"
    )

    MISSING=()

    for jar in "${REQUIRED_JARS[@]}"; do
        docker exec -i $FLINK_CONTAINER test -f /opt/flink/lib/$jar || MISSING+=("$jar")
    done

    if [ ${#MISSING[@]} -ne 0 ]; then
        echo -e "${RED}[ERROR] Missing JAR files in $FLINK_CONTAINER:${NC} ${MISSING[*]}"
        exit 1
    else
        echo -e "${GREEN}✓ All required JARs are present in $FLINK_CONTAINER${NC}"
    fi
}

run_consumer() {
    print_step "Running Kafka → Iceberg consumer inside $FLINK_CONTAINER"

    docker exec -i $FLINK_CONTAINER bash -c "
        set -euo pipefail
        export KAFKA_INTERNAL_PORT=${KAFKA_INTERNAL_PORT:-29092}
        export KAFKA_EXTERNAL_PORT=${KAFKA_EXTERNAL_PORT:-9092}
        export MINIO_ROOT_USER=${MINIO_ROOT_USER:-minio}
        export MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minio123}
        export MINIO_API_PORT=${MINIO_API_PORT:-9000}
        export FLINK_UI_PORT=${FLINK_UI_PORT:-8081}

        echo -e '${YELLOW}[INFO]${NC} Starting Python consumer...'
        python3 $PYTHON_SCRIPT || { echo -e '${RED}[ERROR] Python consumer failed' ; exit 1; }
    "

    if [ $? -eq 0 ]; then
        print_success "Consumer finished successfully"
    else
        print_error "Consumer failed to run"
        exit 1
    fi
}

# --- Main ---
print_step "Checking required JAR files"
check_jars

run_consumer