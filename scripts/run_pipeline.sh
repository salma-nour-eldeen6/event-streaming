#!/bin/bash

# Enable strict error handling
set -e  # Exit on any error
set -u  # Exit on undefined variable

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
KAFKA_SCRIPT="$PROJECT_DIR/kafka/create_kafka_topic.sh"
PRODUCER_SCRIPT="$SCRIPT_DIR/run_kafka_producer.sh"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Error handling function
handle_error() {
    echo -e "${RED}[ERROR]${NC} Pipeline failed at line $1"
    exit 1
}

# Set error trap
trap 'handle_error $LINENO' ERR

# Print functions
print_step() {
    echo -e "${GREEN}[STEP $1]${NC} $2"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if file exists and is executable
check_script() {
    if [ ! -f "$1" ]; then
        print_error "Script not found: $1"
        exit 1
    fi
    if [ ! -x "$1" ]; then
        print_error "Script not executable: $1"
        echo "Run: chmod +x $1"
        exit 1
    fi
}

# Main pipeline
main() {
    echo "==================================="
    echo "   Atlas Streaming Pipeline"
    echo "==================================="
    
    # Step 1: Check required scripts
    print_step "1" "Checking required scripts..."
    check_script "$KAFKA_SCRIPT"
    check_script "$PRODUCER_SCRIPT"
    echo -e "${GREEN}✓${NC} All scripts found"
    
    # Step 2: Create Kafka topic
    echo ""
    print_step "2" "Creating Kafka topic..."
    if bash "$KAFKA_SCRIPT"; then
        echo -e "${GREEN}✓${NC} Topic created successfully"
    else
        print_error "Failed to create topic"
        exit 1
    fi
    
    # Step 3: Run producer
    echo ""
    print_step "3" "Starting Kafka producer..."
    if bash "$PRODUCER_SCRIPT"; then
        echo -e "${GREEN}✓${NC} Producer finished successfully"
    else
        print_error "Producer failed"
        exit 1
    fi
    
    # Success
    echo ""
    echo "==================================="
    echo -e "${GREEN}✓ Pipeline completed successfully${NC}"
    echo "==================================="
}

# Run main function
main



#./scripts/run_pipeline.sh