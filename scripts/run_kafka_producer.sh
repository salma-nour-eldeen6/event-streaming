#!/bin/bash

PROJECT_DIR="$HOME/atlas-streaming"
VENV_DIR="$PROJECT_DIR/.venv"
PRODUCER_SCRIPT="$PROJECT_DIR/kafka/producer.py"
 
# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

if ! python3 -c "import kafka" &> /dev/null; then
    echo "Installing kafka-python..."
    pip install --upgrade pip
    pip install kafka-python
fi
 
echo "Running Kafka producer..."
python3 "$PRODUCER_SCRIPT"

deactivate
echo "Producer finished."
