#!/bin/bash
set -euo pipefail

echo "Submitting Silver Flink SQL job..."

docker exec -d sql-client \
./bin/sql-client.sh -f /opt/flink/silver-ping-job.sql

echo "Silver job submitted successfully."