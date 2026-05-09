#!/bin/bash
set -euo pipefail

echo "Submitting Bronze Flink SQL job..."

docker exec -d sql-client \
./bin/sql-client.sh -f /opt/flink/bronze-job.sql

echo "Bronze job submitted successfully."