#!/bin/bash
set -euo pipefail

echo "Running Bronze Flink SQL job..."
docker exec sql-client ./bin/sql-client.sh -f /opt/flink/bronze-job.sql
echo "Bronze job submitted."