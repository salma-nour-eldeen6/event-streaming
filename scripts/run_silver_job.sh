#!/bin/bash
set -euo pipefail

echo "Running Silver Flink SQL job..."
docker exec sql-client ./bin/sql-client.sh -f /opt/flink/silver-ping-job.sql
echo "Silver job submitted."