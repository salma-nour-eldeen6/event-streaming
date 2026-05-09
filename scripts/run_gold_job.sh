#!/bin/bash
set -euo pipefail

echo "Running Gold Flink SQL job..."
docker exec sql-client ./bin/sql-client.sh -f /opt/flink/gold-job.sql
echo "Gold job finished."