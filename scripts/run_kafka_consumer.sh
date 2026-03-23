#!/bin/bash
set -euo pipefail

echo "[STEP] Checking sql-client container..."

if docker ps --format '{{.Names}}' | grep -q '^sql-client$'; then
  echo "✓ sql-client is running"
else
  echo "[ERROR] sql-client is not running"
  exit 1
fi

echo "[STEP] Showing recent sql-client logs..."
docker logs --tail 50 sql-client