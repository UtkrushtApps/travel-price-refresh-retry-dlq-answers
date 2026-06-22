#!/usr/bin/env bash
set -euo pipefail

cd /root/task

echo "[1/5] Installing Python dependencies..."
pip install -q -r requirements.txt

echo "[2/5] Starting RabbitMQ via docker compose..."
docker compose up -d

echo "[3/5] Waiting for RabbitMQ to become healthy..."
for i in $(seq 1 30); do
  status=$(docker inspect --format '{{.State.Health.Status}}' tripforge_rabbitmq 2>/dev/null || echo "starting")
  if [ "$status" = "healthy" ]; then
    echo "RabbitMQ is healthy."
    break
  fi
  echo "  ...broker status: $status (attempt $i)"
  sleep 3
  if [ "$i" -eq 30 ]; then
    echo "RabbitMQ did not become healthy in time." >&2
    exit 1
  fi
done

echo "[4/5] Compiling project sources..."
python -m compileall -q app tests

echo "[5/5] Running import smoke checks..."
python -c "from app.main import app; print('FastAPI app import OK')"
python -c "import app.rabbitmq, app.worker, app.schemas; print('Module imports OK')"

echo "Readiness checks passed. Starter project is ready."
