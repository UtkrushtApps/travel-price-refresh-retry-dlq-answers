#!/usr/bin/env bash
set -uo pipefail

cd /root/task 2>/dev/null || true

echo "[1/7] Stopping containers via docker compose down..."
docker compose down || true
echo "Containers stopped."

echo "[2/7] Removing containers and volumes..."
docker compose down -v || true
docker volume rm tripforge_rabbitmq_data || true
echo "Volumes removed."

echo "[3/7] Removing task-related networks..."
docker network rm task_default || true

echo "[4/7] Force-removing task images if present..."
docker rmi -f rabbitmq:3.13-management || true

echo "[5/7] Pruning unused Docker resources..."
docker system prune -a --volumes -f || true

echo "[6/7] Removing /root/task directory..."
rm -rf /root/task || true

echo "[7/7] Cleanup steps finished."
echo "Cleanup completed successfully!"
