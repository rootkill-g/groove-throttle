#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

echo "Starting integration environment with docker-compose..."
docker-compose up -d --build

# wait for services
wait_for_port() {
  local host=$1
  local port=$2
  local retries=30
  local i=0
  until nc -z "$host" "$port"; do
    i=$((i+1))
    if [ "$i" -gt "$retries" ]; then
      echo "Timed out waiting for $host:$port" >&2
      docker-compose logs
      exit 1
    fi
    echo "Waiting for $host:$port...";
    sleep 1
  done
}

echo "Waiting for Redis..."
wait_for_port 127.0.0.1 6379

echo "Waiting for Mongo..."
wait_for_port 127.0.0.1 27017

echo "Waiting for Crawler..."
wait_for_port 127.0.0.1 8081

# Run the Rust app in background
echo "Starting Rust app (cargo run) ..."
cargo run --quiet &
APP_PID=$!
trap 'echo "Stopping app ($APP_PID)"; kill $APP_PID || true; echo "Tearing down docker-compose"; docker-compose down' EXIT

# give app a moment to start
sleep 2

TEST_URLS='["https://example.com/a","https://example.com/b","https://example.com/missing"]'

echo "Posting test request to API..."
RESPONSE=$(curl -sS -X POST http://127.0.0.1:8000/api -H "Content-Type: application/json" -d "$TEST_URLS")

echo "Response from API:"
echo "$RESPONSE"

echo "Integration test finished - cleaning up (docker-compose down and kill app)."

# cleanup handled by trap
exit 0

