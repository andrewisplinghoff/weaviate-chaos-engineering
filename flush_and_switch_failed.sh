#!/bin/bash

set -e

function wait_weaviate() {
  echo "Wait for Weaviate to be ready"
  for _ in {1..120}; do
    if curl -sf -o /dev/null localhost:8080/v1/.well-known/ready; then
      echo "Weaviate is ready"
      return 0
    fi

    echo "Weaviate is not ready, trying again in 1s"
    sleep 1
  done
  echo "ERROR: Weaviate is not ready after 120s"
  exit 1
}

echo "Building all required containers"
( cd apps/flush-and-switch-failed/ && docker build -t flush_and_switch_failed . )

echo "Starting Weaviate..."
docker-compose -f apps/weaviate/docker-compose-c11y.yml up -d

wait_weaviate

echo "Run flush_and_switch_failed.py"
docker run --network host -t flush-and-switch-failed python3 flush_and_switch_failed.py

echo "Passed!"
