#!/bin/bash

set -e

SIZE=600000

function wait_weaviate() {
  echo "Wait for Weaviate to be ready"
  for _ in {1..120}; do
    if curl -sf -o /dev/null localhost:8080; then
      echo "Weaviate is ready"
      break
    fi

    echo "Weaviate is not ready, trying again in 1s"
    sleep 1
  done
}

echo "Building all required containers"
( cd apps/bm25-corruption/ && docker build -t importer . )

echo "Starting Weaviate..."
docker-compose -f apps/weaviate/docker-compose.yml up -d

wait_weaviate

echo "Starting importing and killing"
docker run \
  --network host \
  --rm -it \
  -v "$PWD:$PWD" \
  -w "$PWD" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --name importer \
  importer sh -c "python3 /app/run.py --mode=docker"

echo "Passed!"
