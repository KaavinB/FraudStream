#!/bin/sh
set -eu
# Existing topic partitions are not changed by the idempotent topic initializer.
# Scaling requires an operator-selected compatible topic configuration first.
for workers in 1 2 4; do
  docker compose up -d --scale processor="$workers" processor
  CONSUMER_WORKERS="$workers" .venv/bin/python -m benchmarks.pipeline --concurrency 25 --count 1000
done
