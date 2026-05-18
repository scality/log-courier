#!/usr/bin/env bash
# Wait for the backbeat lifecycle pipeline to be ready, then verify
# the bucket-processor and object-processor kafka consumers are
# stable (not stuck in a rebalance loop).
set -e

# shellcheck source=lib/check-consumer-stable.sh
. "$(dirname "$0")/lib/check-consumer-stable.sh"

# Backbeat lifecycle: wait only on the bucket-processor and
# object-processor kafka consumers being ready (using their logs).
# We deliberately do not wait on the conductor's first successful batch:
# listObject(usersBucket) returns NoSuchBucket until the first
# user creates a bucket, and usersBucket is created.
echo "Waiting for backbeat lifecycle pipeline to be ready..."
lc_ready=false
for i in {1..120}; do
  if docker exec workbench-backbeat sh -c 'grep -q "lifecycle bucket processor running!" /logs/lifecycle-bucket-processor_*.log 2>/dev/null' \
    && docker exec workbench-backbeat sh -c 'grep -q "lifecycle object processor successfully started" /logs/lifecycle-object-processor_*.log 2>/dev/null'; then
    lc_ready=true
    break
  fi
  sleep 1
done
if [ "$lc_ready" = false ]; then
  echo "ERROR: lifecycle pipeline did not become ready in time"
  exit 1
fi
echo "✓ Backbeat lifecycle pipeline ready"

check_consumer_stable lifecycle-bucket-processor bucket-processor "lifecycle bucket processor running!"
check_consumer_stable lifecycle-object-processor object-processor "lifecycle object processor running!"
