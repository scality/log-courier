#!/usr/bin/env bash
# Wait for three backbeat CRR readiness signals; until all are
# up, replication writes stay at PENDING. The probes on :4042
# (queue-populator) and :4043 (queue-processor) return 200
# before their internal state is populated, so check the logs
# instead; the :4045 (status-processor) probe is reliable.
# qp_count=3 matches workbench's S3Metadata.RaftSessions default.
set -e

echo "Waiting for backbeat CRR pipeline to be ready..."
crr_ready=false
for i in {1..120}; do
  qp_count=$(docker exec workbench-backbeat sh -c 'cat /logs/crr-queue-populator_*.log 2>/dev/null | grep -c "log reader is ready to populate" || true')
  sproc=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:4045/_/ready || echo 000)
  if [ "${qp_count:-0}" -ge 3 ] \
    && docker exec workbench-backbeat sh -c 'grep -q "queue processor is ready to consume replication entries" /logs/crr-queue-processor_*.log 2>/dev/null' \
    && [ "$sproc" = "200" ]; then
    crr_ready=true
    break
  fi
  sleep 1
done
if [ "$crr_ready" = false ]; then
  echo "ERROR: CRR pipeline did not become ready in time"
  echo "--- queue-populator log ---"
  docker exec workbench-backbeat sh -c 'cat /logs/crr-queue-populator_*.log' || true
  echo "--- queue-processor log ---"
  docker exec workbench-backbeat sh -c 'cat /logs/crr-queue-processor_*.log' || true
  echo "--- status-processor probe (:4045) ---"
  curl -s "http://localhost:4045/_/ready" || true
  echo
  exit 1
fi
echo "✓ Backbeat CRR pipeline ready"
