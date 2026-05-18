# Sourced helper for backbeat readiness scripts.
#
# check_consumer_stable PROC_PREFIX LABEL RUNNING_MARKER
#
# Workaround for a BackbeatConsumer stuck in a rebalance loop: a kafka
# consumer can enter a cycle where it logs rdkafka.assign / rdkafka.revoke
# every 1-2s and never consumes any entries. A stable consumer logs
# exactly one rdkafka.assign per process start; a stuck one logs 5+
# within 10s. Detect by counting rdkafka.assign lines after the latest
# RUNNING_MARKER, and if stuck, restart via supervisord and re-check
# once. Fail if still stuck after one restart.
check_consumer_stable() {
  local proc_prefix="$1"
  local label="$2"
  local running_marker="$3"
  echo "Verifying ${label} consumer is stable..."
  for attempt in 1 2; do
    sleep 10
    local assigns
    assigns=$(docker exec workbench-backbeat sh -c "
      awk '/${running_marker}/ {seen=1; a=0; next}
           seen && /rdkafka\\.assign/ {a++}
           END {if (seen) print a+0}' /logs/${proc_prefix}_*.log 2>/dev/null
    ")
    if [ -z "$assigns" ]; then
      echo "ERROR: stability check could not find 'running!' marker in ${label} logs"
      exit 1
    fi
    if [ "$assigns" -le 2 ]; then
      echo "✓ ${label} consumer stable ($assigns assigns since last start)"
      return 0
    fi
    if [ "$attempt" = "2" ]; then
      echo "ERROR: ${label} consumer still stuck after restart ($assigns assigns)"
      exit 1
    fi
    echo "WARN: ${label} consumer stuck ($assigns assigns); restarting"
    # grep -c on a glob prints "file:count" per file; pipe via cat so
    # we always get a single integer even after log rotation. `|| true`
    # keeps `set -e` happy when grep -c finds 0 matches (exit 1).
    local prev_running
    prev_running=$(docker exec workbench-backbeat sh -c "cat /logs/${proc_prefix}_*.log 2>/dev/null | grep -c '${running_marker}' || true")
    # The backbeat federation image uses ochinchina/supervisord (a
    # Go port) which exposes its CLI as `supervisord ctl` -- there
    # is no separate `supervisorctl` binary on PATH. Pass -c so
    # ctl uses the same socket as the running daemon. ochinchina's
    # restart expects the process name (e.g. lifecycle-bucket-processor_0
    # for numprocs=1), not the program name, so look it up via
    # status rather than hardcoding the suffix. ochinchina prints
    # status with ANSI color codes (e.g. \x1b[0;32m...\x1b[0m), so
    # strip them before matching with awk.
    local proc
    proc=$(docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status \
      | sed 's/\x1b\[[0-9;]*m//g' \
      | awk "/^${proc_prefix}_[0-9]+[[:space:]]/ {print \$1; exit}")
    if [ -z "$proc" ]; then
      echo "ERROR: ${proc_prefix} not found in supervisord status"
      docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status || true
      exit 1
    fi
    # ochinchina/supervisord's `ctl stop` and `ctl restart` return success
    # without actually terminating the process (same PID and rdkafka client
    # UUID persist), likely because they don't wait for SIGTERM to take
    # effect and the Node wrapper doesn't propagate it. Send SIGKILL via
    # `ctl signal KILL` instead, wait for supervisord to notice the dead
    # process, then `start` to spawn a fresh one. Log status at each step.
    echo "DEBUG: ${proc} status before signal:"
    docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status "$proc" || true
    docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf signal KILL "$proc" || true
    # Poll until supervisord no longer reports the process as Running.
    for i in {1..15}; do
      sleep 1
      if ! docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status "$proc" \
        | sed 's/\x1b\[[0-9;]*m//g' | grep -q '\bRunning\b'; then
        break
      fi
    done
    echo "DEBUG: ${proc} status after KILL:"
    docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status "$proc" || true
    docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf start "$proc" || true
    echo "DEBUG: ${proc} status after start:"
    docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status "$proc" || true
    # Wait for the new instance's "running!" marker to appear.
    local new_running=${prev_running:-0}
    for i in {1..60}; do
      new_running=$(docker exec workbench-backbeat sh -c "cat /logs/${proc_prefix}_*.log 2>/dev/null | grep -c '${running_marker}' || true")
      if [ "${new_running:-0}" -gt "${prev_running:-0}" ]; then
        break
      fi
      sleep 1
    done
    if [ "${new_running:-0}" -le "${prev_running:-0}" ]; then
      echo "ERROR: ${label} did not re-emit 'running!' within 60s after restart"
      docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status || true
      exit 1
    fi
  done
}
