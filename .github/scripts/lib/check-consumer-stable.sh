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

# Prints the pid supervisord reports for PROC, or nothing if it is not
# Running.
supervised_pid() {
  docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status "$1" \
    | sed 's/\x1b\[[0-9;]*m//g' \
    | awk '$2 == "Running" {sub(",", "", $4); print $4}'
}

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
    # The pid supervisord tracks is npm, which runs the consumer as
    # npm -> sh -> node. `ctl stop`, `ctl restart` and `ctl signal KILL`
    # only signal npm: node is left running with npm's stdout/stderr
    # pipes open, so supervisord keeps reporting the old pid as Running
    # and never respawns it. ochinchina puts each program in its own
    # process group (pgid = npm's pid), so kill the whole group and let
    # autorestart spawn a fresh instance.
    local old_pid
    old_pid=$(supervised_pid "$proc")
    if [ -z "$old_pid" ]; then
      echo "ERROR: ${proc} is not Running, cannot restart it"
      docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status || true
      exit 1
    fi
    docker exec workbench-backbeat bash -c "kill -9 -- -${old_pid}"
    local new_pid=""
    for i in {1..15}; do
      sleep 1
      new_pid=$(supervised_pid "$proc")
      if [ -n "$new_pid" ] && [ "$new_pid" != "$old_pid" ]; then
        break
      fi
    done
    if [ -z "$new_pid" ] || [ "$new_pid" = "$old_pid" ]; then
      echo "ERROR: ${proc} was not respawned within 15s after killing process group ${old_pid}"
      docker exec workbench-backbeat supervisord ctl -c /conf/supervisord.conf status || true
      exit 1
    fi
    echo "${proc} restarted (pid ${old_pid} -> ${new_pid})"
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
