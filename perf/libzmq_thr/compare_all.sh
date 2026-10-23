#!/usr/bin/env bash
# Fair native-vs-native PUSH->PULL streaming throughput: gozmq vs libzmq,
# over the transports that bridge two processes. TRANSPORTS defaults to
# "tcp ipc"; set TRANSPORTS=tcp for the trustworthy cross-implementation table.
#
# (inproc:// is single-process by definition, so a two-binary gozmq-vs-libzmq
# comparison is impossible; tls+tcp:// is a gozmq-only scheme libzmq does not
# speak. Both are out of scope here — use the in-repo Go benchmarks for inproc.)
#
# Both sides run as two processes over the chosen transport with NO CGO —
# gozmq via perf/zmqperf, libzmq via its own C local_thr + remote_thr
# (compiled from the canonical libzmq perf sources).
#
# Robustness: each measurement is retried until it yields a valid (>0) result
# (up to MAX_TRIES), every process is wrapped in `timeout`, TCP ports are never
# reused (TIME_WAIT avoidance), and a short cooldown separates runs.
#
# CAVEAT — cross-impl IPC ratios are NOT apples-to-apples. The libzmq C client
# (remote_thr.c) uses libzmq's stock SNDHWM (1000) and does a per-message
# zmq_msg_init_size + memset, whereas gozmq's zmqperf uses a 1M HWM and
# reuses a single pre-allocated payload. Over TCP these differences are not the
# bottleneck (libzmq still wins at 32B), but over IPC they appear to inflate
# gozmq's relative throughput. Treat the TCP table as the trustworthy
# cross-implementation comparison; read the IPC libzmq column as "stock libzmq
# perf-tool settings," not a tuned ceiling.
#
# Requires: gcc, libzmq-dev (pkg-config libzmq), a built Go toolchain.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp" /tmp/gz-zmq-bench-$$-*.sock' EXIT

gcc -O2 -o "$tmp/c_local_thr"  "$here/local_thr.c"  $(pkg-config --cflags --libs libzmq)
gcc -O2 -o "$tmp/c_remote_thr" "$here/remote_thr.c" $(pkg-config --cflags --libs libzmq)
( cd "$root" && go build -o "$tmp/zmqperf" ./perf/zmqperf )

MAX_TRIES=5
PER_RUN_TIMEOUT=40
PORT=5900            # monotonically increasing; never reused
ipc_seq=0

one_run() { # kind transport size count -> echoes msg/s (0 on failure)
  local kind=$1 tr=$2 size=$3 count=$4 ep srv
  srv="$tmp/srv"
  if [ "$tr" = tcp ]; then
    ep="tcp://127.0.0.1:$PORT"; PORT=$((PORT+1))
  else
    ipc_seq=$((ipc_seq+1)); ep="ipc:///tmp/gz-zmq-bench-$$-${ipc_seq}.sock"; rm -f "${ep#ipc://}"
  fi
  : > "$srv"
  if [ "$kind" = c ]; then
    timeout "$PER_RUN_TIMEOUT" "$tmp/c_local_thr"  "$ep" "$size" "$count" > "$srv" 2>/dev/null & local p=$!
    sleep 0.5
    timeout "$PER_RUN_TIMEOUT" "$tmp/c_remote_thr" "$ep" "$size" "$count" >/dev/null 2>&1 || true
    wait $p 2>/dev/null || true
  else
    timeout "$PER_RUN_TIMEOUT" "$tmp/zmqperf" local_thr  -endpoint "$ep" -size "$size" -count "$count" > "$srv" 2>/dev/null & local p=$!
    sleep 0.5
    timeout "$PER_RUN_TIMEOUT" "$tmp/zmqperf" remote_thr -endpoint "$ep" -size "$size" -count "$count" >/dev/null 2>&1 || true
    wait $p 2>/dev/null || true
  fi
  [ "$tr" = ipc ] && rm -f "${ep#ipc://}"
  sed -n 's/^mean throughput: \([0-9]*\) \[msg\/s\].*/\1/p' "$srv" 2>/dev/null | head -1 || echo 0
}

best() { # kind transport size count -> best valid msg/s over MAX_TRIES (0 if all fail)
  local b=0 t out
  for t in $(seq 1 "$MAX_TRIES"); do
    out=$(one_run "$1" "$2" "$3" "$4"); out=${out:-0}
    b=$(awk -v a="$b" -v c="$out" 'BEGIN{print (c>a)?c:a}')
    sleep 0.3
    # Two good reads is enough; stop early once we have a stable non-zero.
    [ "$t" -ge 2 ] && [ "$b" != 0 ] && break
  done
  echo "$b"
}

for tr in ${TRANSPORTS:-tcp ipc}; do
  echo "## transport: $tr"
  printf "%-6s | %10s %9s | %10s %9s | %s\n" "size" "gozmq" "Gb/s" "libzmq" "Gb/s" "gozmq/zmq"
  for spec in "32:1500000" "256:1500000" "1024:800000" "8192:300000" "65536:80000"; do
    size=${spec%:*}; count=${spec#*:}
    gz=$(best go "$tr" "$size" "$count")
    zm=$(best c  "$tr" "$size" "$count")
    awk -v gz="$gz" -v zm="$zm" -v s="$size" 'BEGIN{
      l=(s>=1024)?sprintf("%dKB",s/1024):sprintf("%dB",s);
      gzs=(gz>0)?sprintf("%7.2fM",gz/1e6):"   FAIL";
      zms=(zm>0)?sprintf("%7.2fM",zm/1e6):"   FAIL";
      r =(gz>0&&zm>0)?sprintf("%.2fx",gz/zm):"  -";
      printf "%-6s | %s %9.2f | %s %9.2f | %s\n", l, gzs, gz*s*8/1e9, zms, zm*s*8/1e9, r }'
  done
  echo
done
