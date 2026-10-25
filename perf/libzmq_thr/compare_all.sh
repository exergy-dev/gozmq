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
# gozmq via perf/zmqperf, libzmq via the repo-written minimal C local_thr.c +
# remote_thr.c in this directory, compiled against the system libzmq (they are
# NOT libzmq's canonical perf sources).
#
# Two tables are printed per run: first the homogeneous gozmq-vs-libzmq table
# (format unchanged so REPORT.md rows stay comparable), then a second table
# with the two mixed pairings, gozmq PUSH -> libzmq PULL and libzmq PUSH ->
# gozmq PULL, which separate sender cost from receiver cost.
#
# Overrides: TRANSPORTS (default "tcp ipc"); COUNT_SCALE multiplies every
# message count (default 1; e.g. COUNT_SCALE=0.01 for a quick smoke run).
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
# GOZMQ_LOCAL_FLAGS passes extra flags to gozmq's receiver, e.g.
# GOZMQ_LOCAL_FLAGS=-pool for the receive buffer pool (WithRecvBufferPool).
# GOZMQ_GOGC passes -gogc to both gozmq processes (unset: Go's defaults).
# libzmq has no collector; at 64 KB x 70k msg/s Go's default pacer runs a
# cycle every ~60 messages, and GOZMQ_GOGC=400 is the usual tuning for a
# receiver at that rate. REPORT.md shows defaults first, options after.
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

one_run() { # recv_kind send_kind transport size count -> echoes msg/s (0 on failure)
  local rk=$1 sk=$2 tr=$3 size=$4 count=$5 ep srv
  srv="$tmp/srv"
  if [ "$tr" = tcp ]; then
    ep="tcp://127.0.0.1:$PORT"; PORT=$((PORT+1))
  else
    ipc_seq=$((ipc_seq+1)); ep="ipc:///tmp/gz-zmq-bench-$$-${ipc_seq}.sock"; rm -f "${ep#ipc://}"
  fi
  : > "$srv"
  if [ "$rk" = c ]; then
    timeout "$PER_RUN_TIMEOUT" "$tmp/c_local_thr"  "$ep" "$size" "$count" > "$srv" 2>/dev/null & local p=$!
  else
    # shellcheck disable=SC2086 # GOZMQ_LOCAL_FLAGS is a flag list
    timeout "$PER_RUN_TIMEOUT" "$tmp/zmqperf" local_thr  -endpoint "$ep" -size "$size" -count "$count" ${GOZMQ_GOGC+-gogc $GOZMQ_GOGC} ${GOZMQ_LOCAL_FLAGS-} > "$srv" 2>/dev/null & local p=$!
  fi
  sleep 0.5
  if [ "$sk" = c ]; then
    timeout "$PER_RUN_TIMEOUT" "$tmp/c_remote_thr" "$ep" "$size" "$count" >/dev/null 2>&1 || true
  else
    timeout "$PER_RUN_TIMEOUT" "$tmp/zmqperf" remote_thr -endpoint "$ep" -size "$size" -count "$count" ${GOZMQ_GOGC+-gogc $GOZMQ_GOGC} >/dev/null 2>&1 || true
  fi
  wait $p 2>/dev/null || true
  [ "$tr" = ipc ] && rm -f "${ep#ipc://}"
  sed -n 's/^mean throughput: \([0-9]*\) \[msg\/s\].*/\1/p' "$srv" 2>/dev/null | head -1 || echo 0
}

best() { # recv_kind send_kind transport size count -> best valid msg/s over MAX_TRIES (0 if all fail)
  local b=0 t out
  for t in $(seq 1 "$MAX_TRIES"); do
    out=$(one_run "$1" "$2" "$3" "$4" "$5"); out=${out:-0}
    b=$(awk -v a="$b" -v c="$out" 'BEGIN{print (c>a)?c:a}')
    sleep 0.3
    # Two good reads is enough; stop early once we have a stable non-zero.
    [ "$t" -ge 2 ] && [ "$b" != 0 ] && break
  done
  echo "$b"
}

scaled() { awk -v c="$1" -v k="${COUNT_SCALE:-1}" 'BEGIN{n=int(c*k); print (n<2)?2:n}'; }

for tr in ${TRANSPORTS:-tcp ipc}; do
  echo "## transport: $tr"
  printf "%-6s | %10s %9s | %10s %9s | %s\n" "size" "gozmq" "Gb/s" "libzmq" "Gb/s" "gozmq/zmq"
  for spec in "32:1500000" "256:1500000" "1024:800000" "8192:300000" "65536:80000"; do
    size=${spec%:*}; count=$(scaled "${spec#*:}")
    gz=$(best go go "$tr" "$size" "$count")
    zm=$(best c  c  "$tr" "$size" "$count")
    awk -v gz="$gz" -v zm="$zm" -v s="$size" 'BEGIN{
      l=(s>=1024)?sprintf("%dKB",s/1024):sprintf("%dB",s);
      gzs=(gz>0)?sprintf("%7.2fM",gz/1e6):"   FAIL";
      zms=(zm>0)?sprintf("%7.2fM",zm/1e6):"   FAIL";
      r =(gz>0&&zm>0)?sprintf("%.2fx",gz/zm):"  -";
      printf "%-6s | %s %9.2f | %s %9.2f | %s\n", l, gzs, gz*s*8/1e9, zms, zm*s*8/1e9, r }'
  done
  echo
done

# Second table: the mixed pairings. Columns are sender>receiver.
for tr in ${TRANSPORTS:-tcp ipc}; do
  echo "## mixed pairings, transport: $tr"
  printf "%-6s | %10s %9s | %10s %9s\n" "size" "go>zmq" "Gb/s" "zmq>go" "Gb/s"
  for spec in "32:1500000" "256:1500000" "1024:800000" "8192:300000" "65536:80000"; do
    size=${spec%:*}; count=$(scaled "${spec#*:}")
    gc=$(best c  go "$tr" "$size" "$count")   # gozmq PUSH -> libzmq PULL
    cg=$(best go c  "$tr" "$size" "$count")   # libzmq PUSH -> gozmq PULL
    awk -v gc="$gc" -v cg="$cg" -v s="$size" 'BEGIN{
      l=(s>=1024)?sprintf("%dKB",s/1024):sprintf("%dB",s);
      a=(gc>0)?sprintf("%7.2fM",gc/1e6):"   FAIL";
      b=(cg>0)?sprintf("%7.2fM",cg/1e6):"   FAIL";
      printf "%-6s | %s %9.2f | %s %9.2f\n", l, a, gc*s*8/1e9, b, cg*s*8/1e9 }'
  done
  echo
done
