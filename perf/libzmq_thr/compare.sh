#!/usr/bin/env bash
# Fair native-vs-native PUSH->PULL streaming throughput: gozmq vs libzmq.
#
# Both sides run as TWO PROCESSES over TCP loopback with NO CGO — gozmq via
# its perf/local_thr + perf/remote_thr tools, libzmq via its own C local_thr +
# remote_thr (compiled here from the canonical libzmq perf sources). This is
# the only apples-to-apples comparison: an in-process CGO binding penalizes
# libzmq with a cgocall + a per-message buffer copy on every Send/Recv and
# understates its native throughput several-fold.
#
# Requires: gcc, libzmq-dev (pkg-config libzmq), a built Go toolchain.
# Usage: perf/libzmq_thr/compare.sh
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT

gcc -O2 -o "$tmp/c_local_thr"  "$here/local_thr.c"  $(pkg-config --cflags --libs libzmq)
gcc -O2 -o "$tmp/c_remote_thr" "$here/remote_thr.c" $(pkg-config --cflags --libs libzmq)
( cd "$root" && go build -o "$tmp/gz_local_thr" ./perf/local_thr && go build -o "$tmp/gz_remote_thr" ./perf/remote_thr )

run() { # kind size count port -> msg/s (best of 2)
  local kind=$1 size=$2 count=$3 base=$4 best=0
  for off in 0 1; do
    local ep="tcp://127.0.0.1:$((base+off))" out
    if [ "$kind" = c ]; then
      "$tmp/c_local_thr" "$ep" "$size" "$count" > "$tmp/srv" 2>/dev/null & local p=$!; sleep 0.4
      "$tmp/c_remote_thr" "$ep" "$size" "$count" >/dev/null 2>&1; wait $p
    else
      "$tmp/gz_local_thr" -endpoint "$ep" -size "$size" -count "$count" > "$tmp/srv" 2>/dev/null & local p=$!; sleep 0.4
      "$tmp/gz_remote_thr" -endpoint "$ep" -size "$size" -count "$count" >/dev/null 2>&1; wait $p
    fi
    out=$(grep -oP 'mean throughput: \K[0-9]+' "$tmp/srv" || echo 0)
    best=$(awk -v a="$best" -v b="$out" 'BEGIN{print (b>a)?b:a}')
  done
  echo "$best"
}

printf "%-6s | %10s %9s | %10s %9s | %s\n" "size" "gozmq" "Gb/s" "libzmq" "Gb/s" "gozmq/zmq"
port=5800
for spec in "32:1500000" "256:1500000" "1024:800000" "8192:300000" "65536:80000"; do
  size=${spec%:*}; count=${spec#*:}
  gz=$(run go $size $count $port); port=$((port+2))
  zm=$(run c  $size $count $port); port=$((port+2))
  awk -v gz="$gz" -v zm="$zm" -v s="$size" 'BEGIN{
    l=(s>=1024)?sprintf("%dKB",s/1024):sprintf("%dB",s);
    printf "%-6s | %7.2fM %9.2f | %7.2fM %9.2f | %.2fx\n", l, gz/1e6, gz*s*8/1e9, zm/1e6, zm*s*8/1e9, gz/zm }'
done
