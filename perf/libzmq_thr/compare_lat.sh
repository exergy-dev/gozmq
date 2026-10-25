#!/usr/bin/env bash
# REQ->REP round-trip latency: gozmq vs libzmq, including the two mixed
# pairings (a gozmq client against a libzmq server and the reverse), over TCP.
#
# CONVENTION: every number printed is the FULL ROUND TRIP in microseconds
# (one REQ send plus the REP echo coming back), NOT the one-way time.
#   - zmqperf remote_lat prints the full round trip as "average latency".
#   - the C remote_lat.c prints ONE-WAY (round trip / 2, libzmq's own
#     convention) as "average latency" and the full round trip as
#     "roundtrip latency"; this script parses the latter, so the two sides
#     are never mixed up. Halve the table for libzmq-style one-way figures.
#
# The C tools are repo-written minimal versions (local_lat.c, remote_lat.c)
# compiled against the system libzmq; they are not libzmq's own perf sources.
#
# Overrides: SIZES (default "32 1024"), LAT_COUNT (round trips, default
# 50000), MAX_TRIES (default 5).
#
# Robustness: same as compare_all.sh. Each process is wrapped in `timeout`,
# TCP ports are never reused, a measurement is retried until valid and the
# best (lowest) valid run is kept, with a short cooldown between runs.
#
# Requires: gcc, libzmq-dev (pkg-config libzmq), a built Go toolchain.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT

# shellcheck disable=SC2046 # pkg-config output is a flag list
gcc -O2 -o "$tmp/c_local_lat"  "$here/local_lat.c"  $(pkg-config --cflags --libs libzmq)
# shellcheck disable=SC2046
gcc -O2 -o "$tmp/c_remote_lat" "$here/remote_lat.c" $(pkg-config --cflags --libs libzmq)
( cd "$root" && go build -o "$tmp/zmqperf" ./perf/zmqperf )

MAX_TRIES=${MAX_TRIES:-5}
PER_RUN_TIMEOUT=60
COUNT=${LAT_COUNT:-50000}
PORT=6100            # monotonically increasing; never reused

one_run() { # client server size -> echoes full round-trip us (0 on failure)
  local cli=$1 srv=$2 size=$3 ep out="$tmp/out" p
  ep="tcp://127.0.0.1:$PORT"; PORT=$((PORT+1))
  : > "$out"
  if [ "$srv" = c ]; then
    timeout "$PER_RUN_TIMEOUT" "$tmp/c_local_lat" "$ep" "$size" "$COUNT" >/dev/null 2>&1 & p=$!
  else
    timeout "$PER_RUN_TIMEOUT" "$tmp/zmqperf" local_lat -endpoint "$ep" -size "$size" -count "$COUNT" >/dev/null 2>&1 & p=$!
  fi
  sleep 0.5
  if [ "$cli" = c ]; then
    timeout "$PER_RUN_TIMEOUT" "$tmp/c_remote_lat" "$ep" "$size" "$COUNT" > "$out" 2>/dev/null || true
    sed -n 's/^roundtrip latency: \([0-9.]*\) \[us\].*/\1/p' "$out" | head -1
  else
    timeout "$PER_RUN_TIMEOUT" "$tmp/zmqperf" remote_lat -endpoint "$ep" -size "$size" -count "$COUNT" > "$out" 2>/dev/null || true
    sed -n 's/^average latency: \([0-9.]*\) \[us\].*/\1/p' "$out" | head -1
  fi
  kill "$p" 2>/dev/null || true
  wait "$p" 2>/dev/null || true
}

best() { # client server size -> lowest valid round trip over MAX_TRIES (0 if all fail)
  local b=0 t out
  for t in $(seq 1 "$MAX_TRIES"); do
    out=$(one_run "$1" "$2" "$3"); out=${out:-0}
    b=$(awk -v a="$b" -v c="$out" 'BEGIN{print (c>0 && (a==0 || c<a))?c:a}')
    sleep 0.3
    # Two runs is enough once we have a valid one.
    [ "$t" -ge 2 ] && [ "$b" != 0 ] && break
  done
  echo "$b"
}

fmt() { awk -v v="$1" 'BEGIN{ if (v>0) printf "%9.2f", v; else printf "%9s", "FAIL" }'; }

echo "## REQ->REP round-trip latency over tcp, FULL ROUND TRIP in microseconds ($COUNT round trips)"
echo "## (libzmq's own tools print one-way = round trip / 2; this table does not)"
printf "%-6s | %9s %9s | %9s %9s | %s\n" "size" "go>go" "zmq>zmq" "go>zmq" "zmq>go" "go/zmq"
for size in ${SIZES:-32 1024}; do
  gg=$(best go go "$size")
  cc=$(best c  c  "$size")
  gc=$(best go c  "$size")
  cg=$(best c  go "$size")
  l=$([ "$size" -ge 1024 ] && echo "$((size/1024))KB" || echo "${size}B")
  r=$(awk -v a="$gg" -v b="$cc" 'BEGIN{ if (a>0 && b>0) printf "%.2fx", a/b; else printf "  -" }')
  printf "%-6s | %s %s | %s %s | %s\n" "$l" "$(fmt "$gg")" "$(fmt "$cc")" "$(fmt "$gc")" "$(fmt "$cg")" "$r"
done
echo
echo "columns are client>server: go>go = gozmq REQ to gozmq REP, zmq>zmq = libzmq to libzmq,"
echo "go>zmq = gozmq REQ to libzmq REP, zmq>go = libzmq REQ to gozmq REP."
echo "go/zmq is the ratio of the two homogeneous cells (go>go / zmq>zmq); lower is better."
