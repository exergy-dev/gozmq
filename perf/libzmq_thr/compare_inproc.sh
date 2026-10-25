#!/usr/bin/env bash
# gozmq vs libzmq over inproc://. inproc cannot span processes, so each side is
# ONE process whose peer runs on a second thread (libzmq: pthread) or goroutine
# (gozmq), as in libzmq's own perf/inproc_thr and inproc_lat:
#   - throughput: PUSH on the peer thread, PULL on the main one (inproc_thr.c,
#     `zmqperf inproc_thr`), msg/s measured on the PULL after the first message.
#   - latency: REQ on the main thread, REP echoing on the peer (inproc_lat.c,
#     `zmqperf inproc_lat`).
#
# CONVENTION: latency is the FULL ROUND TRIP in microseconds, not libzmq's
# one-way (round trip / 2). Both inproc tools print that figure as
# "average latency".
#
# The C tools are repo-written minimal versions compiled against the system
# libzmq, not libzmq's own perf sources. They keep libzmq's stock HWM (1000)
# and allocate per message; zmqperf raises the HWM to 1M and reuses one
# payload, as in compare_all.sh. zmqperf receives into the buffer pool and
# releases every message by default (GOZMQ_LOCAL_FLAGS=-pool=false for plain
# memory); GOZMQ_GOGC passes -gogc.
#
# Overrides: COUNT_SCALE (multiplier on every count, default 1; 0.05 for a
# smoke run), MAX_TRIES (default 5), THR_SIZES (default "32 256 1024 8192
# 65536"), LAT_SIZES (default "32 1024"), LAT_COUNT (default 50000).
# Each measurement is retried until valid and the best valid run is kept
# (highest throughput, lowest latency); every process runs under `timeout`.
#
# Requires: gcc, libzmq-dev (pkg-config libzmq), a built Go toolchain.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT

for t in inproc_thr inproc_lat; do
  # shellcheck disable=SC2046 # pkg-config output is a flag list
  gcc -O2 -o "$tmp/c_$t" "$here/$t.c" $(pkg-config --cflags --libs libzmq) -lpthread
done
( cd "$root" && go build -o "$tmp/zmqperf" ./perf/zmqperf )

MAX_TRIES=${MAX_TRIES:-5}
PER_RUN_TIMEOUT=60

scaled() { awk -v c="$1" -v k="${COUNT_SCALE:-1}" 'BEGIN{n=int(c*k); print (n<2)?2:n}'; }

one_run() { # impl kind size count -> echoes msg/s (thr) or round-trip us (lat); empty on failure
  local impl=$1 kind=$2 size=$3 count=$4 out="$tmp/out"
  : > "$out"
  if [ "$impl" = c ]; then
    timeout "$PER_RUN_TIMEOUT" "$tmp/c_inproc_$kind" "$size" "$count" > "$out" 2>/dev/null || true
  else
    # shellcheck disable=SC2086 # GOZMQ_LOCAL_FLAGS is a flag list
    timeout "$PER_RUN_TIMEOUT" "$tmp/zmqperf" "inproc_$kind" -size "$size" -count "$count" ${GOZMQ_GOGC+-gogc $GOZMQ_GOGC} ${GOZMQ_LOCAL_FLAGS-} > "$out" 2>/dev/null || true
  fi
  if [ "$kind" = thr ]; then
    sed -n 's/^mean throughput: \([0-9]*\) \[msg\/s\].*/\1/p' "$out" | head -1
  else
    sed -n 's/^average latency: \([0-9.]*\) \[us\].*/\1/p' "$out" | head -1
  fi
}

best() { # impl kind size count -> best valid value over MAX_TRIES (0 if all fail)
  local b=0 t out
  for t in $(seq 1 "$MAX_TRIES"); do
    out=$(one_run "$@"); out=${out:-0}
    b=$(awk -v a="$b" -v c="$out" -v k="$2" 'BEGIN{
      if (k=="thr") print (c>a)?c:a; else print (c>0 && (a==0 || c<a))?c:a }')
    sleep 0.3
    [ "$t" -ge 2 ] && [ "$b" != 0 ] && break
  done
  echo "$b"
}

label() { awk -v s="$1" 'BEGIN{ if (s>=1024) printf "%dKB", s/1024; else printf "%dB", s }'; }

echo "## inproc PUSH->PULL throughput (one process, peer on a thread/goroutine)"
printf "%-6s | %10s %9s | %10s %9s | %s\n" "size" "gozmq" "Gb/s" "libzmq" "Gb/s" "gozmq/zmq"
for size in ${THR_SIZES:-32 256 1024 8192 65536}; do
  case $size in
    32|256) base=3000000 ;; 1024) base=1500000 ;; 8192) base=600000 ;; *) base=150000 ;;
  esac
  count=$(scaled "$base")
  gz=$(best go thr "$size" "$count")
  zm=$(best c  thr "$size" "$count")
  awk -v gz="$gz" -v zm="$zm" -v l="$(label "$size")" -v s="$size" 'BEGIN{
    gzs=(gz>0)?sprintf("%7.2fM",gz/1e6):"   FAIL";
    zms=(zm>0)?sprintf("%7.2fM",zm/1e6):"   FAIL";
    r =(gz>0&&zm>0)?sprintf("%.2fx",gz/zm):"  -";
    printf "%-6s | %s %9.2f | %s %9.2f | %s\n", l, gzs, gz*s*8/1e9, zms, zm*s*8/1e9, r }'
done
echo

COUNT=$(scaled "${LAT_COUNT:-50000}")
echo "## inproc REQ->REP round-trip latency, FULL ROUND TRIP in microseconds ($COUNT round trips)"
echo "## (libzmq's own tools print one-way = round trip / 2; this table does not)"
printf "%-6s | %9s %9s | %s\n" "size" "gozmq" "libzmq" "gozmq/zmq"
for size in ${LAT_SIZES:-32 1024}; do
  gz=$(best go lat "$size" "$COUNT")
  zm=$(best c  lat "$size" "$COUNT")
  awk -v gz="$gz" -v zm="$zm" -v l="$(label "$size")" 'BEGIN{
    gzs=(gz>0)?sprintf("%9.2f",gz):sprintf("%9s","FAIL");
    zms=(zm>0)?sprintf("%9.2f",zm):sprintf("%9s","FAIL");
    r =(gz>0&&zm>0)?sprintf("%.2fx",gz/zm):"  -";
    printf "%-6s | %s %s | %s\n", l, gzs, zms, r }'
done
echo
echo "gozmq/zmq is the ratio of the two cells; for latency lower is better."
