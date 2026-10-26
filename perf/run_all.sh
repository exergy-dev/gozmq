#!/usr/bin/env bash
# run_all.sh: the whole performance suite in one run, into one directory.
#
#   perf/run_all.sh                       # everything
#   COUNT_SCALE=0.02 BENCHTIME=200x perf/run_all.sh   # smoke run (a few minutes)
#
# Steps, each skippable:
#   1. env.txt      machine, OS, Go, libzmq, git revision, load
#   2. gobench.txt  every in-repo benchmark (./perf, the root package's poller
#                   rows and the pipe microbenchmarks in ./internal/pipe),
#                   benchstat-ready; SKIP_GOBENCH=1 skips it
#   3. compare/     perf/libzmq_thr/compare.sh, gozmq vs native libzmq over
#                   every pattern, transport and security mechanism
#                   (compare.txt is the printed tables, results.tsv one row per
#                   cell); SKIP_LIBZMQ=1 skips it
#
# Overrides: OUT (default perf/results/run-<UTC timestamp>, gitignored),
# BENCHTIME (default 1s), BENCH_COUNT (default 1; 6+ for benchstat
# confidence intervals), BENCH (the -bench regexp, default "."),
# GOMAXPROCS for the Go benchmarks (default 8, as in REPORT.md), and every
# compare.sh variable (TRANSPORTS, SECURITY, THR_PATTERNS, LAT_PATTERNS,
# THR_SIZES, LAT_SIZES, MIXED, COUNT_SCALE, MAX_TRIES, HWM, C_HWM, ...).
#
# Run it on an otherwise idle machine and never two suites at once: every
# number here is a wall-clock rate on shared CPUs. The regression gate for a
# change is still perf/benchgate.sh, which interleaves a base and a
# candidate; this script records where the tree stands.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
out="${OUT:-$root/perf/results/run-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$out"

{
  echo "date:     $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "revision: $(git -C "$root" rev-parse --short HEAD)$(git -C "$root" diff --quiet || echo ' (dirty)')"
  echo "branch:   $(git -C "$root" rev-parse --abbrev-ref HEAD)"
  echo "go:       $(go version)"
  echo "libzmq:   $(pkg-config --modversion libzmq 2>/dev/null || echo 'not found')"
  echo "kernel:   $(uname -srm)"
  echo "cpu:      $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//') ($(nproc) threads)"
  echo "memory:   $(free -m 2>/dev/null | awk '/^Mem:/{print $2 " MiB total, " $7 " MiB available"}')"
  echo "load:     $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)"
  echo "settings: GOMAXPROCS=${GOMAXPROCS:-8} BENCHTIME=${BENCHTIME:-1s} BENCH_COUNT=${BENCH_COUNT:-1} COUNT_SCALE=${COUNT_SCALE:-1}"
} | tee "$out/env.txt"
echo

if [ "${SKIP_GOBENCH:-0}" != 1 ]; then
  echo "== in-repo benchmarks -> $out/gobench.txt"
  ( cd "$root" && GOMAXPROCS=${GOMAXPROCS:-8} go test ./perf/ ./internal/pipe/ . -run '^$' \
      -bench "${BENCH:-.}" -benchmem -benchtime "${BENCHTIME:-1s}" -count "${BENCH_COUNT:-1}" -timeout 6h ) \
    | tee "$out/gobench.txt"
  echo
fi

if [ "${SKIP_LIBZMQ:-0}" != 1 ]; then
  echo "== gozmq vs libzmq -> $out/compare/"
  mkdir -p "$out/compare"
  OUT="$out/compare" "$here/libzmq_thr/compare.sh" | tee "$out/compare/compare.txt"
fi

echo
echo "results in $out"
