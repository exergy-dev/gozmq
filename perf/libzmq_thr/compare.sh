#!/usr/bin/env bash
# compare.sh: gozmq vs native libzmq across every socket pattern, transport
# and security mechanism both libraries speak, plus the gozmq-only cells
# (TLS), printed as one table per transport and security mechanism.
#
# Both sides are the same tool: perf/zmqperf (Go) and zmqperf.c (this
# directory, compiled against the system libzmq), with identical commands,
# flags, patterns and output. Two-process transports (tcp, ipc, tls) run a
# local (receiver or echo server) and a remote (sender or client) process;
# inproc runs each library's one-process inproc_thr / inproc_lat with the
# peer on a goroutine or pthread. No cgo anywhere.
#
# Throughput patterns (msg/s at the receiver, timed after the first message):
#   push_pull, pub_sub, pub_sub_prefix (a 4-byte prefix subscription that
#   matches every message), pub_sub_fan4 (one PUB, four SUBs; the slowest
#   subscriber's rate), xpub_xsub, xpub_xsub_fan4, pair, dealer_router
#   (DEALER -> ROUTER), router_dealer (ROUTER -> DEALER), stream_in (raw TCP
#   -> STREAM) and stream_out (STREAM -> raw TCP). The raw TCP peer of the
#   two stream patterns is always the Go tool; they run over tcp, NULL only.
# Latency patterns (full round trip in microseconds, client sends, server
#   echoes): req_rep, req_router, dealer_rep, dealer_router, pair.
#
# Columns: go = gozmq on both sides, zmq = libzmq on both sides, ratio =
# go/zmq (throughput: higher is better for gozmq; latency: lower is), and,
# for two-process ZMQ-to-ZMQ cells, the mixed pairings sender>receiver
# (throughput) or client>server (latency): go>zmq and zmq>go.
#
# Fairness: both tools set SNDHWM = RCVHWM = $HWM (default 1,000,000) on
# every socket, infinite linger on senders, send one reused payload and
# receive into one reused message (zmq_send copies it into a message; gozmq
# sends it without a copy), and release every message they receive. A
# libzmq ROUTER sender sets ZMQ_ROUTER_MANDATORY so it blocks at HWM like
# gozmq's instead of dropping. A throughput run is valid only if every
# message arrived (a PUB drop or a slow joiner fails the run, which is then
# retried); each cell is the best valid run of up to MAX_TRIES, stopping
# after two valid ones. A run that fails with an error message (bad flag,
# bind failure) is not retried; the table shows ERR and the log has the
# message.
#
# Overrides (environment):
#   TRANSPORTS    default "tcp ipc inproc tls" (tls is gozmq only)
#   SECURITY      default "null curve" (curve over tcp and ipc, both libraries)
#   THR_PATTERNS  default: every throughput pattern above
#   LAT_PATTERNS  default: every latency pattern above
#   THR_SIZES     default "32 256 1024 8192 65536"
#   LAT_SIZES     default "32 1024 65536"
#   MIXED         1 (default) runs the mixed pairings, 0 skips them
#   COUNT_SCALE   multiplier on every count (0.02 for a smoke run)
#   MAX_TRIES     default 3
#   HWM           both libraries' HWM (default 1000000); C_HWM overrides the
#                 libzmq side alone (C_HWM=1000 is libzmq's stock setting)
#   PUB_HWM       HWM for the pub_sub and xpub_xsub cells, both libraries
#                 (default 0, unlimited): a PUB drops at HWM by design, and
#                 a sender that outruns its own session (libzmq at 32 B and
#                 256 B, any CURVE PUB) would drop past 1,000,000 queued
#                 messages and never complete a 1.5M-message run
#   GOZMQ_LOCAL_FLAGS  extra flags for gozmq receivers (-pool=false)
#   GOZMQ_GOGC    -gogc for every gozmq process (unset: Go's defaults)
#   OUT           directory for results.tsv and runs.log (default
#                 perf/results/compare-<UTC timestamp>, gitignored)
#
# Requires: gcc, libzmq-dev (pkg-config libzmq), a Go toolchain.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
tmp="$(mktemp -d)"
out="${OUT:-$root/perf/results/compare-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$out"
trap 'rm -f /tmp/gz-cmp-$$-*.sock; rm -rf "$tmp"' EXIT

# shellcheck disable=SC2046 # pkg-config output is a flag list
gcc -O2 -o "$tmp/czmqperf" "$here/zmqperf.c" $(pkg-config --cflags --libs libzmq) -lpthread
( cd "$root" && go build -o "$tmp/gozmqperf" ./perf/zmqperf )

TRANSPORTS=${TRANSPORTS:-tcp ipc inproc tls}
SECURITY=${SECURITY:-null curve}
THR_PATTERNS=${THR_PATTERNS:-push_pull pub_sub pub_sub_prefix pub_sub_fan4 xpub_xsub xpub_xsub_fan4 pair dealer_router router_dealer stream_in stream_out}
LAT_PATTERNS=${LAT_PATTERNS:-req_rep req_router dealer_rep dealer_router pair}
THR_SIZES=${THR_SIZES:-32 256 1024 8192 65536}
LAT_SIZES=${LAT_SIZES:-32 1024 65536}
MIXED=${MIXED:-1}
MAX_TRIES=${MAX_TRIES:-3}
HWM=${HWM:-1000000}
C_HWM=${C_HWM:-$HWM}
PUB_HWM=${PUB_HWM:-0}
PER_RUN_TIMEOUT=90
results="$out/results.tsv"
log="$out/runs.log"
: > "$results"
: > "$log"
echo 6000 > "$tmp/port"

scaled() { awk -v c="$1" -v k="${COUNT_SCALE:-1}" 'BEGIN{n=int(c*k); print (n<2)?2:n}'; }
thr_count() { case $1 in 32|256) scaled 1500000 ;; 1024) scaled 800000 ;; 8192) scaled 300000 ;; *) scaled 80000 ;; esac; }
lat_count() { case $1 in 65536) scaled 5000 ;; *) scaled 20000 ;; esac; }
label() { awk -v s="$1" 'BEGIN{ if (s>=1024) printf "%dKB", s/1024; else printf "%dB", s }'; }

# next_endpoint transport -> "bind_endpoint connect_endpoint"; ports are
# never reused (TIME_WAIT), the counter lives in a file so subshells share it.
next_endpoint() {
  local p
  p=$(( $(cat "$tmp/port") + 1 )); echo "$p" > "$tmp/port"
  case $1 in
    tcp) echo "tcp://127.0.0.1:$p tcp://127.0.0.1:$p" ;;
    tls) echo "tls+tcp://127.0.0.1:$p tls+tcp://127.0.0.1:$p" ;;
    ipc) echo "ipc:///tmp/gz-cmp-$$-$p.sock ipc:///tmp/gz-cmp-$$-$p.sock" ;;
  esac
}

bin() { [ "$1" = c ] && echo "$tmp/czmqperf" || echo "$tmp/gozmqperf"; }
# flags impl role(local|remote) -> per-implementation extra flags
flags() {
  if [ "$1" = c ]; then echo "-hwm $C_HWM"; return; fi
  local f="-hwm $HWM"
  [ -n "${GOZMQ_GOGC:-}" ] && f="$f -gogc $GOZMQ_GOGC"
  [ "$2" = local ] && f="$f ${GOZMQ_LOCAL_FLAGS:-}"
  echo "$f"
}

# split_pattern name -> "base extra-flags subs"
split_pattern() {
  case $1 in
    pub_sub_prefix) echo "pub_sub -prefix=aaaa 1" ;;
    *_fan[0-9]*) echo "${1%_fan*} - ${1##*_fan}" ;;
    *) echo "$1 - 1" ;;
  esac
}

# run_log label file...: append outputs to the log; echo the error line of
# the first file that has one (a Go log.Fatal or a C perror/die line).
run_err() {
  local f
  for f in "$@"; do
    { echo "--- $f"; cat "$f"; } >> "$log"
  done
  grep -h -v -E '^(message size|message count|roundtrip count|received|mean throughput|average latency|roundtrip rate)' "$@" 2>/dev/null \
    | grep -v -E '^\s*$' | head -1 || true
}

# one_thr recv_impl send_impl transport sec pattern size count -> msg/s, "" (invalid) or "ERR ..."
one_thr() {
  local ri=$1 si=$2 tr=$3 sec=$4 pat=$5 size=$6 count=$7
  local base extra subs sflags=() ep bep cep i pids=() outs=() so err
  read -r base extra subs < <(split_pattern "$pat")
  [ "$extra" = - ] && extra=
  [ "$sec" = curve ] && sflags+=(-curve)
  # The last -hwm wins in both tools, so this follows $(flags ...).
  local hwm_over=""
  case $base in pub_sub|xpub_xsub) hwm_over="-hwm $PUB_HWM" ;; esac
  if [ "$tr" = inproc ]; then
    so="$tmp/o.inproc"
    # shellcheck disable=SC2046,SC2086
    timeout "$PER_RUN_TIMEOUT" "$(bin "$ri")" inproc_thr -pattern "$base" $extra -subs "$subs" -size "$size" -count "$count" $(flags "$ri" local) $hwm_over > "$so" 2>&1 || true
    err=$(run_err "$so")
    [ -n "$err" ] && { echo "ERR $err"; return; }
    sed -n 's/^mean throughput: \([0-9]*\) \[msg\/s\].*/\1/p' "$so" | head -1
    return
  fi
  read -r bep cep < <(next_endpoint "$tr")
  local remote_flags=(-pattern "$base" -size "$size" -count "$count" "${sflags[@]}")
  # shellcheck disable=SC2206
  [ -n "$extra" ] && remote_flags+=($extra)
  if [ "$subs" -gt 1 ]; then
    # Fan-out: the sender binds and waits; every receiver connects.
    so="$tmp/o.send"
    # shellcheck disable=SC2046
    timeout "$PER_RUN_TIMEOUT" "$(bin "$si")" remote_thr -mode bind -endpoint "$bep" -subs "$subs" -wait 1s "${remote_flags[@]}" $(flags "$si" remote) $hwm_over > "$so" 2>&1 & pids+=($!)
    sleep 0.2
    for i in $(seq "$subs"); do
      outs+=("$tmp/o.recv$i")
      # shellcheck disable=SC2046
      timeout "$PER_RUN_TIMEOUT" "$(bin "$ri")" local_thr -mode connect -endpoint "$cep" "${remote_flags[@]}" $(flags "$ri" local) $hwm_over > "$tmp/o.recv$i" 2>&1 & pids+=($!)
    done
    wait "${pids[@]}" 2>/dev/null || true
  else
    outs=("$tmp/o.recv1"); so="$tmp/o.send"
    # stream_in's sender and stream_out's receiver are the Go raw TCP peer.
    [ "$base" = stream_in ] && si=go
    [ "$base" = stream_out ] && ri=go
    # shellcheck disable=SC2046
    timeout "$PER_RUN_TIMEOUT" "$(bin "$ri")" local_thr -endpoint "$bep" "${remote_flags[@]}" $(flags "$ri" local) $hwm_over > "${outs[0]}" 2>&1 & pids+=($!)
    sleep 0.3
    # shellcheck disable=SC2046
    timeout "$PER_RUN_TIMEOUT" "$(bin "$si")" remote_thr -endpoint "$cep" "${remote_flags[@]}" $(flags "$si" remote) $hwm_over > "$so" 2>&1 || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
  [ "$tr" = ipc ] && rm -f "${bep#ipc://}"
  err=$(run_err "$so" "${outs[@]}")
  [ -n "$err" ] && { echo "ERR $err"; return; }
  # Valid only if every receiver got everything; the rate is the slowest's.
  for i in "${outs[@]}"; do
    grep -q '^mean throughput: .*msg/s' "$i" || { echo ""; return; }
  done
  sed -n 's/^mean throughput: \([0-9]*\) \[msg\/s\].*/\1/p' "${outs[@]}" | sort -n | head -1
}

# one_lat server_impl client_impl transport sec pattern size count -> round-trip us, "" or "ERR ..."
one_lat() {
  local svi=$1 cli=$2 tr=$3 sec=$4 pat=$5 size=$6 count=$7 sflags=() bep cep so err
  [ "$sec" = curve ] && sflags+=(-curve)
  so="$tmp/o.client"
  if [ "$tr" = inproc ]; then
    # shellcheck disable=SC2046
    timeout "$PER_RUN_TIMEOUT" "$(bin "$svi")" inproc_lat -pattern "$pat" -size "$size" -count "$count" $(flags "$svi" local) > "$so" 2>&1 || true
    err=$(run_err "$so")
  else
    read -r bep cep < <(next_endpoint "$tr")
    # shellcheck disable=SC2046
    timeout "$PER_RUN_TIMEOUT" "$(bin "$svi")" local_lat -endpoint "$bep" -pattern "$pat" -size "$size" -count "$count" "${sflags[@]}" $(flags "$svi" local) > "$tmp/o.server" 2>&1 & local p=$!
    sleep 0.3
    # shellcheck disable=SC2046
    timeout "$PER_RUN_TIMEOUT" "$(bin "$cli")" remote_lat -endpoint "$cep" -pattern "$pat" -size "$size" -count "$count" "${sflags[@]}" $(flags "$cli" remote) > "$so" 2>&1 || true
    wait $p 2>/dev/null || true
    [ "$tr" = ipc ] && rm -f "${bep#ipc://}"
    err=$(run_err "$tmp/o.server" "$so")
  fi
  [ -n "$err" ] && { echo "ERR $err"; return; }
  sed -n 's/^average latency: \([0-9.]*\) \[us\].*/\1/p' "$so" | head -1
}

# best kind a b transport sec pattern size count -> best valid value, "" or "ERR ..."
best() {
  local kind=$1; shift
  local b="" v t valid=0
  for t in $(seq "$MAX_TRIES"); do
    v=$("one_$kind" "$@")
    case $v in
      ERR*) echo "$v"; return ;;
      "") ;;
      *) valid=$((valid + 1))
         b=$(awk -v a="$b" -v c="$v" -v k="$kind" 'BEGIN{
               if (a == "") print c; else if (k == "thr") print (c > a) ? c : a; else print (c < a) ? c : a }') ;;
    esac
    sleep 0.3
    [ "$valid" -ge 2 ] && break
  done
  echo "$b"
}

fmt_thr() { case $1 in ERR*) printf "%9s" ERR ;; "") printf "%9s" FAIL ;; *) awk -v v="$1" 'BEGIN{printf "%8.2fM", v/1e6}' ;; esac; }
fmt_lat() { case $1 in ERR*) printf "%9s" ERR ;; "") printf "%9s" FAIL ;; *) awk -v v="$1" 'BEGIN{printf "%9.2f", v}' ;; esac; }
ratio() { awk -v a="$1" -v b="$2" 'BEGIN{ if (a+0 > 0 && b+0 > 0) printf "%6.2fx", a/b; else printf "%7s", "-" }'; }

# record kind transport sec pattern size pairing value
record() { printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$@" >> "$results"; }
errnote() { case $2 in ERR*) echo "    $1: ${2#ERR }" ;; esac; }

pattern_ok() { # pattern transport sec -> 0 if the cell exists
  case $1 in
    stream_*) [ "$2" = tcp ] && [ "$3" = null ] ;;
    *) case $3 in curve) [ "$2" = tcp ] || [ "$2" = ipc ] ;; *) true ;; esac ;;
  esac
}

for tr in $TRANSPORTS; do
  for sec in $SECURITY; do
    { [ "$sec" = curve ] && { [ "$tr" = inproc ] || [ "$tr" = tls ]; }; } && continue
    golib_only=0; [ "$tr" = tls ] && golib_only=1
    mixed=$MIXED; { [ "$tr" = inproc ] || [ "$golib_only" = 1 ]; } && mixed=0
    sec_label=$sec; [ "$tr" = tls ] && sec_label="tls"
    echo "## throughput, $tr, $sec_label (msg/s at the receiver; ratio go/zmq, higher is better)"
    printf "%-15s %-6s | %9s %9s %7s" pattern size go zmq ratio
    [ "$mixed" = 1 ] && printf " | %9s %9s" "go>zmq" "zmq>go"
    echo
    for pat in $THR_PATTERNS; do
      pattern_ok "$pat" "$tr" "$sec" || continue
      for size in $THR_SIZES; do
        count=$(thr_count "$size")
        gg=$(best thr go go "$tr" "$sec" "$pat" "$size" "$count")
        record thr "$tr" "$sec_label" "$pat" "$size" go "$gg"
        cc=""; [ "$golib_only" = 0 ] && { cc=$(best thr c c "$tr" "$sec" "$pat" "$size" "$count"); record thr "$tr" "$sec_label" "$pat" "$size" zmq "$cc"; }
        printf "%-15s %-6s | %s %s %s" "$pat" "$(label "$size")" "$(fmt_thr "$gg")" "$([ "$golib_only" = 0 ] && fmt_thr "$cc" || printf "%9s" n/a)" "$(ratio "$gg" "$cc")"
        if [ "$mixed" = 1 ] && [ "${pat#stream_}" = "$pat" ]; then
          gc=$(best thr c go "$tr" "$sec" "$pat" "$size" "$count")   # gozmq sender -> libzmq receiver
          cg=$(best thr go c "$tr" "$sec" "$pat" "$size" "$count")   # libzmq sender -> gozmq receiver
          record thr "$tr" "$sec_label" "$pat" "$size" "go>zmq" "$gc"
          record thr "$tr" "$sec_label" "$pat" "$size" "zmq>go" "$cg"
          printf " | %s %s" "$(fmt_thr "$gc")" "$(fmt_thr "$cg")"
        fi
        echo
        errnote go "$gg"; errnote zmq "$cc"; errnote "go>zmq" "${gc:-}"; errnote "zmq>go" "${cg:-}"
        gc=""; cg=""
      done
    done
    echo
    echo "## latency, $tr, $sec_label (full round trip in us; ratio go/zmq, lower is better)"
    printf "%-15s %-6s | %9s %9s %7s" pattern size go zmq ratio
    [ "$mixed" = 1 ] && printf " | %9s %9s" "go>zmq" "zmq>go"
    echo
    for pat in $LAT_PATTERNS; do
      pattern_ok "$pat" "$tr" "$sec" || continue
      for size in $LAT_SIZES; do
        count=$(lat_count "$size")
        gg=$(best lat go go "$tr" "$sec" "$pat" "$size" "$count")
        record lat "$tr" "$sec_label" "$pat" "$size" go "$gg"
        cc=""; [ "$golib_only" = 0 ] && { cc=$(best lat c c "$tr" "$sec" "$pat" "$size" "$count"); record lat "$tr" "$sec_label" "$pat" "$size" zmq "$cc"; }
        printf "%-15s %-6s | %s %s %s" "$pat" "$(label "$size")" "$(fmt_lat "$gg")" "$([ "$golib_only" = 0 ] && fmt_lat "$cc" || printf "%9s" n/a)" "$(ratio "$gg" "$cc")"
        if [ "$mixed" = 1 ]; then
          gc=$(best lat c go "$tr" "$sec" "$pat" "$size" "$count")   # gozmq client -> libzmq server
          cg=$(best lat go c "$tr" "$sec" "$pat" "$size" "$count")   # libzmq client -> gozmq server
          record lat "$tr" "$sec_label" "$pat" "$size" "go>zmq" "$gc"
          record lat "$tr" "$sec_label" "$pat" "$size" "zmq>go" "$cg"
          printf " | %s %s" "$(fmt_lat "$gc")" "$(fmt_lat "$cg")"
        fi
        echo
        errnote go "$gg"; errnote zmq "$cc"; errnote "go>zmq" "${gc:-}"; errnote "zmq>go" "${cg:-}"
        gc=""; cg=""
      done
    done
    echo
  done
done

echo "results: $results"
echo "run log: $log"
