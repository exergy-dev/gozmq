# perf/

Benchmarks, the benchmark gate, a libzmq-comparable command-line tool, and
one script that runs all of it.

```
perf/
  run_all.sh                               # the whole suite into perf/results/run-<timestamp>/
  benchmark_test.go, fanin_bench_test.go,  # go test -bench
  tls_bench_test.go, matrix_bench_test.go
  cli_test.go                              # end-to-end check of zmqperf, every pattern
  benchgate.sh                             # working tree vs a base commit (the regression rule is in its header)
  zmqperf/                                 # local_thr | remote_thr | local_lat | remote_lat | inproc_thr | inproc_lat, -pattern P
  libzmq_thr/zmqperf.c                     # the same tool written against libzmq
  libzmq_thr/compare.sh                    # gozmq vs libzmq: every pattern x transport x security
  ../internal/pipe/bench_test.go           # pipe microbenchmarks, both pipe kinds
  REPORT.md                                # measured numbers and method
```

## Everything at once

```sh
perf/run_all.sh                                    # full suite, about an hour and a half
COUNT_SCALE=0.02 BENCHTIME=200x perf/run_all.sh    # smoke run, a few minutes
```

`run_all.sh` writes `env.txt` (machine, Go, libzmq, revision, load),
`gobench.txt` (every in-repo benchmark, benchstat-ready) and `compare/`
(the libzmq matrix: `compare.txt` as printed, `results.tsv` one row per
cell, `runs.log` every process's output) under `perf/results/` (gitignored).
`SKIP_GOBENCH=1` or `SKIP_LIBZMQ=1` drops a half; `BENCHTIME`, `BENCH_COUNT`,
`BENCH` and `GOMAXPROCS` (default 8) shape the Go half and every
`compare.sh` variable below passes through. Run it on an idle machine and
never alongside another benchmark. A regression check for a change is still
`benchgate.sh`, which interleaves base and candidate.

## Benchmarks

```sh
go test -bench=. -benchmem -benchtime=3s ./perf/     # everything
go test -bench=/64KB -benchtime=1s ./perf/           # one size across patterns
```

Names follow `Benchmark<Pattern><What>_<Transport>/<size>`. Every pattern
runs over every transport it supports:

| pattern | inproc | TCP | IPC | TLS | CURVE (TCP) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| PUSH/PULL throughput | yes | yes | yes | yes | yes |
| REQ/REP round trip | yes | yes | yes | yes | yes |
| ROUTER/DEALER round trip | yes | yes | yes | yes | |
| DEALER to ROUTER and ROUTER to DEALER streaming | yes | yes | yes | yes | |
| PUB/SUB throughput | yes | yes | yes | yes | yes |
| PUB/SUB fan-out to three SUBs | yes | yes | yes | | |
| XPUB/XSUB throughput | yes | yes | yes | yes | |
| PAIR throughput and round trip | yes | yes | yes | yes | |
| STREAM send and receive against raw TCP | | yes | | | |

Scenario rows sit on top of that: PUSH/PULL fan-in, large frames and
PUB/SUB with a topic frame over inproc and TCP; REQ/REP throughput over
inproc, TCP and IPC; the wide and selective fan-outs, idle ROUTER peers and
XPUB subscription churn over inproc; connection handshakes over TCP with
and without CURVE.

The less obvious rows:

- `PushPullLarge_Inproc` (8KB, 64KB, 1MB): inproc hands the payload over
  without a copy, so this shows the handoff cost by size.
- `PushPullLarge_TCP` has `pool=false` and `pool=true` variants.
- `RouterDealerThroughput_*` is ROUTER to DEALER (the DEALER says hello so
  the ROUTER has its route); `DealerRouterThroughput_*` the other way.
- `StreamRecv_TCP` and `StreamSend_TCP` pair a STREAM socket with a plain
  `net.Conn`; MB/s is the figure, the chunking is the kernel's.
- The fan-in, wide fan-out, idle-peer and subscription-churn rows are in
  `fanin_bench_test.go`; the TLS rows that predate the matrix in
  `tls_bench_test.go`; everything added to complete the table above in
  `matrix_bench_test.go`.

Every benchmark releases the messages it receives (`m.Release()`), so it
measures the same thing whether or not the receive pool is on.

Throughput benchmarks call `b.SetBytes`, so `-benchmem` output includes
MB/s; latency benchmarks report ns/op per round trip. Never benchmark with
`-race`: it multiplies synchronisation cost by about 5×. Benchmarks do not
run in CI; a hot-path change runs `perf/benchgate.sh <base-commit>` locally
and quotes the benchstat output.

## zmqperf

One binary, the same commands, flags and output as libzmq's perf tools, so
either side of the wire can be libzmq. The local side binds and the remote
side connects (`-mode bind|connect` swaps that):

```sh
go run ./perf/zmqperf local_thr  -endpoint tcp://*:5555         -size 256 -count 1000000   # start first
go run ./perf/zmqperf remote_thr -endpoint tcp://127.0.0.1:5555 -size 256 -count 1000000
go run ./perf/zmqperf local_lat  -endpoint tcp://*:5555         -size 32  -count 100000
go run ./perf/zmqperf remote_lat -endpoint tcp://127.0.0.1:5555 -size 32  -count 100000
go run ./perf/zmqperf inproc_thr -size 32 -count 1000000   # sender goroutine -> receiver
go run ./perf/zmqperf inproc_lat -size 32 -count 100000    # client -> echo goroutine, full round trip in us
```

`-pattern` picks the socket pair. Throughput: `push_pull` (default),
`pub_sub` (`-prefix` sets the subscription; `-subs N` runs N subscribers in
`inproc_thr`, where the sender then binds), `xpub_xsub`, `pair`,
`dealer_router`, `router_dealer`, `stream_in` (raw TCP to STREAM) and
`stream_out` (STREAM to raw TCP; the raw side of both is always this Go
code). Latency: `req_rep` (default), `req_router`, `dealer_rep`,
`dealer_router`, `pair`; the server echoes the whole message.
`perf/zmqperf/patterns.go` documents what each side does.

`-size` and `-count` must match on both ends. Other flags: `-hwm` (both
HWMs on every socket, default 1,000,000, 0 for unlimited), `-curve` (CURVE
with fixed public test keys, the binding side is the server), `-wait` (how
long a PUB waits for subscriptions, default 500ms), `-idle` (a receiver
gives up after this long without a message and exits non-zero, so a lossy
run never reports a rate), `-pool` (default true: receive into the buffer
pool and release every message, as libzmq's `local_thr` does with
`zmq_msg_close`; `-pool=false` is plain memory), `-gogc N`, and
`-cpuprofile`/`-memprofile` on the throughput commands (`*.pprof` files are
gitignored). `tls+tcp://` endpoints register the TLS transport with a
self-signed loopback certificate; `ipc://` works too.

Throughput receivers print `received: N` and, only if all `count` messages
arrived, the rate. Latency is the full round trip in microseconds; libzmq's
own `remote_lat` prints half of that.

## Against native libzmq

`libzmq_thr/zmqperf.c` is `zmqperf` written against libzmq (repo-written and
minimal, not libzmq's perf sources; needs gcc and libzmq-dev): the same
commands, flags, patterns and output, so `libzmq_thr/compare.sh` can put
either library on either side of every cell. It runs every throughput and
latency pattern over tcp, ipc, inproc and tls (gozmq only, libzmq has no
TLS transport), with NULL and CURVE security (CURVE over tcp and ipc), at
32 B to 64 KB, and prints one table per transport and mechanism: gozmq on
both sides, libzmq on both sides, the ratio, and the two mixed pairings.
Each cell is the best valid run of up to `MAX_TRIES` (stopping at two
valid ones); a run that lost messages is invalid and retried, a run that
failed with an error is reported as ERR without retries.

Both tools give every socket the same HWM (`HWM`, default 1,000,000),
infinite linger on senders, one reused payload and one reused receive
message; a libzmq ROUTER sender sets `ZMQ_ROUTER_MANDATORY` so it blocks at
HWM like gozmq's. The remaining asymmetry is the API: `zmq_send` copies the
payload into a message, gozmq sends the caller's slice. `C_HWM=1000` gives
the libzmq side its stock HWM, the setting the pre-matrix scripts used.

```sh
perf/libzmq_thr/compare.sh                                        # the full matrix
TRANSPORTS=tcp SECURITY=null THR_PATTERNS=push_pull perf/libzmq_thr/compare.sh
COUNT_SCALE=0.02 MAX_TRIES=1 perf/libzmq_thr/compare.sh           # smoke run
```

Overrides: `TRANSPORTS`, `SECURITY`, `THR_PATTERNS`, `LAT_PATTERNS`,
`THR_SIZES`, `LAT_SIZES`, `MIXED`, `COUNT_SCALE`, `MAX_TRIES`, `HWM`,
`C_HWM`, `GOZMQ_LOCAL_FLAGS`, `GOZMQ_GOGC`, `OUT`; the script header has
the details.

## Pipe microbenchmarks

`internal/pipe/bench_test.go` benchmarks the pipe hot paths in both pipe kinds
as sub-benchmarks: `/queue` is `pipe.NewPair` (the chunked queue the
sessions use) and `/channel` is `pipe.NewChannelPair` (used by inproc).

```sh
GOMAXPROCS=8 taskset -c 0-7 go test ./internal/pipe -run '^$' -bench Pipe_ -count 6 -benchmem
```

Results and the machine they came from are in `REPORT.md`.
