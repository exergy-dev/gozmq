# perf/

Benchmarks, the benchmark gate, and a libzmq-comparable command-line tool.

```
perf/
  benchmark_test.go, fanin_bench_test.go   # go test -bench
  cli_test.go                              # end-to-end check of zmqperf
  benchgate.sh                             # working tree vs a base commit (the regression rule is in its header)
  zmqperf/                                 # local_thr | remote_thr | local_lat | remote_lat | inproc_thr | inproc_lat
  libzmq_thr/                              # compare_all.sh / compare_lat.sh (two-process) and compare_inproc.sh (one-process) vs native libzmq
  ../internal/pipe/bench_test.go           # pipe microbenchmarks, both pipe kinds
  REPORT.md                                # measured numbers and method
```

## Benchmarks

```sh
go test -bench=. -benchmem -benchtime=3s ./perf/     # everything
go test -bench=/64KB -benchtime=1s ./perf/           # one size across patterns
```

Names follow `Benchmark<Pattern><What>_<Transport>/<size>`. Transports are
Inproc, TCP and IPC (a unix socket under `b.TempDir()`). Beyond the
TCP/Inproc ladders, the less obvious rows are:

- `PushPullLarge_Inproc` (8KB, 64KB, 1MB): inproc hands the payload over
  without a copy, so this shows the handoff cost by size.
- `PubSubThroughput_Inproc` and `PubSubTopicThroughput_Inproc` (32B, 64B):
  the inproc twins of the TCP rows.
- `PushPullThroughput_IPC` (32B, 1KB, 64KB), `ReqRepRoundTrip_IPC` and
  `ReqRepThroughput_IPC` (32B).
- `PushPullLarge_TCP` has `pool=false` and `pool=true` variants.

Every benchmark releases the messages it receives (`m.Release()`), so it
measures the same thing whether or not the receive pool is on.

The whole suite runs in a few minutes at `-benchtime=1s`.

Throughput benchmarks call `b.SetBytes`, so `-benchmem` output includes
MB/s; latency benchmarks report ns/op per round trip. Never benchmark with
`-race`: it multiplies synchronisation cost by about 5×. Benchmarks do not
run in CI; a hot-path change runs `perf/benchgate.sh <base-commit>` locally
and quotes the benchstat output.

## zmqperf

One binary, four subcommands, the same flags and output as libzmq's perf
tools, so either side of the wire can be libzmq:

```sh
go run ./perf/zmqperf local_thr  -endpoint tcp://*:5555         -size 256 -count 1000000   # start first
go run ./perf/zmqperf remote_thr -endpoint tcp://127.0.0.1:5555 -size 256 -count 1000000
go run ./perf/zmqperf local_lat  -endpoint tcp://*:5555         -size 32  -count 100000
go run ./perf/zmqperf remote_lat -endpoint tcp://127.0.0.1:5555 -size 32  -count 100000
```

`-size` and `-count` must match on both ends. The receiver
releases every message, as libzmq's `local_thr` does with `zmq_msg_close`, so
`-pool` (default true) receives into the buffer pool; `-pool=false` is the
plain-memory opt-out. The flag is always passed to the library explicitly, so
the tool behaves the same whatever the library's own default is. `-gogc N`
sets the collector's percent (libzmq has no collector, and at 64 KB frames
Go's default pacer collects every ~60 messages); unset, the runtime's setting
applies. `ipc://` endpoints work too. `inproc://` needs both sockets in one
process, so two more subcommands run the peer on a goroutine, as libzmq's
`perf/inproc_thr` and `inproc_lat` do on a thread:

```sh
go run ./perf/zmqperf inproc_thr -size 32 -count 1000000   # PUSH goroutine -> PULL
go run ./perf/zmqperf inproc_lat -size 32 -count 100000    # REQ -> REP goroutine, full round trip in us
``` The throughput commands accept `-cpuprofile` and
`-memprofile`; `*.pprof` files are gitignored.

## Against native libzmq

`libzmq_thr/compare_all.sh` compiles the minimal C `local_thr.c`/`remote_thr.c`
in that directory (repo-written, not libzmq's canonical perf sources; needs
gcc and libzmq-dev) against the system libzmq and runs them against
`zmqperf` over TCP and IPC, retrying each measurement until it is valid.
A second table covers the two mixed pairings (gozmq PUSH to libzmq PULL and
the reverse). `TRANSPORTS` and `COUNT_SCALE` (a multiplier on the message
counts) override the defaults.
Only the TCP table is apples to apples: libzmq's tool uses its default HWM of
1000 and allocates per message, and over IPC that visibly flatters gozmq.

`libzmq_thr/compare_lat.sh` does the same for REQ/REP round-trip latency
(`local_lat.c`/`remote_lat.c`) at 32 B and 1 KB, for all four pairings
(gozmq or libzmq on each side). It prints the full round trip in microseconds;
libzmq's own tools print one-way (round trip / 2), which the script avoids by
reading the `roundtrip latency` line of `remote_lat.c`.
`libzmq_thr/compare_inproc.sh` is the inproc comparison: `inproc_thr.c` and
`inproc_lat.c` (one process, peer on a pthread) against `zmqperf inproc_thr` /
`inproc_lat`, PUSH/PULL at 32 B, 256 B, 1 KB, 8 KB, 64 KB and REQ/REP at 32 B
and 1 KB, best valid of up to `MAX_TRIES`. Latency is the full round trip in
microseconds, as in `compare_lat.sh`. Overrides: `COUNT_SCALE`, `MAX_TRIES`,
`THR_SIZES`, `LAT_SIZES`, `LAT_COUNT`, `GOZMQ_LOCAL_FLAGS`, `GOZMQ_GOGC`.
The C tools keep libzmq's default HWM and allocate per message, zmqperf raises
the HWM and reuses its payload, so read the ratio as "stock perf tool" against
"stock perf tool".

```sh
perf/libzmq_thr/compare_inproc.sh                 # full table
COUNT_SCALE=0.05 perf/libzmq_thr/compare_inproc.sh  # smoke run
```

## Pipe microbenchmarks

`internal/pipe/bench_test.go` benchmarks the pipe hot paths in both pipe kinds
as sub-benchmarks: `/queue` is `pipe.NewPair` (the chunked queue the
sessions use) and `/channel` is `pipe.NewChannelPair` (used by inproc).

```sh
GOMAXPROCS=8 taskset -c 0-7 go test ./internal/pipe -run '^$' -bench Pipe_ -count 6 -benchmem
```

Results and the machine they came from are in `REPORT.md`.
