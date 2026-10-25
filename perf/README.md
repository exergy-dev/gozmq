# perf/

Benchmarks, the benchmark gate, and a libzmq-comparable command-line tool.

```
perf/
  benchmark_test.go, fanin_bench_test.go   # go test -bench
  cli_test.go                              # end-to-end check of zmqperf
  benchgate.sh                             # working tree vs a base commit (the regression rule is in its header)
  zmqperf/                                 # local_thr | remote_thr | local_lat | remote_lat, as in libzmq's perf/
  libzmq_thr/                              # compare_all.sh / compare_lat.sh: two-process gozmq vs native libzmq
  REPORT.md                                # measured numbers and method
```

## Benchmarks

```sh
go test -bench=. -benchmem -benchtime=3s ./perf/     # everything
go test -bench=/64KB -benchtime=1s ./perf/           # one size across patterns
```

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

`-size` and `-count` must match on both ends. `local_thr` receives into the
buffer pool and releases each message (`-pool=false` for plain memory), and
the tool runs the Go collector at GC percent 400 (`-gogc N` overrides it,
negative leaves the runtime's setting): libzmq has no collector, and at
64 KB frames Go's default pacer would collect every ~60 messages. `ipc://`
endpoints work too;
`inproc://` cannot, since it needs both sockets in one process (use the Go
benchmarks for inproc). The throughput commands accept `-cpuprofile` and
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
Results and the machine they came from are in `REPORT.md`.
