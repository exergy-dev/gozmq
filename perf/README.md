# perf/

Benchmarks, the benchmark gate, and a libzmq-comparable command-line tool.

```
perf/
  benchmark_test.go, fanin_bench_test.go   # go test -bench
  cli_test.go                              # end-to-end check of zmqperf
  benchgate.sh                             # working tree vs a base commit (the regression rule is in its header)
  zmqperf/                                 # local_thr | remote_thr | local_lat | remote_lat, as in libzmq's perf/
  libzmq_thr/                              # compare_all.sh: two-process gozmq vs native libzmq throughput
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

`-size` and `-count` must match on both ends. `ipc://` endpoints work too;
`inproc://` cannot, since it needs both sockets in one process (use the Go
benchmarks for inproc). The throughput commands accept `-cpuprofile` and
`-memprofile`; `*.pprof` files are gitignored.

## Against native libzmq

`libzmq_thr/compare_all.sh` builds libzmq's own `local_thr`/`remote_thr`
from their C sources (needs gcc and libzmq-dev) and runs them against
`zmqperf` over TCP and IPC, retrying each measurement until it is valid.
Only the TCP table is apples to apples: libzmq's tool uses its default HWM of
1000 and allocates per message, and over IPC that visibly flatters gozmq.
Results and the machine they came from are in `REPORT.md`.
