# gozmq perf/

The `perf/` directory mirrors libzmq's [`perf/` suite](https://github.com/zeromq/libzmq/tree/master/perf)
and layers a Go benchmark suite on top. It serves two audiences:

1. CI / regression tracking — the Go `Benchmark*` functions run under
   `go test -bench=.` and feed per-commit benchmark tracking.
2. Operators and library authors — the four standalone CLI tools
   (`local_lat`, `remote_lat`, `local_thr`, `remote_thr`) reproduce the
   classic libzmq latency/throughput numbers so you can compare `gozmq`
   against a libzmq process on the other side of the same wire.

## Layout

```
perf/
  benchmark_test.go     # BenchmarkReqRepRoundTrip_*, BenchmarkPushPullThroughput_*, BenchmarkPubSubFanout_*
  main_test.go          # TestMain (goleak)
  local_lat/main.go     # binds REP, echoes each message back
  remote_lat/main.go    # connects REQ, measures round-trip latency
  local_thr/main.go     # binds PULL, measures observed throughput
  remote_thr/main.go    # connects PUSH, blasts N messages
  README.md             # you are here
```

## Running the Go benchmarks

```bash
# Full run, 3 seconds per benchmark, with allocation + MB/s accounting.
go test -bench=. -benchmem -benchtime=3s ./perf/

# Quick smoke run (CI; ~30 seconds total).
go test -bench=. -benchmem -benchtime=500ms ./perf/

# Filter to one pattern × one transport.
go test -bench=BenchmarkReqRepRoundTrip_Inproc -benchtime=3s ./perf/

# Just the 64KB sub-benchmarks across all patterns.
go test -bench=/64KB -benchtime=1s ./perf/
```

Every throughput benchmark calls `b.SetBytes(msgSize)` so `-benchmem`
reports `MB/s` in its output. REQ/REP latency benchmarks report `ns/op`
— divide by two for one-way latency if you want the libzmq-style figure.

Do NOT run these with `-race`. The race detector multiplies all
synchronisation costs by ~5× and makes the numbers useless for
comparisons.

Each benchmark's teardown is leak-checked by `go.uber.org/goleak` via
`TestMain` → `testutil.VerifyTestMain`, matching the rest of the module.

## Running the standalone CLI tools

The four CLIs are exactly the four libzmq `perf/` binaries, one process
on each end of the wire. They take the same three flags — `-endpoint`,
`-size`, `-count` — and print a libzmq-compatible banner plus a summary.
`local_thr` and `remote_thr` also accept `-cpuprofile` and `-memprofile`
for baseline capture.

### Latency example (REQ/REP, TCP loopback)

Terminal A — the local (REP) side:

```bash
go run ./perf/local_lat -endpoint tcp://*:5555 -size 32 -count 100000
```

Terminal B — the remote (REQ) side:

```bash
go run ./perf/remote_lat -endpoint tcp://127.0.0.1:5555 -size 32 -count 100000
```

Output (from `remote_lat`, numbers illustrative):

```
message size: 32 [B]
roundtrip count: 100000
average latency: 14.527 [us]
roundtrip rate: 68836 [msg/s]
```

### Throughput example (PUSH/PULL, TCP loopback)

Start the receiver first — it binds and waits for the first message
before starting its timer (matching libzmq's convention):

```bash
go run ./perf/local_thr  -endpoint tcp://*:5555           -size 256 -count 1000000
go run ./perf/remote_thr -endpoint tcp://127.0.0.1:5555   -size 256 -count 1000000
```

Output (from `local_thr`):

```
message size: 256 [B]
message count: 1000000
mean throughput: 523417 [msg/s]
mean throughput: 1071.958 [Mb/s]
```

### Over inproc

Inproc requires the two sockets to live in the same process, so the
standalone CLI tools are TCP/IPC only. Use the Go benchmarks for inproc
numbers (`BenchmarkReqRepRoundTrip_Inproc`, `BenchmarkPushPullThroughput_Inproc`,
`BenchmarkPubSubFanout_Inproc`).

### Over IPC

```bash
go run ./perf/local_lat  -endpoint ipc:///tmp/gozmq-lat -size 32 -count 100000
go run ./perf/remote_lat -endpoint ipc:///tmp/gozmq-lat -size 32 -count 100000
```

### Against libzmq

One side `gozmq`, other side libzmq's `remote_lat` from the libzmq
distribution — wire format is ZMTP 3.1 NULL, so they interoperate:

```bash
# gozmq REP side
go run ./perf/local_lat -endpoint tcp://*:5555 -size 32 -count 100000

# libzmq REQ side
./remote_lat tcp://127.0.0.1:5555 32 100000
```

Use this to validate that any gozmq regression is *ours* and not a
property of the benchmark setup.

## Baseline capture

The throughput CLIs can emit pprof artifacts directly:

```bash
mkdir -p perf/baselines/$(date +%F)

go run ./perf/local_thr \
  -endpoint tcp://*:5555 \
  -size 64 \
  -count 500000 \
  -cpuprofile perf/baselines/$(date +%F)/local-64.cpu.pprof \
  -memprofile perf/baselines/$(date +%F)/local-64.mem.pprof

go run ./perf/remote_thr \
  -endpoint tcp://127.0.0.1:5555 \
  -size 64 \
  -count 500000 \
  -cpuprofile perf/baselines/$(date +%F)/remote-64.cpu.pprof \
  -memprofile perf/baselines/$(date +%F)/remote-64.mem.pprof
```

Recommended capture sequence for a fresh baseline:

```bash
mkdir -p perf/baselines/$(date +%F)

go test -bench=. -benchmem -benchtime=3s -count=5 ./perf/ \
  | tee perf/baselines/$(date +%F)/bench.txt

/tmp/bench.sh | tee perf/baselines/$(date +%F)/matrix.txt
```

## Targets

Design §11 calls out three aspirational numbers. They are aspirational —
Wave 4 D2 is the perf-tuning wave. The benchmark harness you are reading
reports actual current numbers, and the Wave-3 gate is just "the harness
works and produces a number" — not "the number hits the target."

| Pattern                | Transport      | Target         |
|------------------------|----------------|----------------|
| PUSH/PULL throughput   | inproc         | ≥ 1,000,000 msg/s |
| PUSH/PULL throughput   | TCP loopback   | ≥ 500,000 msg/s   |
| REQ/REP round-trip     | inproc, median | < 10 µs           |

Frame pooling, `net.Buffers`-batched writes, and zero-copy inproc pipes
are the known high-leverage optimisations pending in D2.
