# Performance report

Measured 2026-10-23 on the `cleanup` branch (v0.2.0 candidate) against
`v0.1.1`. Machine: Intel i9-14900KF (32 threads), WSL2 Linux, Go 1.25.3,
`GOMAXPROCS=8` for the Go benchmarks. Earlier rounds of tuning and the
numbers they produced are in the git history of this file.

## Method

- **In-repo benchmarks**: `perf/benchgate.sh <base> . 10` builds the `./perf`
  and root test binaries for the base commit and the working tree, runs them
  interleaved ten times each with `-benchmem -benchtime 1s`, and compares with
  benchstat. The pass rule is in the script's header: no significant (p < 0.05)
  sec/op regression above 5%, allocs/op never higher, B/op within 5%.
- **Against native libzmq**: `perf/libzmq_thr/compare_all.sh` builds libzmq's
  own `local_thr`/`remote_thr` from the C sources and runs them against
  `perf/zmqperf` as two processes over TCP loopback, no cgo on either side,
  best valid run of up to five per cell. gozmq's tool uses a 1,000,000
  message HWM and reuses one payload; libzmq's uses its default HWM of 1000
  and allocates per message. Over TCP that is not the bottleneck.

## gozmq vs native libzmq 4.3.4, PUSH/PULL over TCP loopback

| size | gozmq | libzmq | ratio |
| :--- | ---: | ---: | ---: |
| 32 B | 5.39M msg/s (1.38 Gb/s) | 8.99M msg/s (2.30 Gb/s) | 0.60× |
| 256 B | 4.26M msg/s (8.73 Gb/s) | 3.76M msg/s (7.70 Gb/s) | 1.13× |
| 1 KB | 2.36M msg/s (19.3 Gb/s) | 2.36M msg/s (19.3 Gb/s) | 1.00× |
| 8 KB | 0.43M msg/s (28.3 Gb/s) | 0.40M msg/s (26.3 Gb/s) | 1.07× |
| 64 KB | 0.09M msg/s (45.7 Gb/s) | 0.11M msg/s (55.9 Gb/s) | 0.82× |

The 32 B gap is scheduler-bound (goroutine wakeups dominate a profile there)
and does not move with allocation tuning. The 64 KB gap was not isolated in
this run; the comparison harness runs gozmq's receiver without
`WithRecvBufferPool`, so the in-repo `PushPullLarge_TCP/pool=true` rows are
the place to see what pooling buys on large frames.

## In-repo benchmarks, cleanup branch vs v0.1.1

Every row passed the gate. Representative rows (sec/op, base → candidate):

| benchmark | v0.1.1 | cleanup | change |
| :--- | ---: | ---: | ---: |
| PushPullThroughput_TCP/32B | 172.9 ns | 171.9 ns | ~ |
| PushPullThroughput_TCP/1KB | 509.2 ns | 510.4 ns | ~ |
| PushPullThroughput_TCP/64KB | 14.57 µs | 14.98 µs | ~ |
| PushPullThroughput_Inproc/32B | 74.4 ns | 68.1 ns | −8.5% |
| PushPullLarge_TCP/pool=false/1MB | 223 µs | 233 µs | ~ (±21%) |
| PushPullLarge_TCP/pool=true/1MB | 73.8 µs | 74.2 µs | ~ |
| ReqRepRoundTrip_TCP/32B | 31.2 µs | 31.4 µs | ~ |
| ReqRepRoundTrip_Inproc/32B | 1.092 µs | 1.067 µs | −2.2% |
| RouterDealerLatency_TCP/32B | 30.1 µs | 30.2 µs | ~ |
| RouterIdlePeers_Inproc/idle=1000 | 1.067 µs | 1.067 µs | ~ |
| PushPullFanIn_TCP/pushers=16 | 85.4 ns | 88.6 ns | +3.7% |
| ConnectHandshake_TCP | 156.7 µs | 156.1 µs | ~ |
| Poller_PollReady (root) | 121.0 ns | 121.7 ns | ~ |
| PushPullCurve_TCP/64B (new) | | 552 ns, 9 allocs | |
| PushPullCurve_TCP/64KB (new) | | 87.5 µs, 9 allocs | |

allocs/op and B/op are unchanged on every row (5 allocs per PUSH/PULL
delivery, 25 per REQ/REP round trip, 16 per ROUTER/DEALER round trip, 255 per
TCP connect and handshake). The inproc rows read a few percent faster, but
the base run had ±10% variance on them where the candidate had ±1%, so the
cause was not isolated and the difference may be run conditions.

## Decisions these numbers rest on

- **Plain memory by default, pool opt-in.** Received frames are ordinary
  allocations; `WithRecvBufferPool` gives large-frame receivers pooled
  buffers with an explicit `Release`. An earlier default-on pool with a
  finalizer safety net was removed after it corrupted retained frames.
- **Bodies above 128 KiB are allocated as they arrive** (the stalled-peer
  bound). Frames up to 256 KiB prove delivery through the 16 KiB buffered
  reader and cost nothing extra; larger ones copy at most 1/16 of the body.
- **Batched writes**: up to 32 queued messages (2 MiB) per `writev`, with
  frame headers appended into a reusable scratch buffer.
- **No goroutine per pipe or engine**; a multi-peer receive walks only the
  pipes with data.
- **Measured non-starters**: a `*Msg` sync.Pool (near-zero hit rate on the
  producer→consumer path), per-message contexts in REQ, and a reactor-style
  shared I/O goroutine (unnecessary at the measured scale).
