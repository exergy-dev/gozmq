# Performance report

Measured 2026-10-23 on the `perf-0.3` branch (v0.3.0 candidate) against
`main` at v0.2.0. Machine: Intel i9-14900KF (32 threads), WSL2 Linux, Go
1.25.3, `GOMAXPROCS=8` for the Go benchmarks. Earlier rounds and the numbers
they produced are in the git history of this file; absolute figures move
with machine load between sessions, so compare ratios and within-run columns,
not tables from different dates.

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
  `GOZMQ_LOCAL_FLAGS=-pool` runs gozmq's receiver with `WithRecvBufferPool`
  and a `Release` per message.

## gozmq vs native libzmq 4.3.4, PUSH/PULL over TCP loopback

Plain memory (the default):

| size | gozmq | libzmq | ratio |
| :--- | ---: | ---: | ---: |
| 32 B | 4.08M msg/s (1.05 Gb/s) | 7.05M msg/s (1.81 Gb/s) | 0.58× |
| 256 B | 3.09M msg/s (6.34 Gb/s) | 2.10M msg/s (4.30 Gb/s) | 1.47× |
| 1 KB | 1.48M msg/s (12.1 Gb/s) | 1.32M msg/s (10.9 Gb/s) | 1.12× |
| 8 KB | 0.28M msg/s (18.5 Gb/s) | 0.27M msg/s (17.8 Gb/s) | 1.04× |
| 64 KB | 0.05M msg/s (25.0 Gb/s) | 0.05M msg/s (28.5 Gb/s) | 0.88× |

Receiver with `WithRecvBufferPool` (`GOZMQ_LOCAL_FLAGS=-pool`):

| size | gozmq | libzmq | ratio |
| :--- | ---: | ---: | ---: |
| 32 B | 4.33M msg/s (1.11 Gb/s) | 7.21M msg/s (1.85 Gb/s) | 0.60× |
| 256 B | 3.13M msg/s (6.41 Gb/s) | 2.22M msg/s (4.55 Gb/s) | 1.41× |
| 1 KB | 1.52M msg/s (12.4 Gb/s) | 1.42M msg/s (11.7 Gb/s) | 1.07× |
| 8 KB | 0.53M msg/s (34.9 Gb/s) | 0.32M msg/s (21.0 Gb/s) | 1.66× |
| 64 KB | 0.11M msg/s (58.5 Gb/s) | 0.06M msg/s (31.6 Gb/s) | 1.85× |

Where the two gaps come from, measured on the in-repo PUSH/PULL rows:

- **32 B** is not syscall-bound: `strace -c` on 1M messages shows about one
  `writev` per 32 messages (the batch cap) and one `read` per 33. A CPU
  profile puts ~25% in `runtime.lock2`/`procyield`/`unlock2` on the pipe's
  channel, i.e. producer and consumer contending for the queue, plus the
  wakeups. The pipes are multi-producer/multi-consumer (concurrent `SendMsg`
  and `RecvMsg` are supported), so a lock-free ring does not fit; a
  deferred-wake experiment is tracked separately.
- **64 KB** is allocation-bound: each frame is a fresh 64 KiB `make`, zeroed
  and then collected. `GOGC=400` alone gives −17% on that row, `GOGC=off` is
  2× *slower* (page faults), and the pool removes the allocation entirely,
  which is the second table.

## In-repo benchmarks, perf-0.3 vs v0.2.0

Every row passed the gate. sec/op, base → candidate (PUB/SUB rows from a
focused rerun of the same gate after the last SubTree change):

| benchmark | v0.2.0 | perf-0.3 | change |
| :--- | ---: | ---: | ---: |
| PushPullThroughput_TCP/32B | 254.4 ns | 258.1 ns | ~ |
| PushPullThroughput_TCP/64B | 269.9 ns | 250.8 ns | ~ (−7%, p=0.09) |
| PushPullThroughput_TCP/1KB | 898.6 ns | 883.6 ns | ~ |
| PushPullThroughput_TCP/64KB | 30.36 µs | 30.33 µs | ~ |
| PushPullThroughput_Inproc/1KB | 103.9 ns | 98.6 ns | −5.1% |
| PushPullLarge_TCP/pool=false/1MB | 401 µs | 388 µs | ~ |
| PushPullLarge_TCP/pool=true/1MB | 148 µs | 143 µs | ~ |
| PushPullCurve_TCP/64B | 721.5 ns | 635.9 ns | −11.9% |
| PushPullCurve_TCP/64KB | 135.8 µs | 99.3 µs | −26.9% |
| ReqRepRoundTrip_TCP/32B | 58.4 µs | 59.1 µs | ~ |
| ReqRepRoundTrip_Inproc/32B | 1.446 µs | 1.415 µs | −2.1% |
| RouterDealerLatency_TCP/32B | 58.9 µs | 56.1 µs | ~ |
| PubSubFanout_Inproc/32B | 290.4 ns | 257.3 ns | −11.4% |
| PubSubFanoutWide_Inproc/subs=64 | 10.64 µs | 9.24 µs | ~ (−13%, p=0.11) |
| XPubXSubLatency_Inproc/32B | 99.95 ns | 81.28 ns | −18.7% |
| XPubXSubThroughput_Inproc/32B | 131.2 ns | 109.2 ns | −16.8% |
| XPubSubscribeChurn_Inproc | 2.218 µs | 2.159 µs | −2.6% |
| PushPullFanIn_TCP/pushers=2 | 215.5 ns | 197.4 ns | −8.4% |
| ConnectHandshake_TCP | 402 µs | 394 µs | ~ |
| Poller_PollReady (root) | 149.0 ns | 150.6 ns | ~ |
| PushPullThroughput_TLS/32B (new) | | 690 ns, 3 allocs | |
| PubSubFanoutSelective_Inproc/subs=64 (new) | | 41.3 µs, 1 alloc | |
| ConnectHandshake_Curve_TCP (new) | | 1.54 ms, 403 allocs | |

allocs/op fell on every path that changed and rose nowhere: 5 → 3 per
PUSH/PULL delivery over TCP (NULL and CURVE alike; CURVE was 8), 25 → 15 per
REQ/REP round trip, 16 → 10 per ROUTER/DEALER round trip, 35 → 34 per
subscription event. B/op moved with the allocations and never up. Of the
original 5 PUSH/PULL allocations, two were the frame reader's one-byte
header arrays escaping through `io.ReadFull`'s interface call; the three
left are the sender's `*Msg`, the receiver's copy of the body and the
receiver's `*Msg`.

## Decisions these numbers rest on

- **Plain memory by default, pool opt-in.** Received frames are ordinary
  allocations; `WithRecvBufferPool` gives large-frame receivers pooled
  buffers with an explicit `Release`. An earlier default-on pool with a
  finalizer safety net was removed after it corrupted retained frames. The
  second libzmq table is what the opt-in buys.
- **Bodies above 128 KiB are allocated as they arrive** (the stalled-peer
  bound). Frames up to 256 KiB prove delivery through the 16 KiB buffered
  reader and cost nothing extra; larger ones copy at most 1/16 of the body.
- **One encode path**: every frame's header is appended to the writer's
  scratch and header and body go to `writev` by reference. A mechanism that
  rewrites bodies either implements `security.FrameAppender` and seals into
  the same scratch (CURVE) or returns fresh memory, which the `Mech` contract
  now requires. Up to 32 queued messages (2 MiB) per `writev`; PING/PONG
  commands open the batch. Over TLS the batch is one record.
- **Subscription matching**: a PUB answers the subscribe-to-all case from an
  atomic flag per peer and walks a selective tree under that peer's read
  lock. A copy-on-write trie was tried and measured: the data path gained the
  same, but every Subscribe cloned its path (+68% allocs on
  XPubSubscribeChurn), so it was dropped. Selective prefixes still cost one
  tree walk per peer per message (the new `PubSubFanoutSelective` row: 64
  peers, 8-byte prefixes, ~650 ns per peer); libzmq's single shared trie that
  yields the matching pipe set is the next step if that path matters.
- **No goroutine per pipe or engine**; a multi-peer receive walks only the
  pipes with data.
- **Measured non-starters**: a `*Msg` sync.Pool (near-zero hit rate on the
  producer→consumer path), per-message contexts in REQ, a reactor-style
  shared I/O goroutine (unnecessary at the measured scale), and the
  copy-on-write subscription trie above.
