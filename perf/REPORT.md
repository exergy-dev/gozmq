# Performance report

Measured 2026-10-25 on the `perf-parity` branch against `main` at v0.3.0.
Machine: Intel i9-14900KF (32 threads), WSL2 Linux, Go 1.25.3,
`GOMAXPROCS=8` for the Go benchmarks, machine otherwise idle. Earlier
rounds and the numbers they produced are in the git history of this file;
absolute figures move with machine load between sessions, so compare ratios
and within-run columns, not tables from different dates.

## Method

- **In-repo benchmarks**: `perf/benchgate.sh <base> . 10` builds the `./perf`
  and root test binaries for the base commit and the working tree, runs them
  interleaved ten times each with `-benchmem -benchtime 1s`, and compares with
  benchstat. The pass rule is in the script's header: no significant (p < 0.05)
  sec/op regression above 5%, allocs/op never higher, B/op within 5%.
- **Against native libzmq**: `perf/libzmq_thr/compare_all.sh` builds the
  repo's minimal C `local_thr`/`remote_thr` against the system libzmq and
  runs them against `perf/zmqperf` as two processes over TCP loopback, no
  cgo on either side, best valid run of up to five per cell; it also prints
  the two mixed pairings (gozmq PUSH → libzmq PULL and the reverse).
  `compare_lat.sh` does the same for REQ/REP round trips. gozmq's tool uses a
  1,000,000 message HWM and reuses one payload; libzmq's uses its default
  HWM of 1000 and allocates per message. `zmqperf` runs at GC percent 400
  unless `-gogc` says otherwise (`GOZMQ_GOGC` in the scripts; see the 64 KB
  note); `GOZMQ_LOCAL_FLAGS=-pool` runs the receiver with
  `WithRecvBufferPool`.

## gozmq vs native libzmq 4.3.4, PUSH/PULL over TCP loopback

`zmqperf` defaults (GC percent 400, see below), three consecutive runs,
ratio = gozmq/libzmq:

| size | gozmq | libzmq | ratio (3 runs) |
| :--- | ---: | ---: | ---: |
| 32 B | 9.17–9.69M msg/s (2.4 Gb/s) | 8.35–8.94M msg/s (2.2 Gb/s) | **1.08–1.10×** |
| 256 B | 8.07–8.44M (17 Gb/s) | 3.59–3.77M (7.6 Gb/s) | 2.14–2.35× |
| 1 KB | 3.12–3.32M (26 Gb/s) | 2.24–2.30M (18.5 Gb/s) | 1.36–1.48× |
| 8 KB | 0.51–0.54M (34 Gb/s) | 0.40–0.41M (26.5 Gb/s) | 1.27–1.33× |
| 64 KB | 0.10M (53 Gb/s) | 0.10–0.11M (55–59 Gb/s) | 0.89–0.98× |

Go's default pacing (`GOZMQ_GOGC=100`), three runs:

| size | gozmq | libzmq | ratio (3 runs) |
| :--- | ---: | ---: | ---: |
| 32 B | 9.27–9.71M | 8.61–8.91M | 1.04–1.10× |
| 256 B | 6.73M | 3.70M | 1.82× |
| 1 KB | 2.45–2.50M | 2.25–2.32M | 1.05–1.11× |
| 8 KB | 0.39M | 0.38–0.40M | 0.99–1.01× |
| 64 KB | 0.07M (37 Gb/s) | 0.10–0.12M (50–62 Gb/s) | 0.60–0.73× |

Receiver with `WithRecvBufferPool` (`GOZMQ_LOCAL_FLAGS=-pool`, Go's default
pacing, one run):

| size | gozmq | libzmq | ratio |
| :--- | ---: | ---: | ---: |
| 32 B | 9.60M | 8.86M | 1.08× |
| 256 B | 6.98M | 3.71M | 1.88× |
| 1 KB | 2.49M | 2.27M | 1.10× |
| 8 KB | 0.76M (50 Gb/s) | 0.41M (27 Gb/s) | 1.85× |
| 64 KB | 0.15M (78 Gb/s) | 0.11M (57 Gb/s) | 1.38× |

The 64 KB default-path remainder is the zeroing of each fresh 64 KiB body
(`memclrNoHeapPointers`, 12% of the receiver's profile; `make` zeroes, malloc
does not) plus the copy of the first 16 KiB out of the buffered reader;
the pool removes the first and is what a receiver at that rate uses.

Before this branch (`main`, same session, interleaved with the branch's
binary): 32 B ran at 3.4–5.3M msg/s, 0.6× of libzmq. The mixed pairings at
32 B are 8.6M (gozmq PUSH → libzmq PULL) and 9.1M (libzmq PUSH → gozmq PULL),
so neither side is the slow one any more.

REQ/REP round trip over TCP, 50,000 round trips, full round trip in µs
(`compare_lat.sh`; libzmq's own `remote_lat` prints one-way = half of this):

| size | gozmq↔gozmq | libzmq↔libzmq | gozmq REQ → libzmq REP | libzmq REQ → gozmq REP |
| :--- | ---: | ---: | ---: | ---: |
| 32 B | 61.0 | 76.6 | 69.8 | 70.2 |
| 1 KB | 62.6 | 77.6 | 70.1 | 70.4 |

### The 64 KB row and GC pacing

With Go's default GOGC the 64 KB receiver is GC-bound, and this branch made
that visible: the old pipe allocated a channel of HWM slots per direction up
front, which at zmqperf's 1,000,000 HWM was 8 MB of pointers per direction
and acted as accidental GC ballast (live heap 19 MB → a cycle every ~290
messages). The chunked queue allocates storage as the queue deepens, so the
receiver's live heap is ~3 MB and the pacer runs a cycle every ~40 messages
(1904 cycles per 80,000 messages vs 279; measured with `GODEBUG=gctrace=1`).
The old build at the default HWM of 1000 never had the ballast and runs the
64 KB row at the same speed as this branch, so applications were not
getting that benefit either. A receiver at 64 KB × 70k msg/s either tunes
the collector (GOGC=200 already puts this branch ahead of the old one at
88k vs 82k msg/s; GOGC=400 is within 2–11% of libzmq) or uses
`WithRecvBufferPool`, which removes the allocation entirely. `zmqperf`
therefore runs at GC percent 400 by default (`-gogc` overrides it; the
scripts take `GOZMQ_GOGC`), and the Go-default table above is kept so the
pacing effect stays visible.

## What changed, and where the 32 B time went

Baseline CPU profile of `BenchmarkPushPullThroughput_TCP/32B` on `main`
(166 ns/op, 3 allocs): 31% of samples in `chansend`/`chanrecv` (the pipe's
channel lock, contended between producer and consumer on every message),
12% spinning in `procyield` under that lock, 11% in syscalls (one writev per
32 messages = per ~1 KiB, and the receiver's reads mirrored that size), 12%
in malloc/GC for three allocations per delivery. Three changes, each on its
own branch and gate:

1. **Writer coalescing** (`internal/session/writer.go`, `transport/tcp.go`):
   bodies up to 1 KiB are copied into the writer's scratch right behind
   their header, so consecutive small frames are one contiguous run and one
   iovec; the batch is bounded by copied bytes (64 KiB), iovecs (1000) and
   total bytes (2 MiB) instead of 32 messages; a fully coalesced batch goes
   out as one `write(2)`. Small CURVE frames are rotated in place so sealed
   body and header form one run too. Wire bytes are unchanged. Per 1M × 32 B
   messages the sender went from 31,251 `writev` to 675 `write`, the receiver
   from 19,702 `read` to 2,100. Single-message latency is untouched: the
   batch only ever coalesces messages already queued.
2. **One allocation per small received message** (`msg.FromFramesCopy`,
   `internal/session/reader.go`): the reader keeps a message's frames in its
   4 KiB scratch until the last frame arrives, then copies them all into one
   allocation sized to the same Go size class as the old `*Msg` plus body.
   PUSH/PULL over TCP: 3 → 2 allocations per delivery; REQ/REP 14 → 10 per
   round trip; ROUTER/DEALER 10 → 8; a two-frame topic+payload message 6 → 3.
3. **Chunked pipe queue with deferred wakes** (`internal/pipe`): each pipe
   direction is a 256-slot chunked FIFO (libzmq's `yqueue` shape) with a
   producer-side and a consumer-side mutex, so producers never share a lock
   with consumers; published counters are the only cross-side traffic. The
   session reader publishes with `TrySendDeferred` and `Flush`es once per
   read batch (before every blocking read, every 256 messages, before it
   would block on HWM, and on exit), so a receiver is woken once per batch
   instead of once per message. HWM is now a count: storage grows with the
   actual depth and a ring of eight spare chunks recycles it without
   allocating.

An uncontended push+pop on the new queue is ~35 ns against ~26 ns for the
channel; that is the trade-off below. (The branch history keeps the round-1
commit "back direction with the chunked queue (kill criteria NOT met)": that
was the queue without deferred wakes, superseded by the commits after it.)

## In-repo benchmarks, perf-parity vs v0.3.0 (count 10, idle machine)

sec/op, base → candidate; every row with p < 0.05 is listed, plus the rows
the report has always tracked:

| benchmark | v0.3.0 | perf-parity | change |
| :--- | ---: | ---: | ---: |
| PushPullThroughput_TCP/32B | 165.9 ns | 101.1 ns | −39.0% |
| PushPullThroughput_TCP/64B | 171.5 ns | 112.3 ns | −34.5% |
| PushPullThroughput_TCP/1KB | 496.7 ns | 456.3 ns | −8.1% |
| PushPullThroughput_TCP/64KB | 14.94 µs | 16.04 µs | ~ (p=0.12) |
| PushPullThroughput_Inproc/32B | 68.7 ns | 55.9 ns | −18.7% |
| PushPullThroughput_TLS/32B | 162.5 ns | 116.3 ns | −28.4% |
| PushPullThroughput_TLS/64KB | 107.4 µs | 102.2 µs | −4.9% |
| PushPullCurve_TCP/64B | 459.3 ns | 403.4 ns | −12.2% |
| PushPullFanIn_TCP/pushers=2 | 125.5 ns | 85.9 ns | −31.6% |
| PushPullFanIn_TCP/pushers=16 | 92.0 ns | 73.7 ns | −19.8% |
| PushPullFanIn_Inproc/pushers=2 | 73.3 ns | 43.2 ns | −41.0% |
| XPubXSubThroughput_Inproc/32B | 79.3 ns | 42.1 ns | −47.0% |
| XPubXSubLatency_Inproc/32B | 59.6 ns | 61.2 ns | +2.7% |
| ReqRepRoundTrip_TCP/32B | 29.8 µs | 29.4 µs | ~ |
| ReqRepRoundTrip_Inproc/32B | 1.040 µs | 1.123 µs | **+7.9%** |
| RouterDealerLatency_Inproc/32B | 868.9 ns | 947.6 ns | **+9.1%** |
| RouterDealerLatency_TCP/1KB | 30.6 µs | 31.6 µs | +3.2% |
| RouterIdlePeers_Inproc/idle=0 | 881 ns | 970 ns | **+10.0%** |
| PubSubFanout_Inproc/32B | 174.9 ns | 198.4 ns | ~ (p=1.0) |
| PubSubFanoutWide_Inproc/subs=64 | 5.17 µs | 6.49 µs | **+25.6%** |
| PubSubFanoutSelective_Inproc/subs=64 | 26.2 µs | 27.3 µs | +4.1% |
| PushPullLarge_TCP/pool=true/8KB | 1.23 µs | 1.42 µs | +15% (p=0.04; 6-run re-check ~, p=0.39) |
| ConnectHandshake_TCP | 162.5 µs | 154.4 µs | −5.0% |
| Poller_PollReady (root) | 125.5 ns | 124.2 ns | ~ |
| PubSubThroughput_TCP/32B (new) | | 287.7 ns, 1 alloc | |
| PubSubTopicThroughput_TCP/32B (new) | | 176.6 ns, 3 allocs | |

allocs/op fell on every TCP path (PUSH/PULL 3 → 2, REQ/REP 14 → 10,
ROUTER/DEALER 10 → 8, fan-in 3 → 2, TLS 3 → 2) and rose nowhere. B/op is
within 5% on every row except the PUB fan-out inproc rows (64 → 69 B,
+7.8%: the queue's chunk turnover under 3 subscribers), and fell 20% on the
handshake rows (no HWM-sized channel buffer per peer).

**The trade-off, stated plainly.** The two-lock queue costs about 20 ns more
than a channel per uncontended hop with a parked receiver (lock, counter
atomics, wake token, then a locked pop instead of the channel's direct
handoff into the waiting goroutine). Paths that are one such hop per message
pay it: inproc REQ/REP and ROUTER/DEALER round trips (+8–9% on ~1 µs), the
wide PUB fan-out (64 parked subscribers, +26%). Paths with any contention or
batching gain 20–50%. Bounded spinning before parking (8, 64 and 512 polls)
made the latency rows worse and was dropped. A direct-handoff fast path
(deliver through the wake token when the queue is empty and a consumer is
parked) would close this gap and is the next step if those rows matter;
reverting the single merge commit of the pipe branch gives the previous
pipe with the writer and allocation changes kept (32 B then at ~0.75×).

## Decisions these numbers rest on

- **Plain memory by default, pool opt-in.** Received frames are ordinary
  allocations; `WithRecvBufferPool` gives large-frame receivers pooled
  buffers with an explicit `Release`. An earlier default-on pool with a
  finalizer safety net was removed after it corrupted retained frames.
- **Bodies above 128 KiB are allocated as they arrive** (the stalled-peer
  bound). Frames up to 256 KiB prove delivery through the 16 KiB buffered
  reader and cost nothing extra; larger ones copy at most 1/16 of the body.
  The read buffer stays 16 KiB: bufio reads straight into the caller's
  buffer only when the request is at least the buffer size, so a 64 KiB
  buffer would copy more of a 64 KiB body, not less.
- **One encode path**: every frame's header is appended to the writer's
  scratch; bodies up to 1 KiB follow it there, larger bodies go to `writev`
  by reference. A mechanism that rewrites bodies either implements
  `security.FrameAppender` and seals into the same scratch (CURVE) or returns
  fresh memory, which the `Mech` contract requires. A batch is bounded by
  64 KiB copied, 1000 iovecs, 2 MiB total and 4096 messages; PING/PONG
  commands open it. Over TLS a batch is one `Write`, which crypto/tls splits
  into records.
- **Pipes are chunked queues, HWM is a count.** The engines' `End` API is
  unchanged; `TrySendDeferred`/`Flush` exist for the one single-producer
  caller that can batch its wakeups (the session reader). Concurrent
  `SendMsg`/`RecvMsg` on one socket stay supported through the per-side
  mutexes.
- **Subscription matching**: a PUB answers the subscribe-to-all case from an
  atomic flag per peer and walks a selective tree under that peer's read
  lock. A copy-on-write trie was tried and dropped (+68% allocs per
  subscription event).
- **No goroutine per pipe or engine**; a multi-peer receive walks only the
  pipes with data.
- **Measured non-starters**: a `*Msg` sync.Pool (near-zero hit rate on the
  producer→consumer path), per-message contexts in REQ, a reactor-style
  shared I/O goroutine, the copy-on-write subscription trie, a lock-free
  queue *without* deferred wakes (round 1 of this branch: the per-message
  wake token cost what the channel lock had cost; +68% on the pipe stream
  microbenchmark, no TCP gain), and spinning before parking.
