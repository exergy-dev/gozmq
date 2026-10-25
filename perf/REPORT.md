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
  HWM of 1000 and allocates per message. Both run at Go's defaults;
  `GOZMQ_GOGC=400` passes `-gogc 400` to the gozmq processes and
  `GOZMQ_LOCAL_FLAGS=-pool` runs the receiver with `WithRecvBufferPool`
  (see the 64 KB note).

## gozmq vs native libzmq 4.3.4, PUSH/PULL over TCP loopback

Go's default settings (plain-memory receive, GOGC 100), three consecutive
runs on the final branch, ratio = gozmq/libzmq:

| size | gozmq | libzmq | ratio (3 runs) |
| :--- | ---: | ---: | ---: |
| 32 B | 9.13–9.34M msg/s (2.4 Gb/s) | 8.56–8.81M msg/s (2.2 Gb/s) | **1.04–1.09×** |
| 256 B | 6.14–6.68M (13 Gb/s) | 3.61–3.72M (7.5 Gb/s) | 1.70–1.81× |
| 1 KB | 2.40–2.49M (20 Gb/s) | 2.25–2.34M (18.7 Gb/s) | 1.05–1.09× |
| 8 KB | 0.38–0.39M (25 Gb/s) | 0.36–0.39M (24 Gb/s) | 1.00–1.08× |
| 64 KB | 0.07M (35 Gb/s) | 0.11–0.12M (56–61 Gb/s) | 0.57–0.64× |

Options for the large-frame case (not defaults), one run each. Receiver
with `WithRecvBufferPool` (`GOZMQ_LOCAL_FLAGS=-pool`, GOGC 100):

| size | gozmq | libzmq | ratio |
| :--- | ---: | ---: | ---: |
| 32 B | 8.90M | 9.04M | 0.98× |
| 256 B | 6.35M | 3.65M | 1.74× |
| 1 KB | 2.47M | 2.21M | 1.12× |
| 8 KB | 0.76M (50 Gb/s) | 0.37M (24 Gb/s) | 2.04× |
| 64 KB | 0.12M (63 Gb/s) | 0.11M (58 Gb/s) | 1.08× |

Plain memory with the collector at GOGC 400 (`GOZMQ_GOGC=400`):

| size | gozmq | libzmq | ratio |
| :--- | ---: | ---: | ---: |
| 32 B | 8.98M | 9.05M | 0.99× |
| 256 B | 8.02M | 3.64M | 2.20× |
| 1 KB | 3.33M | 2.38M | 1.40× |
| 8 KB | 0.50M | 0.37M | 1.33× |
| 64 KB | 0.10M (51 Gb/s) | 0.11M (59 Gb/s) | 0.87× |

(Earlier three-run tables of the options, before the inproc pipe change,
which does not touch TCP: pool 32 B 1.08×, 64 KB 1.38×; GOGC 400 32 B
1.08–1.10×, 64 KB 0.89–0.98×. The 32 B row sits at 1.0–1.1× in every
configuration; single runs wobble ±5%.)

The 64 KB row with plain memory at Go's defaults is GC-paced (next
section); with GOGC 400 it is parity within libzmq's own variance: in
interleaved two-process rounds gozmq is stable at 97–102k msg/s while libzmq
alternates between ~65k and ~100k+ (GOMAXPROCS 2, 4, 8 and 32 make no
difference on the gozmq side), and best-of-N picks libzmq's good mode. The
default-path remainder is the zeroing of each fresh 64 KiB body
(`memclrNoHeapPointers`, 12% of the receiver's profile; `make` zeroes, malloc
does not) and the netpoll park/wake per read that returns EAGAIN, both
runtime-level; the pool removes the first and is what a receiver at that
rate uses.

Before this branch (`main`, same session, interleaved with the branch's
binary): 32 B ran at 3.4–5.3M msg/s, 0.6× of libzmq. The mixed pairings at
32 B are 8.6M (gozmq PUSH → libzmq PULL) and 9.1M (libzmq PUSH → gozmq PULL),
so neither side is the slow one any more.

REQ/REP round trip over TCP, 50,000 round trips, full round trip in µs
(`compare_lat.sh`; libzmq's own `remote_lat` prints one-way = half of this):

| size | gozmq↔gozmq | libzmq↔libzmq | gozmq REQ → libzmq REP | libzmq REQ → gozmq REP |
| :--- | ---: | ---: | ---: | ---: |
| 32 B | 63.4 | 80.2 | 71.7 | 71.6 |
| 1 KB | 63.8 | 80.4 | 72.3 | 74.9 |

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
`WithRecvBufferPool`, which removes the allocation entirely. Both are options (`-pool`, `-gogc`); the headline table runs Go's
defaults.

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

sec/op, base → candidate; every row with p < 0.05 and a change above 2%
is listed, plus the rows the report has always tracked. Rows the gate
flagged were re-run at count 16 and 20 (noted):

| benchmark | v0.3.0 | perf-parity | change |
| :--- | ---: | ---: | ---: |
| PushPullThroughput_TCP/32B | 170.5 ns | 103.8 ns | −39.1% |
| PushPullThroughput_TCP/64B | 176.0 ns | 114.5 ns | −35.0% |
| PushPullThroughput_TCP/1KB | 501.3 ns | 461.6 ns | −7.9% |
| PushPullThroughput_TCP/64KB | 14.9 µs | 16.0 µs | ~ |
| PushPullThroughput_Inproc/32B | 69.9 ns | 79.4 ns | +13.6% at count 10; ~ at count 16 (76.2 → 79.7, p=0.055) |
| PushPullThroughput_TLS/32B | 165.5 ns | 117.7 ns | −28.9% |
| PushPullThroughput_TLS/64B | 182.8 ns | 132.4 ns | −27.5% |
| PushPullCurve_TCP/64B | 469.4 ns | 408.2 ns | −13.1% |
| PushPullFanIn_TCP/pushers=2 | 127.1 ns | 88.2 ns | −30.6% |
| PushPullFanIn_TCP/pushers=16 | 91.6 ns | 72.0 ns | −21.4% |
| PushPullFanIn_Inproc/pushers=2 | 74.5 ns | 78.4 ns | +5.2% at count 10; ~ at count 16 (p=0.80) |
| PushPullLarge_TCP/pool=false/1MB | 223 µs | 251 µs | +12% at count 10; +2.9% at count 16 |
| PushPullLarge_TCP/pool=true/8KB | 1.27 µs | 1.44 µs | +13% at count 10; +6.4% at count 16; +10% at count 20 (see below) |
| ReqRepRoundTrip_TCP/32B | 31.4 µs | 30.3 µs | −3.6% |
| ReqRepRoundTrip_Inproc/32B | 1.062 µs | 1.067 µs | +0.5% |
| RouterDealerLatency_Inproc/32B | 888 ns | 897 ns | +1.0% |
| RouterIdlePeers_Inproc/idle=0 | 899 ns | 908 ns | ~ |
| PubSubFanoutWide_Inproc/subs=64 | 5.39 µs | 5.47 µs | ~ (count 16) |
| XPubXSubThroughput_Inproc/32B | 78 ns | 72 ns | ~ |
| XPubXSubLatency_Inproc/32B | 60.8 ns | 61.3 ns | +0.9% |
| ConnectHandshake_TCP | 169 µs | 163 µs | −4.0% |
| Poller_PollReady (root) | 125 ns | 124 ns | ~ |
| PubSubThroughput_TCP/32B (new) | | 305 ns, 1 alloc | |
| PubSubTopicThroughput_TCP/32B (new) | | 179 ns, 3 allocs | |

**The one open row.** `PushPullLarge_TCP/pool=true/8KB` (8 KiB frames into
the opt-in receive pool, ~1.3 µs per message) reads 6–13% slower than
v0.3.0 across count 10, 16 and 20 runs, with ±11–24% run-to-run spread.
Bisecting it interleaved (ten rounds, five binaries) gives a gradual drift
with no significant step: v0.3.0 1.26 µs, writer+alloc 1.34 (p=0.18), pipe
1.38 (p=0.05), current 1.40 (p=0.06). Two targeted fixes were tried and
reverted: flushing the wake immediately for lent messages (no change) and
capping by-reference bodies at 32 per batch, the old granularity (+20%,
worse). The pooled 64 KB and 1 MB rows and the plain 8 KB row are
unchanged, and the two-process 8 KB comparison with the pool is 2.0× of
libzmq, so this is left as measured.

allocs/op fell on every TCP path (PUSH/PULL 3 → 2, REQ/REP 14 → 10,
ROUTER/DEALER 10 → 8, fan-in 3 → 2, TLS 3 → 2) and rose nowhere; inproc
allocs/op and B/op are identical to v0.3.0. B/op is within 5% on every row
and fell 20% on the handshake rows (no HWM-sized channel buffer per TCP
peer).

**Why inproc pipes stayed on channels.** The two-lock queue costs about
20 ns more than a channel per uncontended hop with a parked receiver (lock,
counter atomics, wake token, then a locked pop instead of the channel's
direct handoff into the waiting goroutine). With the queue on every pipe,
paths that are one such hop per message paid it: inproc REQ/REP and
ROUTER/DEALER round trips +8–9% on ~1 µs, the 64-subscriber inproc fan-out
+26%. Bounded spinning before parking (8, 64, 512 polls) made those rows
worse; a direct-handoff fast path (branch `pp-pipe3`, not merged) recovered
only ~2 of the 8 points and needed a FIFO guard on every receive path. The
inproc patterns have no batching producer to amortise wakeups, so inproc
pairs (`pipe.NewChannelPair`) now keep the previous channel implementation
and session pairs use the queue: the inproc round-trip and fan-out rows are
back at main's numbers, the inproc streaming rows return to main's level
instead of the queue's (XPUB/XSUB inproc throughput 78 → 72 ns rather than
42 ns), and every TCP row keeps its gain. Both kinds run the same pipe test
suite.

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
- **Session pipes are chunked queues, inproc pipes are channels.** The
  engines' `End` API is unchanged; `TrySendDeferred`/`Flush` exist for the
  one single-producer caller that can batch its wakeups (the session
  reader). Concurrent `SendMsg`/`RecvMsg` on one socket stay supported
  through the per-side mutexes (queue kind) or the channel itself.
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
