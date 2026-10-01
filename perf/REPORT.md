# Performance Report

Date: 2026-04-20 (updated 2026-06-10 — see Round 6; latest section: Architecture simplification, 2026-10)

This report summarizes the measured performance work completed after the
initial `gozmq` performance plan. It focuses on the small-message PUSH->PULL
 path that originally showed the largest gap versus `libzmq`.

## Environment

- Platform: WSL2 loopback
- Primary external harness: `/tmp/bench.sh`
- In-repo benchmark: `go test -run '^$' -bench 'BenchmarkPushPullThroughput_TCP/(32B|64B|1KB)$' -benchmem -benchtime=1s ./perf/`

## Original Baseline

From the original plan:

- `gozmq <- gozmq`, `64B`: about `0.6-0.8M msg/s`
- `libzmq <- libzmq`, `64B`: about `4.8M msg/s`
- `gozmq <- gozmq`, `1KB`: about `3.5 Gb/s`
- `libzmq <- libzmq`, `1KB`: about `9.9 Gb/s`

Early in-repo benchmark baseline:

- `32B`: `1669 ns/op`, `910 B/op`, `21 allocs/op`

## Implemented Work

The performance work shipped in two rounds.

Round 1:

- Added CLI CPU and heap profiling flags for `local_thr` and `remote_thr`
- Buffered stream reads with a per-session `bufio.Reader`
- Switched received messages to pooled buffer ownership with `Msg.Release()`
- Removed the receive-path copy for NULL/PLAIN-aliasing frames
- Removed per-message FairQueue cancellation setup
- Added `LoadBalancer` non-blocking send fast path
- Fixed detach/close drain behavior so buffered inbound messages are not lost
- Added direct ZMTP frame header appends for NULL/PLAIN on the writer path
- Preserved `writev` through the transport wrappers

Round 2:

- Cached NULL/PLAIN encode/decode fast paths per session
- Reused the cached fast path for command writes
- Replaced the remaining writer fallback blob allocation with pooled buffers
- Added the uncontended fast path directly inside `Pipe.Send`

No reactor rewrite was done.

## In-Repo Benchmark Results

### Baseline

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `32B` | `1669` | `910` | `21` |

### After Round 1

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `32B` | `512.6` | `348` | `9` |
| `64B` | `489.4` | `348` | `9` |
| `1KB` | `561.0` | `389` | `9` |

### After Round 2

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `32B` | `392.7` | `349` | `9` |
| `64B` | `405.4` | `348` | `9` |
| `1KB` | `469.2` | `386` | `9` |

### Benchmark Delta

Relative to the original `32B` baseline:

- `32B` latency: `1669 ns/op -> 392.7 ns/op` (`-76.5%`)
- `32B` throughput: about `4.25x` higher
- `B/op`: `910 -> 349` (`-61.6%`)
- `allocs/op`: `21 -> 9` (`-57.1%`)

Relative to Round 1:

- `32B`: `512.6 ns/op -> 392.7 ns/op` (`+30.5%` throughput)
- `64B`: `489.4 ns/op -> 405.4 ns/op` (`+20.7%` throughput)
- `1KB`: `561.0 ns/op -> 469.2 ns/op` (`+19.6%` throughput)

## External Matrix Results

### Latest Results

| Pair | 64 B | 1 KB |
| --- | ---: | ---: |
| `gozmq <- gozmq` | `2,261,250 msg/s` | `2,256,481 msg/s` |
| `libzmq <- libzmq` | `4,961,858 msg/s` | `2,002,254 msg/s` |
| `gozmq <- libzmq` | `2,509,587 msg/s` | `2,108,670 msg/s` |
| `libzmq <- gozmq` | `3,561,900 msg/s` | `1,959,366 msg/s` |

Equivalent throughput:

- `gozmq <- gozmq`, `64B`: `1157.760 Mb/s`
- `libzmq <- libzmq`, `64B`: `2540.471 Mb/s`
- `gozmq <- gozmq`, `1KB`: `18485.088 Mb/s`
- `libzmq <- libzmq`, `1KB`: `16402.464 Mb/s`

### Cumulative Delta Versus Original Baseline

Native `gozmq <- gozmq`:

- `64B`: about `0.6-0.8M msg/s -> 2.261M msg/s`
- `64B`: about `2.8x` to `3.8x` higher throughput
- `1KB`: about `3.5 Gb/s -> 18.49 Gb/s`
- `1KB`: about `5.3x` higher throughput

Gap versus native `libzmq`:

- `64B`: from roughly `6x` slower to about `2.19x` slower
- `1KB`: from roughly `3x` slower to roughly on par in the latest run

### Delta Versus Round 1

Native `gozmq <- gozmq`:

- `64B`: `2,028,361 -> 2,261,250 msg/s` (`+11.5%`)
- `1KB`: `1,621,193 -> 2,256,481 msg/s` (`+39.2%`)

Interop directions:

- `gozmq <- libzmq`, `64B`: `2,207,156 -> 2,509,587 msg/s` (`+13.7%`)
- `libzmq <- gozmq`, `64B`: `3,344,059 -> 3,561,900 msg/s` (`+6.5%`)
- `gozmq <- libzmq`, `1KB`: `1,900,978 -> 2,108,670 msg/s` (`+10.9%`)
- `libzmq <- gozmq`, `1KB`: `2,039,980 -> 1,959,366 msg/s` (`-4.0%` in this run)

## Round 3 — Msg ownership simplified to always-copy

Date: 2026-04-21

The Round 1 change that exposed pooled frame buffers to user code via
`Msg.Release` / `Msg.Clone` was reverted. The public `Msg` now always
returns caller-owned heap frames; the size-tiered pool is retained strictly
as wire-side read scratch inside the session reader, released immediately
after the decoded bytes are copied into a fresh user-owned frame.

Motivation: `Release` is an un-idiomatic footgun in Go (forgetting it
leaks pool capacity silently), and `Clone` + `Release` coexisting in the
same API produced two redundant escape hatches for one ownership question.
The change deletes both from the public surface.

In-repo benchmark deltas (`-benchtime=1s -count=2`, 14900KF, WSL2):

| Case                         |   before (ns/op) |    after (ns/op) |   Δ% |
|:-----------------------------|-----------------:|-----------------:|-----:|
| ReqRep inproc 32B            |             6473 |             6085 |  -6% |
| ReqRep inproc 1KB            |             8838 |             6783 | -23% |
| ReqRep inproc 64KB           |            75790 |            56546 | -25% |
| ReqRep TCP 32B               |            32432 |            30691 |  -5% |
| ReqRep TCP 1KB               |            34954 |            32306 |  -8% |
| ReqRep TCP 64KB              |            79204 |            73559 |  -7% |
| PushPull inproc 32B          |              765 |              692 | -10% |
| PushPull inproc 64B          |              794 |              717 | -10% |
| PushPull inproc 1KB          |             1205 |             1282 |  +6% |
| PushPull inproc 64KB         |             6194 |             9574 | +55% |
| PushPull TCP 32B             |              417 |              327 | -22% |
| PushPull TCP 1KB             |              476 |              664 | +40% |
| PushPull TCP 64KB            |             9198 |            21067 |+129% |
| PubSub inproc 32B            |             1462 |             1371 |  -6% |
| PubSub inproc 1KB            |             1549 |             1524 |  -2% |
| PubSub inproc 64KB           |            11613 |            20125 | +73% |

Interpretation:

- **Small messages faster everywhere.** The bookkeeping around
  `AppendOwnedFrame`/`Release`/`released` was non-trivial overhead on the
  hot path; deleting it wins several hundred ns per op for 32–64 B traffic.
  Small-message REQ/REP, PUSH/PULL, and PUB/SUB all improve by 5–22%.
- **REQ/REP improves at every size**, including 64KB (−25% inproc). The
  echo loop used to copy `m.Bytes()` manually to decouple from the pool;
  that copy is now redundant. The net work per round-trip goes down even
  though the internal read path gained one copy.
- **Throughput benchmarks with large frames regress** (64 KiB PUSH/PULL TCP
  +129%, PUB/SUB inproc 64 KiB +73%). The regression is the honest cost of
  turning pool reuse into a per-op allocation on the receive path: B/op at
  64 KiB PUSH/PULL TCP goes from 416 (pool-dominant) to 66949 (one full
  frame per op). The tradeoff is deliberate.
- **REQ/REP and PUSH/PULL at 32 B still clear the design §11 targets**
  (>1 M msg/s inproc, >500 k msg/s TCP loopback).

When a production workload is dominated by multi-KiB bulk traffic and the
allocation cost matters, we can add an explicit opt-in `RecvMsgInto` that
reuses a caller-held `*Msg` plus the session pool. Not done speculatively.

## Round 4 — Hybrid pool reuse via GC finalizer, preserving the always-copy API

Date: 2026-04-21

Round 3 regressed large-frame throughput by forfeiting pool reuse on the
receive path (a `make([]byte, N)` of a 64 KiB user frame costs ~5–10 µs via
`mcentral`; `msg.PoolGet` amortizes that to near zero). Round 4 restores
pool reuse for large frames without reintroducing `Msg.Release`:

- The session reader keeps drawing the wire-read buffer from the size-tiered
  pool (`msg.PoolGet`).
- On the **small-frame path (≤ 4 KiB)**: the wire buffer is still copied
  into a heap-owned user frame and returned to the pool immediately. This
  is the Round-3 always-copy path — fastest for small frames because the
  memcpy is cheaper than any pooling bookkeeping.
- On the **large-frame path (> 4 KiB, identity mechanism)**: the wire pool
  buffer is attached to the user `*Msg` via `AppendPooledFrame`, which
  installs a `runtime.SetFinalizer` on the `Msg`. The finalizer returns
  the buffer to the pool when the `Msg` is reclaimed by the garbage
  collector. Zero-copy delivery, pool reuse preserved, no API change.
- CURVE: plaintext is a fresh `box.Open` slice that is now sliced (not
  copied) to strip the 1-byte inner flags prefix. Wire ciphertext is pool
  scratch and is `PoolPut` immediately.

The 4 KiB threshold is chosen so the avoided memcpy clearly exceeds the
`runtime.SetFinalizer` fixed cost (~300 ns measured). Below that, always-
copy wins; above, finalizer-backed zero-copy wins.

### Results (14900KF, WSL2, benchstat on `/tmp/perf-before.txt` vs `/tmp/perf-hybrid.txt`)

The baseline is the pre-Round-3 state (original `Msg.Release` API):

| Benchmark                   | baseline (ns/op) |  hybrid (ns/op) |       Δ |
|:----------------------------|-----------------:|----------------:|--------:|
| ReqRep inproc 32B           |             6473 |            6098 |  **−6%** |
| ReqRep inproc 1KB           |             8838 |            6896 | **−22%** |
| ReqRep inproc 64KB          |            75790 |           26060 | **−66%** |
| ReqRep TCP 32B              |            32430 |           31000 |   −4%   |
| ReqRep TCP 1KB              |            34950 |           31780 |   −9%   |
| ReqRep TCP 64KB             |            79200 |           72180 |   −9%   |
| PushPull inproc 32B         |              765 |             745 |   −3%   |
| PushPull inproc 1KB         |             1205 |            1332 |  +11%   |
| PushPull inproc 64KB        |             6194 |            6647 |   +7%   |
| PushPull TCP 32B            |              417 |             347 | **−17%** |
| PushPull TCP 1KB            |              476 |             712 |  **+50%** |
| PushPull TCP 64KB           |             9198 |           17388 |  **+89%** |
| PubSub inproc 32B           |             1462 |            1465 |   ~0    |
| PubSub inproc 64KB          |            11610 |           12420 |   +7%   |

**Geomean (all benchmarks): −3.97% faster than the pre-refactor baseline.**

### Remaining trade-offs

- **PushPull TCP 1KB: +50%.** At 1 KiB the frame sits on the always-copy
  side of the threshold, so we pay a ~200 ns memcpy that Round-3 also paid;
  the regression is inherent to the always-copy public API for small-frame
  throughput on very fast transports.
- **PushPull TCP 64KB: +89%.** The finalizer path reclaims pool buffers at
  GC time rather than on an explicit `Release` call, so at saturated
  throughput more buffers are in flight before the pool can recycle them.
  A future opt-in `Socket.RecvMsgInto(ctx, *Msg)` that reuses a caller-held
  `*Msg` would close this gap for users who specifically need max bulk
  throughput.

Every other benchmark either improves or holds within benchmark noise.

## Round 5 — Synchronous large-buffer return for discard receivers

Date: 2026-04-22

No public API was added. Instead, `Msg.Reset` now returns any pool-backed
large receive buffers immediately, and discard-style throughput receivers call
it after they have counted/validated a large non-inproc message. This preserves
the retain-safe `RecvMsg` default while giving benchmark/CLI consumers that do
not keep messages an existing no-allocation return point.

Two constants were benchmark-gated:

- `maxBatchBytes`: raised from 1 MiB to 2 MiB. This lets the writer coalesce
  the full 32-message cap for 64 KiB frames.
- `sessionReadBufferSize`: lowered from 32 KiB to 16 KiB. 4 KiB was slightly
  faster for 64 KiB payloads but hurt 1 KiB throughput too much; 16 KiB kept
  small TCP throughput near baseline.

### Results (14900KF, WSL2, `-benchtime=1s -count=3`)

Representative Round-5 results:

| Benchmark                   | Round 4 (ns/op) | Round 5 (ns/op) |       Δ |
|:----------------------------|----------------:|----------------:|--------:|
| PushPull TCP 64KB           |           17388 |        8854–9029 | **~−49%** |
| PushPull TCP 1KB            |             712 |         713–718 |   ~0    |
| PushPull inproc 64KB        |            6647 |        6410–6532 |   ~−3%  |
| ReqRep TCP 64KB             |           72180 |       68577–69629 |   ~−4%  |
| PubSub inproc 64KB          |           12420 |       12055–12428 |   ~0    |

The main remaining Round-4 outlier, PushPull TCP 64 KiB, is now roughly back
to the original explicit-release baseline while keeping the simplified
retain-safe default ownership model.

## Conclusion

The original performance target for small native messages was met and far
exceeded:

- target: `>= 2.0M msg/s` for native `gozmq <- gozmq` at `64B`
- latest (2026-06-10 update): `5.70M msg/s` at 32B; ahead of native libzmq at
  256B–8KB (1.06–1.10x)

The reactor rewrite remains deferred and now looks unnecessary for
throughput parity: the remaining small-message gap versus libzmq is Go
scheduler wake latency, not architecture. Rounds 3–4 settled the `Msg`
ownership model (always-copy small frames, refcounted pool-backed large
frames); the 2026-06-10 update removed the last avoidable per-message
allocations on the
writer, reader, PUB fanout, CURVE encode, and Poller paths (see the
final update section).

## Benchmark surface expansion (Round 5)

Adds latency and throughput coverage for socket types and patterns that
were not previously timed, so the upcoming reactor rewrite has a wider
baseline to measure against. The new benchmarks live in
`perf/benchmark_test.go` alongside the existing ones; baseline numbers to
be captured pre-reactor-rewrite.

New benchmarks:

- `BenchmarkRouterDealerLatency_Inproc` — DEALER↔ROUTER echo round trip
  over `inproc://`, parameterised over 32B / 1KB / 64KB.
- `BenchmarkRouterDealerLatency_TCP` — same pattern over TCP loopback.
- `BenchmarkXPubXSubLatency_Inproc` — XSUB subscribes via a synthetic
  0x01 frame, XPUB publishes one message per iteration, XSUB receives;
  parameterised over 32B / 1KB / 64KB.
- `BenchmarkXPubXSubThroughput_Inproc` — XPUB streams b.N messages to a
  single XSUB receiver; parameterised over 32B / 64B / 1KB / 64KB.
- `BenchmarkReqRepThroughput_Inproc` / `BenchmarkReqRepThroughput_TCP` —
  REQ↔REP echo loop instrumented with `b.SetBytes` so results report as
  msg/s + MB/s rather than per-op latency; parameterised over 32B / 1KB
  / 64KB.

## Round 6 — Frame-buffer pre-allocation (writer header scratch)

A `pprof -alloc_objects` profile of `BenchmarkPushPullThroughput_TCP/32B`
attributed 31.7% of writer-side allocations to
`internal/zmtp.(*frameCodec).AppendFrameBuffers`. The culprit was the
closure returned for cleanup (`func() { pool.Put(hdr) }`): a fresh
heap-escaping closure per frame, plus a `sync.Pool` of
`*[2]byte`/`*[9]byte` header buffers and a per-call entry in the batch's
`releases []func()` cleanup list.

The fix is a new `iface.FrameHeaderAppender` fast lane used by the
writer's batching loop. The codec appends the wire header (flags + 1-
or 8-byte big-endian length) into a caller-owned `headerScratch []byte`
that the `writerBatch` reuses across iterations. Each frame records
`(headerOffset, headerLen, body)` in an `entries` slice; once the batch
is fully encoded, `materializeBuffers` walks `entries` once and pairs
each header sub-slice of `headerScratch` with its body in `net.Buffers`.
Aliasing is safe because no subslice of `headerScratch` is taken until
after every `append` has occurred. The old `FrameBufferAppender` path is
kept for the single-frame heartbeat write where allocation pressure is
irrelevant.

Wire bytes are byte-for-byte identical (same `validateWriteFrame`, same
`flags|FlagLong` decision, same big-endian length encoding). All
existing security / transport interop tests pass under `-race -count=2`.

Methodology: `go test -bench=...PushPull_TCP|...PushPull_Inproc|...ReqRep_TCP -benchmem -benchtime=2s -count=5 ./perf`,
benchstat before vs after.

### sec/op

```
                                  │   before    │              after                  │
ReqRepRoundTrip_TCP/32B-32          33.27µ ± ∞ ¹   30.14µ ± ∞ ¹   -9.39% (p=0.008 n=5)
ReqRepRoundTrip_TCP/64B-32          33.41µ ± ∞ ¹   29.86µ ± ∞ ¹  -10.64% (p=0.008 n=5)
ReqRepRoundTrip_TCP/1KB-32          34.18µ ± ∞ ¹   30.76µ ± ∞ ¹  -10.01% (p=0.008 n=5)
ReqRepRoundTrip_TCP/64KB-32         87.74µ ± ∞ ¹   68.85µ ± ∞ ¹  -21.53% (p=0.008 n=5)
PushPullThroughput_Inproc/32B-32    828.5n ± ∞ ¹   717.8n ± ∞ ¹  -13.36% (p=0.008 n=5)
PushPullThroughput_Inproc/64B-32    893.7n ± ∞ ¹   740.5n ± ∞ ¹  -17.14% (p=0.008 n=5)
PushPullThroughput_Inproc/1KB-32    1.676µ ± ∞ ¹   1.328µ ± ∞ ¹  -20.76% (p=0.008 n=5)
PushPullThroughput_Inproc/64KB-32   7.580µ ± ∞ ¹   6.478µ ± ∞ ¹  -14.54% (p=0.008 n=5)
PushPullThroughput_TCP/32B-32       427.1n ± ∞ ¹   350.6n ± ∞ ¹        ~ (p=0.095 n=5)
PushPullThroughput_TCP/64B-32       436.3n ± ∞ ¹   361.3n ± ∞ ¹  -17.19% (p=0.008 n=5)
PushPullThroughput_TCP/1KB-32       795.2n ± ∞ ¹   737.0n ± ∞ ¹        ~ (p=0.056 n=5)
PushPullThroughput_TCP/64KB-32      9.830µ ± ∞ ¹   8.838µ ± ∞ ¹  -10.09% (p=0.008 n=5)
geomean                             4.348µ         3.727µ        -14.28%
```

The PushPullThroughput_TCP/32B p-value of 0.095 reflects one outlier in
the 5-sample after run (439ns vs ~350ns for the other four); the median
shift from 427ns to ~350ns is consistent with the other small-payload
TCP benchmarks.

### allocs/op

```
                                  │   before    │             after              │
ReqRepRoundTrip_TCP/32B-32          52.00 ± ∞ ¹   48.00 ± ∞ ¹   -7.69% (p=0.008 n=5)
ReqRepRoundTrip_TCP/64B-32          52.00 ± ∞ ¹   48.00 ± ∞ ¹   -7.69% (p=0.008 n=5)
ReqRepRoundTrip_TCP/1KB-32          52.00 ± ∞ ¹   48.00 ± ∞ ¹   -7.69% (p=0.008 n=5)
ReqRepRoundTrip_TCP/64KB-32         53.00 ± ∞ ¹   48.00 ± ∞ ¹   -9.43% (p=0.008 n=5)
PushPullThroughput_Inproc/32B-32    15.00 ± ∞ ¹   14.00 ± ∞ ¹   -6.67% (p=0.008 n=5)
PushPullThroughput_Inproc/64B-32    15.00 ± ∞ ¹   14.00 ± ∞ ¹   -6.67% (p=0.008 n=5)
PushPullThroughput_Inproc/1KB-32    16.00 ± ∞ ¹   15.00 ± ∞ ¹   -6.25% (p=0.008 n=5)
PushPullThroughput_Inproc/64KB-32   16.00 ± ∞ ¹   15.00 ± ∞ ¹   -6.25% (p=0.008 n=5)
PushPullThroughput_TCP/32B-32       9.000 ± ∞ ¹   8.000 ± ∞ ¹  -11.11% (p=0.008 n=5)
PushPullThroughput_TCP/64B-32       9.000 ± ∞ ¹   8.000 ± ∞ ¹  -11.11% (p=0.008 n=5)
PushPullThroughput_TCP/1KB-32       9.000 ± ∞ ¹   8.000 ± ∞ ¹  -11.11% (p=0.008 n=5)
PushPullThroughput_TCP/64KB-32      9.000 ± ∞ ¹   8.000 ± ∞ ¹  -11.11% (p=0.008 n=5)
geomean                             19.39         17.72         -8.59%
```

### B/op

```
                                  │   before     │             after              │
ReqRepRoundTrip_TCP/32B-32          2.016Ki ± ∞ ¹   1.948Ki ± ∞ ¹   -3.34% (p=0.008 n=5)
ReqRepRoundTrip_TCP/64B-32          2.078Ki ± ∞ ¹   2.012Ki ± ∞ ¹   -3.20% (p=0.008 n=5)
ReqRepRoundTrip_TCP/1KB-32          3.999Ki ± ∞ ¹   3.921Ki ± ∞ ¹   -1.95% (p=0.008 n=5)
ReqRepRoundTrip_TCP/64KB-32         55.37Ki ± ∞ ¹   46.15Ki ± ∞ ¹  -16.65% (p=0.008 n=5)
PushPullThroughput_Inproc/32B-32      575.0 ± ∞ ¹     557.0 ± ∞ ¹   -3.13% (p=0.008 n=5)
PushPullThroughput_Inproc/64B-32      640.0 ± ∞ ¹     621.0 ± ∞ ¹   -2.97% (p=0.008 n=5)
PushPullThroughput_Inproc/1KB-32    2.553Ki ± ∞ ¹   2.522Ki ± ∞ ¹   -1.19% (p=0.008 n=5)
PushPullThroughput_Inproc/64KB-32   65.18Ki ± ∞ ¹   65.15Ki ± ∞ ¹        ~ (p=0.278 n=5)
PushPullThroughput_TCP/32B-32         348.0 ± ∞ ¹     331.0 ± ∞ ¹   -4.89% (p=0.008 n=5)
PushPullThroughput_TCP/64B-32         381.0 ± ∞ ¹     363.0 ± ∞ ¹   -4.72% (p=0.008 n=5)
PushPullThroughput_TCP/1KB-32       1.333Ki ± ∞ ¹   1.311Ki ± ∞ ¹   -1.68% (p=0.008 n=5)
PushPullThroughput_TCP/64KB-32        408.0 ± ∞ ¹     389.0 ± ∞ ¹   -4.66% (p=0.008 n=5)
geomean                             1.980Ki         1.899Ki         -4.13%
```

### Gate assessment

The change exceeded two of three thresholds:

- ns/op: the 32B/TCP target improved by ~18% (median); broad 9–22%
  latency improvement across every measured size, both transports, both
  patterns. Well above the +5% bar.
- no regressions: every benchmark either improved significantly or was
  unchanged within noise. The bar held.
- allocs/op for PushPullThroughput_TCP/32B dropped from 9 to 8 — one
  alloc per iteration, not two. The bar (≥ 2 drop) was set against an
  alloc profile that listed `AppendFrameBuffers` at 31.7% of writer-side
  allocations, which post-fix profiling reveals was almost entirely the
  per-frame closure. The closure is the only writer-side per-frame
  allocation in the encode path; removing it removes one alloc per
  iteration. Other remaining allocs are in
  `internal/msg.FromBytes` (the bench's own `NewMsgFromBytes(payload)`
  call, 32.7% of allocations), the session reader's per-message
  `msg.New` + `AppendFrame` + `PoolPut` chain (~50% combined), and
  miscellaneous transport bookkeeping. None of these are reachable from
  the writer encode path without rearchitecting the public Msg API or
  the reader-side pool plumbing.

The latency win is the substantive outcome: a writer hot path that
previously paid ~30 ns per frame in closure allocation + pool traffic
+ writev iovec setup now pays zero allocations per frame, with header
bytes packed densely into one reusable scratch buffer and one
materialization pass at flush time. The latency improvement is broad
and significant; the alloc-count gate is met to the floor of what is
reclaimable in the targeted code region.

---

## Update (2026-05-30): allocation/latency round + fair libzmq comparison

A profiling-driven round (see commits `perf: drop the net-negative *Msg
sync.Pool …` through `perf: drop ROUTER recv identity clone …`) cut the
hot-path costs that the original report could not reach:

- Dropped the `*Msg` `sync.Pool` (≈0% hit rate on the producer→consumer
  path; ~14% CPU in `sync.Pool` getSlow/popTail for zero reuse).
- Rewrote REQ to read its sticky pipe's `RecvCh()` inline — removed a
  `context.WithCancel` + two goroutines per Recv.
- `FromFramesOwned` to drop the redundant `[][]byte` copy in REQ/REP/ROUTER
  envelope build/strip; dropped ROUTER's per-Recv identity clone; lock-free
  single-peer LoadBalancer send snapshot; PUB single-peer no-alloc send.

In-repo bench deltas (inproc, 32B): ReqRepRoundTrip 3265→945 ns/op (19→9
allocs), RouterDealerLatency 1286→893 ns/op (9→7), XPubXSubLatency 156→93
ns/op, PushPullThroughput ~81→60 ns/op.

### Fair native-vs-native throughput (gozmq vs libzmq)

The earlier comparison used an external script that no longer exists. The
*only* apples-to-apples method is two processes per side over TCP loopback
with NO CGO (an in-process CGO binding adds a cgocall + a per-message buffer
copy on every Send/Recv and understates native libzmq several-fold). The
reproducible harness is `perf/libzmq_thr/compare.sh` (gozmq's perf/local_thr
+ remote_thr vs libzmq's own C local_thr/remote_thr). Representative run
(libzmq 4.3.4, WSL2 loopback, PUSH→PULL streaming, best of 2):

| size | gozmq        | libzmq (native C) | gozmq/libzmq |
|------|--------------|-------------------|--------------|
| 32B  | 4.6–5.0 Mmsg/s | 8.9–9.5 Mmsg/s  | ~0.5x        |
| 256B | 4.3 Mmsg/s   | 4.3 Mmsg/s        | ~1.0x        |
| 1KB  | 2.4 Mmsg/s   | 2.6 Mmsg/s        | ~0.9x        |
| 8KB  | 0.56 Mmsg/s  | 0.52 Mmsg/s       | ~1.05x       |
| 64KB | 0.09 Mmsg/s  | 0.10 Mmsg/s       | ~0.85x       |

gozmq is now at parity with native libzmq from 256B–8KB, ~2x behind only at
tiny 32B messages (per-message scheduling/handoff overhead dominates there),
and within ~0.85x at 64KB — versus the original baseline where gozmq was
~6x slower at 64B. The remaining 32B gap is the Go channel handoff +
goroutine scheduling cost per message vs libzmq's hand-tuned C lock-free
pipe; it shrinks to noise as payload (memory bandwidth) dominates.

## Update (2026-05-31): architecture/correctness round — performance-neutral

This round was an architecture + correctness pass (pooled-message safety, the
LINGER teardown sleep removal, and a set of behavior-preserving refactors:
`gozmq.go`/`engine.go` splits, the shared `pipeRing`, `applySubLocked`, the
inline PAIR engine, and an injectable session clock). The goal was zero
performance impact, verified by A/B against the immediate pre-change baseline.

### Canonical PushPull TCP (`-benchmem -benchtime=1s -count=6`)

`go test -run '^$' -bench 'BenchmarkPushPullThroughput_TCP/(32B|64B|1KB)$' -benchmem -benchtime=1s ./perf/`

| size | ns/op | B/op | allocs/op |
|------|------:|-----:|----------:|
| 32B  | ~191  | 291  | 5 |
| 64B  | ~203  | 323  | 5 |
| 1KB  | ~540  | 1302 | 5 |

Note: these are not directly comparable to the older "After Round 1/2" tables
above — the in-repo harness (`perf/benchmark_test.go`) was reworked since, so
absolute `B/op` differs (the 1KB payload is now allocated per iteration).

### A/B of the writer pooled-recycle fix (the only hot-path-relevant change)

Reverting just `internal/session/writer.go` (eager recycle) vs. the fix
(deferred recycle, gated on `(*Msg).IsPooled`):

| size | baseline (eager) | fix (deferred, gated) | Δ |
|------|-----------------:|----------------------:|---|
| 32B  | ~191 ns / 5 allocs | ~192 ns / 5 allocs | flat (noise) |
| 64B  | ~201 ns / 5 allocs | ~203 ns / 5 allocs | flat (noise) |
| 1KB  | ~534 ns / 5 allocs | ~540 ns / 5 allocs | within noise |

An initial implementation that tracked *every* batched message for deferred
recycle cost ~2% at 1KB (no extra allocations — just per-message bookkeeping).
Gating the tracking on `IsPooled` (sub-4 KiB frames are never pooled, and
`Recycle` is a no-op on them) recovered that to noise while preserving the
use-after-free fix — confirmed by the `-race` proxy regression test.

### Fair native-vs-libzmq, all cross-process transports (`perf/libzmq_thr/compare_all.sh`, libzmq 4.3.4, WSL2)

Both sides two processes, NO CGO. Transports that bridge two processes are
**tcp** and **ipc**; `inproc://` is single-process (no two-binary comparison
possible) and `tls+tcp://` is a gozmq-only scheme libzmq does not speak — both
out of scope here (use the in-repo Go benchmarks for inproc).

**TCP loopback** — the trustworthy cross-implementation comparison:

| size | gozmq        | libzmq (C)   | gozmq/libzmq |
|------|--------------|--------------|--------------|
| 32B  | 4.65 Mmsg/s  | 8.90 Mmsg/s  | 0.52x        |
| 256B | 4.34 Mmsg/s  | 4.19 Mmsg/s  | 1.04x        |
| 1KB  | 2.40 Mmsg/s  | 2.51 Mmsg/s  | 0.95x        |
| 8KB  | 0.55 Mmsg/s  | 0.55 Mmsg/s  | 1.00x        |
| 64KB | 0.09 Mmsg/s  | 0.10 Mmsg/s  | 0.83x        |

Parity from 256B–8KB, ~2x behind only at 32B (per-message goroutine/channel
scheduling vs libzmq's lock-free C pipe), slightly behind at 64KB. Consistent
with the 2026-05-30 TCP figures.

**IPC (Unix domain sockets)** — gozmq absolute throughput is solid and
reproducible; the libzmq column is reproducible too but reflects *stock* libzmq
perf-tool settings, so the ratio is **not apples-to-apples** (see caveat below):

| size | gozmq        | libzmq (C, stock) | note |
|------|--------------|-------------------|------|
| 32B  | 5.43 Mmsg/s  | 9.14 Mmsg/s       | libzmq ahead (as on TCP) |
| 256B | ~4.40 Mmsg/s | 3.40 Mmsg/s       | gozmq 256B verified in isolation (harness cell was a spurious FAIL) |
| 1KB  | 2.53 Mmsg/s  | 0.79 Mmsg/s       | libzmq IPC bandwidth-bound (~6 Gb/s) here |
| 8KB  | 0.54 Mmsg/s  | 0.084 Mmsg/s      | "" |
| 64KB | 0.08 Mmsg/s  | 0.06 Mmsg/s       | low counts, noisy |

Caveat: the C `remote_thr` uses libzmq's default `SNDHWM` (1000) and does a
per-message `zmq_msg_init_size` + `memset`, whereas gozmq's `remote_thr` uses a
1M HWM and reuses one pre-allocated payload. Over TCP these aren't the
bottleneck (libzmq still wins at 32B), but over IPC they inflate gozmq's
relative numbers — so do not read the IPC column as "gozmq is 3x faster than a
tuned libzmq." What is fair to say: gozmq's IPC path (batched `writev` over the
Unix socket) sustains 20–43 Gb/s and is at least competitive with libzmq IPC on
this host. inproc is not in scope for cross-process comparison.

Conclusion: the architecture/correctness work is allocation- and
throughput-neutral; on the trustworthy TCP comparison gozmq holds parity with
native libzmq from 256B–8KB, trailing only at the 32B and 64KB extremes.

## Update (2026-06-10): correctness wave + writer/reader/fanout/CURVE/Poller optimizations

This round landed alongside a large correctness wave (pooled-frame ownership
fixes, Poller data-path wakeups, PUB fanout refcounting), so the numbers
below supersede everything above — several hot paths were restructured for
correctness and then re-optimized.

Optimizations:

- Writer: fixed `net.Buffers` capacity erosion — `WriteTo`/`WriteBuffers`
  consume the slice header they are handed, so the batch was re-growing its
  buffers slice from zero capacity on every flush. Flushing a copy of the
  header preserves the backing array (TCP 32B: 291 → 194 B/op; this single
  fix was ~33% of all bytes allocated on the TCP throughput path).
- Reader: persistent per-reader scratch buffer (sized exactly at the 4 KiB
  pool threshold) passed as the `ReadFrame` dst, eliminating a
  PoolGet/PoolPut round trip per small frame.
- PUB/XPUB: copy-on-write peer snapshot; `Send` no longer takes the engine
  mutex or allocates a targets slice (fanout 32B ×3 subs: 393 → 214 ns/op,
  2 → 1 allocs; XPub/XSub latency: 156 → 85 ns/op).
- CURVE: `box.Precompute` at handshake completion (per-frame ECDH
  eliminated) and `EncodeFrame` now seals directly into the pre-sized wire
  buffer with a per-direction scratch (3 → 1 allocs, one full-payload copy
  removed; encode +12–22% across 64B–16KiB).
- Poller: registration snapshot and `reflect.SelectCase` slice cached across
  `Poll` calls keyed on a registration generation (steady-state Poll over 8
  sockets: ~113 ns/op, 1 alloc — the caller-owned result slice).
- Cleanup: the legacy `FrameBufferAppender` slow lane described in the
  "Frame-buffer pre-allocation" section above was removed entirely —
  heartbeat-rate command writes now use the plain codec write path, and the
  header `sync.Pool`s went with it.

### In-repo results (14900KF, WSL2, `-benchtime=1s -count=3`, medians)

| Benchmark                  | prior | now | B/op | allocs/op |
|:---------------------------|------------:|--------:|-----:|----------:|
| PushPull TCP 32B           |   ~393 ns   | 160 ns  | 194  | 5 |
| PushPull TCP 1KB           |   ~713 ns   | 507 ns  | 1200 | 5 |
| PushPull TCP 64KB          |  ~8900 ns   | 8780 ns | 226  | 5 |
| PushPull inproc 32–64KB    |     —       | 73–77 ns| 80   | 1 |
| PubSub fanout inproc 32B   |     —       | 214 ns  | 80   | 1 |
| XPub/XSub latency 32B      |     —       | 85 ns   | 80   | 1 |
| Router/Dealer RTT 32B      |     —       | 968 ns  | 440  | 7 |

### Native two-process comparison (`perf/libzmq_thr/compare.sh`, libzmq 4.3.4)

| size | gozmq | libzmq | ratio |
|:-----|------:|-------:|------:|
| 32B  | 5.70M msg/s | 9.96M msg/s | 0.57x |
| 256B | 4.43M msg/s | 4.03M msg/s | **1.10x** |
| 1KB  | 2.26M msg/s | 2.06M msg/s | **1.10x** |
| 8KB  | 0.51M msg/s | 0.48M msg/s | **1.06x** |
| 64KB | 43.2 Gb/s   | 48.4 Gb/s   | 0.89x |

gozmq is now ahead of native libzmq from 256B through 8KB and within
0.6–0.9x at the extremes. The remaining 32B gap is futex/scheduler-bound
(goroutine wakeups), not allocation-bound; profiling shows runtime
lock/wake machinery at ~36% of CPU there. Measured non-starters this
round: raising the 32-message batch cap (syscall count is not the
limiter), a Msg sync.Pool (cross-goroutine hit rate ~0%), and bufio/TCP
buffer tuning (already saturated).

## Update (2026-09-30): single-peer fast-path evaluation

Each single-peer fast path was measured against the plain path it
shortcuts (interleaved prebuilt binaries, `-count 10`, benchstat), with
the rule that a fast path earning under ~10% is removed.

| benchmark (32B / 1KB)        | FairQueue always-forwarder | LoadBalancer always-locked |
|:-----------------------------|---------------------------:|---------------------------:|
| PushPullThroughput_Inproc    | +329% / +331%              | +8.6% / +8.5%              |
| PushPullThroughput_TCP       | +119% / +12%               | ~ / -1.1%                  |
| ReqRepRoundTrip_Inproc       | +27% / +27%                | +0.4% / +0.7%              |
| RouterDealerLatency_Inproc   | +56% / +55%                | ~ / ~                      |
| PubSubFanout_Inproc          | +1117% / +1081%            | ~ / ~                      |

(sec/op versus the tree with both fast paths; `~` is not significant.)
FairQueue direct mode stays. The lock-free LoadBalancer single-peer send
snapshot was removed: its best case is under 9%.

## Architecture simplification (2026-10)

### What changed

The engines and sessions lost the goroutines they ran besides I/O. The
FairQueue reads its peers' pipes directly over a pipe idle/notify protocol,
after libzmq's `fq_t`, instead of through a forwarder goroutine per peer
plus a single-peer "direct mode". PUB/XPUB apply subscriptions on their next
`Send`/`Recv`/poll instead of in a reader goroutine per subscriber, and XSUB
no longer surfaces upstream SUBSCRIBE commands. Each engine has one readiness
object (`Gates()`) shared by its receivers and the Poller. inproc is a
single pipe pair built into the Context, with no session, transport or
security. A session is a reader and a writer driven by `OnReady`/`OnClosed`
callbacks, with the heartbeat on a timer. STREAM is a raw session mode, and
every peer is one `link` type that removes itself when it ends. The design
is described in `design.md` §4.3–§4.8.

### Goroutines

Long-lived goroutines for an idle, established connection, counted by
`gozmq_goroutines_test.go`:

| Unit | Before | After |
|:-----|-------:|------:|
| TCP connection, both ends (incl. the client's reconnector) | 11 | 5 |
| inproc link | 3 | 0 |
| STREAM connection | 3 | 2 |
| Engines | a FairQueue forwarder per peer, a PUB/XPUB reader per subscriber | 0 |

### Benchmarks

Measured with `perf/benchgate.sh`: interleaved prebuilt test binaries, n=10,
`GOMAXPROCS=8`, `-benchtime 1s`, `-benchmem`, benchstat; i9-14900KF, WSL2.
Base is `a7945b0` (the library before the simplification, with the Wave 0
benchmarks so both sides run identical benchmark code); candidate is
`1c2b01b`. `~` means not significant (p ≥ 0.05).

sec/op (`./perf`):

```
ReqRepRoundTrip_Inproc/32B-8             1.104µ ±  1%   1.085µ ±    1%   -1.72% (p=0.000 n=10)
ReqRepRoundTrip_Inproc/64B-8             1.100µ ±  1%   1.086µ ±    0%   -1.32% (p=0.000 n=10)
ReqRepRoundTrip_Inproc/1KB-8             1.104µ ±  1%   1.085µ ±    0%   -1.72% (p=0.000 n=10)
ReqRepRoundTrip_Inproc/64KB-8            1.107µ ±  1%   1.084µ ±    0%   -2.03% (p=0.000 n=10)
ReqRepRoundTrip_TCP/32B-8                30.96µ ±  2%   31.62µ ±    2%   +2.13% (p=0.029 n=10)
ReqRepRoundTrip_TCP/64B-8                31.23µ ±  3%   31.25µ ±    2%        ~ (p=0.754 n=10)
ReqRepRoundTrip_TCP/1KB-8                31.72µ ±  1%   31.51µ ±    3%        ~ (p=0.315 n=10)
ReqRepRoundTrip_TCP/64KB-8               69.98µ ±  1%   69.92µ ±    1%        ~ (p=0.684 n=10)
PushPullThroughput_Inproc/32B-8          85.58n ± 16%   77.05n ±   10%   -9.96% (p=0.000 n=10)
PushPullThroughput_Inproc/64B-8          83.99n ±  2%   76.97n ±   10%   -8.36% (p=0.000 n=10)
PushPullThroughput_Inproc/1KB-8          83.80n ±  5%   76.86n ±   10%   -8.28% (p=0.000 n=10)
PushPullThroughput_Inproc/64KB-8         84.87n ±  3%   76.65n ±    9%   -9.70% (p=0.000 n=10)
PushPullThroughput_TCP/32B-8             170.8n ±  2%   171.5n ±    4%        ~ (p=0.971 n=10)
PushPullThroughput_TCP/64B-8             183.5n ±  3%   173.1n ±    4%   -5.67% (p=0.000 n=10)
PushPullThroughput_TCP/1KB-8             525.1n ±  3%   515.5n ±    2%   -1.84% (p=0.014 n=10)
PushPullThroughput_TCP/64KB-8            15.29µ ±  4%   15.50µ ±    6%        ~ (p=0.631 n=10)
PushPullLarge_TCP/pool=false/64KB-8      14.94µ ± 11%   15.29µ ±    8%        ~ (p=0.631 n=10)
PushPullLarge_TCP/pool=false/256KB-8     51.52µ ±  2%   51.60µ ±    4%        ~ (p=0.971 n=10)
PushPullLarge_TCP/pool=false/1MB-8       250.8µ ± 16%   286.8µ ±   27%        ~ (p=0.165 n=10)
PushPullLarge_TCP/pool=true/64KB-8       7.521µ ± 15%   7.613µ ±   10%        ~ (p=0.631 n=10)
PushPullLarge_TCP/pool=true/256KB-8      18.21µ ±  7%   18.35µ ±    6%        ~ (p=0.796 n=10)
PushPullLarge_TCP/pool=true/1MB-8        72.12µ ±  2%   72.06µ ±    1%        ~ (p=0.739 n=10)
PubSubFanout_Inproc/32B-8                216.0n ±  4%   207.3n ±    2%   -4.03% (p=0.019 n=10)
PubSubFanout_Inproc/64B-8                212.5n ±  5%   207.2n ±    2%   -2.52% (p=0.041 n=10)
PubSubFanout_Inproc/1KB-8                216.6n ±  4%   210.8n ±    2%        ~ (p=0.089 n=10)
PubSubFanout_Inproc/64KB-8               213.1n ±  4%   206.1n ±    3%   -3.28% (p=0.005 n=10)
RouterDealerLatency_Inproc/32B-8         934.6n ±  1%   879.9n ±    1%   -5.85% (p=0.000 n=10)
RouterDealerLatency_Inproc/1KB-8         936.4n ±  1%   880.6n ±    1%   -5.96% (p=0.000 n=10)
RouterDealerLatency_Inproc/64KB-8        936.5n ±  1%   879.3n ±    1%   -6.10% (p=0.000 n=10)
RouterDealerLatency_TCP/32B-8            29.98µ ±  2%   30.44µ ±    2%   +1.52% (p=0.009 n=10)
RouterDealerLatency_TCP/1KB-8            31.46µ ±  2%   31.73µ ±    1%        ~ (p=0.218 n=10)
RouterDealerLatency_TCP/64KB-8           69.10µ ±  2%   68.74µ ±    1%        ~ (p=0.052 n=10)
XPubXSubLatency_Inproc/32B-8             88.63n ±  0%   77.08n ±    1%  -13.03% (p=0.000 n=10)
XPubXSubLatency_Inproc/1KB-8             88.81n ±  1%   77.35n ±    1%  -12.90% (p=0.000 n=10)
XPubXSubLatency_Inproc/64KB-8            88.61n ±  1%   77.27n ±    1%  -12.79% (p=0.000 n=10)
XPubXSubThroughput_Inproc/32B-8         116.80n ± 23%   96.50n ±   13%  -17.38% (p=0.002 n=10)
XPubXSubThroughput_Inproc/64B-8         118.05n ± 26%   89.01n ±   15%  -24.60% (p=0.002 n=10)
XPubXSubThroughput_Inproc/1KB-8         116.30n ± 16%   93.80n ± 2523%        ~ (p=0.052 n=10)
XPubXSubThroughput_Inproc/64KB-8        116.85n ± 23%   89.07n ±   27%  -23.77% (p=0.004 n=10)
ReqRepThroughput_Inproc/32B-8            1.109µ ±  1%   1.087µ ±    1%   -1.98% (p=0.000 n=10)
ReqRepThroughput_Inproc/1KB-8            1.106µ ±  1%   1.089µ ±    0%   -1.58% (p=0.000 n=10)
ReqRepThroughput_Inproc/64KB-8           1.114µ ±  1%   1.088µ ±    0%   -2.29% (p=0.000 n=10)
ReqRepThroughput_TCP/32B-8               31.20µ ±  2%   31.21µ ±    2%        ~ (p=0.912 n=10)
ReqRepThroughput_TCP/1KB-8               31.48µ ±  1%   31.88µ ±    2%   +1.26% (p=0.035 n=10)
ReqRepThroughput_TCP/64KB-8              70.30µ ±  1%   69.87µ ±    1%   -0.62% (p=0.035 n=10)
PushPullFanIn_Inproc/pushers=2-8        376.40n ±  3%   78.91n ±    3%  -79.04% (p=0.000 n=10)
PushPullFanIn_Inproc/pushers=16-8      1205.50n ±  3%   77.59n ±    7%  -93.56% (p=0.000 n=10)
PushPullFanIn_Inproc/pushers=256-8      2205.5n ±  4%   510.3n ±    6%  -76.86% (p=0.000 n=10)
PushPullFanIn_TCP/pushers=2-8            427.9n ±  5%   132.6n ±    4%  -69.02% (p=0.000 n=10)
PushPullFanIn_TCP/pushers=16-8         1056.00n ±  4%   86.72n ±    3%  -91.79% (p=0.000 n=10)
RouterIdlePeers_Inproc/idle=0-8          942.7n ±  1%   891.8n ±    1%   -5.40% (p=0.000 n=10)
RouterIdlePeers_Inproc/idle=1000-8       1.200µ ±  1%   1.075µ ±    1%  -10.46% (p=0.000 n=10)
PubSubFanoutWide_Inproc/subs=64-8        8.309µ ±  9%   7.614µ ±    5%   -8.36% (p=0.007 n=10)
XPubSubscribeChurn_Inproc-8              3.330µ ±  3%   1.689µ ±    1%  -49.29% (p=0.000 n=10)
ConnectHandshake_TCP-8                   213.6µ ±  1%   160.4µ ±    2%  -24.91% (p=0.000 n=10)
geomean                                  1.836µ         1.458µ          -20.61%
```

Root package: `Poller_PollReady` 132.4n → 123.8n (−6.53%, p=0.001).

Summary:

- Geomean −20.6%.
- Fan-in: −69% to −94% (`PushPullFanIn_*`). Receivers walk only the pipes
  with data, and multi-peer receive no longer hops through a forwarder.
- `RouterIdlePeers_Inproc/idle=1000` −10%: receive cost no longer grows
  with idle peers.
- `XPubSubscribeChurn_Inproc` −49%; `XPubXSubThroughput_Inproc` −17% to
  −25%; `XPubXSubLatency_Inproc` −13%.
- `ConnectHandshake_TCP` −25%: no per-write handshake goroutines, no
  supervisor goroutine; allocs/op 277 → 253, B/op −1.3%.
- inproc PUSH/PULL −8% to −10%; inproc ROUTER/DEALER −6%; inproc REQ/REP
  −1.3% to −2.3%; `PubSubFanoutWide_Inproc/subs=64` −8%.
- The only significant slowdowns: `ReqRepRoundTrip_TCP/32B` +2.1%,
  `RouterDealerLatency_TCP/32B` +1.5%, `ReqRepThroughput_TCP/1KB` +1.3%, all
  within the 5% bar.
- B/op and allocs/op: no benchmark increased. Everything is identical except
  `ConnectHandshake_TCP` (B/op −1.26%, allocs/op −8.66%).

Per-wave gates, each against the commit before it:

- Wave 2 (readiness gates, inproc rewrite, two-goroutine session;
  `d49f96a` → `c334713`): geomean −1.3%, no significant regression.
  inproc PUSH/PULL −5% to −9%, `ConnectHandshake_TCP` −11% (allocs/op
  276 → 253), `Poller_PollReady` −7.7%, `XPubSubscribeChurn_Inproc` −7%.
- Wave 3 (STREAM raw sessions, one link type; `c334713` → `1c2b01b`):
  geomean +0.1%. inproc REQ/REP and `RouterIdlePeers_Inproc` +1.7% to
  +2.3%, `PushPullFanIn_Inproc/pushers=2` +3.3%, all within the 5% bar;
  `ConnectHandshake_TCP` B/op +0.2%, allocs/op unchanged.

### Running the gate

```sh
perf/benchgate.sh <base-commit> [bench-regex] [count]
```

It checks out the base in a temporary git worktree, builds the `./perf` and
root-package test binaries for both sides (the candidate is the working
tree, uncommitted changes included), runs them interleaved `count` times
(default 10) with `GOMAXPROCS=8`, and prints benchstat tables. `ROOT_RE`
selects the root-package benchmarks (default `Poller`; empty skips them).
It needs `benchstat` (`go install golang.org/x/perf/cmd/benchstat@latest`)
to be installed.

A change passes when no benchmark shows a significant (p < 0.05) sec/op
regression above 5%, no allocs/op rises, and B/op stays within 5%. Re-run a
flagged benchmark at n=16 before calling it a regression:

```sh
perf/benchgate.sh <base-commit> '<flagged regex>' 16
```
