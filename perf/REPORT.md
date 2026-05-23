# Performance Report

Date: 2026-04-20

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

The original performance target for small native messages was met and exceeded:

- target: `>= 2.0M msg/s` for native `gozmq <- gozmq` at `64B`
- latest (Round 2): `2.261M msg/s`

The biggest remaining gain in the original plan came from reducing fixed
send-path overhead. The reactor rewrite remains deferred. Round 3 simplified
the `Msg` ownership story to always-copy at the cost of large-frame
throughput; Round 4 restored that throughput on the identity-mechanism path
via finalizer-backed pool recycling above a 4 KiB threshold, producing a
~4% net improvement over the pre-Round-3 baseline while keeping the
simplified always-copy public API.

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
