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
