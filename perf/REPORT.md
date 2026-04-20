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

## Conclusion

The original performance target for small native messages was met and exceeded:

- target: `>= 2.0M msg/s` for native `gozmq <- gozmq` at `64B`
- latest: `2.261M msg/s`

The biggest remaining gain came from reducing fixed send-path overhead. The
remaining plan work that was not done is the larger reactor rewrite, which was
explicitly deferred.
