# Performance report

Measured 2026-10-25 on the `perf-parity` branch against `main` at v0.3.0.
Machine: Intel i9-14900KF (32 threads), WSL2 Linux, Go 1.25.3, libzmq
4.3.4, `GOMAXPROCS=8` for the Go benchmarks, machine otherwise idle.
Earlier rounds and the numbers they produced are in the git history of this
file; absolute figures move with machine load between sessions, so compare
ratios and within-run columns, not tables from different dates.

Everything below runs at Go's default runtime settings: no `GOGC`, no
memory limit, no `GOMAXPROCS` change for the two-process tools. The one
library default that changed on this branch is that large received frames
are pooled (`WithRecvBufferPool`, "Decisions" below).

## Method

- **In-repo benchmarks**: `perf/benchgate.sh <base> . 10` builds the `./perf`
  and root test binaries for the base commit and the working tree, runs them
  interleaved ten times each with `-benchmem -benchtime 1s`, and compares with
  benchstat. The pass rule is in the script's header: no significant (p < 0.05)
  sec/op regression above 5%, allocs/op never higher, B/op within 5%. The
  suite covers every pattern over every transport it supports (the table in
  `perf/README.md`), small and large frames, CURVE, fan-in and fan-out, and
  the pipe microbenchmarks run both pipe kinds.
- **Against native libzmq**: `perf/libzmq_thr/compare.sh` runs `perf/zmqperf`
  and its libzmq twin `perf/libzmq_thr/zmqperf.c` (same commands, flags,
  patterns and output) as two processes over tcp and ipc, as one process
  each over inproc, and gozmq alone over tls, for every socket pattern, with
  NULL and CURVE, plus the mixed pairings; no cgo on either side. Both tools
  set the same HWM (1,000,000) on every socket, send one reused payload and
  release every message; a run that lost messages is retried, each cell is
  the best valid run of up to three. `perf/run_all.sh` runs it after the
  in-repo benchmarks; "The full matrix" below is its first complete run.
  The PUSH/PULL and REQ/REP sections after it were measured earlier the
  same day with the pre-matrix scripts (`compare_all.sh`, `compare_lat.sh`,
  `compare_inproc.sh`, in git history up to 996df67), whose C tools kept
  libzmq's stock HWM of 1000 and allocated a message per send, against
  gozmq's 1,000,000.

## The full matrix (2026-10-26)

`perf/run_all.sh` at 5f3f295's tree (perf-matrix), first complete run, idle
machine: 1,160 cells, none failed. Ratio = gozmq/libzmq with each library on
both sides; throughput higher is better, round trip lower is better. Raw
msg/s, the mixed pairings and every process's output are in
`perf/results/run-full-20261025/` on the measuring machine (gitignored);
`compare.sh` reprints any slice of it.

**Read the 256 B–8 KB throughput ratios with care.** In this matrix libzmq's
PUSH/PULL at 1 KB runs at 1.0M msg/s, against 2.3M in the pre-matrix
comparison the same day; the main difference is the libzmq sender's HWM
(1,000,000 here, its stock 1000 there). The mixed pairings put the cost on
the libzmq sender (gozmq PUSH to libzmq PULL 2.35M, libzmq PUSH to gozmq
PULL 0.88M at 1 KB). The cause is not yet verified; until a `C_HWM=1000`
rerun settles it, treat those ratios as "same settings on both sides", not
"libzmq at its best". The 32 B, 64 KB and latency rows match the pre-matrix
figures. The PUB/XPUB cells run at HWM 0 on both sides (`PUB_HWM`), because
a 1.5M-message run otherwise drops at a 1M HWM.

#### tcp, NULL: throughput

| pattern | 32 B | 256 B | 1 KB | 8 KB | 64 KB |
| :--- | ---: | ---: | ---: | ---: | ---: |
| push_pull | 1.48× | 2.25× | 2.53× | 3.76× | 2.12× |
| pub_sub | 1.53× | 2.86× | 2.68× | 3.54× | 1.63× |
| pub_sub_prefix | 1.13× | 2.79× | 2.86× | 3.67× | 1.42× |
| pub_sub_fan4 | 1.43× | 7.57× | 10.83× | 22.67× | 10.24× |
| xpub_xsub | 1.67× | 2.69× | 2.78× | 3.47× | 1.47× |
| xpub_xsub_fan4 | 1.24× | 6.91× | 9.69× | 23.32× | 10.60× |
| pair | 1.45× | 2.33× | 2.64× | 3.53× | 1.63× |
| dealer_router | 1.02× | 2.13× | 2.54× | 3.52× | 1.40× |
| router_dealer | 1.09× | 2.32× | 2.42× | 3.62× | 1.52× |
| stream_in | 0.91× | 1.10× | 1.03× | 1.13× | 1.07× |
| stream_out | 0.03× | 0.16× | 0.48× | 1.91× | 1.71× |

#### tcp, NULL: round trip

| pattern | 32 B | 1 KB | 64 KB |
| :--- | ---: | ---: | ---: |
| req_rep | 0.78× | 0.80× | 0.68× |
| req_router | 0.79× | 0.80× | 0.68× |
| dealer_rep | 0.79× | 0.80× | 0.72× |
| dealer_router | 0.80× | 0.80× | 0.67× |
| pair | 0.80× | 0.80× | 0.67× |

#### ipc, NULL: throughput

| pattern | 32 B | 256 B | 1 KB | 8 KB | 64 KB |
| :--- | ---: | ---: | ---: | ---: | ---: |
| push_pull | 1.34× | 3.85× | 5.20× | 7.45× | 1.86× |
| pub_sub | 1.49× | 3.92× | 4.97× | 7.29× | 1.89× |
| pub_sub_prefix | 1.31× | 4.13× | 5.24× | 7.84× | 1.88× |
| pub_sub_fan4 | 1.58× | 5.26× | 6.73× | 10.83× | 4.39× |
| xpub_xsub | 1.38× | 4.09× | 5.42× | 7.37× | 1.84× |
| xpub_xsub_fan4 | 1.92× | 5.24× | 7.03× | 11.11× | 4.39× |
| pair | 1.41× | 4.03× | 5.22× | 7.66× | 1.86× |
| dealer_router | 1.19× | 3.72× | 5.12× | 7.61× | 1.89× |
| router_dealer | 1.16× | 3.55× | 4.69× | 7.46× | 1.83× |

#### ipc, NULL: round trip

| pattern | 32 B | 1 KB | 64 KB |
| :--- | ---: | ---: | ---: |
| req_rep | 0.92× | 0.88× | 0.82× |
| req_router | 0.91× | 0.90× | 0.82× |
| dealer_rep | 0.90× | 0.89× | 0.82× |
| dealer_router | 0.92× | 0.91× | 0.80× |
| pair | 0.91× | 0.90× | 0.80× |

#### inproc, NULL: throughput

| pattern | 32 B | 256 B | 1 KB | 8 KB | 64 KB |
| :--- | ---: | ---: | ---: | ---: | ---: |
| push_pull | 1.33× | 3.72× | 4.82× | 10.43× | 39.27× |
| pub_sub | 2.78× | 8.93× | 10.04× | 12.78× | 44.55× |
| pub_sub_prefix | 1.43× | 4.40× | 5.80× | 7.69× | 22.18× |
| pub_sub_fan4 | 129.78× | 178.52× | 177.31× | 234.78× | 241.86× |
| xpub_xsub | 2.82× | 8.19× | 9.72× | 18.12× | 75.19× |
| xpub_xsub_fan4 | 134.69× | 151.21× | 168.56× | 208.64× | 212.38× |
| pair | 1.16× | 3.46× | 4.48× | 5.71× | 35.73× |
| dealer_router | 0.70× | 3.23× | 4.98× | 5.91× | 22.87× |
| router_dealer | 1.00× | 3.00× | 3.23× | 5.14× | 20.94× |

#### inproc, NULL: round trip

| pattern | 32 B | 1 KB | 64 KB |
| :--- | ---: | ---: | ---: |
| req_rep | 0.05× | 0.05× | 0.05× |
| req_router | 0.05× | 0.05× | 0.07× |
| dealer_rep | 0.04× | 0.04× | 0.05× |
| dealer_router | 0.04× | 0.04× | 0.04× |
| pair | 0.04× | 0.03× | 0.03× |

#### tcp, CURVE: throughput

| pattern | 32 B | 256 B | 1 KB | 8 KB | 64 KB |
| :--- | ---: | ---: | ---: | ---: | ---: |
| push_pull | 1.27× | 1.67× | 1.78× | 1.42× | 0.63× |
| pub_sub | 1.27× | 1.81× | 1.79× | 1.39× | 0.64× |
| pub_sub_prefix | 1.29× | 1.87× | 1.75× | 1.38× | 0.65× |
| pub_sub_fan4 | 3.85× | 5.00× | 5.59× | 5.39× | 2.68× |
| xpub_xsub | 1.26× | 1.80× | 1.77× | 1.35× | 0.64× |
| xpub_xsub_fan4 | 3.81× | 5.02× | 5.55× | 5.45× | 2.69× |
| pair | 1.28× | 1.66× | 1.77× | 1.39× | 0.65× |
| dealer_router | 1.24× | 1.65× | 1.72× | 1.36× | 0.64× |
| router_dealer | 1.25× | 1.68× | 1.73× | 1.39× | 0.64× |

#### tcp, CURVE: round trip

| pattern | 32 B | 1 KB | 64 KB |
| :--- | ---: | ---: | ---: |
| req_rep | 0.79× | 0.78× | 1.37× |
| req_router | 0.79× | 0.79× | 1.40× |
| dealer_rep | 0.78× | 0.79× | 1.39× |
| dealer_router | 0.79× | 0.80× | 1.36× |
| pair | 0.79× | 0.78× | 1.30× |

#### ipc, CURVE: throughput

| pattern | 32 B | 256 B | 1 KB | 8 KB | 64 KB |
| :--- | ---: | ---: | ---: | ---: | ---: |
| push_pull | 1.39× | 2.02× | 1.50× | 1.17× | 0.45× |
| pub_sub | 1.38× | 2.16× | 1.54× | 1.17× | 0.44× |
| pub_sub_prefix | 1.40× | 2.21× | 1.48× | 1.17× | 0.44× |
| pub_sub_fan4 | 3.38× | 4.18× | 3.22× | 2.30× | 1.25× |
| xpub_xsub | 1.39× | 2.16× | 1.53× | 1.18× | 0.44× |
| xpub_xsub_fan4 | 3.31× | 4.14× | 3.16× | 2.35× | 1.25× |
| pair | 1.39× | 2.00× | 1.49× | 1.17× | 0.44× |
| dealer_router | 1.37× | 1.96× | 1.49× | 1.16× | 0.44× |
| router_dealer | 1.35× | 2.02× | 1.45× | 1.15× | 0.44× |

#### ipc, CURVE: round trip

| pattern | 32 B | 1 KB | 64 KB |
| :--- | ---: | ---: | ---: |
| req_rep | 0.87× | 0.91× | 1.35× |
| req_router | 0.91× | 0.91× | 1.36× |
| dealer_rep | 0.90× | 0.91× | 1.38× |
| dealer_router | 0.91× | 0.92× | 1.35× |
| pair | 0.90× | 0.92× | 1.35× |

#### tls (gozmq only)

libzmq has no TLS transport. gozmq over TLS against gozmq over TCP, same
run: PUSH/PULL 13.8M vs 14.0M at 32 B, 1.9M vs 2.6M at 1 KB, 0.08M vs 0.75M
at 8 KB and 0.01M vs 0.15M at 64 KB; the other patterns follow the same
shape; round trips 62–64 µs at 32 B–1 KB and 108–110 µs at 64 KB (TCP 60–62
and 72–75 µs).

### What the matrix shows

- **gozmq is ahead on every ZMTP pattern over tcp, ipc and inproc with NULL**
  except two inproc cells: DEALER to ROUTER at 32 B (0.70×) and ROUTER to
  DEALER at 32 B (1.00×). Over TCP the same two patterns are the narrowest
  wins at 32 B (1.02× and 1.09×).
- **STREAM sending is the one large gap**: gozmq writes each STREAM message
  with its own `write(2)` (`internal/session/raw.go`), flat at about 0.44M
  msg/s up to 1 KB (0.03× at 32 B, 0.48× at 1 KB). STREAM receiving is near
  parity (0.91–1.13×); its 32 B run moves only 48 MB, too short to trust.
  Being fixed on branch `stream-perf`.
- **CURVE at 64 KB is slower than libzmq**: 0.63–0.65× throughput over tcp,
  0.44–0.45× over ipc, and 1.30–1.40× the round-trip time; below 64 KB CURVE
  is 1.15–2.2× ahead.
- **TLS large frames** are the slowest transport (8 KB at a tenth of TCP),
  the cost of crypto/tls records, not of the session.
- **libzmq fan-out collapses**: four subscribers over inproc run at
  0.03–0.07M msg/s (130–240× behind gozmq), about two context switches per
  message, with each reader draining faster than the publisher fills its
  pipe; from 256 B up libzmq's four-subscriber rows fall 7–23× behind over
  tcp and 4–11× over ipc, subject to the HWM caveat above.
- **Round trips**: gozmq is 20–33% faster over tcp, 8–20% over ipc, and
  about 20 times faster over inproc (about 1 µs against about 24 µs, a
  goroutine handoff against libzmq's eventfd wake).

## gozmq vs native libzmq 4.3.4, PUSH/PULL over TCP loopback

Three consecutive runs at 3605aa8 (every library change except the two
allocation-footprint commits after it), ratio = gozmq/libzmq; a fourth run
at the branch head follows the table:

| size | gozmq | libzmq | ratio (3 runs) |
| :--- | ---: | ---: | ---: |
| 32 B | 12.9–13.2M msg/s (3.3 Gb/s) | 8.4–9.1M msg/s (2.1–2.3 Gb/s) | **1.42–1.57×** |
| 256 B | 6.1–6.4M (12.5–13.1 Gb/s) | 3.5–3.6M (7.2–7.3 Gb/s) | 1.73–1.78× |
| 1 KB | 2.29–2.45M (18.7–20.1 Gb/s) | 2.28–2.33M (18.7–19.1 Gb/s) | 1.00–1.05× |
| 8 KB | 0.58–0.72M (38–47 Gb/s) | 0.33–0.35M (22–23 Gb/s) | 1.66–2.06× |
| 64 KB | 0.13–0.14M (69–75 Gb/s) | 0.10M (51–54 Gb/s) | **1.30–1.44×** |

Head (332f2ab, one run, after the lease and pipe-allocation commits):
32 B 12.0M vs 8.8M (1.36×), 256 B 1.69×, 1 KB 2.62M vs 2.32M (1.13×),
8 KB 2.01×, 64 KB 0.13M vs 0.11M (1.18×); the in-repo gate of head against
3605aa8 is flat on every streaming row (TCP 32 B 102.0 → 102.7 ns, p=0.33)
and the pipe microbenchmarks are 6–12% faster on the queue kind.

Mixed pairings (run 1; the other runs are within 5%): gozmq PUSH → libzmq
PULL 9.1M at 32 B and 0.09M at 64 KB; libzmq PUSH → gozmq PULL 10.5M at
32 B and 0.08M at 64 KB. Neither side is the slow one at any size.

At v0.3.0 the same table read 0.6× at 32 B and 0.6× at 64 KB (the latter
with the old accidental GC ballast gone, "The 64 KB row" below). Two
earlier reports on this branch had 32 B at 1.04–1.09×: that was with a
`fmt.Sprintf` per message inside `zmqperf` on both ends, found when the
inproc comparison was added; the library did not change between those
tables and this one.

The receive pool is what closes the 64 KB row. With `-pool=false` (plain
memory, the previous default) the receiver runs at ~60k msg/s on this
machine, 0.6× of libzmq, and `GODEBUG=gctrace=1` shows why: 1,437 collections
for 60,000 messages against the 4 MB minimum heap goal, each with two
stop-the-world phases and a round of mark-worker wakeups, plus the zeroing
of every fresh 64 KiB body and the scavenger returning and re-faulting the
freed spans (32k minor faults). A pooled receiver allocates nothing per
message and makes no collection at all in the timed window.

### ipc

One run (`TRANSPORTS=ipc`), stock libzmq tool settings on the libzmq side
except where noted:

| size | gozmq | libzmq | ratio |
| :--- | ---: | ---: | ---: |
| 32 B | 12.5M | 9.6M | 1.31× |
| 256 B | 6.1M | 2.6M | 2.35× |
| 1 KB | 2.3M | 0.6M | 3.8× |
| 8 KB | 0.54M | 0.07M | 7.5× |
| 64 KB | 0.09M | 0.05M | 1.83× |

Mixed pairings: gozmq PUSH → libzmq PULL 8.9M at 32 B and 0.07M at 64 KB;
libzmq PUSH → gozmq PULL 10.1M and 0.04M. Read the libzmq column as "stock
perf-tool settings" (HWM 1000, allocation per message), as the script's
header says; ipc ratios overstate gozmq.

The gozmq PUSH → libzmq PULL pairing over ipc needs the C receiver's
RCVHWM raised to the message count (the script does this for that cell
only), and the reason is a libzmq receiver behaviour: it stops polling the
socket once its receive pipe is at HWM, a unix socket reports EPOLLHUP as
soon as the peer closes, and libzmq's engine takes that as an I/O error in
the stopped state and never drains the kernel queue, so the tail of a
burst is lost and its tool waits forever (96,548 of 100,000 messages arrive
at RCVHWM 1000, all 1,000,000 with it raised; gozmq's sender writes every
byte either way, verified with strace). It does not happen over TCP, where
there is no HUP until both directions close, and the libzmq↔libzmq ipc
cell completes with the stock setting (presumably because its sender's own
SNDHWM of 1000 keeps the receiver's pipe from filling).

### REQ/REP round trip over TCP

50,000 round trips, full round trip in µs (`compare_lat.sh`; libzmq's own
`remote_lat` prints one-way = half of this):

| size | gozmq↔gozmq | libzmq↔libzmq | gozmq REQ → libzmq REP | libzmq REQ → gozmq REP |
| :--- | ---: | ---: | ---: | ---: |
| 32 B | 63.4 | 83.0 | 74.5 | 73.5 |
| 1 KB | 64.1 | 81.9 | 74.4 | 74.4 |

### inproc (one process, peer on a thread or goroutine)

`compare_inproc.sh`, PUSH/PULL throughput and REQ/REP full round trip:

| size | gozmq | libzmq | ratio |
| :--- | ---: | ---: | ---: |
| 32 B | 12.9M msg/s | 8.5M msg/s | 1.52× |
| 256 B | 12.5M | 3.9M | 3.2× |
| 1 KB | 12.0M | 3.1M | 3.9× |
| 8 KB | 11.6M | 0.93M | 12.5× |
| 64 KB | 11.2M | 0.20M | 55× |
| round trip 32 B | 1.13 µs | 24.9 µs | 0.05× |
| round trip 1 KB | 1.11 µs | 25.3 µs | 0.04× |

gozmq's inproc delivery hands the sender's frame over without a copy, so
its rate is flat in the frame size; libzmq's C tool allocates and fills a
message per send and its receiver copies out of the pipe, so the large-frame
ratios measure that, not a library gap. The round trip compares a goroutine
handoff with libzmq's thread-to-thread mailbox (eventfd) wake.

### The 64 KB row and GC pacing

With Go's default GOGC a 64 KB receiver that allocates a fresh body per
message is GC-bound. The old pipe allocated a channel of HWM slots per
direction up front, which at zmqperf's 1,000,000 HWM was 8 MB of pointers
per direction and acted as accidental GC ballast (live heap 19 MB, a cycle
every ~290 messages); the chunked queue allocates as the queue deepens, the
receiver's live heap is ~3 MB and the pacer runs a cycle every ~40 messages.
Applications at the default HWM of 1000 never had that ballast, and no
reader-side change can remove the cost: the allocation itself is the
message's memory, and at GOGC 100 each cycle is a stop-the-world pair plus
the zeroing of every body. GC tuning (`GOGC=400` reached 0.9–1.0×) was
ruled out as a headline by the owner, and a finalizer-driven recycle was
analysed and rejected (the collector can give a buffer back at most once per
cycle, so at GOGC 100 it caps at half the allocations and only after the
heap has grown). The receive pool, already in the library as an opt-in with
an explicit-`Release` contract and no finalizer, became the default instead;
see "Decisions".

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

4. **Pooled large frames by default** (`options.go`, `msg.NewLent`,
   `internal/session/reader.go`): frames above the reader's 4 KiB scratch on
   NULL/PLAIN sessions are read into size-tiered pool buffers and lent to
   the application; `Release` recycles them, forgetting `Release` leaves them
   to the collector. The lease stores its buffers instead of a release
   closure, so a pooled 64 KiB frame costs the same allocations as a plain
   one did (PUSH/PULL 64 KB 3 → 2 per delivery). The audit of every engine
   and helper that rebuilds or forwards received messages, and the stress
   tests that forward 8 KiB, 64 KiB and mixed multipart frames through every
   socket pair with a capture proxy over tcp and inproc, are in
   `gozmq_recvpool_stress_test.go` and `gozmq_msg_ownership_test.go`.
5. **Measurement**: `zmqperf` no longer formats a label per message; the
   inproc and ipc comparisons and the inproc, ipc and large-frame in-repo
   benchmarks were added (`perf/README.md`).

An uncontended push+pop on the new queue is ~35 ns against ~26 ns for the
channel; that is the trade-off below. (The branch history keeps the round-1
commit "back direction with the chunked queue (kill criteria NOT met)": that
was the queue without deferred wakes, superseded by the commits after it.)

## In-repo benchmarks, perf-parity vs v0.3.0 (count 10, idle machine)

sec/op, base → candidate; every row with p < 0.05 and a change above 2%
is listed, plus the rows the report has always tracked. Rows the gate
flagged were re-run at count 16 (noted). The default receive path now
pools large frames, so `PushPullThroughput_TCP/64KB` and `_TLS/64KB`
measure the pooled path; `PushPullLarge_TCP/pool=false` keeps the plain
path visible:

| benchmark | v0.3.0 | perf-parity | change |
| :--- | ---: | ---: | ---: |
| PushPullThroughput_TCP/32B | 171.2 ns | 106.6 ns | −37.7% |
| PushPullThroughput_TCP/64B | 177.9 ns | 116.1 ns | −34.7% |
| PushPullThroughput_TCP/1KB | 524.9 ns | 486.6 ns | −7.3% |
| PushPullThroughput_TCP/64KB | 16.36 µs, 3 allocs, 64 KiB/op | 8.60 µs, 2 allocs, 195 B/op | −47.4% |
| PushPullThroughput_TLS/32B | 167.7 ns | 120.1 ns | −28.3% |
| PushPullThroughput_TLS/64B | 186.7 ns | 133.6 ns | −28.4% |
| PushPullThroughput_TLS/1KB | 708.5 ns | 667.9 ns | −5.7% |
| PushPullThroughput_TLS/64KB | 112.5 µs | 90.9 µs | −19.2% |
| PushPullCurve_TCP/64B | 473.3 ns | 415.2 ns | −12.3% |
| PushPullFanIn_TCP/pushers=2 | 130.4 ns | 90.7 ns | −30.5% |
| PushPullFanIn_TCP/pushers=16 | 92.9 ns | 71.2 ns | −23.3% |
| ReqRepRoundTrip_TCP/64B | 30.95 µs | 29.88 µs | −3.4% |
| ReqRepRoundTrip_TCP/64KB | 73.3 µs | 69.6 µs | −5.1% |
| RouterDealerLatency_TCP/64KB | 72.7 µs | 69.0 µs | −5.1% |
| ConnectHandshake_TCP | 178.9 µs, 256 allocs | 170.5 µs, 252 allocs | −4.7% |
| PushPullThroughput_Inproc/32B | 71.0 ns | 72.3 ns | +1.8% |
| PushPullThroughput_Inproc/64B | 70.8 ns | 73.4 ns | +3.6% |
| ReqRepRoundTrip_Inproc/32B | 1.067 µs | 1.060 µs | −0.7% |
| RouterDealerLatency_Inproc/32B | 900 ns | 908 ns | +0.9% |
| RouterIdlePeers_Inproc/idle=0 | 905 ns | 919 ns | +1.6% |
| PubSubFanoutWide_Inproc/subs=64 | 5.58 µs | 5.79 µs | ~ |
| XPubXSubThroughput_Inproc/32B | 82.6 ns | 78.4 ns | ~ |
| XPubXSubLatency_Inproc/32B | 60.9 ns | 61.9 ns | +1.6% |
| Poller_PollReady (root) | 126.7 ns | 127.8 ns | +0.8% |
| PushPullLarge_TCP/pool=true/8KB | 1.36 µs | 1.66 µs | +22% at count 10; ~ at count 16 (1.29 → 1.36 µs, p=0.11) |
| PushPullLarge_TCP/pool=false/1MB | 268 µs | 290 µs | +8% at count 10; ~ at count 16 (255 → 245 µs, p=0.47) |
| PushPullFanIn_Inproc/pushers=16 | 76.4 ns | 87.6 ns | +15% at count 10; ~ at count 16 (72.9 → 83.3 ns ±14%, p=0.06) |
| PushPullFanIn_Inproc/pushers=2 | 75.2 ns | 75.7 ns | ~ at count 10; +4.4% at count 16 (p=0.02) |

New rows (no base): PushPullThroughput_IPC 100.7 ns / 458 ns / 8.86 µs at
32 B / 1 KB / 64 KB, ReqRepRoundTrip_IPC/32B 9.10 µs, PushPullLarge_Inproc
72.6–75.5 ns at 8 KB–1 MB (the handoff is size-independent),
PubSubThroughput_Inproc/32B 78.0 ns, PubSubTopicThroughput_Inproc/32B
209.8 ns, PubSubThroughput_TCP/32B 281.7 ns, PubSubTopicThroughput_TCP/32B
182.5 ns.

allocs/op fell on every TCP path (PUSH/PULL 3 → 2 at every size, REQ/REP
14 → 10, ROUTER/DEALER 10 → 8, fan-in 3 → 2, TLS 3 → 2, handshake
256 → 252) and rose nowhere; inproc allocs/op and B/op are identical to
v0.3.0. B/op is within 5% on every row (REQ/REP TCP 64 B +3.9%, 832 → 864
bytes, is the largest increase) and fell 99% on the 64 KB TCP rows.

**The noisy rows.** `PushPullLarge_TCP/pool=true/8KB` (8 KiB frames,
~1.3 µs per message, the default receive path at that size) has read 6–22%
slower than v0.3.0 at count 10 in every round of this branch and within
noise at count 16 and 20 every time (this round: 1.29 → 1.36 µs, p=0.11,
±6–8%). Bisecting it interleaved (ten rounds, five binaries) gave a
gradual drift with no significant step: v0.3.0 1.26 µs, writer+alloc 1.34
(p=0.18), pipe 1.38 (p=0.05), current 1.40 (p=0.06). Two targeted fixes
were tried and reverted: flushing the wake immediately for lent messages
(no change) and capping by-reference bodies at 32 per batch, the old
granularity (+20%, worse). The two-process 8 KB comparison is 1.7–2.1× of
libzmq, so it is left as measured. `PushPullFanIn_Inproc/pushers=16` and
`/pushers=2` are each flagged in one of the two runs and not the other
(±14% spread); `pool=false/1MB` the same (±8–14%).

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

## Simplification pass (branch `simplify`, on top of perf-parity)

A pass over the whole tree after parity: 538 fewer non-test lines
(15,617 → 15,079), hand-rolled helpers replaced by the standard library,
the ROUTER/STREAM send skeleton, the PUSH/PULL/DEALER engine, the engines'
close state, the mechanisms' handshake reply decoding and the pipe's drain
state each written once, and the HWM-0 inproc pipe moved from a channel
plus overflow slice to the chunked queue. It also fixed a STREAM send that
leaked a pooled buffer's lease reference when its peer vanished mid-send.

Gate against the branch base (count 10, idle machine; base adds the two
HWM-0 rows so they have numbers): sec/op geomean −2.4%, allocs/op identical
on every row, B/op within 5% except `PubSubThroughput_TCP/64B` (+6% with
+16% more messages delivered per op, its `msgs-delivered` metric; see the
recheck note below). Rows that moved, all p<0.05:

| benchmark | perf-parity | simplify | change |
| :--- | ---: | ---: | ---: |
| ReqRepRoundTrip_InprocHWM0/32B | 1.066 µs | 1.132 µs | +6.1% (queue kind, accepted) |
| PushPullThroughput_InprocHWM0/32B | 104.3 ns | 49.7 ns | −52% |
| ReqRepRoundTrip_Inproc/32B–64KB | 1.063–1.070 µs | 1.045–1.048 µs | −1.7…−2.3% |
| RouterDealerLatency_Inproc/* | 908–910 ns | 888–890 ns | −2.2% |
| PushPullThroughput_TCP/64B | 118.1 ns | 112.6 ns | −4.7% |
| PushPullThroughput_IPC/32B | 103.5 ns | 100.6 ns | −2.9% |
| PushPullThroughput_TLS/32B, 64B | 120.2, 135.0 ns | 117.2, 129.8 ns | −2.5%, −3.9% |
| PushPullThroughput_TLS/1KB | 663.8 ns | 672.0 ns | +1.2% |
| PubSubTopicThroughput_Inproc/32B, 64B | 209.6, 208.7 ns | 192.9, 184.8 ns | −8%, −11% |
| PubSubFanoutWide_Inproc/subs=64 | 5.89 µs | 5.50 µs | −6.7% |
| PushPullFanIn_Inproc/pushers=2 | 78.7 ns | 74.9 ns | −4.8% |

Every other row is within noise (TCP 32 B 104.3 → 102.9 ns, p=0.085;
Poller_PollReady 127.6 → 128.8 ns, p=0.10; CURVE 64 B 416 → 412 ns). The
CURVE frame transform itself got faster from the branchless flag
translation and precomputed nonce templates (interleaved count 12 in the
curve package: encode 1 KiB −10%, 16 KiB −17%, decode 64 B −14%).

Two-process TCP against libzmq at head, one run: 32 B 11.5M vs 9.0M
(1.28×), 256 B 1.76×, 1 KB 1.10×, 8 KB 1.85×, 64 KB 2.12×; mixed pairings
9.0M and 10.5M at 32 B. Every row stays above parity; the 32 B ratio sits
at the low end of the parity runs' spread because this libzmq sample was
at its high end (9.0M).

Verification at head: `go test -race ./...`, 20× the pipe package and the
idle/wake tests, fuzz (14 targets, 10 s each), interop against libzmq,
lint 0 issues, deadcode empty, CGO-off build.

Rechecks: `PubSubThroughput_TCP/{32B,64B}` at count 16 are within noise on
every column (64 B 321 → 312 ns p=0.34, B/op 138.5 → 137.0 p=0.82, allocs
1 → 1), so the count-10 B/op reading was the delivered-count swing of a
PUB that drops at HWM. The handshake rows after the last consolidation
commit (count 6): `ConnectHandshake_TCP` 251 → 242 allocs and
`ConnectHandshake_Curve_TCP` 400 → 387 allocs per connect, time flat.

## Decisions these numbers rest on

- **Pooled large frames by default, explicit Release, no finalizer.**
  Frames above 4 KiB received on NULL/PLAIN sessions are lent from the
  size-tiered pool; `Release` recycles them and forgetting it is safe.
  `WithRecvBufferPool(false)` restores plain memory per socket; STREAM
  sockets are always plain. The v0.1 default-on pool corrupted data through
  a `*Msg` finalizer (recycling under retained frame slices), a `Release`
  that ran while `writev` still referenced the buffers, and a ROUTER send
  that mutated the caller's message; the current design has none of the
  three (no finalizer, `SendMsg` holds its own reference, `Derive` for
  every rebuilt message) and is what the opt-in shipped with in v0.2.
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
