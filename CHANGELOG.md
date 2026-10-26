# Changelog

All notable changes to `gozmq` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html); while
it is 0.x, a minor release may include breaking changes, each listed with a
migration hint.

## [Unreleased]

### Added

- `perf/run_all.sh`: the whole performance suite in one run (environment,
  every in-repo benchmark, the libzmq matrix) into
  `perf/results/run-<timestamp>/` (gitignored).
- `perf/libzmq_thr/compare.sh` and `perf/libzmq_thr/zmqperf.c`: gozmq
  against native libzmq for every socket pattern (PUSH/PULL, PUB/SUB with
  prefix and four-subscriber fan-out, XPUB/XSUB, PAIR, DEALER/ROUTER both
  ways, STREAM both ways, REQ/REP, REQ/ROUTER, DEALER/REP, DEALER/ROUTER
  and PAIR round trips) over tcp, ipc, inproc and tls (gozmq only), with
  NULL and CURVE, including the mixed pairings. Both tools set the same HWM
  on every socket; a lossy run is retried, never reported.
- `zmqperf` flags `-pattern`, `-mode`, `-hwm`, `-prefix`, `-subs`,
  `-curve`, `-wait` and `-idle`; the existing commands and defaults are
  unchanged (`push_pull` and `req_rep`), and throughput receivers now print
  `received: N` and exit non-zero on a short count.
- `perf/matrix_bench_test.go`: the in-repo benchmarks that complete the
  pattern-by-transport table in `perf/README.md` (PAIR, DEALER/ROUTER
  streaming both ways, XPUB/XSUB over TCP, IPC and TLS, PUB/SUB over IPC
  and TLS and fanned out over TCP and IPC, REQ/REP and ROUTER/DEALER over
  TLS and IPC, REQ/REP and PUB/SUB with CURVE, STREAM send and receive).

### Removed

- `perf/libzmq_thr/compare_all.sh`, `compare_lat.sh`, `compare_inproc.sh`
  and their six C tools, superseded by `compare.sh` and `zmqperf.c`
  (`TRANSPORTS=tcp THR_PATTERNS=push_pull LAT_PATTERNS=req_rep` is the old
  pair of tables; `C_HWM=1000` restores the stock libzmq HWM they used).

## [0.4.0] - 2026-10-25

A throughput release: PUSH/PULL over TCP is at or above native libzmq at
every size in the two-process comparison at Go's default runtime settings
(32 B 1.4–1.6× from 0.6×, 64 KB 1.3–1.4× from 0.6×), inproc and ipc are
compared as well, and the one behavioural change is that large received
frames are pooled by default. No wire change. See `perf/REPORT.md`.

### Added

- `msg.FromFramesCopy` / `NewMsgFromFramesCopy`: a message whose frames are
  copied into one message-owned allocation (what the reader now builds for
  small received messages).
- `msg.NewLent`: a lent message whose lease stores the buffers it gives
  back (one allocation with the message), used by the session reader for
  pooled frames instead of a release closure.
- `perf/libzmq_thr/compare_lat.sh` and the C `local_lat`/`remote_lat` tools:
  REQ/REP round-trip comparison against libzmq in all four pairings;
  `compare_all.sh` prints the two mixed PUSH/PULL pairings and takes
  `COUNT_SCALE` and `GOZMQ_GOGC`. `compare_inproc.sh` with the C
  `inproc_thr`/`inproc_lat` tools and the `zmqperf inproc_thr`/`inproc_lat`
  subcommands compare inproc (one process, peer on a thread/goroutine).
  New benchmarks `PubSubThroughput_{TCP,Inproc}`,
  `PubSubTopicThroughput_{TCP,Inproc}`, `PushPullLarge_Inproc`,
  `PushPullThroughput_IPC`, `ReqRepRoundTrip_IPC`, `ReqRepThroughput_IPC`.

### Changed

- **The receive buffer pool is now the default** (`WithRecvBufferPool`).
  Received frames larger than 4 KiB on NULL/PLAIN sessions (tcp/ipc) are
  lent from the pool, and `Release` really recycles them; at 64 KB this
  removes the garbage-collector pacing that held the receiver to about 60%
  of libzmq. Forgetting `Release` stays safe (there is no finalizer; the
  buffer is simply collected). STREAM sockets are unaffected (the option is
  silently off; an explicit `WithRecvBufferPool(true)` there is still an
  error). Migration: code that calls `Release` and then keeps using the
  message's frames (or a slice from `Bytes`/`Frames`) must copy them first,
  or create the socket with `WithRecvBufferPool(false)`; a `typed.Codec`
  whose `Unmarshal` retains its input must not be used at the default.
  `Socket.RecvBytes` and the `typed` wrappers already copy.
- The session writer copies frame bodies up to 1 KiB behind their headers
  so a run of small frames is one iovec, bounds a batch by bytes and iovecs
  (64 KiB copied, 1000 iovecs, 2 MiB total) instead of 32 messages, and
  sends a fully coalesced batch with one `write(2)`. Per million 32 B
  messages the sender makes 675 syscalls instead of 31,251 and the receiver
  2,100 reads instead of 19,702. Wire bytes are identical.
- One allocation per small received message: PUSH/PULL over TCP 3 → 2
  allocations per delivery, REQ/REP 14 → 10 per round trip, ROUTER/DEALER
  10 → 8, two-frame topic+payload 6 → 3.
- Session pipes are chunked two-lock queues (256-slot chunks, libzmq's
  `yqueue` shape) instead of HWM-sized channels: producers and consumers no
  longer share a lock, the session reader wakes a receiver once per read
  batch instead of once per message, and a TCP peer's HWM no longer
  allocates its full capacity up front (a peer at HWM 1000 used 8 KiB per
  direction before any message; the zmqperf tool's 1,000,000 HWM used
  8 MiB). TCP rows gain 20–40% (PUSH/PULL 32 B −39%, fan-in −20…−31%, TLS
  32 B −28%). Inproc pipes keep the channel implementation, because their
  one-hop-per-message patterns are ~20 ns per hop cheaper on a channel;
  inproc rows are unchanged.
- The reader stamps the heartbeat's inbound clock once per read batch
  instead of once per message.
- A generics sweep found the library already generic where it pays
  (`typed`, the `msg` co-allocation tiers, every atomic an
  `atomic.Pointer[T]`); what it changed is the tests, which now share
  `internal/testutil`'s `RequireRecv`, `RequireNoRecv` and `Async` instead
  of about 60 hand-written timeout selects (400 fewer test lines; non-test
  lines 15,079 → 15,163, 67 of them the helpers in testutil.go, which counts
  as non-test by filename), and two internal folds: `typed` decodes through one `unmarshal[T]`, the inproc
  registry deletes from its maps of slices through one helper. No behaviour
  change.
- `zmqperf local_thr` divides by the messages it actually timed (`count-1`),
  releases every received message as libzmq's tool does with
  `zmq_msg_close` (`-pool=false` opts out to plain memory), formats its
  failure labels only on error (a `Sprintf` per message was 28% of the
  inproc profile and taxed the TCP figures of earlier reports), and runs at
  the runtime's GC setting (`-gogc` is an option, not a default).

### Removed

A simplification pass (538 fewer non-test lines, 15,617 → 15,079, no wire change):
hand-rolled helpers replaced by the standard library, duplicated engine,
mechanism and pipe code shared, dead branches dropped. Removals, with the
replacement:

- `gozmq.NewMsg` and `msg.New`: use `NewMsgFromFrames()` / `msg.FromFrames()`
  with no arguments, which builds the same empty message.
- `msg.NewWithRelease`: use `msg.NewLent(put, bufs, frames...)`, which
  takes the buffers to hand back instead of a closure (one allocation with
  the message). A lease now has one callback; `Lent`, `Retain`, `Release`
  and `Derive` are unchanged.
- `Option` and `ContextOption` are plain function types
  (`func(*socketConfig) error`, `func(*Context)`) instead of interfaces.
  Code that only passes `With*` values is unaffected; code that
  implemented the interface (impossible outside the package) does not exist.

### Fixed

- A STREAM `Send` whose peer pipe had closed between the identity lookup
  and the write leaked the lease reference it had taken on a pooled frame,
  so the buffer never returned to the pool. It is released on every
  failure path now, as ROUTER already did; both engines have regression
  tests through every outcome (unknown identity, vanished peer, counted
  drop).

### Changed (simplification)

- The mechanism options (`WithPlainAuth`, `WithPlainClient`,
  `WithCurveServer`, `WithCurveClient`, `WithSecurityMech`) validate their
  arguments when applied, so `NewSocket` reports a nil authenticator, a
  wrong key length or an over-long credential from the option itself;
  applying two different mechanism options is reported as
  "<option> and <option> are mutually exclusive". `errors.Is` targets are
  unchanged; wording is.
- An inproc pipe with HWM 0 (unbounded) uses the chunked queue instead of
  a channel with an overflow slice. Default-HWM inproc pipes still use the
  channel, so their rows are unchanged; the HWM-0 round trip costs the
  queue's known ~6% (new gate rows `ReqRepRoundTrip_InprocHWM0` and
  `PushPullThroughput_InprocHWM0`), and HWM-0 streaming is twice as fast.
- `Bind` emits `EventListening` before its accept goroutine starts, so it
  always precedes the first `EventAccepted`.
- `security` exports the handshake helpers the shipped mechanisms share
  (`Malformed`, `AuthFailed`, `RequireInput`, `RequireNoInput`,
  `DecodeReply`), usable by an out-of-tree `Mech`.
- CURVE translates the per-frame MESSAGE flags without branches and keeps
  its nonce templates precomputed (8–17% faster frame encode/decode on
  bodies of 1 KiB and up); PLAIN/NULL/CURVE share one reply decoder.
- `zmqperf`'s subcommands share their timed loops; `remote_lat` releases
  its replies like the other receivers.

## [0.3.0] - 2026-10-23

A performance release: fewer allocations on every path, one frame encode
path, and lock-free subscribe-to-all matching, with one contract change on
`security.Mech`. Breaking changes are listed first, with a migration hint.

### Breaking changes

- **`security.Mech` gains an ownership rule for `EncodeFrame` and
  `DecodeFrame`.** The returned slice must be either the input itself or
  memory the mechanism neither reuses nor modifies after returning: the
  session now hands encoded bodies to the kernel and decoded bodies to the
  application by reference instead of copying them. The shipped mechanisms
  already comply. Migration for an out-of-tree mechanism that recycled an
  output buffer: return a fresh slice, or implement the new optional
  `security.FrameAppender` (`AppendEncodeFrame`), which seals straight into
  the writer's buffer and is what CURVE does.

### Changed

- Fewer allocations on every path: PUSH/PULL over TCP goes from 5 to 3
  allocations per delivery, REQ/REP from 25 to 15 per round trip,
  ROUTER/DEALER from 16 to 10, CURVE from 8 to 3 per message. The frame
  reader no longer allocates its header fields, the writer has one encode
  path for every mechanism, and a REP keeps a plain request's envelope by
  reference.
- A PUB answers the subscribe-to-all case from an atomic flag per peer
  instead of taking that peer's subscription lock on every message; a
  selective tree is still walked under the read lock.
- CURVE derives each handshake key once (four fewer X25519 operations per
  side per connection) and the reader reuses one ciphertext buffer for its
  large frames.
- Over `tls+tcp` a write batch is one TLS record and one syscall instead of
  one per frame header and body, and `TCPTransport`'s socket options
  (TCP_NODELAY, buffer sizes) now apply to TLS connections; they were
  silently skipped.
- PING/PONG commands join the write batch instead of two unbatched writes
  each.
- `typed.Receiver`, `typed.Subscriber` and their `Handle` loops release each
  received message after decoding it, so `WithRecvBufferPool` recycles
  through them. A `typed.Codec`'s `Unmarshal` must not keep a reference to
  its input (encoding/json does not).
- New benchmarks: `PushPullThroughput_TLS`, `PubSubFanoutSelective_Inproc`
  and `ConnectHandshake_Curve_TCP`.

## [0.2.0] - 2026-10-23

A cleanup release: the same wire behaviour and defaults as 0.1.1, with a
smaller library, a faster test suite and shorter documentation. Breaking
changes are listed first, each with a migration hint.

### Breaking changes

- **`typed/codecjson` is gone; use `typed.JSONCodec`.** The sixteen-line
  package added an import path for a codec that needs no dependency.
  Migration: replace `codecjson.Codec{}` with `typed.JSONCodec{}` and drop
  the import.
- **`Poller.Modify` is removed.** `Poller.Add` has always replaced the mask
  of a registered socket, so `Modify` was an alias. Migration: call `Add`.
- **`ErrHWM` is removed.** Nothing returned it; its own documentation said
  so. Migration: delete any `errors.Is(err, gozmq.ErrHWM)` check, which
  could never be true.
- **The perf commands are one binary.** `perf/local_thr`, `remote_thr`,
  `local_lat` and `remote_lat` are subcommands of `perf/zmqperf` with the
  same flags and output: `go run ./perf/zmqperf local_thr ...`.

### Changed

- `Socket.Close` classifies "already shutting down" errors with `errors.Is`
  only; the string match on "use of closed network connection" is gone. A
  third-party `transport.Conn` whose Close error does not wrap
  `net.ErrClosed` now surfaces that error from `Close`. The shared
  classifier also treats `io.EOF`, a cancelled or expired context,
  `ErrSocketClosed` and `ErrContextClosed` as expected teardown errors, which
  `Close` previously would have surfaced.
- A SUB socket replays its subscriptions to a newly connected publisher in
  map order rather than subscription order. The set is identical; only the
  order of the SUBSCRIBE frames on the wire changes.
- Six of the twelve `examples/` programs duplicated the package's own
  `Example*` functions and are gone; `security_plain` and `security_curve`
  are one `examples/security` program with a `-mech` flag.

### Fixed

- `Close` now unblocks a ROUTER or STREAM `Send` parked at the peer's
  high-water mark, as it already did for every other socket type; before,
  only the call's context could end such a send.

### Internal

- One ZMTP command codec in `internal/zmtp` replaces the copies in the
  session, PLAIN and CURVE; one shutdown-error classifier, one identity
  framing type, one transport `Conn` adapter, and TLS reuses the TCP
  dial/listen wiring. Standard-library replacements throughout (`slices`,
  `maps`, `min`/`max`, `clear`, `box.Overhead`, `rand.N`).
- Tests: every suite runs in parallel with shared helpers in
  `internal/testutil`; duplicate tests at two levels, per-engine copies of
  the conformance table and the per-transport copies of shared tests are
  gone; `pgregory.net/rapid` is no longer a dependency. `go test ./...`
  takes about 5 s instead of 29 s.
- Docs: `design.md` is architecture only, `perf/REPORT.md` is the current
  numbers and method, and CI runs race and coverage in one job.

## [0.1.1] - 2026-10-23

First public release. A pure-Go implementation of the ZeroMQ socket patterns
and the ZMTP 3.1 wire protocol: no cgo, no libzmq, no build tags.

(0.1.1 rather than 0.1.0 because a `v0.1.0` tag was cut in the private
repository before the pre-release review; see [0.1.0] below.)

Requires Go 1.25 or newer.

### What ships

- **Socket types**: `REQ`, `REP`, `DEALER`, `ROUTER`, `PUB`, `SUB`, `XPUB`,
  `XSUB`, `PUSH`, `PULL`, `PAIR`, `STREAM`.
- **Transports**: `tcp://`, `tcp6://`, `ipc://`, `inproc://` (built into the
  Context), and the gozmq-specific `tls+tcp://`. Third-party transports plug
  in through `transport.Transport` and `(*Context).RegisterTransport`.
- **Security**: NULL, PLAIN, CURVE (RFC 25/26) and RFC 27 ZAP via
  `WithZAPHandler`; custom mechanisms through `security.Mech`.
- **Operations**: reconnect with exponential backoff and jitter, ZMTP 3.1
  heartbeats, high-water marks, cancellable linger (`CloseCtx`), live
  `Unbind` / `Disconnect` / `Subscribe`, monitor events, per-socket metrics,
  `slog` integration.
- **Ergonomics**: every blocking call takes a `context.Context`; `Poller` for
  `zmq_poll`-style multiplexing; `Proxy`; `SendBytes` / `RecvBytes`; the
  generic `typed` layer with a JSON codec; opt-in pooled receive buffers
  (`WithRecvBufferPool`) with lent-message semantics.
- **Interop**: tested against libzmq 4.3.4 in both directions by the
  build-tagged `interop/` suite (matrix in `interop/README.md`).

Every intentional difference from libzmq is listed in
`docs/COMPATIBILITY.md`; the threat model and hardening notes are in
`SECURITY.md`.

### Security properties worth knowing

These were fixed during the pre-release review and are listed so that
anyone comparing gozmq's behaviour with a ZMTP reading knows where it is
deliberately strict:

- Inbound frame bodies are allocated as their bytes arrive. A peer that
  announces a huge frame and stalls pins at most 17× the bytes it sent plus
  128 KiB per connection, not `WithMaxMessageSize` (64 MiB by default).
  Handshake frames are capped at 64 KiB on both sides, independent of the
  data cap.
- CURVE derives a MESSAGE's `MORE` and `COMMAND` bits from the encrypted
  inner flags byte only; the unauthenticated outer wire flags are ignored,
  as libzmq does.
- `NewSocket` fails for an incomplete security option (`WithPlainAuth(nil)`
  and `WithCurveClient` with empty keys used to run NULL silently; PLAIN
  credentials over 255 bytes used to fail every handshake) instead of
  constructing a socket that cannot do what was configured.
- `WithMaxMessageSize` bounds the whole logical message on both sides (the
  send check sums the frames); for CURVE it is a plaintext bound.
- A peer-advertised `Identity` must obey ZMTP 3.1 (1–255 bytes, no leading
  zero byte); a PING context is limited to 16 bytes. Violations end the
  session.
- A handshake in which both ends claim the server role (two
  `WithCurveServer` or two `WithPlainAuth` sockets) fails at the greeting
  instead of waiting out `WithHandshakeTimeout`.
- `WithSendHWM` / `WithRecvHWM` are capped at `MaxHWM` (2^20 messages); a
  positive HWM is allocated per peer connection, and 0 remains "unbounded".
- `SendMsg(ctx, nil)` returns an error wrapping `ErrFSMViolation` on every
  socket type; `CloseCtx(nil)` behaves as `CloseCtx(context.Background())`.

### History

gozmq was developed privately from early 2026. Earlier snapshots were never
published to a module proxy, so this release has no upgrade path to
document; the development history is in the git log.

## [0.1.0] - 2026-10-01

Tagged in the private repository before the pre-release review and never
pushed to GitHub or a module proxy. It has the memory-exhaustion, CURVE
and crash issues listed under 0.1.1, so it is not available and should not
be used; the first published version is 0.1.1.

[Unreleased]: https://github.com/exergy-dev/gozmq/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/exergy-dev/gozmq/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/exergy-dev/gozmq/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/exergy-dev/gozmq/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/exergy-dev/gozmq/releases/tag/v0.1.1
[0.1.0]: https://github.com/exergy-dev/gozmq/commit/62a43b78a3561a7b8db530ce710dc57dc09b0bcd
