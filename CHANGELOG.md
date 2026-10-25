# Changelog

All notable changes to `gozmq` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html); while
it is 0.x, a minor release may include breaking changes, each listed with a
migration hint.

## [Unreleased]

A throughput release: small-message PUSH/PULL over TCP now matches native
libzmq (32 B: 1.04–1.10× in the two-process comparison, from 0.6×), with no
wire or API change. One additive export. See `perf/REPORT.md` for the
tables and the trade-off on inproc request/reply latency.

### Added

- `msg.FromFramesCopy` / `NewMsgFromFramesCopy`: a message whose frames are
  copied into one message-owned allocation (what the reader now builds for
  small received messages).
- `perf/libzmq_thr/compare_lat.sh` and the C `local_lat`/`remote_lat` tools:
  REQ/REP round-trip comparison against libzmq in all four pairings;
  `compare_all.sh` prints the two mixed PUSH/PULL pairings and takes
  `COUNT_SCALE` and `GOZMQ_GOGC`. New benchmarks `PubSubThroughput_TCP` and
  `PubSubTopicThroughput_TCP`.

### Changed

- The session writer copies frame bodies up to 1 KiB behind their headers
  so a run of small frames is one iovec, bounds a batch by bytes and iovecs
  (64 KiB copied, 1000 iovecs, 2 MiB total) instead of 32 messages, and
  sends a fully coalesced batch with one `write(2)`. Per million 32 B
  messages the sender makes 675 syscalls instead of 31,251 and the receiver
  2,100 reads instead of 19,702. Wire bytes are identical.
- One allocation per small received message: PUSH/PULL over TCP 3 → 2
  allocations per delivery, REQ/REP 14 → 10 per round trip, ROUTER/DEALER
  10 → 8, two-frame topic+payload 6 → 3.
- Pipes are chunked two-lock queues (256-slot chunks, libzmq's `yqueue`
  shape) instead of HWM-sized channels: producers and consumers no longer
  share a lock, the session reader wakes a receiver once per read batch
  instead of once per message, and a HWM no longer allocates its full
  capacity up front (a peer at HWM 1000 used 8 KiB per direction before any
  message; the zmqperf tool's 1,000,000 HWM used 8 MiB). Throughput rows
  gain 20–50% (TCP 32 B −39%, XPUB/XSUB inproc −47%, PUB fan-out 2 peers
  −41%); inproc REQ/REP and ROUTER/DEALER round trips cost 8–9% more
  (about 80 ns on 1 µs) and a 64-subscriber inproc fan-out 26% more,
  because an uncontended hop to a parked receiver is ~20 ns dearer than a
  channel handoff.
- The reader stamps the heartbeat's inbound clock once per read batch
  instead of once per message.
- `zmqperf local_thr` divides by the messages it actually timed (`count-1`),
  and `zmqperf` runs at GC percent 400 by default (`-gogc` overrides it):
  with the HWM-sized channel buffers gone, a 64 KB receiver's live heap is a
  few MB and Go's default pacer collects every ~60 messages; the report
  shows the default-pacing figures alongside.

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

[Unreleased]: https://github.com/exergy-dev/gozmq/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/exergy-dev/gozmq/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/exergy-dev/gozmq/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/exergy-dev/gozmq/releases/tag/v0.1.1
[0.1.0]: https://github.com/exergy-dev/gozmq/commit/62a43b78a3561a7b8db530ce710dc57dc09b0bcd
