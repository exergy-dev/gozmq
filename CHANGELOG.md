# Changelog

All notable changes to `gozmq` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html); while
it is 0.x, a minor release may include breaking changes, each listed with a
migration hint.

## [Unreleased]

## [0.2.0] - unreleased

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

[Unreleased]: https://github.com/exergy-dev/gozmq/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/exergy-dev/gozmq/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/exergy-dev/gozmq/releases/tag/v0.1.1
[0.1.0]: https://github.com/exergy-dev/gozmq/commit/62a43b78a3561a7b8db530ce710dc57dc09b0bcd
