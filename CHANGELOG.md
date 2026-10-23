# Changelog

All notable changes to `gozmq` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html); while
it is 0.x, a minor release may include breaking changes, each listed with a
migration hint.

## [Unreleased]

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

[Unreleased]: https://github.com/exergy-dev/gozmq/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/exergy-dev/gozmq/releases/tag/v0.1.1
