# gozmq ↔ libzmq interop tests

This package holds the cross-implementation tests that prove `gozmq`
is wire-compatible with libzmq. Everything here is gated behind the
`interop` build tag so that the default `gozmq` build stays free of CGO
and any C-side dependencies.

Only these tests bring CGO into the module. The rest of `gozmq` must
continue to build and test with `CGO_ENABLED=0`.

## Prerequisites

- `libzmq` development headers and shared library (≥ 4.2, 4.3.x
  exercised in CI). On Debian/Ubuntu: `apt-get install libzmq3-dev`.
- `pkg-config libzmq` must succeed. The CGO preamble relies on
  `#cgo pkg-config: libzmq` to pick up `-lzmq` and the header path.

Verify locally:

```
pkg-config --modversion libzmq
pkg-config --cflags --libs libzmq
```

## Running

```
go test -tags interop -race ./interop/...
```

The `-race` flag is required per design §10.5. `go vet` and the default
`CGO_ENABLED=0 go build ./...` must continue to pass on the rest of the
module; the `interop` tag guarantees those builds never pull in this
directory's CGO sources.

## Scope

Every test drives one `gozmq` socket against one `libzmq` socket. A test is
written once against two interchangeable ends (`link_test.go`); a `direction`
value says which implementation binds, so each case runs both ways —
`gozmq`-binds and `libzmq`-binds — from one table row.

- `zmq_wrap.go` — thin CGO wrapper over libzmq (context, socket,
  bind/connect, multipart send/recv, subscribe, last-endpoint).
- `zmq_wrap_test.go` — one smoke test that the wrapper itself works
  (libzmq PAIR ↔ libzmq PAIR round-trip, plus a bogus-option rejection).
- `link_test.go`, `mech_test.go` — the socket adapter, the `direction`
  value and the NULL/PLAIN/CURVE mechanism table.
- `patterns_test.go` — socket patterns over NULL, plus load-balance,
  router-mandatory, XPUB-verbose, STREAM, non-`tcp://` transports and
  payload edges.
- `security_test.go` — PLAIN and CURVE handshakes and ZAP.
- `lifecycle_test.go` — heartbeat, disconnect, linger, reconnect,
  max-message-size.
- `proxy_test.go`, `zap_helper_test.go` — a TCP frame-mutating splicer and
  the libzmq-side ZAP responder.

## Coverage Snapshot

Mechanism × pattern over `tcp://`, each cell run in both directions:

| Pattern | NULL | PLAIN | CURVE |
| --- | --- | --- | --- |
| PAIR | yes | yes | yes |
| REQ/REP | yes | yes | no |
| DEALER/ROUTER | yes | yes | no |
| PUB/SUB | yes | yes | no |
| XPUB/XSUB | yes | no | no |
| PUSH/PULL | yes | no | no |

- **PLAIN** (`TestSecurity`): good-credential accept, bad-password reject
  (and no delivery after), empty-username accept, and REQ/REP, DEALER/ROUTER
  and PUB/SUB transparency over an authenticated link. An oversized (256-byte)
  username is skipped: libzmq 4.3.x refuses it in `zmq_setsockopt`, so it
  never reaches the wire.
- **CURVE** (`TestSecurity`): PAIR round-trip, a wrong pinned server key that
  never completes a handshake, and (`TestCurve_CorruptedWelcome`) a one-byte
  flip inside the WELCOME cookie box that the client must reject.
- **ZAP** (`TestZAP`): NULL, PLAIN and CURVE, accept and reject, with each
  implementation as the authenticating server; PLAIN also proves the ZAP
  verdict overrides the local callback, and every case asserts the handler
  was consulted.
- **Payload edges** (`TestPayloadEdges`): empty frames (including an empty
  middle frame), the 255/256/257-byte framing boundary, 1 MiB and 16 MiB
  frames, and a 32-frame multipart.
- **Transports beyond `tcp://`** (`TestTransports`): `ipc://` filesystem and
  `ipc://@` abstract (Linux) namespaces, and `tcp6://` — on the libzmq side
  `tcp://` plus `ZMQ_IPV6=1`, its native IPv6 model. `STREAM` raw-TCP framing
  is `TestStream`.
- **Heartbeat** (`TestHeartbeat`): over NULL, PLAIN and CURVE, an idle
  session survives past the timeout and carries data afterward with no
  heartbeat command leaking into user data, then the gozmq peer observes the
  libzmq close.
- **Lifecycle** (`lifecycle_test.go`): client-side `Disconnect`,
  receiver-enforced max-message-size, reconnect after close/rebind, and
  `LINGER` drain-vs-drop — each in both directions.

## Conventions

- Errors surfaced from libzmq use the prefix `libzmq: <op>: <strerror>`
  per the design-wide rule.
- Every test uses `t.Cleanup` to close both contexts and all sockets, so
  goleak can enforce no-leak on exit via the package `TestMain`.
- libzmq spawns pthreads for its I/O reactor; those are outside the Go
  runtime and invisible to goleak, so no whitelist is needed.
  If a future test blocks a Go goroutine inside a long-running cgocall
  past `TestMain`, add an explicit `goleak.IgnoreTopFunction` with a
  comment documenting which function and why.
