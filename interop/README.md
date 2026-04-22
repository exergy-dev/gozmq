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

The interop suite covers the wrapper plus the gozmq engines that exist
today:

- `zmq_wrap.go` — thin CGO wrapper over libzmq (context, socket,
  bind/connect, multipart send/recv, subscribe, last-endpoint).
- `zmq_wrap_test.go` — smoke test that the wrapper itself works
  (libzmq PAIR ↔ libzmq PAIR round-trip).
- `pair_test.go` — `gozmq` PAIR ↔ `libzmq` PAIR, both directions.
- `push_pull_test.go` — `gozmq` PUSH/PULL ↔ `libzmq` PULL/PUSH, both
  directions, plus a load-balance assertion (one gozmq PUSH fanning 9
  messages across three libzmq PULL peers, expecting 3 per peer).

The suite also covers DEALER/ROUTER, REQ/REP, PUB/SUB, XPUB/XSUB, CURVE,
PLAIN, ZAP, lifecycle behavior, and transport-specific edge cases.

## Coverage Snapshot

Current tagged coverage over `tcp://` looks like this:

| Pattern | NULL | PLAIN | CURVE |
| --- | --- | --- | --- |
| PAIR | yes | yes | yes |
| REQ/REP | yes | yes | no |
| DEALER/ROUTER | yes | yes | no |
| PUB/SUB | yes | yes | no |
| XPUB/XSUB | yes | no | no |
| PUSH/PULL | yes | no | no |

Current PLAIN coverage includes:

- PAIR round-trips both directions.
- Bad-password rejection both directions.
- Empty-username acceptance both directions.
- Oversized-credential rejection.
- REQ/REP, DEALER/ROUTER, and PUB/SUB transparency over authenticated links.

Current ZAP coverage includes:

- NULL accept/reject with libzmq as the authoritative server.
- NULL accept/reject with gozmq as the authoritative server.
- PLAIN accept/reject with libzmq as the authoritative server.
- PLAIN accept/reject with gozmq as the authoritative server.
- CURVE accept/reject with libzmq as the authoritative server.
- CURVE accept/reject with gozmq as the authoritative server.

Current payload-edge coverage includes:

- Zero-length frames, including an empty middle frame in multipart.
- Frame-size boundaries at 255, 256, and 257 bytes.
- Large frames at 1 MiB and 16 MiB.
- 32-frame multipart messages.

Current transport coverage beyond `tcp://` includes:

- `tcp6://` PAIR interop both directions. On the libzmq side this uses `tcp://` plus `ZMQ_IPV6=1`, which is libzmq's native IPv6 model.
- `ipc://` filesystem-namespace PAIR interop both directions.
- `ipc://@name` abstract-namespace PAIR interop both directions on Linux.
- `STREAM` raw-TCP framing interop both directions over `tcp://`.

Current heartbeat coverage includes:

- Idle-session survival for at least five heartbeat intervals in both directions.
- `EventDisconnected` on the monitored gozmq peer after the libzmq side closes mid-idle.

Current lifecycle coverage includes:

- Client-side `Disconnect` teardown in both directions.
- Receiver-enforced max-message-size teardown in both directions.
- Reconnect after close/rebind in both directions.
- `LINGER` drain-vs-drop semantics in both directions.

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
