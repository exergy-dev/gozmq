# gozmq

[![ci](https://github.com/exergy-dev/gozmq/actions/workflows/ci.yml/badge.svg)](https://github.com/exergy-dev/gozmq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/exergy-dev/gozmq.svg)](https://pkg.go.dev/github.com/exergy-dev/gozmq)
[![Go Report Card](https://goreportcard.com/badge/github.com/exergy-dev/gozmq)](https://goreportcard.com/report/github.com/exergy-dev/gozmq)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A pure-Go implementation of the ZeroMQ socket patterns and the ZMTP 3.1 wire
protocol. No cgo, no libzmq, no build tags — `CGO_ENABLED=0 go build` just
works, everywhere the Go toolchain does.

## Why this one

**It is actually CGO-free.** The popular Go binding, `pebbe/zmq4`, wraps
libzmq through cgo: you inherit a C build dependency, lose cross-compilation,
and pay a cgo transition on every call. `gozmq` is Go all the way down. The
only cgo in this repository is the build-tagged interop test harness that
checks us against a real libzmq.

**It talks to libzmq.** Wire compatibility is a first-class goal, not an
aspiration. CI runs the [`interop/`](interop) suite against libzmq 4.3.x in
both directions; the matrix is in [`interop/README.md`](interop/README.md).

**It is fast enough that the tradeoff is gone.** Measured two-process over
loopback against libzmq, gozmq is within about 10% of libzmq from 256 B
through 64 KB and behind on 32 B messages, where goroutine wakeups dominate.
The numbers, the method and the tools to reproduce them are in
[`perf/REPORT.md`](perf/REPORT.md); measure your own workload before you
switch if it is tiny messages at millions per second.

## Install

Requires Go 1.25 or newer.

```sh
go get github.com/exergy-dev/gozmq
```

## Hello world

```go
ctx := gozmq.NewContext()
defer ctx.Close()

server, _ := ctx.NewSocket(gozmq.Pair)
defer server.Close()
_ = server.Bind("inproc://hello")

client, _ := ctx.NewSocket(gozmq.Pair)
defer client.Close()
_ = client.Connect("inproc://hello")

callCtx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

_ = client.SendBytes(callCtx, []byte("ping"))
msg, _ := server.RecvBytes(callCtx)

fmt.Printf("%s\n", msg) // ping
```

Every blocking call takes a `context.Context` — there is no `SNDTIMEO` or
`RCVTIMEO` to configure, because cancellation and deadlines are already
Go's job.

## A pub/sub forwarder

`Proxy` is the `zmq_proxy` equivalent, and the XSUB/XPUB device is what most
deployments eventually need — publishers connect to one stable address,
subscribers to another, and either side can churn freely:

```go
xsub, _ := ctx.NewSocket(gozmq.XSub)
_ = xsub.Connect("tcp://publisher.internal:5555")

xpub, _ := ctx.NewSocket(gozmq.XPub)
_ = xpub.Bind("tcp://*:5556")

// Subscriptions travel upstream through the proxy on their own.
// Pass a fourth socket instead of nil to tap the traffic.
if err := gozmq.Proxy(ctx.Ctx(), xsub, xpub, nil); err != nil {
    log.Fatal(err)
}
```

## What's supported

- **Socket types**: `REQ`, `REP`, `DEALER`, `ROUTER`, `PUB`, `SUB`, `XPUB`,
  `XSUB`, `PUSH`, `PULL`, `PAIR`, `STREAM`.
- **Transports**: `tcp://`, `tcp6://`, `ipc://`, `inproc://`, and
  `tls+tcp://` (gozmq-specific — libzmq has no TLS transport). Third-party
  transports are a supported extension point.
- **Security**: NULL, PLAIN, CURVE, and RFC 27 ZAP.
- **Operations**: reconnect with exponential backoff and jitter, ZMTP 3.1
  heartbeats, high-water marks, cancellable linger, live `Unbind` /
  `Disconnect` / `Subscribe`, monitor events, per-socket metrics, and
  `slog` integration.
- **Ergonomics**: `Poller` for `zmq_poll`-style multiplexing, `Proxy` for
  devices, `SendBytes` / `RecvBytes` for single-frame payloads, and a
  generic [`typed`](typed) layer for codec-backed messages.

For the precise inventory — including what is *not* implemented and every
place behaviour differs from libzmq — see
**[docs/COMPATIBILITY.md](docs/COMPATIBILITY.md)**.

## Stability

Semantic versioning, pre-1.0: a 0.x minor may break the API, and every such
change is listed in `CHANGELOG.md` with a migration hint. From v1.0.0 the
compatibility promise covers everything exported outside `internal/`.

## Security defaults

The default mechanism is NULL: no authentication, no encryption, matching
libzmq. Use CURVE, or PLAIN under `tls+tcp://`, on anything but trusted
segments. The PLAIN and CURVE server/client role comes from the option
(`WithPlainAuth` / `WithPlainClient`, `WithCurveServer` / `WithCurveClient`),
not from which side binds, and a CURVE server authorizes nobody until a
`security.ZAPHandler` is installed with `gozmq.WithZAPHandler`. Message
ownership, `inproc://` and STREAM semantics are in the package
documentation and [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).

## Documentation

- **API reference** — [pkg.go.dev](https://pkg.go.dev/github.com/exergy-dev/gozmq).
  The package doc covers security, defaults, STREAM, polling, close
  semantics, and the concurrency model.
- **libzmq compatibility** — [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).
- **Runnable demos** — [`examples/`](examples), plus the `Example*`
  functions in the package tests.
- **Architecture** — [`design.md`](design.md).
- **Performance** — [`perf/REPORT.md`](perf/REPORT.md) and the libzmq-comparable
  tool in [`perf/`](perf).
- **Contributing** — [`CONTRIBUTING.md`](CONTRIBUTING.md).
- **Security policy** — [`SECURITY.md`](SECURITY.md).

## License

MIT — see [LICENSE](LICENSE).
