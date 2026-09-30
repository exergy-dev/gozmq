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
aspiration. CI runs the [`interop/`](interop) suite against libzmq in both
directions, covering every socket pattern, NULL/PLAIN/CURVE, and ZAP.

**It is fast enough that the tradeoff is gone.** Measured two-process against
libzmq 4.3.4, `gozmq` is *ahead* from 256 B through 8 KB:

| Message size | gozmq | libzmq 4.3.4 | ratio |
| :--- | ---: | ---: | ---: |
| 32 B | 5.70M msg/s | 9.96M msg/s | 0.57× |
| 256 B | 4.43M msg/s | 4.03M msg/s | **1.10×** |
| 1 KB | 2.26M msg/s | 2.06M msg/s | **1.10×** |
| 8 KB | 0.51M msg/s | 0.48M msg/s | **1.06×** |
| 64 KB | 43.2 Gb/s | 48.4 Gb/s | 0.89× |

The 32 B gap is real and is scheduler-bound — goroutine wakeups, roughly 36%
of CPU in a profile there — not something more allocation tuning fixes. If
your workload is tiny messages at millions per second, measure before you
switch. Full methodology and the complete matrix are in
[`perf/REPORT.md`](perf/REPORT.md).

## Install

Requires Go 1.22 or newer.

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

`gozmq` follows semantic versioning. From v1.0.0, everything exported outside
`internal/` is covered by the compatibility promise, with one documented
exception: the methods in the [`msg`](msg) package marked *"Internal plumbing
— NOT covered by the compatibility promise"*, which are exported only so
gozmq's own packages can reach them across a boundary.

Breaking changes wait for a major version. Additions may land in any minor.
`CHANGELOG.md` records everything user-visible.

## Notes

**Message ownership.** Frames returned by `RecvMsg` are ordinary Go memory
that belongs to you: keep them, slice them, or re-send the message. There is
nothing to release; `Msg.Release` and `Msg.Retain` are no-ops kept for
compatibility.

**Security defaults.** The default mechanism is NULL: no authentication, no
encryption, matching libzmq. That is fine for `inproc`, `ipc`, and trusted
network segments. For anything else use CURVE, or PLAIN under `tls+tcp://`.

## Documentation

- **API reference** — [pkg.go.dev](https://pkg.go.dev/github.com/exergy-dev/gozmq).
  The package doc covers security, defaults, STREAM, polling, close
  semantics, and the concurrency model.
- **libzmq compatibility** — [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).
- **Runnable demos** — [`examples/`](examples), one self-contained program
  per pattern.
- **Architecture** — [`design.md`](design.md): layering, protocol, security,
  and test strategy.
- **Performance** — [`perf/REPORT.md`](perf/REPORT.md) and the libzmq-comparable
  CLI tools in [`perf/`](perf).
- **Contributing** — [`CONTRIBUTING.md`](CONTRIBUTING.md).
- **Security policy** — [`SECURITY.md`](SECURITY.md).

## License

MIT — see [LICENSE](LICENSE).
