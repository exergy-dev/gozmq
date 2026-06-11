# gozmq

`gozmq` is a pure-Go implementation of the ZeroMQ socket patterns and the
ZMTP 3.1 wire protocol. It is CGO-free: every build works with
`CGO_ENABLED=0` (the only cgo in the repository is the optional
build-tagged libzmq interop test harness under `interop/`).

## Install

Requires Go 1.22 or newer.

```sh
go get github.com/rkm/gozmq
```

## Minimal Example

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

_ = client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("ping")))
msg, _ := server.RecvMsg(callCtx)
defer msg.Release()

fmt.Printf("%s\n", msg.Bytes())
```

## Supported Features

- Socket types: `REQ`, `REP`, `DEALER`, `ROUTER`, `PUB`, `SUB`, `XPUB`,
  `XSUB`, `PUSH`, `PULL`, `PAIR`, and `STREAM`.
- Transports: `tcp://`, `tcp6://`, `ipc://`, `inproc://`, and opt-in
  `tls+tcp://` via the transport registry.
- Security: NULL by default, PLAIN, CURVE, and RFC 27 ZAP integration.
- Polling: a `Poller` multiplexes blocking readiness across many sockets in
  the spirit of `zmq_poll` (`PollIn` / `PollOut`).
- Operations: reconnect with backoff, heartbeat, high-water marks, linger
  (cancellable via `CloseCtx`), live `Unbind` / `Disconnect`, monitor events,
  metrics, typed helpers, and build-tagged libzmq interop tests.
- Convenience: `SendBytes` / `RecvBytes` for single-frame payloads with no
  message-release bookkeeping.

## Notes

Messages returned by `Socket.RecvMsg` should be released when the caller is
finished with them:

```go
msg, err := sock.RecvMsg(ctx)
if err != nil {
	return err
}
defer msg.Release()
```

The default security mechanism is NULL. Use CURVE, or PLAIN over `tls+tcp://`,
for untrusted networks.

## Documentation

- **API reference**: [pkg.go.dev/github.com/rkm/gozmq](https://pkg.go.dev/github.com/rkm/gozmq)
  (the package documentation covers security, defaults, STREAM, polling,
  close semantics, and more).
- **Runnable demos**: the [`examples/`](examples/) directory — one
  self-contained program per pattern (PAIR, REQ/REP, PUB/SUB, PUSH/PULL,
  ROUTER/DEALER, monitoring, PLAIN, CURVE, STREAM, typed helpers).
- **Architecture**: [`design.md`](design.md) — layering, protocol,
  security, and testing strategy.
- **Interop**: `interop/` holds the build-tagged libzmq compatibility tests.
