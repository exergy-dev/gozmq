# gozmq

`gozmq` is a pure-Go implementation of the ZeroMQ socket patterns and the
ZMTP 3.1 wire protocol. It has no CGO dependency in normal builds and is
intended to build with `CGO_ENABLED=0`.

## Install

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
- Operations: reconnect, heartbeat, high-water marks, linger, monitor events,
  metrics, typed helpers, and build-tagged libzmq interop tests.

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

See `doc.go` for full package documentation, `examples/` for runnable demos,
and `interop/` for libzmq compatibility tests.
