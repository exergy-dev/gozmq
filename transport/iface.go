package transport

import (
	"context"
	"io"
	"time"
)

// Transport turns an endpoint string into dial/listen operations for a
// single scheme (tcp, ipc, inproc, tls+tcp).
//
// The implementations in this package cover every scheme gozmq ships. A
// Transport supplied by an application is registered on a Context with
// (*gozmq.Context).RegisterTransport, which is how the TLS transport — the
// one built-in that needs caller-supplied configuration — is installed:
//
//	tr, err := transport.NewTLSTransport(tlsCfg)
//	if err != nil {
//		return err
//	}
//	ctx := gozmq.NewContext()
//	ctx.RegisterTransport(tr)
//	sock, _ := ctx.NewSocket(gozmq.Router)
//	_ = sock.Bind("tls+tcp://0.0.0.0:5555")
//
// The same hook accepts an out-of-tree implementation, so a scheme gozmq
// does not ship (QUIC, WebSocket, a test harness that injects faults) can be
// wired in without forking. Such a Transport carries ZMTP frames end to end
// exactly as tcp:// does — sessions, security mechanisms, and socket
// patterns are all unaffected by the scheme underneath.
type Transport interface {
	// Scheme reports the URL scheme this transport handles, without the "://"
	// suffix (e.g. "tcp", "inproc").
	Scheme() string

	// Dial connects to endpoint and returns a Conn ready for ZMTP framing.
	// Respects ctx cancellation and deadline.
	Dial(ctx context.Context, endpoint string) (Conn, error)

	// Listen binds to endpoint and returns a Listener. For wildcard
	// endpoints (e.g. "tcp://*:0") the returned Listener's Endpoint()
	// reports the resolved address.
	Listen(ctx context.Context, endpoint string) (Listener, error)
}

// Conn is a single ZMTP connection underneath the session layer. Read and
// Write deliver raw bytes; the session layer applies framing and security
// codecs on top.
//
// A net.Conn satisfies everything here except the two endpoint accessors,
// so wrapping one is the usual way to implement this interface.
type Conn interface {
	io.Reader
	io.Writer
	io.Closer

	// LocalEndpoint returns the local side in ZMQ string form (e.g.
	// "tcp://127.0.0.1:5555"). May be empty for inproc.
	LocalEndpoint() string
	// RemoteEndpoint returns the peer in ZMQ string form.
	RemoteEndpoint() string

	// SetDeadline matches net.Conn's semantic. The session layer uses it
	// to bound the handshake; per-direction deadlines are not part of the
	// contract.
	SetDeadline(t time.Time) error
}

// Listener accepts incoming ZMTP connections on a bound endpoint.
type Listener interface {
	// Accept blocks until an incoming connection arrives or ctx cancels.
	//
	// Errors: once the listener is closed, Accept must return an error
	// matching net.ErrClosed (errors.Is); when ctx ends, ctx.Err(). Those
	// are the only terminal errors. Anything else is treated as a failure
	// of one connection attempt (EMFILE, a failed socket option, a peer
	// that reset before accept): a socket reports it as EventAcceptFailed
	// and calls Accept again after a short, capped backoff. An
	// implementation whose Accept keeps failing after Close without
	// matching net.ErrClosed therefore keeps its socket retrying.
	//
	// Accept should not perform per-connection protocol work such as a TLS
	// handshake: the socket accepts sequentially, so a slow peer would stall
	// every connection queued behind it. Defer such work to the Conn's first
	// Read or Write, which the session bounds with SetDeadline.
	Accept(ctx context.Context) (Conn, error)
	// Endpoint returns the actual bound endpoint (wildcards resolved).
	Endpoint() string
	// Close stops accepting and unblocks in-flight Accept calls.
	Close() error
}
