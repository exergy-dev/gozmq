// Package pipeiface holds the pipe-layer contracts: the bidirectional
// message queue that connects a session (or a direct inproc bypass) to a
// socket engine, plus the direct-pipe handoff types the inproc transport
// implements.
//
// It is a leaf package so that both internal/iface and the public transport
// package can depend on it without a cycle: transport needs the direct-pipe
// types for its inproc bypass, while transport itself is where the public
// Transport / Conn / Listener interfaces live.
package pipeiface

import (
	"context"

	"github.com/exergy-dev/gozmq/msg"
)

// Pipe is one end of a bidirectional bounded message queue. A Session and
// a SocketEngine sit on opposite ends; a Send on one end surfaces as a
// Recv on the other. Each direction has its own high-water mark.
type Pipe interface {
	// Send enqueues m for the other end. Blocks until space is available or
	// ctx cancels. Returns ErrHWM is NOT used by Pipe directly — HWM
	// behavior is expressed as a blocking Send; non-blocking behavior is
	// achieved by passing a ctx with zero deadline.
	Send(ctx context.Context, m *msg.Msg) error

	// Recv dequeues a message posted by the other end. Blocks until one is
	// available or ctx cancels.
	Recv(ctx context.Context) (*msg.Msg, error)

	// Close terminates this end of the pipe. Messages already in flight may
	// still be delivered; the other end sees io.EOF on subsequent Recv.
	Close() error

	// Closed returns a channel that closes when either end has been Closed.
	Closed() <-chan struct{}

	// PeerIdentity returns the ZMTP Identity advertised by the peer in its
	// READY metadata. Empty when the peer did not set one.
	PeerIdentity() []byte
}

// DirectPipeOffer represents a pre-created pipe pair offered to the bind-side
// socket when the dialer detects that session-layer bypass is possible (e.g.
// inproc transport within the same Context). The bind side calls Accept on its
// DirectPipeAcceptor interface to receive these offers instead of raw Conns.
type DirectPipeOffer struct {
	// BindPipe is the pipe end for the bind-side (server) engine.
	BindPipe Pipe
	// DialerSocketType is the ZMTP Socket-Type name of the dialing socket
	// (e.g. "PUSH", "SUB") for compatibility checking.
	DialerSocketType string
	// Endpoint is the resolved endpoint string (e.g. "inproc://foo").
	Endpoint string
}

// DirectPipeAcceptor is an optional interface implemented by Listeners that
// support session-layer bypass. When detected by Socket.Connect, the dialer
// pushes a DirectPipeOffer through the acceptor instead of going through the
// normal Dial->Conn->Session path. Socket.acceptLoop detects offers arriving
// via AcceptDirectPipe and calls engine.AttachPipe directly, skipping ZMTP
// greeting, handshake, and codec.
type DirectPipeAcceptor interface {
	// AcceptDirectPipe blocks until a direct-pipe offer arrives or ctx cancels.
	AcceptDirectPipe(ctx context.Context) (*DirectPipeOffer, error)
	// OfferDirectPipe pushes an offer to the bind side. Blocks if the accept
	// buffer is full.
	OfferDirectPipe(offer *DirectPipeOffer) error
}

// DirectPipeDialer is an optional interface implemented by transports that
// support direct-pipe bypass (session-layer skip). The Socket layer probes
// for this interface at Connect time; when present and the listener is
// already bound, it uses the direct path instead of Dial+Session.
type DirectPipeDialer interface {
	// LookupDirectAcceptor returns the DirectPipeAcceptor for endpoint, or
	// nil when the endpoint is not yet bound (in which case the caller falls
	// back to the normal Dial path).
	LookupDirectAcceptor(endpoint string) DirectPipeAcceptor
}
