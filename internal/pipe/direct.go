package pipe

import "context"

// DirectOffer is a pre-created pipe end offered to a bind-side socket when
// the dialer can skip the session layer (inproc within one Context). The
// bind side receives it from its DirectAcceptor instead of a raw Conn.
type DirectOffer struct {
	// BindPipe is the pipe end for the bind-side (server) engine.
	BindPipe *End
	// DialerSocketType is the ZMTP Socket-Type name of the dialing socket
	// (e.g. "PUSH", "SUB") for compatibility checking.
	DialerSocketType string
	// Endpoint is the resolved endpoint string (e.g. "inproc://foo").
	Endpoint string
}

// DirectAcceptor is implemented by Listeners that support the session
// bypass. Socket.Connect pushes a DirectOffer through it instead of going
// Dial→Conn→Session, and the bind side's accept loop attaches the offered
// pipe straight to its engine, skipping greeting, handshake and codec.
type DirectAcceptor interface {
	// AcceptDirectPipe blocks until an offer arrives or ctx ends.
	AcceptDirectPipe(ctx context.Context) (*DirectOffer, error)
	// OfferDirectPipe pushes an offer to the bind side, blocking while the
	// accept queue is full until it drains, the listener closes, or ctx
	// ends.
	OfferDirectPipe(ctx context.Context, offer *DirectOffer) error
}

// DirectDialer is implemented by transports that support the session
// bypass. Socket.Connect probes for it; when the endpoint is already bound
// it uses the direct path instead of Dial+Session.
type DirectDialer interface {
	// LookupDirectAcceptor returns the DirectAcceptor for endpoint, or nil
	// when the endpoint is not yet bound (the caller then falls back to
	// the normal Dial path).
	LookupDirectAcceptor(endpoint string) DirectAcceptor
}
