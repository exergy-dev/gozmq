// Package zmqerr holds the error sentinels returned across gozmq. It
// exists so internal packages (session, socket, transport, zmtp, security)
// can wrap these sentinels without importing the public gozmq package —
// which would create an import cycle once gozmq itself imports those
// internal packages to wire the driver.
//
// The root gozmq package re-exports every sentinel here as a variable
// alias (e.g. gozmq.ErrSocketClosed = zmqerr.ErrSocketClosed) so the two
// names refer to the same underlying error value. errors.Is works
// transparently across either reference.
package zmqerr

import (
	"context"
	"errors"
	"io"
	"net"
)

var (
	ErrContextClosed    = errors.New("gozmq: context closed")
	ErrSocketClosed     = errors.New("gozmq: socket closed")
	ErrFSMViolation     = errors.New("gozmq: socket state-machine violation")
	ErrHandshakeTimeout = errors.New("gozmq: handshake timeout")
	ErrAuthFailed       = errors.New("gozmq: authentication failed")
	ErrMalformedFrame   = errors.New("gozmq: malformed frame")
	ErrProtocolVersion  = errors.New("gozmq: unsupported ZMTP version")
	ErrRouteUnknown     = errors.New("gozmq: no route to peer")
	ErrEndpointInvalid  = errors.New("gozmq: invalid endpoint")
	ErrEndpointUnknown  = errors.New("gozmq: endpoint not connected or bound")
	ErrTransportUnknown = errors.New("gozmq: unknown transport scheme")

	// ErrNonceExhausted is returned by a CURVE mechanism when a per-direction
	// short-nonce counter reaches its maximum value and would wrap on the
	// next increment. RFC 26 expects rekeying before wrap; since gozmq does
	// not implement rekey, the session is torn down instead. Surfaces on
	// the monitor channel like other mid-traffic mechanism errors.
	ErrNonceExhausted = errors.New("gozmq: curve nonce counter exhausted")
)

// IsShutdown reports whether err is one of the errors an orderly teardown
// produces (peer EOF, a closed conn or pipe, a cancelled context, a closed
// socket or Context) rather than a fault worth surfacing or logging loudly.
func IsShutdown(err error) bool {
	return err != nil && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrSocketClosed) ||
		errors.Is(err, ErrContextClosed))
}
