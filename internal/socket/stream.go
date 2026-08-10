package socket

import (
	"bytes"
	"context"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// streamEngine implements iface.SocketEngine for the STREAM socket type.
//
// STREAM carries raw bytes to and from non-ZMQ peers (HTTP/1.1, SMTP, any
// line-oriented legacy protocol). Per design §9 there is "no ZMTP on the wire":
// the engine never sees greeting, handshake, or frame headers, and inbound
// bytes are whatever read(2) returned.
//
// STREAM shares the identity-addressed core (identityRouter) with ROUTER: each
// peer gets a routing identity (synthesized when the peer is anonymous, which
// is the norm for raw TCP), inbound messages are fair-queued and stamped with
// the source identity, and outbound messages route by their leading identity
// frame. STREAM differs from ROUTER only in its Send framing (it concatenates
// the body frames into one raw byte run) and that it has no post-detach cache
// and is always mandatory.
//
// Zero-length-body half-close: libzmq treats an outbound Msg whose body
// concatenates to zero bytes as "close this peer." The engine forwards an empty
// Msg down the target pipe; the transport adapter translates that into
// conn.Close.
//
// STREAM cannot ride the normal internal/session layer (that layer applies ZMTP
// framing); it attaches raw Pipes sourced from internal/stream.Adapter, which
// pumps conn.Read bytes into the pipe and drains outbound Msgs to conn.Write.
//
// DIVERGENCE from libzmq: ZMQ_STREAM_NOTIFY (default-on in libzmq) makes a
// libzmq STREAM socket deliver a synthetic [identity, empty] message pair on
// every peer connect AND disconnect, so applications can track peer lifetime
// from the recv stream alone. This engine does NOT emit those notifications —
// Recv only ever yields real wire bytes. Implementing them would change recv
// semantics that existing root-level tests (and users) depend on; doing it
// compatibly needs a root-level socket option to opt in, which is out of this
// package's hands. Track peer lifetime via socket monitor events instead.
type streamEngine struct {
	identityRouter
}

// NewStreamEngine returns a new STREAM socket engine.
//
// Pipe boundary contract:
//   - Each inbound pipe Msg carries one body frame: the raw chunk read from
//     the wire.
//   - Each outbound pipe Msg carries one body frame: the bytes to write
//     verbatim. An empty body means "close the conn" (STREAM half-close).
//   - The pipe's PeerIdentity may be empty; the engine synthesizes one.
func NewStreamEngine() iface.SocketEngine {
	// detachCache=false: STREAM has no post-detach identity grace cache.
	return &streamEngine{identityRouter: newIdentityRouter("stream", false)}
}

// Send routes by the leading identity frame and writes the concatenation of the
// remaining frames as a single raw-bytes frame to the peer pipe. STREAM is
// always mandatory: an empty or unknown identity returns ErrRouteUnknown.
func (e *streamEngine) Send(ctx context.Context, m *msg.Msg) error {
	if m == nil || m.NumFrames() == 0 {
		return fmt.Errorf("stream send: missing identity frame: %w", zmqerr.ErrRouteUnknown)
	}
	frames := m.Frames()
	id := frames[0]
	body := frames[1:]

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("stream send: %w", zmqerr.ErrSocketClosed)
	}
	p, ok := e.pipeOf[string(id)]
	e.mu.Unlock()

	if !ok {
		return fmt.Errorf("stream send: identity %q: %w", id, zmqerr.ErrRouteUnknown)
	}

	// Concatenate body frames into one byte run (matching libzmq's coalescing
	// of multipart STREAM sends). An empty result is the half-close sentinel,
	// still handed down as a well-formed Msg for the writer to act on.
	out := msg.FromBytes(bytes.Join(body, nil))
	if err := p.Send(ctx, out); err != nil {
		return err
	}
	e.sent.Add(1)
	return nil
}

// Metrics reports the STREAM counters. Dropped delegates entirely to the
// FairQueue (ctx-cancel/close drops); STREAM never drops on Send (unknown
// identities surface as errors).
func (e *streamEngine) Metrics() iface.SocketMetrics {
	return e.metrics(0)
}

var (
	_ iface.SocketEngine = (*streamEngine)(nil)
	_ Pollable           = (*streamEngine)(nil)
)
