package socket

import (
	"context"
	"fmt"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// streamEngine implements Engine for the STREAM socket type.
//
// STREAM carries raw bytes to and from non-ZMQ peers (HTTP/1.1, SMTP, any
// line-oriented legacy protocol). There is no ZMTP on the wire:
// the engine never sees greeting, handshake, or frame headers, and inbound
// bytes are whatever read(2) returned.
//
// STREAM shares the identity-addressed core (identityRouter) with ROUTER: each
// peer gets a routing identity (synthesized when the peer is anonymous, which
// is the norm for raw TCP), inbound messages are fair-queued and stamped with
// the source identity, and outbound messages route by their leading identity
// frame. STREAM differs from ROUTER only in its Send framing (the peer's
// writer puts the body frames on the wire as one byte run) and that it is
// always mandatory.
//
// Zero-length-body half-close: libzmq treats an outbound Msg whose body
// concatenates to zero bytes as "close this peer." The engine forwards such
// a Msg down the target pipe like any other; the session translates it into
// conn.Close.
//
// STREAM peers run internal/session in raw mode (no ZMTP framing), which
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
//   - Each outbound pipe Msg is the message as sent, [identity, body...]:
//     the writer puts the concatenation of the frames after the first on
//     the wire verbatim. If they hold no bytes at all (an identity-only
//     message, or empty body frames), the Msg means "close the conn"
//     (STREAM half-close).
//   - The pipe's PeerIdentity may be empty; the engine synthesizes one.
func NewStreamEngine() Engine {
	return &streamEngine{identityRouter: newIdentityRouter("stream")}
}

// Send routes by the leading identity frame and hands m itself, identity
// frame included, to the peer pipe, whose writer sends the concatenation
// of the remaining frames as raw bytes: no per-message copy or derived
// message. On success the pipe takes m's reference; on failure it stays
// with the caller. STREAM is always mandatory: an empty or unknown
// identity, or a peer whose pipe closed before it was detached, returns
// ErrRouteUnknown.
func (e *streamEngine) Send(ctx context.Context, m *msg.Msg) error {
	if m == nil || m.NumFrames() == 0 {
		return fmt.Errorf("stream send: missing identity frame: %w", zmqerr.ErrRouteUnknown)
	}
	id := m.Frames()[0]
	p, err := e.lookup(id, "stream", true)
	if err != nil {
		return err
	}
	// Mandatory, so sendTo's sent flag adds nothing to its error.
	_, err = e.sendTo(ctx, p, m, id, "stream", true)
	return err
}

var _ Engine = (*streamEngine)(nil)
