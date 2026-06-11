package gozmq

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// This file holds the package-wide value types (SocketType, Msg) and the
// small cross-cutting Socket helpers (emit, logger, handshake-event
// classification). The Context lifecycle lives in context.go; the Socket
// type and its Send/Recv/Bind/Connect surface in socket.go; session and
// mechanism wiring in wiring.go; the inproc direct-pipe bypass in
// directpipe.go; and the LINGER teardown in close.go.

// SocketType enumerates the ZMTP socket patterns.
type SocketType uint8

const (
	// Req is the lock-step request half of REQ/REP: strictly alternating
	// Send then Recv, load-balanced across connected REP/ROUTER peers.
	Req SocketType = iota + 1
	// Rep is the lock-step reply half of REQ/REP: strictly alternating
	// Recv then Send, answering the peer the request arrived from.
	Rep
	// Dealer is the asynchronous REQ generalization: sends load-balance
	// across peers, receives fair-queue, no Send/Recv ordering rules.
	Dealer
	// Router is the asynchronous REP generalization: each received
	// message is prefixed with the peer's identity frame, and Send routes
	// by leading identity frame (see WithRouterMandatory).
	Router
	// Pub fans every sent message out to all subscribed peers; slow
	// subscribers at HWM are dropped per message. Pub never receives.
	Pub
	// Sub receives messages matching subscribed prefixes (see
	// Socket.Subscribe / WithSubscribe). Sub never sends.
	Sub
	// XPub is Pub plus visibility: subscription SUBSCRIBE/CANCEL messages
	// from peers surface via Recv (see WithXPubVerbose).
	XPub
	// XSub is Sub plus manual control: subscriptions are sent downstream
	// as explicit messages, enabling proxy topologies.
	XSub
	// Push pipelines sent messages round-robin across connected Pull
	// peers. Push never receives.
	Push
	// Pull fair-queues messages from connected Push peers. Pull never
	// sends.
	Pull
	// Pair is an exclusive 1:1 bidirectional link with no routing or
	// load-balancing; exactly one peer at a time.
	Pair
	// Stream exchanges raw TCP bytes with non-ZMQ peers: no ZMTP
	// handshake, framing, or security. Messages are [identity, payload]
	// pairs. See the Socket.Connect doc for its single-dial semantics.
	Stream
)

// socketTypeNames maps a SocketType to its ZMTP "Socket-Type" metadata
// value, indexed by the constant value.
var socketTypeNames = [...]string{
	Req:    "REQ",
	Rep:    "REP",
	Dealer: "DEALER",
	Router: "ROUTER",
	Pub:    "PUB",
	Sub:    "SUB",
	XPub:   "XPUB",
	XSub:   "XSUB",
	Push:   "PUSH",
	Pull:   "PULL",
	Pair:   "PAIR",
	Stream: "STREAM",
}

func socketTypeName(t SocketType) string {
	if int(t) < len(socketTypeNames) && socketTypeNames[t] != "" {
		return socketTypeNames[t]
	}
	return fmt.Sprintf("SocketType(%d)", uint8(t))
}

// String implements fmt.Stringer using the ZMTP "Socket-Type" metadata
// names; unknown values render as "SocketType(n)".
func (t SocketType) String() string { return socketTypeName(t) }

// Msg is a ZMTP multipart message. Frames delivered by RecvMsg are
// caller-owned and safe to retain, stash, or pass between goroutines.
//
// Some large inbound frames are backed by buffers drawn from an internal
// size-tiered pool; call (*Msg).Release when finished to return those
// buffers to the pool. Release is always safe to call (it is a no-op for
// *Msg values that carry no pooled frames, including any *Msg you
// constructed via NewMsg / NewMsgFromBytes / NewMsgFromFrames), so the
// safest pattern is to call Release on every received *Msg once done.
// A safety-net finalizer reclaims pooled buffers for callers that drop
// a *Msg without calling Release, but it is not the primary path.
//
// Msg is an alias for an internal type, so its method set does not
// render on this page. The SUPPORTED methods — the only ones covered by
// the compatibility promise — are:
//
//	Frames() [][]byte        // all frames, by reference
//	NumFrames() int          // frame count
//	Bytes() []byte           // sole frame; panics on multipart
//	SingleFrame() ([]byte, bool)
//	TotalSize() int          // sum of frame lengths
//	AppendFrame(b []byte)    // append by reference (no copy)
//	Retain()                 // add a reference for cross-goroutine shares
//	Release()                // drop a reference; last one recycles pools
//	IsSubscription() bool    // XPUB: SUBSCRIBE/CANCEL control message
//
// Any other exported method visible on the underlying type is internal
// plumbing (pool ownership transfer between engine layers) and may
// change or disappear in any release.
type Msg = msg.Msg

// NewMsg returns an empty message.
func NewMsg() *Msg { return msg.New() }

// NewMsgFromBytes wraps b in a single-frame message (no copy).
func NewMsgFromBytes(b []byte) *Msg { return msg.FromBytes(b) }

// NewMsgFromFrames wraps the provided frames in a multipart message.
func NewMsgFromFrames(frames ...[]byte) *Msg { return msg.FromFrames(frames...) }

// monitorState bundles the monitor channel and event mask so they can be
// swapped atomically without holding a mutex on the hot emit() path.
type monitorState struct {
	ch   chan<- Event
	mask EventType
}

// emit publishes a lifecycle Event on the socket's configured monitor
// channel, non-blocking. Delivery is best-effort: if the channel is
// full we drop the event because backpressuring the socket's hot path on
// a slow consumer is never the right call — monitor channels are
// user-controlled and a misconfigured consumer must not stall Send/Recv.
//
// The monitor mask filters by event type bitmask. Events whose Type
// bit is not set in cfg.monitorMask are discarded silently (and not
// counted as drops — they were never intended for delivery).
func (s *Socket) emit(eventType EventType, endpoint, reason string) {
	ms := s.monitorCfg.Load()
	if ms == nil || ms.ch == nil {
		return
	}
	if ms.mask&eventType == 0 {
		return
	}
	ev := Event{
		Type:     eventType,
		Endpoint: endpoint,
		Time:     time.Now(),
		Reason:   reason,
	}
	select {
	case ms.ch <- ev:
	default:
	}
}

// logger returns the socket's configured logger, falling back to
// slog.Default when none was installed via WithLogger. Centralising
// the fallback keeps the call sites terse.
func (s *Socket) logger() *slog.Logger {
	if s.cfg.logger != nil {
		return s.cfg.logger
	}
	return slog.Default()
}

// classifyHandshakeEvent maps a handshake error to the appropriate
// Event type. Errors wrapping ErrAuthFailed surface as
// EventHandshakeFailedAuth; errors wrapping ErrMalformedFrame or
// ErrProtocolVersion surface as EventHandshakeFailedProtocol. Any
// other error (dial drop mid-handshake, timeout) also falls under
// protocol failure by convention — the user's monitor consumer can
// inspect Reason for finer classification.
func classifyHandshakeEvent(err error) EventType {
	if errors.Is(err, zmqerr.ErrAuthFailed) {
		return EventHandshakeFailedAuth
	}
	// ErrMalformedFrame, ErrProtocolVersion, and any other error (dial drop
	// mid-handshake, timeout) all classify as a protocol failure.
	return EventHandshakeFailedProtocol
}
