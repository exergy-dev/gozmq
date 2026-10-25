package gozmq

import (
	"cmp"
	"fmt"
	"log/slog"
	"time"

	"github.com/exergy-dev/gozmq/msg"
)

// This file holds the package-wide value types (SocketType, Msg) and the
// small cross-cutting Socket helpers (emit, logger, handshake-event
// classification). The Context lifecycle lives in context.go; the Socket
// type and its Send/Recv/Bind/Connect surface in socket.go; session and
// mechanism wiring (STREAM's raw-mode sessions included) in wiring.go;
// inproc in inproc.go; and the LINGER teardown in close.go.

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
	// load-balancing; exactly one peer at a time. A peer disconnect is not
	// reported: Recv delivers what the old peer left buffered, then Send
	// and Recv wait for the next peer to attach.
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

// Msg is a ZMTP multipart message. Frames delivered by RecvMsg larger than
// 4 KiB are, by default, lent from a buffer pool: valid until the message's
// final Release, then recycled (see [WithRecvBufferPool], which also
// explains how to opt out, and the msg package's "Lent frames"). Smaller
// frames, and all frames with WithRecvBufferPool(false), are ordinary Go
// memory owned by the caller, for which Release and Retain do nothing.
//
// Msg is an alias for [msg.Msg]; see that package for the full method
// set. There is no need to import it: the alias makes every method reachable
// as gozmq.Msg, and the NewMsg* constructors below cover construction (an
// empty message is NewMsgFromFrames()).
type Msg = msg.Msg

// NewMsgFromBytes wraps b in a single-frame message (no copy).
func NewMsgFromBytes(b []byte) *Msg { return msg.FromBytes(b) }

// NewMsgFromFrames wraps the provided frames in a multipart message.
func NewMsgFromFrames(frames ...[]byte) *Msg { return msg.FromFrames(frames...) }

// NewMsgFromFramesCopy returns a multipart message holding copies of the
// provided frames, so the caller may reuse them at once. Small messages
// cost a single allocation; see [msg.FromFramesCopy].
func NewMsgFromFramesCopy(frames ...[]byte) *Msg { return msg.FromFramesCopy(frames...) }

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
	return cmp.Or(s.cfg.logger, slog.Default())
}
