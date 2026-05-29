package socket

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// streamEngine implements iface.SocketEngine for the STREAM socket type.
//
// STREAM carries raw bytes to and from non-ZMQ peers such as an HTTP/1.1
// client, an SMTP server, or any line-oriented legacy protocol. Per design
// §9 it has "no ZMTP on the wire": the engine never sees greeting,
// handshake, or frame headers, and the bytes that arrive at the Pipe end
// are whatever the OS read(2) happened to return.
//
// Message model. Because STREAM multiplexes many peers onto one Socket,
// the engine tags every inbound message with a routing identity as its
// leading frame, and expects outbound messages to carry that same identity
// as their leading frame so the reply reaches the right peer:
//
//	Inbound  (from wire):   chunk := read(conn)
//	                        emit Msg{identity, chunk}
//	Outbound (to wire):     Msg frames := {identity, body_1, body_2, …}
//	                        write(identity->conn, body_1 || body_2 || …)
//
// Peer identity. AttachPipe honors a peer's advertised PeerIdentity if one
// is set (rare for STREAM — raw TCP peers never announce one). Otherwise
// the engine synthesizes a 5-byte identity (leading zero byte, four random
// bytes) identical in shape to ROUTER's anonymous-peer convention so the
// application code written against ROUTER works unchanged against STREAM.
//
// Zero-length-body half-close. libzmq interprets an outbound Msg whose
// body concatenates to zero bytes (either a single identity frame with no
// body frames, or identity + one empty body frame) as "close this peer."
// The engine preserves that sentinel by forwarding an empty Msg down the
// target pipe; it is the transport-adapter's job on the other side of
// the pipe to translate "empty raw-bytes frame" into conn.Close. In the
// other direction, the peer closing its side surfaces as the inbound pipe
// closing, which the engine signals via DetachPipe (no special in-band
// marker).
//
// Supported transports. STREAM is meaningful on byte-oriented transports:
// tcp, tls+tcp, and ipc (Unix domain sockets). It is NOT meaningful on
// inproc, which already enforces ZMQ message boundaries.
//
// Concurrency. AttachPipe, DetachPipe, Send, Recv, Close are all
// goroutine-safe. A single streamEngine can multiplex arbitrarily many
// peers; routing state is kept in two maps guarded by `mu`, and fair-
// queued inbound delivery is driven by a FairQueue over the attached
// pipes — the same primitive ROUTER uses.
//
// Framing invariant.
//
// STREAM bypasses ZMTP entirely, which means it CANNOT ride on the normal
// internal/session layer: that layer applies ZMTP framing to every frame
// it writes and expects ZMTP framing on every frame it reads. A STREAM
// Socket must instead attach raw Pipes sourced from a transport-level
// byte pump that forwards conn.Read bytes into the pipe as single-frame
// Msgs and drains the pipe's outbound single-frame Msgs straight to
// conn.Write.
type streamEngine struct {
	fq *FairQueue

	mu     sync.Mutex
	closed bool
	// idOf maps a pipe to the routing identity used for it. Either the
	// peer-advertised PeerIdentity (uncommon for STREAM; the transport
	// usually does not pass one) or a synthesized 5-byte identity.
	idOf map[iface.Pipe][]byte
	// pipeOf maps a routing identity (string key) back to the pipe. Last-
	// writer-wins on identity collisions; Detach clears only if the current
	// mapping still points at the pipe being detached.
	pipeOf map[string]iface.Pipe
	readyW chan struct{}

	sent atomic.Uint64
	recv atomic.Uint64
}

// NewStreamEngine returns a new STREAM socket engine.
//
// STREAM is wired end-to-end via the transport adapter in internal/stream
// (not internal/session, which is intentionally coupled to ZMTP — STREAM
// bypasses ZMTP framing and speaks raw bytes). The adapter owns the per-
// conn reader/writer goroutines and produces the iface.Pipe pairs this
// engine attaches to.
//
// Pipe boundary contract:
//   - Each inbound pipe Msg carries exactly one body frame: the raw chunk
//     read from the wire (length is whatever read(2) returned).
//   - Each outbound pipe Msg carries exactly one body frame: the bytes to
//     write to the wire verbatim. An empty body means "close the conn"
//     (STREAM half-close).
//   - The PeerInfo.Identity on the pipe may be empty; the engine
//     synthesizes one.
func NewStreamEngine() iface.SocketEngine {
	return &streamEngine{
		fq:     NewFairQueue(),
		idOf:   make(map[iface.Pipe][]byte),
		pipeOf: make(map[string]iface.Pipe),
		readyW: make(chan struct{}, 1),
	}
}

// AttachPipe registers p as a newly-connected peer. The engine records
// the peer's routing identity (p.PeerIdentity() if non-empty, otherwise a
// freshly synthesized 5-byte identity) and starts fair-queuing inbound
// messages from p. Re-attaching the same pipe is a no-op.
//
// Returns ErrSocketClosed if the engine has been Closed.
func (e *streamEngine) AttachPipe(p iface.Pipe) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return fmt.Errorf("stream attach: %w", zmqerr.ErrSocketClosed)
	}
	if _, ok := e.idOf[p]; ok {
		e.mu.Unlock()
		return nil
	}
	id := p.PeerIdentity()
	if len(id) == 0 {
		// STREAM peers are typically non-ZMQ and therefore anonymous; use
		// the same synthesized-identity shape as ROUTER so the two engines
		// address their peer tables the same way.
		id = synthesizedIdentity()
	}
	idCopy := bytes.Clone(id)
	e.idOf[p] = idCopy
	e.pipeOf[string(idCopy)] = p
	select {
	case e.readyW <- struct{}{}:
	default:
	}
	e.mu.Unlock()
	return e.fq.Attach(p)
}

// DetachPipe removes p from the routing table and the fair queue. The
// identity-to-pipe index entry is cleared only when it still points at p
// — if a reconnecting peer with the same identity displaced the entry,
// that newer mapping stays intact (matches ROUTER / libzmq semantics).
// Safe to call on an unknown pipe; in that case only the FairQueue is
// updated.
func (e *streamEngine) DetachPipe(p iface.Pipe) error {
	e.mu.Lock()
	if id, ok := e.idOf[p]; ok {
		delete(e.idOf, p)
		if cur, ok := e.pipeOf[string(id)]; ok && cur == p {
			delete(e.pipeOf, string(id))
		}
	}
	e.mu.Unlock()
	return e.fq.Detach(p)
}

// Send routes the message by its leading identity frame and writes the
// concatenation of the remaining frames (the "body") as a single raw-
// bytes frame to the peer pipe. Note that the pipe write itself is still
// a Msg, but the wire-layer peer (the raw-TCP Conn wrapper) will emit
// those bytes without ZMTP framing — that raw-bytes contract is owned by
// internal/stream.Adapter.
//
// Empty leading identity frame → ErrRouteUnknown (cannot route). Unknown
// identity → ErrRouteUnknown (STREAM is always mandatory — libzmq treats
// an unknown peer as a hard error).
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

	// Concatenate body frames. The libzmq STREAM convention is that an
	// identity-only message (no body frames, or a single zero-length body
	// frame) signals "close this peer" — we still hand a frame down so
	// the raw-stream writer sees a well-formed Msg and can apply the
	// close semantics. Bodies are concatenated so the raw stream sees
	// one byte run, matching libzmq's behavior of coalescing multipart
	// STREAM sends.
	var buf []byte
	total := 0
	for _, f := range body {
		total += len(f)
	}
	buf = make([]byte, 0, total)
	for _, f := range body {
		buf = append(buf, f...)
	}
	out := msg.FromBytes(buf)
	if err := p.Send(ctx, out); err != nil {
		return err
	}
	e.sent.Add(1)
	return nil
}

// Recv fair-queues across attached pipes and tags the returned message
// with the source peer's identity as the leading frame. The body frames
// that follow are whatever the raw-stream reader happened to enqueue —
// STREAM makes no promises that application-level message boundaries are
// preserved (it is raw bytes, after all).
func (e *streamEngine) Recv(ctx context.Context) (*msg.Msg, error) {
	m, src, err := e.fq.RecvWithSource(ctx)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	id, ok := e.idOf[src]
	e.mu.Unlock()
	if !ok {
		// Pipe detached between forward and recv; fall back to the peer's
		// advertised identity (usually empty for STREAM) so we still emit
		// a well-formed message.
		id = src.PeerIdentity()
	}
	orig := m.Frames()
	out := make([][]byte, 0, len(orig)+1)
	out = append(out, bytes.Clone(id))
	out = append(out, orig...)
	e.recv.Add(1)
	result := msg.FromFrames(out...)
	result.TransferPool(m)
	return result, nil
}

// Metrics reports the STREAM engine's counters. Dropped delegates to
// the underlying FairQueue (ctx-cancel/close drops mid-handoff); STREAM
// never drops on the send path (unknown identities surface as errors).
// QueueDepth samples the FairQueue.
func (e *streamEngine) Metrics() iface.SocketMetrics {
	return iface.SocketMetrics{
		Sent:       e.sent.Load(),
		Recv:       e.recv.Load(),
		Dropped:    e.fq.Dropped(),
		QueueDepth: e.fq.QueueDepth(),
	}
}

// Close releases engine state, unblocks any pending Recv with
// ErrSocketClosed, and releases references to every attached pipe. The
// underlying pipes themselves are NOT closed — the transport adapter that
// created them owns their lifetime. Subsequent Attach/Send/Recv calls
// return ErrSocketClosed. Close is idempotent.
func (e *streamEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.idOf = nil
	e.pipeOf = nil
	e.mu.Unlock()
	return e.fq.Close()
}

func (e *streamEngine) ReadyRecv() <-chan struct{} { return e.fq.ReadyRecv() }

func (e *streamEngine) ReadyWrite() <-chan struct{} {
	if e.CanSendNow() {
		select {
		case e.readyW <- struct{}{}:
		default:
		}
	}
	return e.readyW
}

func (e *streamEngine) CanRecvNow() bool { return e.fq.HasPending() }

func (e *streamEngine) CanSendNow() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && len(e.pipeOf) > 0
}

// Compile-time interface check.
var _ iface.SocketEngine = (*streamEngine)(nil)
var _ Pollable = (*streamEngine)(nil)
