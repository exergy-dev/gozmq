package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"unsafe"

	"github.com/exergy-dev/gozmq/internal/bufpool"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/msg"
)

// readScratchSize sizes the reader's reusable frame buffer. Frames that
// fit are read into it (no codec allocation) and user frames are then
// copied out, all of a message's frames at once into a single allocation
// (msg.FromFramesCopy); larger frames get their own buffer — from the
// pool with Config.RecvBufferPool, else from the codec's make — which the
// *Msg adopts without a copy.
const readScratchSize = 4 * 1024

// minScratchTail is the least free scratch the reader wants before
// reading the next frame of a message in progress. Below it the pending
// frames are copied out and the whole scratch reused: the codec's header
// read needs 8 spare bytes (zmtp.readHeader allocates otherwise), and a
// tail this small would push even modest frames into their own buffers.
// Must stay >= 8.
const minScratchTail = 512

// runReader is the session's reading goroutine. It performs the
// handshake, builds the pipe pair, hands the engine end to OnReady and
// starts the heartbeat, then runs the traffic-phase read loop.
//
// On a failed handshake it just exits: the writer still flushes whatever
// the handshake queued (an ERROR command, say), and the last goroutine
// out closes the conn.
func (s *Session) runReader() {
	defer s.exit()
	err := s.handshake()
	if err == nil {
		// Seed the last-inbound clock so a heartbeat fired immediately
		// after handshake doesn't trip the timeout.
		s.lastInboundUnix.Store(s.cfg.Now())
		identity := []byte(s.cfg.Mech.PeerMetadata()["Identity"])
		s.sessionPipe, s.enginePipe = pipe.NewPair(s.cfg.RecvHWM, s.cfg.SendHWM, nil, identity)
	}
	close(s.hsOut) // the writer moves on to traffic, or exits on failure
	if err != nil {
		s.hsErr = err
		return
	}

	defer s.shutdown()
	if s.cfg.OnReady != nil {
		if err := s.cfg.OnReady(s.enginePipe); err != nil {
			s.cfg.Logger.Debug("gozmq/session: pipe refused", "err", err)
			return
		}
	}
	s.attached.Store(true)
	s.startHeartbeat()
	s.readLoop()
}

// readLoop is the wire->pipe traffic phase. It loops reading frames;
// user-message frames accumulate into a *msg.Msg until MORE clears, at
// which point the message is published on the session-side pipe.
// Command frames (PING/PONG/SUBSCRIBE/CANCEL/etc.) are handled inline.
// It returns on EOF or any I/O error; runReader then shuts the session
// down so the writer exits too.
func (s *Session) readLoop() {
	// scratch holds the small frames of the message in progress, scratch
	// [:used]; each ReadFrame reads into the rest. Frames of a message
	// stay there until its last frame arrives, when msg.FromFramesCopy
	// copies them all into the message in one allocation (identity
	// mechanisms only). Nothing aliasing scratch may outlive the message:
	// handleCommand copies whatever it keeps, and the frames are copied
	// out at publish or, when scratch runs low, earlier (below).
	scratch := make([]byte, readScratchSize)
	used := 0

	// frames accumulates the message in progress; it is built once the
	// last frame arrives, so a lent message costs no extra allocation.
	// lent holds its pooled frame buffers (recvPool only), which become
	// the message's lease; on an exit mid-message they are left to the
	// GC, which is always safe. Both slices are reused across messages.
	var frames, lent [][]byte
	var currentSize int64 // decoded bytes accumulated into frames
	// Whatever exit path, wake the engine for messages published but not
	// yet flushed (the pipe close in shutdown wakes it too; this keeps the
	// bound explicit).
	defer s.flushInbound()
	for {
		// The next ReadFrame is about to block in a syscall: publish the
		// batch's wakeup and liveness stamp now. (flushReader covers the
		// case where bytes are buffered but the frame is incomplete.)
		if s.inboundOwed > 0 && s.reader.Buffered() == 0 {
			s.flushInbound()
		}
		// Out of scratch for the message in progress: copy its pending
		// frames out and start over at the front. Frames that did not
		// fit scratch already own their memory.
		if s.identityMech && len(scratch)-used < minScratchTail {
			detachFrames(frames, scratch)
			used = 0
		}
		// A blocking read ends when shutdown or Stop closes the conn.
		flags, body, err := s.cfg.Codec.ReadFrame(s.reader, scratch[used:], s.recvPool)
		if err != nil {
			if !zmqerr.IsShutdown(err) {
				s.cfg.Logger.Debug("gozmq/session: reader exit", "err", err)
			}
			return
		}

		// Decode the body through the security mechanism. NULL/PLAIN have
		// no decode step (decoded is body itself); CURVE returns fresh
		// plaintext, which the Mech contract lets the message adopt.
		plainFlags, decoded := flags, body
		if s.decode != nil {
			if plainFlags, decoded, err = s.decode(flags, body); err != nil {
				s.cfg.Logger.Warn("gozmq/session: decode frame failed", "err", err)
				return
			}
		}

		if plainFlags&zmtp.FlagCommand != 0 {
			s.lastInboundUnix.Store(s.cfg.Now())
			if cmdErr := s.handleCommand(decoded); cmdErr != nil {
				s.cfg.Logger.Warn("gozmq/session: command handling failed", "err", cmdErr)
				return
			}
			continue
		}

		// User-message frame. Accumulate until MORE clears.
		if len(frames) == 0 {
			currentSize = 0
		}
		// Enforce the logical-message size cap BEFORE appending: the
		// codec's per-frame limit does not bound a MORE chain, so without
		// this a peer could balloon memory indefinitely (remote OOM DoS).
		currentSize += int64(len(decoded))
		if s.cfg.MaxMessageSize > 0 && currentSize > s.cfg.MaxMessageSize {
			err := fmt.Errorf("%w: multipart message size %d exceeds MaxMessageSize %d",
				zmqerr.ErrMalformedFrame, currentSize, s.cfg.MaxMessageSize)
			s.cfg.Logger.Warn("gozmq/session: reader exit", "err", err)
			return
		}
		frame := decoded
		switch {
		case !s.identityMech:
			// The Mech contract makes decoded either the mechanism's own
			// memory, which the message adopts, or (part of) body, which
			// the reader reuses for the next frame.
			if aliases(decoded, body) {
				frame = bytes.Clone(decoded)
			}
		case len(decoded) == 0 || aliases(decoded, scratch):
			// The frame sits in scratch after the message's earlier
			// frames; keep the view until the message is published.
			// Empty frames count (the REQ/REP delimiter is one) and are
			// plain empty slices, which retain nothing of scratch.
			if len(decoded) == 0 {
				frame = []byte{}
			}
			used += len(decoded)
		case s.pooledRecv && len(body) <= readScratchSize:
			// Too big for the rest of scratch but within what
			// Config.RecvBufferPool promises never to lend: copy it out
			// and recycle the pool buffer.
			frame = slices.Clip(bytes.Clone(decoded))
			bufpool.Put(body)
		case s.pooledRecv:
			// Pooled bodies exist only for identity framing, so frame is
			// body, the pool buffer: the codec guarantees that the body it
			// returns came from pool.Get even when it was read incrementally.
			lent = append(lent, body)
		}
		frames = append(frames, frame)

		if plainFlags&zmtp.FlagMore != 0 {
			continue
		}

		// Last frame in the logical message — publish.
		var m *msg.Msg
		switch {
		case s.identityMech && len(lent) == 0 && allEmptyOrIn(frames, scratch):
			// Every frame is in scratch (or empty): the message and its
			// frame bytes are one allocation.
			m = msg.FromFramesCopy(frames...)
		case len(lent) > 0:
			if s.identityMech {
				detachFrames(frames, scratch)
			}
			m = lendFrames(frames, lent)
			clear(lent)
			lent = lent[:0]
		default:
			if s.identityMech {
				detachFrames(frames, scratch)
			}
			m = msg.FromFrames(frames...)
		}
		used = 0
		clear(frames) // the message has its own copy of the list
		frames = frames[:0]
		if err := s.publishInbound(m); err != nil {
			m.Release() // never queued: the buffers can go back now
			if !zmqerr.IsShutdown(err) {
				s.cfg.Logger.Debug("gozmq/session: publish inbound", "err", err)
			}
			return
		}
	}
}

// aliases reports whether sub's bytes lie inside buf's backing array.
func aliases(sub, buf []byte) bool {
	if len(sub) == 0 || cap(buf) == 0 {
		return false
	}
	p := uintptr(unsafe.Pointer(unsafe.SliceData(sub)))
	b := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	return p >= b && p < b+uintptr(cap(buf))
}

// allEmptyOrIn reports whether every frame is empty or lies inside buf.
func allEmptyOrIn(frames [][]byte, buf []byte) bool {
	for _, f := range frames {
		if len(f) > 0 && !aliases(f, buf) {
			return false
		}
	}
	return true
}

// detachFrames makes frames independent of scratch, in place: the ones
// lying in it are copied out (with capacity clipped to length), so the
// next ReadFrame cannot overwrite them.
func detachFrames(frames [][]byte, scratch []byte) {
	for i, f := range frames {
		if aliases(f, scratch) {
			frames[i] = slices.Clip(bytes.Clone(f))
		}
	}
}

// lendFrames returns a lent message holding frames whose final Release
// returns the pool buffers bufs (bufs is copied; the reader reuses it).
// One allocation for the common one-buffer message.
func lendFrames(frames, bufs [][]byte) *msg.Msg {
	return msg.NewLent(bufpool.Put, bufs, frames...)
}

// publishInbound enqueues a fully-assembled message on the session-side
// pipe, honoring backpressure (HWM) by blocking until either the engine drains a slot or
// the session is cancelled. While blocked it raises
// readerParked so the heartbeat timeout doesn't mistake local engine
// backpressure (no inbound stamps while we're parked here) for a dead
// link, and re-stamps the inbound clock on resume so the idle window
// restarts from the unpark, not from the last pre-park frame.
func (s *Session) publishInbound(m *msg.Msg) error {
	// Non-blocking fast path keeps the readerParked bookkeeping (and the
	// extra inbound stamp) off the common no-backpressure case. The wakeup
	// is deferred to flushInbound, which readLoop runs before every
	// blocking read, at most inboundFlushCap messages later, and below
	// before blocking here.
	if s.sessionPipe.TrySendDeferred(m) {
		s.inboundOwed++
		// Raw mode has no read loop to flush for it: flush per message.
		if s.inboundOwed >= inboundFlushCap || s.cfg.Raw {
			s.flushInbound()
		}
		return nil
	}
	s.flushInbound() // never block with a wakeup owed
	s.readerParked.Store(true)
	err := s.sessionPipe.Send(s.ctx, m)
	s.lastInboundUnix.Store(s.cfg.Now())
	s.readerParked.Store(false)
	if err != nil {
		// Distinguish ctx-cancel/closed-pipe (normal shutdown) from
		// genuine errors.
		if errors.Is(err, context.Canceled) || errors.Is(err, io.ErrClosedPipe) {
			return err
		}
		return fmt.Errorf("gozmq/session: publish inbound: %w", err)
	}
	return nil
}

// inboundFlushCap bounds how many published messages may wait for their
// wakeup while the reader keeps finding buffered input.
const inboundFlushCap = 256

// flushInbound publishes the wakeup owed for messages published with
// TrySendDeferred and stamps the inbound clock once for the whole read
// batch (the heartbeat only needs "traffic since", at batch granularity).
// Reader goroutine only.
func (s *Session) flushInbound() {
	if s.inboundOwed == 0 {
		return
	}
	s.inboundOwed = 0
	s.sessionPipe.Flush()
	s.lastInboundUnix.Store(s.cfg.Now())
}

// flushReader wraps the conn under the session's bufio.Reader and flushes
// before every underlying Read, which is the only place the reader can
// block waiting for the peer. That covers a partial frame in the buffer,
// where Buffered() > 0 yet the next ReadFrame still blocks.
type flushReader struct {
	s *Session
	r io.Reader
}

func (f flushReader) Read(p []byte) (int, error) {
	f.s.flushInbound()
	return f.r.Read(p)
}

// maxPingContextLen is the ZMTP 3.1 limit on the opaque PING context.
const maxPingContextLen = 16

// handleCommand dispatches a COMMAND-flagged body. Unknown commands are
// dropped silently per ZMTP 3.1 §3.
func (s *Session) handleCommand(body []byte) error {
	cmd, err := zmtp.DecodeCommand(body)
	if err != nil {
		return err
	}
	name, rest := cmd.Name, cmd.Body

	switch name {
	case "PING":
		// Echo the PING context as a PONG. PING body layout: 2-byte
		// big-endian TTL + opaque context. PONG echoes the context (no
		// TTL). The reply is handed to the writer goroutine via cmdCh
		// (see the cmdCh field doc for why the reader never writes).
		// enqueueCommand copies ctxBytes, which may alias scratch.
		// ZMTP 3.1 bounds the context at 16 bytes; a longer one is a
		// malformed PING and must not be echoed, or an authenticated peer
		// could queue MaxMessageSize-sized PONGs.
		var ctxBytes []byte
		if len(rest) >= 2 {
			ctxBytes = rest[2:]
		}
		if len(ctxBytes) > maxPingContextLen {
			return fmt.Errorf("%w: PING context %d bytes exceeds %d", zmqerr.ErrMalformedFrame, len(ctxBytes), maxPingContextLen)
		}
		s.enqueueCommand("PONG", ctxBytes)
		return nil
	case "PONG":
		// The lastInbound stamp already records this; nothing else to do.
		return nil
	case "SUBSCRIBE", "CANCEL":
		// Only a publisher's peers subscribe. For PUB/XPUB the command
		// becomes the synthetic single-frame message the engine already
		// takes from legacy peers, which send subscriptions as data: 0x01
		// (SUBSCRIBE) or 0x00 (CANCEL) followed by the prefix. Any other
		// socket type drops it, as libzmq does.
		if !s.cfg.SubscriptionCommands {
			s.cfg.Logger.Debug("gozmq/session: ignoring subscription command", "name", name)
			return nil
		}
		if err := s.publishInbound(msg.FromBytes(zmtp.SubscriptionFrame(name == "SUBSCRIBE", rest))); err != nil {
			if !zmqerr.IsShutdown(err) {
				return fmt.Errorf("gozmq/session: publish subscription: %w", err)
			}
			return err
		}
		return nil
	case "DISCONNECT":
		// Peer requested an orderly shutdown. Surface as a normal exit.
		return io.EOF
	case "ERROR":
		// Treat as an auth/protocol-level failure and exit.
		reason, rerr := zmtp.DecodeErrorReason(rest)
		if rerr != nil {
			reason = "(none)"
		}
		return fmt.Errorf("%w: peer ERROR: %s", zmqerr.ErrSocketClosed, reason)
	default:
		// READY arriving in the traffic phase is a protocol violation
		// but we tolerate unknown commands per spec; just log.
		s.cfg.Logger.Debug("gozmq/session: ignoring unknown command", "name", name)
		return nil
	}
}
