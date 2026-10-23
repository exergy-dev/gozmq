package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"

	"github.com/exergy-dev/gozmq/internal/bufpool"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/msg"
)

// readScratchSize sizes the reader's reusable frame buffer. Frames that
// fit are read into it (no codec allocation) and user frames are then
// copied out; larger frames get their own buffer — from the pool with
// Config.RecvBufferPool, else from the codec's make — which the *Msg
// adopts without a copy.
const readScratchSize = 4 * 1024

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
		peerInfo := pipe.PeerInfo{Identity: identityFromMetadata(s.cfg.Mech)}
		s.sessionPipe, s.enginePipe = pipe.NewPair(s.cfg.RecvHWM, s.cfg.SendHWM, pipe.PeerInfo{}, peerInfo)
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
	// scratch is overwritten by every ReadFrame, so nothing aliasing it
	// may outlive a loop iteration: handleCommand copies whatever it
	// keeps, and user frames read into it are cloned below.
	scratch := make([]byte, readScratchSize)

	// frames accumulates the message in progress; it is built once the
	// last frame arrives, so a lent message costs no extra allocation.
	// lent holds its pooled frame buffers (recvPool only), which become
	// the message's lease; on an exit mid-message they are left to the
	// GC, which is always safe. Both slices are reused across messages.
	var frames, lent [][]byte
	var currentSize int64 // decoded bytes accumulated into frames
	for {
		// Honor cancellation between frames. The blocking io.Read will
		// be interrupted by Conn.Close (called from shutdown/Stop).
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		flags, body, err := s.cfg.Codec.ReadFrame(s.reader, scratch, s.recvPool)
		if err != nil {
			if !isShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: reader exit", "err", err)
			}
			return
		}

		// Decode the body through the security mechanism. For NULL/PLAIN
		// decoded is body itself; CURVE returns a fresh plaintext slice.
		plainFlags, decoded, err := s.decode(flags, body)
		if err != nil {
			s.cfg.Logger.Warn("gozmq/session: decode frame failed", "err", err)
			return
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
			s.lastInboundUnix.Store(s.cfg.Now())
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
		case len(body) <= len(scratch):
			// body was read into scratch and decoded may alias it (it
			// always does for NULL/PLAIN), so copy the frame out before
			// the next ReadFrame overwrites it.
			frame = bytes.Clone(decoded)
		case s.recvPool != nil:
			// recvPool is only set for identity framing, so frame is body,
			// the pool buffer: the codec guarantees that the body it returns
			// came from pool.Get even when it was read incrementally.
			lent = append(lent, body)
		}
		frames = append(frames, frame)

		if plainFlags&zmtp.FlagMore != 0 {
			continue
		}

		// Last frame in the logical message — publish.
		var m *msg.Msg
		if len(lent) > 0 {
			m = lendFrames(frames, lent)
			clear(lent)
			lent = lent[:0]
		} else {
			m = msg.FromFrames(frames...)
		}
		clear(frames) // the message has its own copy of the list
		frames = frames[:0]
		if err := s.publishInbound(m); err != nil {
			m.Release() // never queued: the buffers can go back now
			if !isShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: publish inbound", "err", err)
			}
			return
		}
	}
}

// lendFrames returns a lent message holding frames whose final Release
// returns the pool buffers bufs (bufs is copied; the reader reuses it).
func lendFrames(frames, bufs [][]byte) *msg.Msg {
	if len(bufs) == 1 {
		b := bufs[0]
		return msg.NewWithRelease(func() { bufpool.Put(b) }, frames...)
	}
	owned := slices.Clone(bufs)
	return msg.NewWithRelease(func() {
		for _, b := range owned {
			bufpool.Put(b)
		}
	}, frames...)
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
	// extra inbound stamp) off the common no-backpressure case.
	if s.sessionPipe.TrySend(m) {
		return nil
	}
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

// handleCommand dispatches a COMMAND-flagged body. ZMTP commands are
// length-prefixed: 1 byte name length, name bytes, then body. We
// decode just enough to pick PING/PONG/etc. Unknown commands are
// dropped silently per ZMTP 3.1 §3.
// maxPingContextLen is the ZMTP 3.1 limit on the opaque PING context.
const maxPingContextLen = 16

func (s *Session) handleCommand(body []byte) error {
	if len(body) < 1 {
		return fmt.Errorf("%w: empty command body", zmqerr.ErrMalformedFrame)
	}
	nameLen := int(body[0])
	if 1+nameLen > len(body) {
		return fmt.Errorf("%w: command name overruns body (len=%d body=%d)", zmqerr.ErrMalformedFrame, nameLen, len(body))
	}
	name := string(body[1 : 1+nameLen])
	rest := body[1+nameLen:]

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
		var flag byte = 0x01
		if name == "CANCEL" {
			flag = 0x00
		}
		body := make([]byte, 1+len(rest))
		body[0] = flag
		copy(body[1:], rest)
		if err := s.publishInbound(msg.FromBytes(body)); err != nil {
			if !isShutdownErr(err) {
				return fmt.Errorf("gozmq/session: publish subscription: %w", err)
			}
			return err
		}
		return nil
	case "DISCONNECT":
		// Peer requested an orderly shutdown. Surface as a normal exit.
		return io.EOF
	case "ERROR":
		// Treat as an auth/protocol-level failure and exit. The error
		// text (if any) is in rest after a 1-byte length prefix.
		reason := "(none)"
		if len(rest) >= 1 {
			rl := int(rest[0])
			if 1+rl <= len(rest) {
				reason = string(rest[1 : 1+rl])
			}
		}
		return fmt.Errorf("%w: peer ERROR: %s", zmqerr.ErrSocketClosed, reason)
	default:
		// READY arriving in the traffic phase is a protocol violation
		// but we tolerate unknown commands per spec; just log.
		s.cfg.Logger.Debug("gozmq/session: ignoring unknown command", "name", name)
		return nil
	}
}

// isShutdownErr returns true for errors we expect to see during a normal
// teardown (peer EOF, conn closed by Stop, ctx cancellation). Used by both
// the reader and writer loops to suppress noisy logs.
func isShutdownErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, context.Canceled) || errors.Is(err, zmqerr.ErrSocketClosed) ||
		errors.Is(err, net.ErrClosed)
}
