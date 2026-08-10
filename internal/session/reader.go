package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"unsafe"

	"github.com/exergy-dev/gozmq/internal/bufpool"
	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// largeFrameThreshold picks the boundary between the always-copy receive
// path (simple, cheap) and the refcounted zero-copy path (the pool buffer
// is attached to the *Msg and returned via Release). 4 KiB lets memcpy
// at ~20 GB/s amortise the refcount bookkeeping; below it the copy is
// cheaper than the bookkeeping plus the consumer's mandatory Release.
const largeFrameThreshold = 4 * 1024

// runReader is the wire->pipe traffic-phase goroutine. It loops reading
// frames; user-message frames accumulate into a *msg.Msg until MORE
// clears, at which point the message is published on the session-side
// pipe. Command frames (PING/PONG/SUBSCRIBE/CANCEL/etc.) are handled
// inline.
//
// On EOF or any I/O error it triggers a session-wide shutdown so the
// writer and heartbeat goroutines exit cleanly.
func (s *sessionImpl) runReader() {
	defer s.wg.Done()
	defer s.shutdown()

	// scratch is a persistent dst handed to ReadFrame, sized to EXACTLY
	// largeFrameThreshold. The size is the invariant: any frame that fits
	// in scratch is, by definition, at or below the threshold and thus on
	// the always-copy delivery path below, so scratch-backed bytes never
	// outlive one loop iteration — small user frames are bytes.Clone'd
	// into the Msg, and command bodies are fully consumed by handleCommand
	// (PING/SUBSCRIBE/CANCEL copy whatever they keep) before the next
	// ReadFrame overwrites scratch. Frames larger than the threshold
	// exceed scratch's capacity, so the codec draws them from the frame
	// pool and the zero-copy AppendPooledFrame path is unaffected.
	//
	// scratch itself must NEVER be returned to the frame pool (its 4 KiB
	// capacity matches a pool tier, so bufpool.Put would accept it and a later
	// bufpool.Get could hand it out while the reader still overwrites it on
	// every frame): putBody identity-checks every return site.
	scratch := make([]byte, largeFrameThreshold)
	scratchPtr := unsafe.SliceData(scratch)
	putBody := func(body []byte) {
		if unsafe.SliceData(body) != scratchPtr {
			bufpool.Put(body)
		}
	}

	var current *msg.Msg
	var currentSize int64 // decoded bytes accumulated into current
	for {
		// Honor cancellation between frames. The blocking io.Read will
		// be interrupted by Conn.Close (called from shutdown/Stop).
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		flags, body, err := s.cfg.Codec.ReadFrame(s.reader, scratch)
		if err != nil {
			if !isShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: reader exit", "err", err)
			}
			// body is nil on the header-error paths or a partially-filled
			// buffer on the body-read path — scratch (skipped by putBody)
			// or a pool buffer we can return since no one holds it.
			putBody(body)
			return
		}

		// Decode the body through the security mechanism. For NULL/PLAIN
		// (identity) decoded aliases body and the same pool buffer will
		// back the user Msg; for CURVE decoded is a fresh slice produced
		// by box.Open and body is scratch to return to the pool now.
		plainFlags, decoded, err := s.decode(flags, body)
		if err != nil {
			s.cfg.Logger.Warn("gozmq/session: decode frame failed", "err", err)
			putBody(body)
			return
		}

		if plainFlags&iface.FlagCommand != 0 {
			// handleCommand is synchronous and does not retain decoded
			// past return (SUBSCRIBE/CANCEL copy into a fresh slice
			// before publishing), so releasing/reusing body is safe
			// whether decoded aliased body (or scratch) or not.
			s.lastInboundUnix.Store(s.cfg.Now())
			cmdErr := s.handleCommand(decoded)
			putBody(body)
			if cmdErr != nil {
				s.cfg.Logger.Warn("gozmq/session: command handling failed", "err", cmdErr)
				return
			}
			continue
		}

		// User-message frame. Accumulate until MORE clears.
		//
		// Two delivery paths, chosen by frame size:
		//
		//   - Small (<= largeFrameThreshold): always-copy. The decoded
		//     bytes are copied into a heap-owned frame and the pool
		//     buffer is returned immediately. The extra memcpy is
		//     cheaper than the refcount bookkeeping plus the
		//     consumer's mandatory Release for small frames.
		//
		//   - Large (> largeFrameThreshold) with an identity mechanism
		//     (NULL/PLAIN): attach the pool buffer to the Msg via
		//     AppendPooledFrame, which sets refs=1. The consumer
		//     (Socket.RecvMsg user) returns the buffer to the pool by
		//     calling *Msg.Release when done; a runtime finalizer is
		//     the safety net for callers that drop the *Msg without
		//     Release.
		//
		//   - Large CURVE: decoded is a fresh box.Open slice, not a
		//     pool buffer. Attach as an ordinary heap frame and bufpool.Put
		//     the ciphertext scratch.
		if current == nil {
			s.lastInboundUnix.Store(s.cfg.Now())
			current = msg.New()
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
			msg.Recycle(current)
			putBody(body)
			return
		}
		switch {
		case len(decoded) > largeFrameThreshold && s.identityMech:
			// body cannot alias scratch here: an identity-decoded frame
			// larger than the threshold did not fit in scratch's exactly-
			// threshold capacity, so the codec drew it from the pool.
			current.AppendPooledFrame(decoded, body)
		default:
			frame := bytes.Clone(decoded)
			current.AppendFrame(frame)
			putBody(body)
		}

		if plainFlags&iface.FlagMore != 0 {
			continue
		}

		// Last frame in the logical message — publish.
		if err := s.publishInbound(current); err != nil {
			if !isShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: publish inbound", "err", err)
			}
			return
		}
		current = nil
	}
}

// publishInbound enqueues a fully-assembled message on the session-side
// pipe, honoring backpressure (HWM) by blocking until either the engine
// drains a slot or the session is cancelled. While blocked it raises
// readerParked so the heartbeat timeout doesn't mistake local engine
// backpressure (no inbound stamps while we're parked here) for a dead
// link, and re-stamps the inbound clock on resume so the idle window
// restarts from the unpark, not from the last pre-park frame.
func (s *sessionImpl) publishInbound(m *msg.Msg) error {
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
func (s *sessionImpl) handleCommand(body []byte) error {
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
		// TTL). The reply is handed to the writer goroutine via cmdCh —
		// writing inline here would take writeMu on the read goroutine
		// and deadlock against a writer parked in conn.Write.
		// enqueueCommand copies ctxBytes, which aliases the pooled body.
		var ctxBytes []byte
		if len(rest) >= 2 {
			ctxBytes = rest[2:]
		}
		s.enqueueCommand("PONG", ctxBytes)
		return nil
	case "PONG":
		// The lastInbound stamp already records this; nothing else to do.
		return nil
	case "SUBSCRIBE", "CANCEL":
		// Surface subscription commands to the engine as a synthetic
		// single-frame in-band message where the first byte is 0x01
		// (SUBSCRIBE) or 0x00 (CANCEL) followed by the prefix bytes. This
		// mirrors the exact shape libzmq's XPUB surfaces subscription
		// events to its application, so PUB/XPUB engines can reuse one
		// intercept path for both wire-origin and test-origin events.
		var flag byte = 0x01
		if name == "CANCEL" {
			flag = 0x00
		}
		body := make([]byte, 1+len(rest))
		body[0] = flag
		copy(body[1:], rest)
		synth := msg.New()
		synth.AppendFrame(body)
		// Flag this as a genuine subscription command so a SUB/XSUB
		// receiver distinguishes it from ordinary data that merely
		// begins with 0x01/0x00. PUB/XPUB still classify by content
		// (legacy peers send subscriptions as plain data frames).
		synth.MarkSubscription()
		if err := s.publishInbound(synth); err != nil {
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
