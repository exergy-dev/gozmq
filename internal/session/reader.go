package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
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

	var current *msg.Msg
	for {
		// Honor cancellation between frames. The blocking io.Read will
		// be interrupted by Conn.Close (called from shutdown/Stop).
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		flags, body, err := s.cfg.Codec.ReadFrame(s.reader, nil)
		if err != nil {
			if !isShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: reader exit", "err", err)
			}
			// body is either nil on the header-error paths or a
			// partially-filled pool buffer on the body-read path; in
			// the latter case we can return it since no one holds it.
			msg.PoolPut(body)
			return
		}

		// Decode the body through the security mechanism. For NULL/PLAIN
		// (identity) decoded aliases body and the same pool buffer will
		// back the user Msg; for CURVE decoded is a fresh slice produced
		// by box.Open and body is scratch to return to the pool now.
		plainFlags, decoded, err := s.decode(flags, body)
		if err != nil {
			s.cfg.Logger.Warn("gozmq/session: decode frame failed", "err", err)
			msg.PoolPut(body)
			return
		}

		if plainFlags&iface.FlagCommand != 0 {
			// handleCommand is synchronous and does not retain decoded
			// past return (SUBSCRIBE/CANCEL copy into a fresh slice
			// before publishing), so PoolPut on body is safe whether
			// decoded aliased body or not.
			s.lastInboundUnix.Store(monoNow())
			cmdErr := s.handleCommand(decoded)
			msg.PoolPut(body)
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
		//     pool buffer. Attach as an ordinary heap frame and PoolPut
		//     the ciphertext scratch.
		if current == nil {
			s.lastInboundUnix.Store(monoNow())
			current = msg.New()
		}
		switch {
		case len(decoded) > largeFrameThreshold && s.direct != nil:
			current.AppendPooledFrame(decoded, body)
		default:
			frame := bytes.Clone(decoded)
			current.AppendFrame(frame)
			msg.PoolPut(body)
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
// drains a slot or the session is cancelled.
func (s *sessionImpl) publishInbound(m *msg.Msg) error {
	if err := s.sessionPipe.Send(s.ctx, m); err != nil {
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
		// TTL).
		var ctxBytes []byte
		if len(rest) >= 2 {
			ctxBytes = rest[2:]
		}
		return s.sendCommand("PONG", ctxBytes)
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

// sendCommand writes a COMMAND-flagged frame for one of the
// session-issued ZMTP commands (PONG, PING). It serialises the standard
// 1-byte-name-length + name + body envelope and pushes through the
// codec/mechanism path identically to user data.
func (s *sessionImpl) sendCommand(name string, body []byte) error {
	if len(name) > 255 {
		return fmt.Errorf("gozmq/session: command name %q too long", name)
	}
	buf := make([]byte, 0, 1+len(name)+len(body))
	buf = append(buf, byte(len(name)))
	buf = append(buf, name...)
	buf = append(buf, body...)

	if err := s.writeFrame(iface.FlagCommand, buf); err != nil {
		return fmt.Errorf("gozmq/session: write command %s: %w", name, err)
	}
	return nil
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
