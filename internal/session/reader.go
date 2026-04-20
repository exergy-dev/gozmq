package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
	"unsafe"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmqerr"
)

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
	defer func() {
		if current != nil {
			current.Release()
		}
	}()
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
			if !isExpectedShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: reader exit", "err", err)
			}
			// body is either nil on the mid-header error paths or a
			// partially-filled pool buffer on the body-read path; in
			// the latter case we can still return it since it is no
			// longer referenced by anyone.
			msg.PoolPut(body)
			return
		}

		// Stamp the inbound clock for the heartbeat goroutine. We do
		// this on every inbound frame, not just user data, so a peer's
		// PONG (or any traffic) keeps the link alive.
		s.lastInboundUnix.Store(time.Now().UnixNano())

		// Decode the body through the security mechanism. For NULL this
		// is identity but we never short-circuit the call — Wave 2
		// CURVE integration relies on the call site existing.
		plainFlags, decoded, err := s.cfg.Mech.DecodeFrame(flags, body)
		if err != nil {
			s.cfg.Logger.Warn("gozmq/session: decode frame failed", "err", err)
			// decoded may alias body; it goes out of scope with the
			// return below so releasing body is safe.
			msg.PoolPut(body)
			return
		}

		if plainFlags&iface.FlagCommand != 0 {
			// handleCommand consumes decoded synchronously; for
			// SUBSCRIBE/CANCEL it copies into a fresh slice before
			// publishing, and other command variants do not retain a
			// reference past return. Release the pooled body as soon
			// as handleCommand completes.
			cmdErr := s.handleCommand(decoded)
			msg.PoolPut(body)
			if cmdErr != nil {
				s.cfg.Logger.Warn("gozmq/session: command handling failed", "err", cmdErr)
				return
			}
			continue
		}

		// User-message frame. Accumulate until MORE clears.
		if current == nil {
			current = msg.New()
		}
		switch {
		case decodedAliasesBody(body, decoded):
			current.AppendOwnedFrame(decoded, body)
		case len(decoded) == 0:
			current.AppendFrame(decoded)
			msg.PoolPut(body)
		default:
			frame := msg.PoolGet(len(decoded))
			copy(frame, decoded)
			current.AppendOwnedFrame(frame, frame)
			msg.PoolPut(body)
		}

		if plainFlags&iface.FlagMore != 0 {
			continue
		}

		// Last frame in the logical message — publish.
		if err := s.publishInbound(current); err != nil {
			if !isExpectedShutdownErr(err) {
				s.cfg.Logger.Debug("gozmq/session: publish inbound", "err", err)
			}
			return
		}
		current = nil
	}
}

func decodedAliasesBody(body, decoded []byte) bool {
	if len(body) == 0 || len(decoded) == 0 {
		return len(body) == 0 && len(decoded) == 0
	}
	bodyStart := uintptr(unsafe.Pointer(&body[0]))
	bodyEnd := bodyStart + uintptr(len(body))
	decodedStart := uintptr(unsafe.Pointer(&decoded[0]))
	return decodedStart >= bodyStart && decodedStart < bodyEnd
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
		// Wave-2 B2: surface subscription commands to the engine as a
		// synthetic single-frame in-band message where the first byte
		// is 0x01 (SUBSCRIBE) or 0x00 (CANCEL) followed by the prefix
		// bytes. This is Option C from the Wave-2 B2 plan: it keeps
		// iface.Pipe frozen (no side-channel interface), and it mirrors
		// the exact shape libzmq's XPUB surfaces subscription events
		// to its application, so PUB/XPUB engines can reuse one
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
			if !isExpectedShutdownErr(err) {
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

	wireFlags, encoded, err := s.cfg.Mech.EncodeFrame(iface.FlagCommand, buf)
	if err != nil {
		return fmt.Errorf("gozmq/session: encode command %s: %w", name, err)
	}
	// Note: we serialise command writes by funneling everything through
	// the writer goroutine path would be ideal, but PONG must be replied
	// inline from the reader. The conn's underlying io.Writer isn't
	// guaranteed concurrent-safe; we mitigate by ensuring command writes
	// from the reader and user-message writes from the writer never
	// race on the wire by using a small write-side mutex.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.cfg.Codec.WriteFrame(s.cfg.Conn, wireFlags, encoded); err != nil {
		return fmt.Errorf("gozmq/session: write command %s: %w", name, err)
	}
	return nil
}

// isExpectedShutdownErr returns true for errors we expect to see during
// a normal teardown (peer EOF, conn closed by Stop, ctx cancellation).
// Used to suppress noisy logs.
func isExpectedShutdownErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, context.Canceled) || errors.Is(err, zmqerr.ErrSocketClosed) {
		return true
	}
	// net.Conn close races sometimes report as "use of closed
	// network connection"; check by string as a last resort.
	if err.Error() == "use of closed network connection" {
		return true
	}
	return false
}
