package session

import (
	"encoding/binary"
	"time"
)

// runHeartbeat is the optional ZMTP 3.1 PING/PONG scheduler (design
// §5.4). It fires a PING every HeartbeatInterval; if no inbound frame
// has arrived within HeartbeatTimeout, the session is torn down.
//
// The reader goroutine stamps lastInboundUnix on every inbound frame,
// so any traffic — user data, PONG, or unsolicited PING from the peer
// — keeps the link alive. We only consult that timestamp; we do not
// require an explicit PONG-in-response since the spec lets either side
// initiate PINGs on its own cadence.
func (s *sessionImpl) runHeartbeat() {
	defer s.wg.Done()

	interval := s.cfg.HeartbeatInterval
	timeout := s.cfg.HeartbeatTimeout
	if timeout <= 0 {
		// design default: 2× interval if not configured.
		timeout = 2 * interval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Build a fixed PING body once. ZMTP 3.1 §3.6: 2-byte big-endian
	// TTL (in deciseconds; 0 = no expiry) + opaque context. We pass an
	// empty context — peers echo whatever we send, but we don't
	// correlate, so an empty value is fine.
	ttlDeciseconds := uint16(0)
	pingBody := make([]byte, 2)
	binary.BigEndian.PutUint16(pingBody, ttlDeciseconds)

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.sessionPipe.Closed():
			return
		case <-ticker.C:
			// Liveness check first — if we haven't heard from the peer
			// within the timeout, tear down. We log the reason via
			// slog rather than minting a new package error.
			lastNs := s.lastInboundUnix.Load()
			if lastNs > 0 {
				idle := time.Duration(monoNow() - lastNs)
				if idle >= timeout {
					s.cfg.Logger.Warn(
						"gozmq/session: heartbeat timeout — tearing down",
						"idle", idle, "timeout", timeout,
						"endpoint", s.cfg.Conn.RemoteEndpoint(),
					)
					s.shutdown()
					return
				}
			}
			// Fire a PING. If sendCommand fails the writer/reader
			// shutdown path may never fire (for example, if the peer
			// vanished in a way that only surfaces on write), so treat
			// a heartbeat write failure as terminal and tear the session
			// down here.
			if err := s.sendCommand("PING", pingBody); err != nil {
				s.cfg.Logger.Debug("gozmq/session: heartbeat send failed", "err", err)
				s.shutdown()
				return
			}
		}
	}
}
