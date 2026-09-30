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
		// Documented default (see WithHeartbeat): 3× interval, i.e. 90 s
		// for the 30 s default interval.
		timeout = 3 * interval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Build a fixed PING body once. ZMTP 3.1 §3.6: 2-byte big-endian
	// TTL (in deciseconds; 0 = no expiry) + opaque context. Per the
	// spec's SHOULD we advertise our own timeout as the TTL, telling the
	// peer how long to wait before declaring us dead. (We currently
	// ignore the peer's TTL and rely on our configured timeout.) The
	// context is empty — peers echo whatever we send, but we don't
	// correlate.
	ttlDeciseconds := timeout / (100 * time.Millisecond)
	if ttlDeciseconds > 0xFFFF {
		ttlDeciseconds = 0xFFFF
	}
	pingBody := make([]byte, 2)
	binary.BigEndian.PutUint16(pingBody, uint16(ttlDeciseconds))

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.sessionPipe.Closed():
			return
		case <-ticker.C:
			// Liveness check first — if we haven't heard from the peer
			// within the timeout, tear down. We log the reason via
			// slog rather than minting a new package error. Skip the
			// check while the reader is parked publishing inbound on
			// local engine backpressure: no inbound stamps can happen
			// then, but the link itself is healthy.
			lastNs := s.lastInboundUnix.Load()
			if lastNs > 0 && !s.readerParked.Load() {
				idle := time.Duration(s.cfg.Now() - lastNs)
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
			// Fire a PING via the writer's command channel — only the
			// writer goroutine touches the wire in traffic phase. A
			// write failure surfaces in the writer, which tears the
			// session down; a full channel drops the PING and the next
			// tick retries.
			s.enqueueCommand("PING", pingBody)
		}
	}
}
