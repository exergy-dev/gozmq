package session

import (
	"encoding/binary"
	"time"
)

// startHeartbeat arms the optional ZMTP 3.1 PING/PONG timer (design
// §5.4): every HeartbeatInterval a tick queues a PING; if no inbound
// frame has arrived within HeartbeatTimeout, the session is torn down.
// The timer re-arms itself from each tick, so an idle session holds no
// heartbeat goroutine. Skipped when the interval is 0 or the peer
// negotiated ZMTP 3.0.
//
// The reader stamps lastInboundUnix on every inbound frame, so any
// traffic — user data, PONG, or unsolicited PING from the peer — keeps
// the link alive. We only consult that timestamp; we do not require an
// explicit PONG-in-response since the spec lets either side initiate
// PINGs on its own cadence.
func (s *Session) startHeartbeat() {
	interval := s.cfg.HeartbeatInterval
	if interval <= 0 || !s.peerHeartbeats {
		return
	}
	timeout := s.cfg.HeartbeatTimeout
	if timeout <= 0 {
		// Documented default (see WithHeartbeat): 3× interval, i.e. 90 s
		// for the 30 s default interval.
		timeout = 3 * interval
	}

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

	s.hbMu.Lock()
	defer s.hbMu.Unlock()
	if !s.hbStopped {
		s.hbTimer = time.AfterFunc(interval, func() { s.heartbeatTick(interval, timeout, pingBody) })
	}
}

// heartbeatTick is one heartbeat firing. It holds hbMu throughout, so
// once stopHeartbeat returns no tick is running or will act.
func (s *Session) heartbeatTick(interval, timeout time.Duration, pingBody []byte) {
	s.hbMu.Lock()
	defer s.hbMu.Unlock()
	if s.hbStopped {
		return
	}
	// Liveness check first. Skip it while the reader is parked
	// publishing inbound on local engine backpressure: no inbound stamps
	// can happen then, but the link itself is healthy.
	lastNs := s.lastInboundUnix.Load()
	if lastNs > 0 && !s.readerParked.Load() {
		if idle := time.Duration(s.cfg.Now() - lastNs); idle >= timeout {
			s.cfg.Logger.Warn(
				"gozmq/session: heartbeat timeout — tearing down",
				"idle", idle, "timeout", timeout,
				"endpoint", s.cfg.Conn.RemoteEndpoint(),
			)
			// Closing the conn ends the reader, which shuts the session
			// down (shutdown takes hbMu, so it cannot run from here).
			_ = s.cfg.Conn.Close()
			return
		}
	}
	// The writer puts the PING on the wire; a full channel drops it and
	// the next tick retries.
	s.enqueueCommand("PING", pingBody)
	s.hbTimer.Reset(interval)
}

// stopHeartbeat cancels the heartbeat for good, waiting out a running tick.
func (s *Session) stopHeartbeat() {
	s.hbMu.Lock()
	defer s.hbMu.Unlock()
	s.hbStopped = true
	if s.hbTimer != nil {
		s.hbTimer.Stop()
	}
}
