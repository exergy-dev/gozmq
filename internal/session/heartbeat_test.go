package session

import (
	"encoding/binary"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/zmtp"
)

// --------------------------------------------------------------------------
// fakeClock: deterministic Config.Now source. The heartbeat goroutine's
// ticker still runs on real time (we keep intervals to a few ms), but all
// idle-time arithmetic flows through the injected clock, so tests control
// "how long the peer has been silent" exactly.
// --------------------------------------------------------------------------

type fakeClock struct{ ns atomic.Int64 }

// newFakeClock starts well above zero so the heartbeat's lastNs > 0 guard
// sees the handshake-time seed stamp.
func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(int64(time.Hour))
	return c
}

func (c *fakeClock) Now() int64              { return c.ns.Load() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

// --------------------------------------------------------------------------
// manualPeer drives the remote side of a session by hand: real greeting
// bytes, the one-round fakeMech-style handshake, then raw fakeCodec frames
// under test control. Unlike a full peer session it never replies to
// PINGs unless the test says so — which is exactly what heartbeat-timeout
// tests need.
// --------------------------------------------------------------------------

type manualPeer struct {
	t    *testing.T
	conn *fakeConn
}

type peerFrame struct {
	flags uint8
	body  []byte
}

// command returns the ZMTP command name and remainder if the frame is a
// COMMAND frame, else ("", nil).
func (f peerFrame) command() (string, []byte) {
	if f.flags&zmtp.FlagCommand == 0 || len(f.body) < 1 {
		return "", nil
	}
	nameLen := int(f.body[0])
	if 1+nameLen > len(f.body) {
		return "", nil
	}
	return string(f.body[1 : 1+nameLen]), f.body[1+nameLen:]
}

// newManualPeer completes the greeting + security handshake against the
// session on the other end of conn and returns a peer ready to exchange
// traffic-phase frames.
func newManualPeer(t *testing.T, conn *fakeConn, props map[string]string) *manualPeer {
	t.Helper()
	p := &manualPeer{t: t, conn: conn}

	// Greeting: read the session's 64 bytes first (its write runs
	// concurrently with its read, so a sequential read-then-write here
	// cannot deadlock on the synchronous io.Pipe), then send ours.
	buf := make([]byte, greetingSize)
	_, err := io.ReadFull(conn, buf)
	require.NoError(t, err, "manual peer: read greeting")
	out, err := encodeGreeting("NULL", true)
	require.NoError(t, err, "manual peer: encode greeting")
	_, err = conn.Write(out)
	require.NoError(t, err, "manual peer: write greeting")

	// Security handshake: one round of fake props, mirroring fakeMech.
	flags, _, err := fakeCodec{}.ReadFrame(conn, nil, nil)
	require.NoError(t, err, "manual peer: read handshake frame")
	require.NotZero(t, flags&zmtp.FlagCommand, "manual peer: expected COMMAND handshake frame")
	require.NoError(t,
		fakeCodec{}.WriteFrame(conn, zmtp.FlagCommand, encodeFakeProps(copyOrEmpty(props))),
		"manual peer: write handshake frame")
	return p
}

func copyOrEmpty(props map[string]string) map[string]string {
	if props == nil {
		return map[string]string{}
	}
	return props
}

// startReadLoop pumps inbound frames into a buffered channel until the
// conn dies. Keeping the conn drained matters: io.Pipe writes are
// synchronous, so an unread PING would otherwise wedge the session's
// writer rather than exercise the heartbeat logic.
func (p *manualPeer) startReadLoop() <-chan peerFrame {
	ch := make(chan peerFrame, 256)
	go func() {
		defer close(ch)
		for {
			flags, body, err := fakeCodec{}.ReadFrame(p.conn, nil, nil)
			if err != nil {
				return
			}
			select {
			case ch <- peerFrame{flags: flags, body: body}:
			default: // overflow: drop; tests only count a handful
			}
		}
	}()
	return ch
}

// sendPing writes a ZMTP PING command frame (2-byte TTL + context).
func (p *manualPeer) sendPing(ttl uint16, ctxBytes []byte) {
	p.t.Helper()
	body := make([]byte, 0, 1+4+2+len(ctxBytes))
	body = append(body, 4)
	body = append(body, "PING"...)
	var t2 [2]byte
	binary.BigEndian.PutUint16(t2[:], ttl)
	body = append(body, t2[:]...)
	body = append(body, ctxBytes...)
	require.NoError(p.t, fakeCodec{}.WriteFrame(p.conn, zmtp.FlagCommand, body), "manual peer: write PING")
}

// sendData writes a (possibly multipart) user message.
func (p *manualPeer) sendData(frames ...[]byte) {
	p.t.Helper()
	for i, f := range frames {
		var flags uint8
		if i < len(frames)-1 {
			flags = zmtp.FlagMore
		}
		require.NoError(p.t, fakeCodec{}.WriteFrame(p.conn, flags, f), "manual peer: write data frame")
	}
}

// waitCommand drains frames until a command with the given name arrives.
func waitCommand(t *testing.T, frames <-chan peerFrame, name string, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case f, ok := <-frames:
			require.True(t, ok, "peer conn closed while waiting for %s", name)
			if got, rest := f.command(); got == name {
				return rest
			}
		case <-deadline:
			require.FailNow(t, "timed out waiting for command "+name)
		}
	}
}

// newHeartbeatSession builds a session against a manualPeer with the
// given heartbeat settings and fake clock, and waits for the handshake.
func newHeartbeatSession(t *testing.T, clk *fakeClock, interval, timeout time.Duration, recvHWM int) (Session, *manualPeer) {
	t.Helper()
	connA, connB := newFakeConnPair()
	s := New(Config{
		Conn:              connA,
		Mech:              newFakeMech("NULL", false, nil),
		Codec:             fakeCodec{},
		SendHWM:           8,
		RecvHWM:           recvHWM,
		HeartbeatInterval: interval,
		HeartbeatTimeout:  timeout,
		Now:               clk.Now,
	})
	s.Start()
	t.Cleanup(func() { _ = s.Stop() })
	peer := newManualPeer(t, connB, nil)
	waitHandshake(t, s, "A")
	return s, peer
}

// requireAlive asserts the session has not torn down after a real-time
// grace period covering several heartbeat ticks.
func requireAlive(t *testing.T, s Session, wait time.Duration, msg string) {
	t.Helper()
	select {
	case <-s.Done():
		require.FailNow(t, msg)
	case <-time.After(wait):
	}
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

// TestHeartbeat_PingCadenceAndTTL: with the fake clock frozen (idle
// always 0) the scheduler must emit a PING every interval, carrying our
// timeout as the TTL in deciseconds, and never tear the session down.
func TestHeartbeat_PingCadenceAndTTL(t *testing.T) {
	clk := newFakeClock()
	s, peer := newHeartbeatSession(t, clk, 10*time.Millisecond, 2*time.Second, 8)
	frames := peer.startReadLoop()

	for i := 0; i < 3; i++ {
		rest := waitCommand(t, frames, "PING", 2*time.Second)
		require.GreaterOrEqual(t, len(rest), 2, "PING body missing TTL")
		ttl := binary.BigEndian.Uint16(rest[:2])
		require.Equal(t, uint16(20), ttl, "PING TTL should advertise our 2s timeout in deciseconds")
	}
	requireAlive(t, s, 30*time.Millisecond, "session tore down despite zero idle time")
}

// TestHeartbeat_TimeoutTearsDown: a peer that goes silent past the
// timeout (in fake time) gets the session torn down on the next tick.
func TestHeartbeat_TimeoutTearsDown(t *testing.T) {
	clk := newFakeClock()
	s, peer := newHeartbeatSession(t, clk, 5*time.Millisecond, time.Second, 8)
	frames := peer.startReadLoop()
	_ = frames // pump must run so PING writes don't wedge on io.Pipe

	clk.Advance(2 * time.Second) // idle = 2s > 1s timeout
	waitClosed(t, s.Done(), 2*time.Second, "heartbeat timeout did not tear the session down")
}

// TestHeartbeat_InboundTrafficSuppressesTimeout: traffic from the peer
// re-stamps the idle clock, so a chatty link never times out even when
// no PONGs are sent.
func TestHeartbeat_InboundTrafficSuppressesTimeout(t *testing.T) {
	clk := newFakeClock()
	s, peer := newHeartbeatSession(t, clk, 5*time.Millisecond, time.Second, 8)
	frames := peer.startReadLoop()
	_ = frames

	ctx := testCtx(t, 2*time.Second)
	// Walk fake time forward in sub-timeout steps, sending traffic at
	// each step. idle never reaches the 1s timeout.
	for i := 0; i < 3; i++ {
		clk.Advance(900 * time.Millisecond)
		peer.sendData([]byte("keepalive"))
		m, err := s.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv keepalive %d", i)
		require.Equal(t, "keepalive", string(m.Bytes()))
	}
	requireAlive(t, s, 30*time.Millisecond, "session tore down despite inbound traffic inside the timeout window")

	// Sanity: silence past the timeout still kills it.
	clk.Advance(1100 * time.Millisecond)
	waitClosed(t, s.Done(), 2*time.Second, "session did not tear down after traffic stopped")
}

// TestHeartbeat_ReaderParkedOnBackpressureIsNotTimeout: when the reader
// is blocked publishing inbound on local engine backpressure, no inbound
// stamps can happen — but the link is healthy. The timeout check must
// skip teardown while the reader is parked, and resume normally (with a
// fresh idle window) once the engine drains.
func TestHeartbeat_ReaderParkedOnBackpressureIsNotTimeout(t *testing.T) {
	clk := newFakeClock()
	// RecvHWM 1: the second inbound message parks the reader because
	// nothing drains the engine-facing pipe.
	s, peer := newHeartbeatSession(t, clk, 5*time.Millisecond, time.Second, 1)
	frames := peer.startReadLoop()
	_ = frames

	peer.sendData([]byte("m1")) // fills the pipe's single slot
	peer.sendData([]byte("m2")) // parks the reader in publishInbound
	si := s.(*sessionImpl)
	require.Eventually(t, func() bool { return si.readerParked.Load() },
		2*time.Second, time.Millisecond, "reader did not park on engine backpressure")

	// Way past the timeout in fake time, across many real ticks: the
	// parked reader must suppress teardown.
	clk.Advance(5 * time.Second)
	requireAlive(t, s, 50*time.Millisecond, "heartbeat killed a healthy session while the reader was parked on local backpressure")

	// Drain the engine side: the reader unparks and re-stamps the idle
	// clock, so the stale 5s gap must not be charged against the peer.
	ctx := testCtx(t, 2*time.Second)
	for _, want := range []string{"m1", "m2"} {
		m, err := s.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv %s", want)
		require.Equal(t, want, string(m.Bytes()))
	}
	requireAlive(t, s, 50*time.Millisecond, "session tore down right after unparking despite the re-stamped idle clock")

	// And with the reader unparked, real silence still times out.
	clk.Advance(2 * time.Second)
	waitClosed(t, s.Done(), 2*time.Second, "timeout did not resume after the reader unparked")
}

// TestHeartbeat_DefaultTimeoutIsThreeIntervals: HeartbeatTimeout 0 means
// 3× the interval (the documented 90 s for the 30 s default interval),
// visible as the PING TTL.
func TestHeartbeat_DefaultTimeoutIsThreeIntervals(t *testing.T) {
	clk := newFakeClock()
	_, peer := newHeartbeatSession(t, clk, 100*time.Millisecond, 0, 8)
	frames := peer.startReadLoop()
	rest := waitCommand(t, frames, "PING", 2*time.Second)
	require.GreaterOrEqual(t, len(rest), 2, "PING body missing TTL")
	require.Equal(t, uint16(3), binary.BigEndian.Uint16(rest[:2]),
		"default timeout should be 3× the 100ms interval (3 deciseconds)")
}
