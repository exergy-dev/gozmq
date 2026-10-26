package session

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

// sessionGoroutines counts live goroutines running Session code: the
// reader and writer, plus any heartbeat tick in flight.
func sessionGoroutines() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		if bytes.Contains(g, []byte("gozmq/internal/session.(*Session).")) {
			n++
		}
	}
	return n
}

func requireSessionGoroutines(t *testing.T, want int, msg string) {
	t.Helper()
	require.Eventually(t, func() bool { return sessionGoroutines() == want },
		2*time.Second, time.Millisecond, "%s: got %d session goroutines, want %d", msg, sessionGoroutines(), want)
}

// TestSession_TwoGoroutines: a session runs exactly a reader and a writer
// — during the handshake (no per-write helpers) and in the traffic phase
// with heartbeats on (the heartbeat is a timer, not a goroutine).
func TestSession_TwoGoroutines(t *testing.T) {
	requireSessionGoroutines(t, 0, "baseline")

	// Handshake in progress: the peer never answers our greeting.
	connA, connB := newFakeConnPair()
	s := startTestSession(Config{Conn: connA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{}})
	requireSessionGoroutines(t, 2, "mid-handshake")
	require.NoError(t, s.Stop())
	_ = connB.Close()
	requireSessionGoroutines(t, 0, "after Stop")

	// Traffic phase, heartbeat armed (an hour away, so no tick runs).
	connA, connB = newFakeConnPair()
	sA, sB := startPair(t,
		Config{Conn: connA, Mech: newFakeMech("NULL", false, nil), SendHWM: 4, RecvHWM: 4, HeartbeatInterval: time.Hour},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, nil), SendHWM: 4, RecvHWM: 4, HeartbeatInterval: time.Hour},
	)
	ctx := testutil.Ctx(t, 2*time.Second)
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("x"))))
	_, err := sB.Pipe().RecvOr(ctx, nil)
	require.NoError(t, err)
	requireSessionGoroutines(t, 4, "two established sessions")
	require.NoError(t, sA.Stop())
	waitClosed(t, sB.Done(), 2*time.Second, "peer did not observe teardown")
	requireSessionGoroutines(t, 0, "after teardown")
}

// roundsMech runs a multi-round handshake in which both sides write each
// round's frame before reading the peer's. Over a synchronous io.Pipe that
// only completes if handshake writes proceed concurrently with reads.
type roundsMech struct {
	*fakeMech
	rounds int
	step   int
}

func (m *roundsMech) NextHandshake(in []byte) ([]byte, bool, error) {
	if m.step > 0 && string(in) != fmt.Sprintf("round-%d", m.step-1) {
		return nil, false, fmt.Errorf("round %d: got %q", m.step, in)
	}
	if m.step == m.rounds {
		return nil, true, nil
	}
	m.step++
	return []byte(fmt.Sprintf("round-%d", m.step-1)), false, nil
}

func (m *roundsMech) PeerMetadata() map[string]string { return map[string]string{} }

func TestSession_HandshakeOverSynchronousPipe(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	sA, sB := startPair(t,
		Config{Conn: connA, Mech: &roundsMech{fakeMech: newFakeMech("NULL", false, nil), rounds: 5}, SendHWM: 4, RecvHWM: 4},
		Config{Conn: connB, Mech: &roundsMech{fakeMech: newFakeMech("NULL", true, nil), rounds: 5}, SendHWM: 4, RecvHWM: 4},
	)
	ctx := testutil.Ctx(t, 2*time.Second)
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("after-handshake"))))
	got, err := sB.Pipe().RecvOr(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, "after-handshake", string(got.Bytes()))
}

// TestSession_OnReadyErrorTearsDown: a pipe the engine refuses ends the
// session unattached, with OnClosed reporting no handshake error, and the
// peer sees the connection close.
func TestSession_OnReadyErrorTearsDown(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	var (
		offered  *pipe.End
		gotErr   = errors.New("OnClosed not called")
		attached = true
	)
	s := New(Config{
		Conn: connA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{}, SendHWM: 1, RecvHWM: 1,
		OnReady: func(p *pipe.End) error {
			offered = p
			return errors.New("refused")
		},
		OnClosed: func(err error, a bool) { gotErr, attached = err, a },
	})
	s.Start()
	peer := startTestSession(Config{Conn: connB, Mech: newFakeMech("NULL", true, nil), Codec: fakeCodec{}, SendHWM: 1, RecvHWM: 1})
	defer func() { _ = peer.Stop() }()

	waitClosed(t, s.Done(), 2*time.Second, "refused session did not tear down")
	require.NoError(t, gotErr, "handshake succeeded; OnClosed must report no error")
	require.False(t, attached)
	require.False(t, s.Attached())
	require.NotNil(t, offered)
	require.True(t, offered.IsClosed(), "refused pipe left open")
	waitClosed(t, peer.Done(), 2*time.Second, "peer did not observe the teardown")
	require.NoError(t, s.Stop())
}

// TestSession_OnClosedReportsAttached: a session that attached reports it
// once torn down, with no handshake error.
func TestSession_OnClosedReportsAttached(t *testing.T) {
	t.Parallel()
	sA, _, _, connB := startSessionPair(t, nil, nil, 1)
	require.True(t, sA.Attached())
	_ = connB.Close()
	waitClosed(t, sA.Done(), 2*time.Second, "Done did not fire after peer EOF")
	require.NoError(t, sA.HandshakeErr())
	require.True(t, sA.Attached())
}

// TestHeartbeat_StopRacesTick: Stop arriving while heartbeat ticks fire
// (PINGs, and in half the runs a timeout teardown) must leave no tick
// running or armed. -race and goleak (TestMain) do the checking.
func TestHeartbeat_StopRacesTick(t *testing.T) {
	for i := 0; i < 100; i++ {
		clk := newFakeClock()
		s, peer := newHeartbeatSession(t, clk, 50*time.Microsecond, time.Second, 8)
		_ = peer.startReadLoop()
		if i%2 == 1 {
			clk.Advance(2 * time.Second) // the next tick tears down
		}
		time.Sleep(time.Duration(i%5) * 100 * time.Microsecond)
		require.NoError(t, s.Stop())
		s.hbMu.Lock()
		stopped := s.hbStopped
		s.hbMu.Unlock()
		require.True(t, stopped, "heartbeat still armed after Stop")
		_ = peer.conn.Close()
	}
	requireSessionGoroutines(t, 0, "after the Stop/tick races")
}

// TestHeartbeat_TimeoutWithoutGoroutine: between ticks the session holds
// only its reader and writer, and the timeout teardown runs from a tick.
func TestHeartbeat_TimeoutWithoutGoroutine(t *testing.T) {
	clk := newFakeClock()
	s, peer := newHeartbeatSession(t, clk, 20*time.Millisecond, time.Second, 8)
	frames := peer.startReadLoop()
	waitCommand(t, frames, "PING", 2*time.Second)
	requireSessionGoroutines(t, 2, "between heartbeat ticks")
	clk.Advance(2 * time.Second)
	waitClosed(t, s.Done(), 2*time.Second, "heartbeat timeout did not tear the session down")
}
