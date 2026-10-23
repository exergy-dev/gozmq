package gozmq

import (
	"context"
	"math"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestWaitWritersFlushed_LingerTimeoutDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	start := time.Now()
	err := waitDrained(context.Background(), []<-chan struct{}{make(chan struct{})}, 20*time.Millisecond)
	require.NoError(t, err, "waitDrained on linger timeout")
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 15*time.Millisecond, "returned after %v, want ~linger wait", elapsed)
}

func TestWaitWritersFlushed_CtxCancelDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := waitDrained(ctx, []<-chan struct{}{make(chan struct{})}, -1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestContextReleasesClosedSockets is the fast regression guard for a leak
// the soak suite found: NewSocket appended to Context.sockets and nothing
// ever removed a closed socket, so a long-lived Context — the documented
// one-per-process pattern — retained every socket it had ever created,
// along with each socket's config, engine, and engine-owned maps and
// channels. At ~2 KiB apiece that is tens of megabytes per hundred
// thousand connection cycles.
//
// soak/ catches this over minutes; this catches it in milliseconds.
func TestContextReleasesClosedSockets(t *testing.T) {
	c := NewContext()
	defer func() { _ = c.Close() }()

	const n = 50
	for i := 0; i < n; i++ {
		s, err := c.NewSocket(Pair)
		require.NoError(t, err)
		require.NoError(t, s.Close())
	}

	c.mu.Lock()
	retained := len(c.sockets)
	c.mu.Unlock()
	require.Zero(t, retained, "Context retained %d of %d closed sockets", retained, n)
}

// Sockets still open must remain tracked, so Context.Close can tear them
// down. Guards against "fixing" the leak by never tracking at all.
func TestContextTracksOpenSockets(t *testing.T) {
	c := NewContext()
	defer func() { _ = c.Close() }()

	open, err := c.NewSocket(Pair)
	require.NoError(t, err)
	closed, err := c.NewSocket(Pair)
	require.NoError(t, err)
	require.NoError(t, closed.Close())

	c.mu.Lock()
	tracked := slices.Clone(c.sockets)
	c.mu.Unlock()

	require.Len(t, tracked, 1)
	require.Same(t, open, tracked[0])
}

// TestLinksReturnToBaselineAfterChurn: every kind of link leaves
// Socket.links when its peer goes, so a long-lived socket's link count
// returns to its baseline (none) after 1000 connect/close cycles over tcp,
// inproc and STREAM.
func TestLinksReturnToBaselineAfterChurn(t *testing.T) {
	const cycles = 1000
	requireNoLinks := func(t *testing.T, s *Socket) {
		t.Helper()
		require.Eventually(t, func() bool { return linkCount(s) == 0 }, 5*time.Second, time.Millisecond,
			"%d links left", linkCount(s))
	}
	// A PUSH per cycle connects to a bound PULL, proves the link with a
	// round trip, and closes.
	zmtp := func(endpoint string) func(t *testing.T) {
		return func(t *testing.T) {
			zctx := newInprocContext(t)
			pull := inprocSocket(t, zctx, Pull)
			require.NoError(t, pull.Bind(endpoint))
			for range cycles {
				push, err := zctx.NewSocket(Push)
				require.NoError(t, err)
				require.NoError(t, push.Connect(pull.LastEndpoint()))
				requireRoundTrip(t, push, pull, "x")
				require.NoError(t, push.Close())
			}
			requireNoLinks(t, pull)
		}
	}
	t.Run("tcp", zmtp("tcp://127.0.0.1:0"))
	t.Run("inproc", zmtp("inproc://churn-baseline"))

	// A raw TCP peer per cycle writes a byte the STREAM socket receives,
	// then closes; dial is the STREAM socket's side of establishing it.
	stream := func(t *testing.T, s *Socket, dial func() net.Conn) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for range cycles {
			c := dial()
			_, err := c.Write([]byte("x"))
			require.NoError(t, err)
			m, err := s.RecvMsg(ctx)
			require.NoError(t, err)
			m.Release()
			require.NoError(t, c.Close())
		}
		requireNoLinks(t, s)
	}
	t.Run("stream-accepted", func(t *testing.T) {
		srv := inprocSocket(t, newInprocContext(t), Stream)
		require.NoError(t, srv.Bind("tcp://127.0.0.1:0"))
		addr := strings.TrimPrefix(srv.LastEndpoint(), "tcp://")
		stream(t, srv, func() net.Conn {
			c, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			return c
		})
	})
	t.Run("stream-connected", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer l.Close()
		cli := inprocSocket(t, newInprocContext(t), Stream)
		stream(t, cli, func() net.Conn {
			require.NoError(t, cli.Connect("tcp://"+l.Addr().String()))
			c, err := l.Accept()
			require.NoError(t, err)
			return c
		})
	})
}

func TestInprocHWMSaturates(t *testing.T) {
	require.Equal(t, 0, inprocHWM(0, 5))
	require.Equal(t, 0, inprocHWM(5, 0))
	require.Equal(t, 2*MaxHWM, inprocHWM(MaxHWM, MaxHWM))
	require.Equal(t, 2*MaxHWM, inprocHWM(math.MaxInt, 1), "overflow must saturate, not go negative")
}
