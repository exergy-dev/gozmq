package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

func TestPair_AttachExclusive(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	a1, b1 := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	a2, b2 := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a1, b1, a2, b2)

	mustAttach(t, eng, a1)
	require.ErrorIs(t, eng.AttachPipe(a2), zmqerr.ErrFSMViolation, "second attach")
}

func TestPair_RoundTrip(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	a, b := pipe.NewPair(2, 2, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("ping"))), "send")
	got, err := b.Recv(ctx)
	require.NoError(t, err, "peer recv")
	require.Equal(t, "ping", string(got.Bytes()))

	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("pong"))), "peer send")
	got, err = eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, "pong", string(got.Bytes()))
}

func TestPair_SendBeforeAttachWaits(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	ctx, cancel := context.WithCancel(testCtx(t))
	done := make(chan error, 1)
	go func() { done <- eng.Send(ctx, msg.FromBytes([]byte("x"))) }()
	select {
	case <-done:
		require.Fail(t, "send returned before pipe attached")
	case <-time.After(50 * time.Millisecond):
	}
	// PAIR has no notify path on attach in our impl — we cancel via close.
	cancel()
	select {
	case err := <-done:
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
			"send err = %v, want ctx err", err)
	case <-time.After(time.Second):
		require.Fail(t, "send did not unblock on ctx cancel")
	}
}

func TestPair_CloseUnblocksSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	require.NoError(t, a.Send(context.Background(), msg.FromBytes([]byte("fill"))))
	done := make(chan error, 1)
	go func() { done <- eng.Send(context.Background(), msg.FromBytes([]byte("x"))) }()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, eng.Close(), "close")
	select {
	case err := <-done:
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
	case <-time.After(time.Second):
		require.Fail(t, "send did not unblock on close")
	}
}

func TestPair_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	done := make(chan error, 1)
	go func() {
		_, err := eng.Recv(context.Background())
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, eng.Close(), "close")
	select {
	case err := <-done:
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
	case <-time.After(time.Second):
		require.Fail(t, "recv did not unblock on close")
	}
}

// recvAsync starts eng.Recv in a goroutine and returns channels for its
// result.
func recvAsync(ctx context.Context, eng iface.SocketEngine) (<-chan *msg.Msg, <-chan error) {
	mc := make(chan *msg.Msg, 1)
	ec := make(chan error, 1)
	go func() {
		m, err := eng.Recv(ctx)
		if err != nil {
			ec <- err
			return
		}
		mc <- m
	}()
	return mc, ec
}

// TestPair_PeerCloseDrainsThenWaitsForNextPeer: when the attached peer goes
// away, Recv delivers what it left buffered and then parks for the next
// peer instead of surfacing io.EOF (libzmq just blocks). Covers both the
// "engine observes close first" and "socket layer detaches first" orders.
func TestPair_PeerCloseDrainsThenWaitsForNextPeer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		hwm    int
		detach bool
	}{
		{"hwm4", 4, false},
		{"hwm4-detach", 4, true},
		{"hwm0", 0, false},
		{"hwm0-detach", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eng := socket.NewPairEngine()
			defer eng.Close()
			ctx := testCtx(t)

			a, b := pipe.NewPair(tc.hwm, tc.hwm, pipe.PeerInfo{}, pipe.PeerInfo{})
			defer closePipes(a, b)
			mustAttach(t, eng, a)
			require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("one"))))
			require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("two"))))
			require.NoError(t, b.Close())
			if tc.detach {
				require.NoError(t, eng.DetachPipe(a))
			}

			for _, want := range []string{"one", "two"} {
				got, err := eng.Recv(ctx)
				require.NoError(t, err)
				require.Equal(t, want, string(got.Bytes()))
			}

			mc, ec := recvAsync(ctx, eng)
			select {
			case m := <-mc:
				require.Failf(t, "recv returned without a peer", "got %q", m.Bytes())
			case err := <-ec:
				require.Failf(t, "recv failed without a peer", "err = %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			if !tc.detach {
				require.NoError(t, eng.DetachPipe(a))
			}

			a2, b2 := pipe.NewPair(tc.hwm, tc.hwm, pipe.PeerInfo{}, pipe.PeerInfo{})
			defer closePipes(a2, b2)
			mustAttach(t, eng, a2)
			require.NoError(t, b2.Send(ctx, msg.FromBytes([]byte("three"))))
			select {
			case m := <-mc:
				require.Equal(t, "three", string(m.Bytes()))
			case err := <-ec:
				require.Failf(t, "recv failed", "err = %v", err)
			case <-ctx.Done():
				require.Fail(t, "recv did not see the next peer")
			}
		})
	}
}

// TestPair_DetachedPipeDrainedBeforeNextPeer: messages a departed peer
// left buffered are delivered ahead of the next peer's, even when the
// socket layer detached the old pipe and a new one attached before the
// application called Recv.
func TestPair_DetachedPipeDrainedBeforeNextPeer(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()
	ctx := testCtx(t)

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("old"))))
	require.NoError(t, b.Close())
	require.NoError(t, eng.DetachPipe(a))

	a2, b2 := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a2, b2)
	mustAttach(t, eng, a2)
	require.NoError(t, b2.Send(ctx, msg.FromBytes([]byte("new"))))

	for _, want := range []string{"old", "new"} {
		got, err := eng.Recv(ctx)
		require.NoError(t, err)
		require.Equal(t, want, string(got.Bytes()))
	}
}

// TestPair_AttachReplacesClosedPipe: a new peer may attach while the old,
// already-closed pipe has not been detached yet (the accept path can win
// that race against the old session's detach).
func TestPair_AttachReplacesClosedPipe(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()
	ctx := testCtx(t)

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("old"))))
	require.NoError(t, b.Close())

	a2, b2 := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a2, b2)
	mustAttach(t, eng, a2)
	require.NoError(t, eng.DetachPipe(a), "late detach of the old pipe")
	require.NoError(t, b2.Send(ctx, msg.FromBytes([]byte("new"))))

	for _, want := range []string{"old", "new"} {
		got, err := eng.Recv(ctx)
		require.NoError(t, err)
		require.Equal(t, want, string(got.Bytes()))
	}
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("to-new"))))
	got, err := b2.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "to-new", string(got.Bytes()))
}

// TestPair_SendWaitsAcrossPeerClose: a Send blocked at HWM when the peer
// goes away waits for the next peer instead of returning io.ErrClosedPipe.
func TestPair_SendWaitsAcrossPeerClose(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()
	ctx := testCtx(t)

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("fill"))))

	done := make(chan error, 1)
	go func() { done <- eng.Send(ctx, msg.FromBytes([]byte("next"))) }()
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, b.Close())
	select {
	case err := <-done:
		require.Failf(t, "send returned after peer close", "err = %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, eng.DetachPipe(a))

	a2, b2 := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a2, b2)
	mustAttach(t, eng, a2)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		require.Fail(t, "send did not reach the next peer")
	}
	got, err := b2.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "next", string(got.Bytes()))
}

func TestPair_CloseUnblocksRecvBeforeAttach(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	done := make(chan error, 1)
	go func() {
		_, err := eng.Recv(context.Background())
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, eng.Close(), "close")
	select {
	case err := <-done:
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
	case <-time.After(time.Second):
		require.Fail(t, "recv did not unblock on close")
	}
}
