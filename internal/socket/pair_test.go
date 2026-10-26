package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

func TestPair_AttachExclusive(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	a1, b1 := pipe.NewPair(1, 1, nil, nil)
	a2, b2 := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a1, b1, a2, b2)

	mustAttach(t, eng, a1)
	require.ErrorIs(t, eng.AttachPipe(a2), zmqerr.ErrFSMViolation, "second attach")
}

func TestPair_RoundTrip(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	a, b := pipe.NewPair(2, 2, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("ping"))), "send")
	got, err := b.RecvOr(ctx, nil)
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

	ctx, cancel := context.WithCancel(testutil.Ctx(t))
	// PAIR has no notify path on attach in our impl — we cancel via close.
	err := testutil.RequireUnblocks(t,
		func() error { return eng.Send(ctx, msg.FromBytes([]byte("x"))) },
		cancel)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
		"send err = %v, want ctx err", err)
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
			ctx := testutil.Ctx(t)

			a, b := pipe.NewPair(tc.hwm, tc.hwm, nil, nil)
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

			rc := testutil.AsyncErr(func() (*msg.Msg, error) { return eng.Recv(ctx) })
			testutil.RequireNoRecv(t, rc, 50*time.Millisecond, "recv returned without a peer")
			if !tc.detach {
				require.NoError(t, eng.DetachPipe(a))
			}

			a2, b2 := pipe.NewPair(tc.hwm, tc.hwm, nil, nil)
			defer closePipes(a2, b2)
			mustAttach(t, eng, a2)
			require.NoError(t, b2.Send(ctx, msg.FromBytes([]byte("three"))))
			r := testutil.RequireRecv(t, rc, 5*time.Second, "recv did not see the next peer")
			require.NoError(t, r.Err, "recv failed")
			require.Equal(t, "three", string(r.V.Bytes()))
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
	ctx := testutil.Ctx(t)

	a, b := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("old"))))
	require.NoError(t, b.Close())
	require.NoError(t, eng.DetachPipe(a))

	a2, b2 := pipe.NewPair(4, 4, nil, nil)
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
	ctx := testutil.Ctx(t)

	a, b := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("old"))))
	require.NoError(t, b.Close())

	a2, b2 := pipe.NewPair(4, 4, nil, nil)
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
	got, err := b2.RecvOr(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, "to-new", string(got.Bytes()))
}

// TestPair_SendWaitsAcrossPeerClose: a Send blocked at HWM when the peer
// goes away waits for the next peer instead of returning io.ErrClosedPipe.
func TestPair_SendWaitsAcrossPeerClose(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()
	ctx := testutil.Ctx(t)

	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("fill"))))

	done := testutil.Async(func() error { return eng.Send(ctx, msg.FromBytes([]byte("next"))) })
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, b.Close())
	testutil.RequireNoRecv(t, done, 50*time.Millisecond, "send returned after peer close")
	require.NoError(t, eng.DetachPipe(a))

	a2, b2 := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a2, b2)
	mustAttach(t, eng, a2)
	require.NoError(t, testutil.RequireRecv(t, done, 5*time.Second, "send did not reach the next peer"))
	got, err := b2.RecvOr(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, "next", string(got.Bytes()))
}
