package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq/internal/msg"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/stretchr/testify/require"
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
