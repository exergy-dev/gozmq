package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq/internal/msg"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/stretchr/testify/require"
)

func TestPush_LoadBalances(t *testing.T) {
	t.Parallel()
	eng := socket.NewPushEngine()
	defer eng.Close()

	const n = 3
	const send = 9
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	mustAttach(t, eng, engineEnds...)

	ctx := testCtx(t)
	for i := 0; i < send; i++ {
		require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte{byte(i)})), "send %d", i)
	}

	counts := make([]int, n)
	for i := 0; i < n; i++ {
		drainCtx, c := context.WithTimeout(context.Background(), time.Second)
		for counts[i] < send/n {
			_, err := peerEnds[i].Recv(drainCtx)
			if err != nil {
				break
			}
			counts[i]++
		}
		c()
	}
	for i, c := range counts {
		require.Equal(t, send/n, c, "peer %d got %d messages, expected %d", i, c, send/n)
	}
}

func TestPush_RecvIsFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewPushEngine()
	defer eng.Close()
	_, err := eng.Recv(context.Background())
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation)
}

func TestPush_BlocksOnHWMUnblocksOnDrain(t *testing.T) {
	t.Parallel()
	eng := socket.NewPushEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("one"))), "first send")
	done := make(chan error, 1)
	go func() { done <- eng.Send(ctx, msg.FromBytes([]byte("two"))) }()
	select {
	case err := <-done:
		require.Fail(t, "second send completed early", "%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	drainCtx, dc := context.WithTimeout(context.Background(), time.Second)
	defer dc()
	_, err := b.Recv(drainCtx)
	require.NoError(t, err, "drain")
	select {
	case err := <-done:
		require.NoError(t, err, "second send")
	case <-time.After(time.Second):
		require.Fail(t, "second send did not unblock")
	}
	_, err = b.Recv(drainCtx)
	require.NoError(t, err, "drain two")
}

func TestPush_CloseUnblocksSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewPushEngine()
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
