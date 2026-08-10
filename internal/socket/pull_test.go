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

func TestPull_FairQueues(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
	defer eng.Close()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	mustAttach(t, eng, engineEnds...)

	ctx := testCtx(t)
	want := make(map[byte]bool)
	for i := 0; i < n; i++ {
		body := byte('a' + i)
		want[body] = true
		require.NoError(t, peerEnds[i].Send(ctx, msg.FromBytes([]byte{body})), "peer %d send", i)
	}
	got := make(map[byte]bool)
	for i := 0; i < n; i++ {
		m, err := eng.Recv(ctx)
		require.NoError(t, err, "recv %d", i)
		got[m.Bytes()[0]] = true
	}
	for b := range want {
		require.True(t, got[b], "missing message %c", b)
	}
}

func TestPull_RecvWakesOnAttach(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
	defer eng.Close()

	ctx := testCtx(t)
	type result struct {
		m   *msg.Msg
		err error
	}
	done := make(chan result, 1)
	go func() {
		m, err := eng.Recv(ctx)
		done <- result{m: m, err: err}
	}()
	select {
	case <-done:
		require.Fail(t, "recv returned before any pipe attached")
	case <-time.After(50 * time.Millisecond):
	}
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("hi"))), "peer send")
	mustAttach(t, eng, a)
	select {
	case r := <-done:
		require.NoError(t, r.err, "recv")
		require.Equal(t, "hi", string(r.m.Bytes()))
	case <-time.After(time.Second):
		require.Fail(t, "recv did not wake on attach")
	}
}

func TestPull_SendIsFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
	defer eng.Close()
	err := eng.Send(context.Background(), msg.FromBytes([]byte("x")))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation)
}

func TestPull_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
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
