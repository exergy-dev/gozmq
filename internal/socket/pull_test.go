package socket_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

func TestPull_FairQueues(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
	defer eng.Close()

	const n = 3
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)

	mustAttach(t, eng, engineEnds...)

	ctx := testutil.Ctx(t)
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

	ctx := testutil.Ctx(t)
	type result struct {
		m   *msg.Msg
		err error
	}
	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	// Recv must block until a pipe with a message available is attached.
	r := testutil.RequireUnblocks(t, func() result {
		m, err := eng.Recv(ctx)
		return result{m: m, err: err}
	}, func() {
		require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("hi"))), "peer send")
		mustAttach(t, eng, a)
	})
	require.NoError(t, r.err, "recv")
	require.Equal(t, "hi", string(r.m.Bytes()))
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
	err := testutil.RequireUnblocks(t, func() error {
		_, err := eng.Recv(context.Background())
		return err
	}, func() { require.NoError(t, eng.Close(), "close") })
	require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
}
