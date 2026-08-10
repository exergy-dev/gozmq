package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
	"github.com/stretchr/testify/require"
)

// TestFairQueue_RecvWithSource asserts that RecvWithSource returns both
// the message and the exact pipe it came from, across a 2-pipe setup.
func TestFairQueue_RecvWithSource(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()
	defer fq.Close()

	aEng, aPeer := pipe.NewPair(4, 4,
		pipe.PeerInfo{Identity: []byte("peerA")},
		pipe.PeerInfo{},
	)
	bEng, bPeer := pipe.NewPair(4, 4,
		pipe.PeerInfo{Identity: []byte("peerB")},
		pipe.PeerInfo{},
	)
	defer closePipes(aEng, aPeer, bEng, bPeer)

	require.NoError(t, fq.Attach(aEng), "attach A")
	require.NoError(t, fq.Attach(bEng), "attach B")

	ctx := testCtx(t)

	// Send from B first; assert source is bEng.
	require.NoError(t, bPeer.Send(ctx, msg.FromBytes([]byte("from-B"))), "bPeer send")
	m, src, err := fq.RecvWithSource(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, "from-B", string(m.Bytes()))
	require.Equal(t, bEng, src, "source mismatch: want bEng")

	// Now A.
	require.NoError(t, aPeer.Send(ctx, msg.FromBytes([]byte("from-A"))), "aPeer send")
	m, src, err = fq.RecvWithSource(ctx)
	require.NoError(t, err, "recv2")
	require.Equal(t, "from-A", string(m.Bytes()))
	require.Equal(t, aEng, src, "source mismatch: want aEng")
}

// TestFairQueue_RecvWithSource_CloseError asserts that RecvWithSource
// returns ErrSocketClosed when the queue is closed with no inbound.
func TestFairQueue_RecvWithSource_CloseError(t *testing.T) {
	t.Parallel()
	fq := socket.NewFairQueue()

	ctx := context.Background()
	type result struct {
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, _, err := fq.RecvWithSource(ctx)
		done <- result{err: err}
	}()
	time.Sleep(30 * time.Millisecond)
	require.NoError(t, fq.Close(), "close")
	select {
	case r := <-done:
		require.ErrorIs(t, r.err, zmqerr.ErrSocketClosed)
	case <-time.After(time.Second):
		require.Fail(t, "recv did not unblock on close")
	}
}
