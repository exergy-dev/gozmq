package socket_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// TestFairQueue_RecvWithSource asserts that RecvWithSource returns both
// the message and the exact pipe it came from, across a 2-pipe setup.
func TestFairQueue_RecvWithSource(t *testing.T) {
	t.Parallel()
	fq := newFQ()
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

	ctx := testutil.Ctx(t)

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
	fq := newFQ()

	err := testutil.RequireUnblocks(t, func() error {
		_, _, err := fq.RecvWithSource(context.Background())
		return err
	}, func() { require.NoError(t, fq.Close(), "close") })
	require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
}
