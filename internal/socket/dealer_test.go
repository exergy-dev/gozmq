package socket_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

// TestDealer_DoesNotMangleIdentity asserts that DEALER passes the message
// through unchanged in both directions (no empty-delimiter insertion, no
// identity prepend or strip). This is what differentiates DEALER from REQ,
// and it is the proof that the engine wires Send to its LoadBalancer and
// Recv to its FairQueue; distribution and fairness themselves are tested
// on those components in engine_test.go.
func TestDealer_DoesNotMangleIdentity(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	sent := msg.FromFrames([]byte("hello"), []byte("world"))
	require.NoError(t, eng.Send(ctx, sent), "send")
	got, err := b.RecvOr(ctx, nil)
	require.NoError(t, err, "peer recv")
	require.Equal(t, 2, got.NumFrames(), "outbound frames (no delimiter)")
	require.Equal(t, "hello", string(got.Frames()[0]))
	require.Equal(t, "world", string(got.Frames()[1]))

	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte("re"), []byte("ply"))), "peer send")
	got, err = eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, 2, got.NumFrames(), "inbound frames (no envelope strip)")
	require.Equal(t, "re", string(got.Frames()[0]))
	require.Equal(t, "ply", string(got.Frames()[1]))
}

// TestDealer_CloseUnblocksBoth asserts one Close unblocks a pending Send
// and a pending Recv at the same time, each with ErrSocketClosed.
func TestDealer_CloseUnblocksBoth(t *testing.T) {
	t.Parallel()
	eng := socket.NewDealerEngine()
	// No pipes attached: Recv waits for data, Send waits for a peer.
	requireCloseUnblocks(t, eng,
		func() error { _, err := eng.Recv(context.Background()); return err },
		func() error { return eng.Send(context.Background(), msg.FromBytes([]byte("x"))) },
	)
}
