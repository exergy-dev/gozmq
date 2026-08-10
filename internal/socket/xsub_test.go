package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// TestXSub_SendSubscribeViaMessage: XSUB's Send accepts a synthetic
// subscribe message and forwards it to every attached peer while also
// updating the local filter.
func TestXSub_SendSubscribeViaMessage(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewXSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	ctx := testCtx(t)

	require.NoError(t, eng.Send(ctx, subscribeMsg(true, []byte("topic"))), "xsub send")
	// Peer should receive the synthetic SUBSCRIBE forwarded by XSUB.
	m, err := b.Recv(ctx)
	require.NoError(t, err, "peer recv")
	body := m.Frames()[0]
	require.Equal(t, socket.SubscribeByte, body[0])
	require.Equal(t, "topic", string(body[1:]))

	// Now peer sends a matching data message; Recv surfaces it.
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte("topical"), []byte("payload"))), "peer send")
	got, err := eng.Recv(ctx)
	require.NoError(t, err, "xsub recv")
	require.Equal(t, 2, got.NumFrames())
	require.Equal(t, "topical", string(got.Frames()[0]))
}

// TestXSub_SendDuplicateSubscribeForwardsTwice verifies that app-driven
// synthetic subscribe messages are always forwarded, even when the local
// filter already contains the prefix. Verbose XPUB peers rely on seeing each
// duplicate command.
func TestXSub_SendDuplicateSubscribeForwardsTwice(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewXSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	ctx := testCtx(t)

	sub := subscribeMsg(true, []byte("topic"))
	require.NoError(t, eng.Send(ctx, sub), "xsub send #1")
	require.NoError(t, eng.Send(ctx, sub), "xsub send #2")
	for i := 0; i < 2; i++ {
		m, err := b.Recv(ctx)
		require.NoError(t, err, "peer recv #%d", i+1)
		body := m.Frames()[0]
		require.Equal(t, socket.SubscribeByte, body[0])
		require.Equal(t, "topic", string(body[1:]))
	}
}

// TestXSub_SendNonSubscribeRejected: XSUB Send must be a subscription
// command; anything else is an FSM violation.
func TestXSub_SendNonSubscribeRejected(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewXSubEngine()
	defer eng.Close()

	err := eng.Send(context.Background(), msg.FromBytes([]byte("not a sub msg")))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation)
}

// TestXSub_RecvFiltersByLocalSubscription confirms XSUB still applies
// the local subscription filter on inbound data.
func TestXSub_RecvFiltersByLocalSubscription(t *testing.T) {
	t.Parallel()
	eng, _ := socket.NewXSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testCtx(t)

	require.NoError(t, eng.Send(ctx, subscribeMsg(true, []byte("ok"))), "xsub subscribe")
	// Drain the forwarded subscription so it doesn't collide with data.
	drainCtx, dc := context.WithTimeout(ctx, 100*time.Millisecond)
	_, _ = b.Recv(drainCtx)
	dc()

	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte("bad"), []byte("x"))), "peer send bad")
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte("okay"), []byte("y"))), "peer send good")

	m, err := eng.Recv(ctx)
	require.NoError(t, err, "xsub recv")
	require.Equal(t, "okay", string(m.Frames()[0]))
}
