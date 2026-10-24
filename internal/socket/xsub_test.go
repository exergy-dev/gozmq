package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// TestXSub_SendSubscribeViaMessage: XSUB's Send accepts a synthetic
// subscribe message and forwards it to every attached peer while also
// updating the local filter.
func TestXSub_SendSubscribeViaMessage(t *testing.T) {
	t.Parallel()
	eng := socket.NewXSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	ctx := testutil.Ctx(t)

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
	eng := socket.NewXSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	ctx := testutil.Ctx(t)

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
	eng := socket.NewXSubEngine()
	defer eng.Close()

	err := eng.Send(context.Background(), msg.FromBytes([]byte("not a sub msg")))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation)
}

// TestXSub_RecvFiltersByLocalSubscription confirms XSUB still applies
// the local subscription filter on inbound data.
func TestXSub_RecvFiltersByLocalSubscription(t *testing.T) {
	t.Parallel()
	eng := socket.NewXSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

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

// TestSubXSub_SubscriptionShapedData: data that begins with the 0x01
// subscribe byte is ordinary data for SUB and XSUB alike — it goes through
// the local filter like any other message — with one peer (FairQueue single
// mode) and two.
func TestSubXSub_SubscriptionShapedData(t *testing.T) {
	t.Parallel()
	for _, peers := range []int{1, 2} {
		for _, xsub := range []bool{false, true} {
			var eng socket.Engine
			var sub *socket.SubEngine
			if xsub {
				sub = socket.NewXSubEngine()
				eng = sub
			} else {
				sub = socket.NewSubEngine()
				eng = sub
			}
			ctx := testutil.Ctx(t)
			ends, peerEnds := makePairs(peers, 16, 16)
			mustAttach(t, eng, ends...)
			require.NoError(t, sub.Subscribe([]byte("\x01d")))
			p := peerEnds[0]
			require.NoError(t, p.Send(ctx, subscribeMsg(true, []byte("cmd"))))
			require.NoError(t, p.Send(ctx, subscribeMsg(true, []byte("data"))))

			m, err := eng.Recv(ctx)
			require.NoError(t, err)
			require.Equal(t, "\x01data", string(m.Bytes()), "peers=%d xsub=%v: filtered like data", peers, xsub)
			require.NoError(t, eng.Close())
			closePipes(append(ends, peerEnds...)...)
		}
	}
}
