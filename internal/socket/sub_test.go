package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

// TestSub_FilterDropsNonMatching: a SUB with one subscription drops
// inbound messages whose first frame doesn't have the prefix.
func TestSub_FilterDropsNonMatching(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	require.NoError(t, sub.Subscribe([]byte("go")), "subscribe")
	// Drain the forwarded subscription that Subscribe pushed onto the
	// peer pipe so it doesn't interfere with the test.
	drainCtx, dc := context.WithTimeout(ctx, 100*time.Millisecond)
	_, _ = b.Recv(drainCtx)
	dc()

	// Peer sends three messages: one matches, two don't.
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte("rust"), []byte("x"))), "peer send rust")
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte("gopher"), []byte("y"))), "peer send gopher")
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte("python"), []byte("z"))), "peer send python")

	m, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, 2, m.NumFrames())
	require.Equal(t, "gopher", string(m.Frames()[0]))
	// The next Recv should not yield anything within a short window.
	recvCtx, rc := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer rc()
	_, err = eng.Recv(recvCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestSub_EmptySubscriptionAcceptsEverything: an empty prefix matches
// all topics.
func TestSub_EmptySubscriptionAcceptsEverything(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	require.NoError(t, sub.Subscribe([]byte{}), "subscribe")
	ctx := testutil.Ctx(t)

	// Drain forwarded subscribe.
	drainCtx, dc := context.WithTimeout(ctx, 100*time.Millisecond)
	_, _ = b.Recv(drainCtx)
	dc()

	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte("anything"), []byte("payload"))), "peer send")
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, "anything", string(m.Frames()[0]))
}

// TestSub_ForwardsSubscriptionToAttachedPeer: Subscribe forwards a
// synthetic SUBSCRIBE message to every current peer.
func TestSub_ForwardsSubscriptionToAttachedPeer(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	require.NoError(t, sub.Subscribe([]byte("topic")), "subscribe")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m, err := b.Recv(ctx)
	require.NoError(t, err, "peer recv subscribe")
	require.Equal(t, 1, m.NumFrames(), "single-frame subscription msg")
	body := m.Frames()[0]
	require.Len(t, body, 1+len("topic"))
	require.Equal(t, socket.SubscribeByte, body[0])
	require.Equal(t, "topic", string(body[1:]))
}

// TestSub_ReplaysSubscriptionsOnAttach: when a peer attaches AFTER a
// Subscribe, all current subscriptions are replayed to that peer.
func TestSub_ReplaysSubscriptionsOnAttach(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	// Subscribe before any peer is attached.
	require.NoError(t, sub.Subscribe([]byte("foo")), "subscribe foo")
	require.NoError(t, sub.Subscribe([]byte("bar")), "subscribe bar")

	a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		m, err := b.Recv(ctx)
		require.NoError(t, err, "peer recv %d", i)
		body := m.Frames()[0]
		require.Equal(t, socket.SubscribeByte, body[0], "expected subscribe byte")
		got[string(body[1:])] = true
	}
	require.True(t, got["foo"] && got["bar"], "replayed subs = %v, want foo+bar", got)
}

// TestSub_RefcountedSubscribe: two Subscribe(foo) calls only forward
// one SUBSCRIBE, and require two Unsubscribes before CANCEL forwards.
func TestSub_RefcountedSubscribe(t *testing.T) {
	t.Parallel()
	eng, sub := socket.NewSubEngine()
	defer eng.Close()

	a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	ctx := testutil.Ctx(t)

	require.NoError(t, sub.Subscribe([]byte("x")), "sub1")
	require.NoError(t, sub.Subscribe([]byte("x")), "sub2")
	// Exactly one SUBSCRIBE should surface.
	m, err := b.Recv(ctx)
	require.NoError(t, err, "recv sub")
	require.Equal(t, socket.SubscribeByte, m.Frames()[0][0])
	require.Equal(t, "x", string(m.Frames()[0][1:]))
	// No second subscribe should be pending.
	shortCtx, sc := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = b.Recv(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "expected no second SUBSCRIBE")
	sc()

	// One Unsubscribe: still nothing (refcount still >0).
	require.NoError(t, sub.Unsubscribe([]byte("x")), "unsub1")
	shortCtx, sc = context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = b.Recv(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "unexpected CANCEL after first unsubscribe")
	sc()

	// Second Unsubscribe: CANCEL should surface.
	require.NoError(t, sub.Unsubscribe([]byte("x")), "unsub2")
	m, err = b.Recv(ctx)
	require.NoError(t, err, "recv cancel")
	require.Equal(t, socket.UnsubscribeByte, m.Frames()[0][0])
	require.Equal(t, "x", string(m.Frames()[0][1:]))
}
