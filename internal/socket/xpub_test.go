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

// TestXPub_SurfacesSubscribeToApplication: XPUB's Recv returns the
// synthetic subscription message sent by a peer.
//
// Construct with NewXPubEngineV(true) so dedup is disabled and the first
// (and only) SUBSCRIBE in this test is guaranteed to surface regardless
// of cross-peer refcount accounting.
func TestXPub_SurfacesSubscribeToApplication(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(true)
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)
	ctx := testutil.Ctx(t)

	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("news"))), "peer subscribe")
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "xpub recv")
	require.Equal(t, 1, m.NumFrames(), "xpub recv frames")
	body := m.Frames()[0]
	require.Equal(t, socket.SubscribeByte, body[0])
	require.Equal(t, "news", string(body[1:]))

	// Subsequent publish to the "news" topic should reach the peer
	// because XPUB updated its SubTree before surfacing.
	require.NoError(t, eng.Send(ctx, msg.FromFrames([]byte("news"), []byte("hello"))), "xpub send")
	got, err := b.Recv(ctx)
	require.NoError(t, err, "peer recv data")
	require.Equal(t, 2, got.NumFrames())
	require.Equal(t, "news", string(got.Frames()[0]))
}

// TestXPub_SurfacesUnsubscribe: an unsubscribe message surfaces too.
//
// Uses verbose=true so the refcount bookkeeping of the default-dedup
// mode doesn't swallow the single-peer cancel — verbose guarantees
// every command surfaces regardless of refcount.
func TestXPub_SurfacesUnsubscribe(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(true)
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("t"))), "sub")
	_, err := eng.Recv(ctx)
	require.NoError(t, err, "recv sub")
	require.NoError(t, b.Send(ctx, subscribeMsg(false, []byte("t"))), "cancel")
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "recv cancel")
	body := m.Frames()[0]
	require.Equal(t, socket.UnsubscribeByte, body[0])
	require.Equal(t, "t", string(body[1:]))
}

// TestXPub_CloseUnblocksRecv.
func TestXPub_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(true)

	err := testutil.RequireUnblocks(t, func() error {
		_, err := eng.Recv(context.Background())
		return err
	}, func() { require.NoError(t, eng.Close(), "close") })
	require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
}

// TestXPub_DedupDefault: default (non-verbose) XPUB deduplicates
// repeated SUBSCRIBE commands for the same prefix — only the first is
// surfaced to the application. The second identical SUBSCRIBE updates
// the peer's SubTree (fanout still works) but is silently swallowed by
// the application-facing Recv filter.
func TestXPub_DedupDefault(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(false)
	defer eng.Close()

	a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	// Two identical SUBSCRIBEs from the same peer.
	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("news"))), "sub1")
	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("news"))), "sub2")

	// First recv must return the subscribe event.
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "recv1")
	body := m.Frames()[0]
	require.Equal(t, socket.SubscribeByte, body[0])
	require.Equal(t, "news", string(body[1:]))

	// Second recv must NOT produce a duplicate SUBSCRIBE -- block and
	// time out. Use a short deadline so the test stays fast.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer shortCancel()
	_, err = eng.Recv(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "dedup should have swallowed second event")
}

// TestXPub_VerboseDisablesDedup: constructing with verbose=true causes
// every SUBSCRIBE/CANCEL to surface, including exact duplicates.
func TestXPub_VerboseDisablesDedup(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(true)
	defer eng.Close()

	a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("news"))), "sub1")
	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("news"))), "sub2")

	// Both events must surface.
	for i := 0; i < 2; i++ {
		m, err := eng.Recv(ctx)
		require.NoError(t, err, "recv[%d]", i)
		body := m.Frames()[0]
		require.Equal(t, socket.SubscribeByte, body[0], "event[%d]", i)
		require.Equal(t, "news", string(body[1:]), "event[%d]", i)
	}
}

// TestXPub_DedupLastCancel: in dedup mode the aggregate refcount across
// all peers controls surfacing — the first subscriber surfaces, a
// second peer subscribing to the same prefix is swallowed, and only the
// final unsubscribe (refcount -> 0) surfaces a CANCEL event.
func TestXPub_DedupLastCancel(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(false)
	defer eng.Close()

	a1, b1 := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	a2, b2 := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a1, b1, a2, b2)
	mustAttach(t, eng, a1, a2)

	ctx := testutil.Ctx(t)

	// Two distinct peers subscribe to "news". Only the first should
	// surface.
	require.NoError(t, b1.Send(ctx, subscribeMsg(true, []byte("news"))), "peer1 sub")
	_, err := eng.Recv(ctx)
	require.NoError(t, err, "first sub recv")
	require.NoError(t, b2.Send(ctx, subscribeMsg(true, []byte("news"))), "peer2 sub")

	// No second SUBSCRIBE surfaced.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = eng.Recv(shortCtx)
	shortCancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "expected no second subscribe")

	// Peer1 unsubscribes -- still one subscriber remaining (peer2), so
	// CANCEL is swallowed.
	require.NoError(t, b1.Send(ctx, subscribeMsg(false, []byte("news"))), "peer1 cancel")
	shortCtx2, shortCancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = eng.Recv(shortCtx2)
	shortCancel2()
	require.ErrorIs(t, err, context.DeadlineExceeded, "expected no cancel surface (refcount still 1)")

	// Peer2 unsubscribes -- refcount drops to 0, CANCEL surfaces.
	require.NoError(t, b2.Send(ctx, subscribeMsg(false, []byte("news"))), "peer2 cancel")
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "final cancel recv")
	body := m.Frames()[0]
	require.Equal(t, socket.UnsubscribeByte, body[0])
	require.Equal(t, "news", string(body[1:]))
}

// TestXPub_DetachEmitsCancel: when a peer detaches, XPUB synthesizes a
// CANCEL for every prefix whose aggregate subscriber count hit zero —
// matching libzmq, so an upstream proxy stage can unsubscribe dead
// topics. Runs in both dedup and verbose mode.
func TestXPub_DetachEmitsCancel(t *testing.T) {
	t.Parallel()
	for _, verbose := range []bool{false, true} {
		name := map[bool]string{false: "dedup", true: "verbose"}[verbose]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			eng := socket.NewXPubEngineV(verbose)
			defer eng.Close()

			a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
			defer closePipes(a, b)
			mustAttach(t, eng, a)
			ctx := testutil.Ctx(t)

			// Peer subscribes; the app observes it (which also seeds the
			// aggregate refcount bookkeeping).
			require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("topic"))), "peer subscribe")
			m, err := eng.Recv(ctx)
			require.NoError(t, err, "recv subscribe")
			require.Equal(t, socket.SubscribeByte, m.Frames()[0][0])

			// Peer goes away: the engine must synthesize the CANCEL.
			require.NoError(t, eng.DetachPipe(a), "detach")
			m, err = eng.Recv(ctx)
			require.NoError(t, err, "recv synthesized cancel")
			body := m.Frames()[0]
			require.Equal(t, socket.UnsubscribeByte, body[0], "expected CANCEL")
			require.Equal(t, "topic", string(body[1:]), "cancel prefix")
		})
	}
}

// TestXPub_DetachEmitsNoCancelWhileOtherSubscribersRemain: the synthesized
// CANCEL only fires when the aggregate count hits zero — a second peer
// still subscribed suppresses it.
func TestXPub_DetachEmitsNoCancelWhileOtherSubscribersRemain(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(false)
	defer eng.Close()

	a1, b1 := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	a2, b2 := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a1, b1, a2, b2)
	mustAttach(t, eng, a1, a2)
	ctx := testutil.Ctx(t)

	require.NoError(t, b1.Send(ctx, subscribeMsg(true, []byte("topic"))), "peer1 subscribe")
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "recv first subscribe")
	require.Equal(t, socket.SubscribeByte, m.Frames()[0][0])

	// Second subscriber for the same prefix: dedup-suppressed, but we
	// must wait for the engine to process it before detaching. Poll the
	// engine's metrics-free path via a short Recv timeout.
	require.NoError(t, b2.Send(ctx, subscribeMsg(true, []byte("topic"))), "peer2 subscribe")
	rc, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = eng.Recv(rc)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "duplicate subscribe must be dedup-suppressed")

	// Detach peer1: peer2 still holds the prefix → no CANCEL.
	require.NoError(t, eng.DetachPipe(a1), "detach peer1")
	rc, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = eng.Recv(rc)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "CANCEL emitted while a subscriber remains")

	// Detach peer2: now the aggregate hits zero → CANCEL.
	require.NoError(t, eng.DetachPipe(a2), "detach peer2")
	m, err = eng.Recv(ctx)
	require.NoError(t, err, "recv cancel")
	require.Equal(t, socket.UnsubscribeByte, m.Frames()[0][0])
}

// requireXPubEvent receives the next subscription event from eng and checks
// its kind and prefix.
func requireXPubEvent(ctx context.Context, t *testing.T, eng interface {
	Recv(context.Context) (*msg.Msg, error)
}, subscribe bool, prefix string) {
	t.Helper()
	m, err := eng.Recv(ctx)
	require.NoError(t, err)
	body := m.Frames()[0]
	want := socket.UnsubscribeByte
	if subscribe {
		want = socket.SubscribeByte
	}
	require.Equal(t, want, body[0], "event kind for %q", body[1:])
	require.Equal(t, prefix, string(body[1:]))
}

// TestXPub_DetachBeforeRecvSettlesRefcounts: a peer that subscribes and
// detaches before the application reads the SUBSCRIBE still gets its
// CANCEL synthesized, and leaves no stale refcount that would suppress a
// later subscriber's SUBSCRIBE.
func TestXPub_DetachBeforeRecvSettlesRefcounts(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(false)
	defer eng.Close()
	ctx := testutil.Ctx(t)

	a1, b1 := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a1, b1)
	mustAttach(t, eng, a1)
	require.NoError(t, b1.Send(ctx, subscribeMsg(true, []byte("A"))))
	require.Eventually(t, eng.CanRecvNow, 2*time.Second, time.Millisecond, "SUBSCRIBE queued")
	require.NoError(t, eng.DetachPipe(a1))

	requireXPubEvent(ctx, t, eng, true, "A")
	requireXPubEvent(ctx, t, eng, false, "A")

	a2, b2 := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a2, b2)
	mustAttach(t, eng, a2)
	require.NoError(t, b2.Send(ctx, subscribeMsg(true, []byte("A"))))
	requireXPubEvent(ctx, t, eng, true, "A")
}

// TestXPub_ForeignCancelIgnored: a CANCEL for a prefix the sending peer never
// subscribed to neither surfaces nor releases another peer's subscription.
func TestXPub_ForeignCancelIgnored(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(false)
	defer eng.Close()
	ctx := testutil.Ctx(t)

	good, goodPeer := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	evil, evilPeer := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(good, goodPeer, evil, evilPeer)
	mustAttach(t, eng, good, evil)

	require.NoError(t, goodPeer.Send(ctx, subscribeMsg(true, []byte("A"))))
	requireXPubEvent(ctx, t, eng, true, "A")

	require.NoError(t, evilPeer.Send(ctx, subscribeMsg(false, []byte("A"))))
	require.NoError(t, evilPeer.Send(ctx, subscribeMsg(true, []byte("C"))))
	// Per-peer FIFO: the foreign CANCEL was processed before SUBSCRIBE C.
	requireXPubEvent(ctx, t, eng, true, "C")

	// Only evil's own prefix is released when it leaves.
	require.NoError(t, eng.DetachPipe(evil))
	requireXPubEvent(ctx, t, eng, false, "C")

	// good's CANCEL is the last one for A, so it surfaces.
	require.NoError(t, goodPeer.Send(ctx, subscribeMsg(false, []byte("A"))))
	requireXPubEvent(ctx, t, eng, false, "A")
}

// TestXPub_DetachAppliesQueuedCommandsFirst: DetachPipe itself applies the
// commands a peer queued before leaving, so every SUBSCRIBE surfaces before
// the CANCEL synthesized for it, with no Send or Recv in between.
func TestXPub_DetachAppliesQueuedCommandsFirst(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(false)
	defer eng.Close()
	ctx := testutil.Ctx(t)

	a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	other, otherPeer := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b, other, otherPeer)
	mustAttach(t, eng, a, other)

	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("A"))))
	require.NoError(t, b.Send(ctx, subscribeMsg(true, []byte("B"))))
	require.NoError(t, b.Send(ctx, subscribeMsg(false, []byte("B"))))
	require.NoError(t, b.Close())
	require.NoError(t, eng.DetachPipe(a))
	require.EqualValues(t, 4, eng.Metrics().QueueDepth, "events queued by DetachPipe")

	requireXPubEvent(ctx, t, eng, true, "A")
	requireXPubEvent(ctx, t, eng, true, "B")
	requireXPubEvent(ctx, t, eng, false, "B")
	requireXPubEvent(ctx, t, eng, false, "A")
	require.False(t, eng.CanRecvNow())
}
