package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/socket"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/stretchr/testify/require"
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
	ctx := testCtx(t)

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

	ctx := testCtx(t)

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

	ctx := testCtx(t)

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

	ctx := testCtx(t)

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

	ctx := testCtx(t)

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
