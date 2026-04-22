package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/socket"
	"github.com/rkm/gozmq/internal/zmqerr"
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
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := b.Send(ctx, subscribeMsg(true, []byte("news"))); err != nil {
		t.Fatalf("peer subscribe: %v", err)
	}
	m, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("xpub recv: %v", err)
	}
	if m.NumFrames() != 1 {
		t.Fatalf("xpub recv frames = %d, want 1", m.NumFrames())
	}
	body := m.Frames()[0]
	if body[0] != socket.SubscribeByte || string(body[1:]) != "news" {
		t.Fatalf("xpub surfaced body = %v", body)
	}

	// Subsequent publish to the "news" topic should reach the peer
	// because XPUB updated its SubTree before surfacing.
	if err := eng.Send(ctx, msg.FromFrames([]byte("news"), []byte("hello"))); err != nil {
		t.Fatalf("xpub send: %v", err)
	}
	got, err := b.Recv(ctx)
	if err != nil {
		t.Fatalf("peer recv data: %v", err)
	}
	if got.NumFrames() != 2 || string(got.Frames()[0]) != "news" {
		t.Fatalf("peer recv data = %v", got.Frames())
	}
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
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := b.Send(ctx, subscribeMsg(true, []byte("t"))); err != nil {
		t.Fatalf("sub: %v", err)
	}
	if _, err := eng.Recv(ctx); err != nil {
		t.Fatalf("recv sub: %v", err)
	}
	if err := b.Send(ctx, subscribeMsg(false, []byte("t"))); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	m, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("recv cancel: %v", err)
	}
	body := m.Frames()[0]
	if body[0] != socket.UnsubscribeByte || string(body[1:]) != "t" {
		t.Fatalf("surfaced cancel = %v", body)
	}
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
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, zmqerr.ErrSocketClosed) {
			t.Fatalf("recv err = %v, want ErrSocketClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("recv did not unblock on close")
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
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Two identical SUBSCRIBEs from the same peer.
	if err := b.Send(ctx, subscribeMsg(true, []byte("news"))); err != nil {
		t.Fatalf("sub1: %v", err)
	}
	if err := b.Send(ctx, subscribeMsg(true, []byte("news"))); err != nil {
		t.Fatalf("sub2: %v", err)
	}

	// First recv must return the subscribe event.
	m, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("recv1: %v", err)
	}
	body := m.Frames()[0]
	if body[0] != socket.SubscribeByte || string(body[1:]) != "news" {
		t.Fatalf("first event body = %v", body)
	}

	// Second recv must NOT produce a duplicate SUBSCRIBE — block and
	// time out. Use a short deadline so the test stays fast.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer shortCancel()
	m2, err := eng.Recv(shortCtx)
	if err == nil {
		t.Fatalf("unexpected second event %v — dedup should have swallowed it", m2.Frames()[0])
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recv err = %v, want DeadlineExceeded", err)
	}
}

// TestXPub_VerboseDisablesDedup: constructing with verbose=true causes
// every SUBSCRIBE/CANCEL to surface, including exact duplicates.
func TestXPub_VerboseDisablesDedup(t *testing.T) {
	t.Parallel()
	eng := socket.NewXPubEngineV(true)
	defer eng.Close()

	a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := b.Send(ctx, subscribeMsg(true, []byte("news"))); err != nil {
		t.Fatalf("sub1: %v", err)
	}
	if err := b.Send(ctx, subscribeMsg(true, []byte("news"))); err != nil {
		t.Fatalf("sub2: %v", err)
	}

	// Both events must surface.
	for i := 0; i < 2; i++ {
		m, err := eng.Recv(ctx)
		if err != nil {
			t.Fatalf("recv[%d]: %v", i, err)
		}
		body := m.Frames()[0]
		if body[0] != socket.SubscribeByte || string(body[1:]) != "news" {
			t.Fatalf("event[%d] body = %v", i, body)
		}
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
	if err := eng.AttachPipe(a1); err != nil {
		t.Fatalf("attach peer1: %v", err)
	}
	if err := eng.AttachPipe(a2); err != nil {
		t.Fatalf("attach peer2: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Two distinct peers subscribe to "news". Only the first should
	// surface.
	if err := b1.Send(ctx, subscribeMsg(true, []byte("news"))); err != nil {
		t.Fatalf("peer1 sub: %v", err)
	}
	if _, err := eng.Recv(ctx); err != nil {
		t.Fatalf("first sub recv: %v", err)
	}
	if err := b2.Send(ctx, subscribeMsg(true, []byte("news"))); err != nil {
		t.Fatalf("peer2 sub: %v", err)
	}

	// No second SUBSCRIBE surfaced.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if _, err := eng.Recv(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		shortCancel()
		t.Fatalf("expected no second subscribe, got %v", err)
	}
	shortCancel()

	// Peer1 unsubscribes — still one subscriber remaining (peer2), so
	// CANCEL is swallowed.
	if err := b1.Send(ctx, subscribeMsg(false, []byte("news"))); err != nil {
		t.Fatalf("peer1 cancel: %v", err)
	}
	shortCtx2, shortCancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if _, err := eng.Recv(shortCtx2); !errors.Is(err, context.DeadlineExceeded) {
		shortCancel2()
		t.Fatalf("expected no cancel surface (refcount still 1), got %v", err)
	}
	shortCancel2()

	// Peer2 unsubscribes — refcount drops to 0, CANCEL surfaces.
	if err := b2.Send(ctx, subscribeMsg(false, []byte("news"))); err != nil {
		t.Fatalf("peer2 cancel: %v", err)
	}
	m, err := eng.Recv(ctx)
	if err != nil {
		t.Fatalf("final cancel recv: %v", err)
	}
	body := m.Frames()[0]
	if body[0] != socket.UnsubscribeByte || string(body[1:]) != "news" {
		t.Fatalf("cancel body = %v", body)
	}
}
