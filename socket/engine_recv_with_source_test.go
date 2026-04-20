package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/pipe"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/socket"
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

	if err := fq.Attach(aEng); err != nil {
		t.Fatalf("attach A: %v", err)
	}
	if err := fq.Attach(bEng); err != nil {
		t.Fatalf("attach B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Send from B first; assert source is bEng.
	if err := bPeer.Send(ctx, msg.FromBytes([]byte("from-B"))); err != nil {
		t.Fatalf("bPeer send: %v", err)
	}
	m, src, err := fq.RecvWithSource(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if string(m.Bytes()) != "from-B" {
		t.Fatalf("body = %q, want from-B", m.Bytes())
	}
	if src != bEng {
		t.Fatalf("source mismatch: want bEng, got %v", src)
	}

	// Now A.
	if err := aPeer.Send(ctx, msg.FromBytes([]byte("from-A"))); err != nil {
		t.Fatalf("aPeer send: %v", err)
	}
	m, src, err = fq.RecvWithSource(ctx)
	if err != nil {
		t.Fatalf("recv2: %v", err)
	}
	if string(m.Bytes()) != "from-A" {
		t.Fatalf("body = %q, want from-A", m.Bytes())
	}
	if src != aEng {
		t.Fatalf("source mismatch: want aEng, got %v", src)
	}
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
	if err := fq.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case r := <-done:
		if !errors.Is(r.err, zmqerr.ErrSocketClosed) {
			t.Fatalf("err = %v, want ErrSocketClosed", r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("recv did not unblock on close")
	}
}
