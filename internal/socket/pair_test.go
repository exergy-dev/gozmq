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

func TestPair_AttachExclusive(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	a1, b1 := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	a2, b2 := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a1, b1, a2, b2)

	if err := eng.AttachPipe(a1); err != nil {
		t.Fatalf("first attach: %v", err)
	}
	if err := eng.AttachPipe(a2); !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("second attach = %v, want ErrFSMViolation", err)
	}
}

func TestPair_RoundTrip(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	a, b := pipe.NewPair(2, 2, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := eng.Send(ctx, msg.FromBytes([]byte("ping"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := b.Recv(ctx)
	if err != nil {
		t.Fatalf("peer recv: %v", err)
	}
	if string(got.Bytes()) != "ping" {
		t.Fatalf("got %q, want ping", got.Bytes())
	}

	if err := b.Send(ctx, msg.FromBytes([]byte("pong"))); err != nil {
		t.Fatalf("peer send: %v", err)
	}
	got, err = eng.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if string(got.Bytes()) != "pong" {
		t.Fatalf("got %q, want pong", got.Bytes())
	}
}

func TestPair_SendBeforeAttachWaits(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
	defer eng.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- eng.Send(ctx, msg.FromBytes([]byte("x"))) }()
	select {
	case <-done:
		t.Fatal("send returned before pipe attached")
	case <-time.After(50 * time.Millisecond):
	}
	// PAIR has no notify path on attach in our impl — we cancel via close.
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("send err = %v, want ctx err", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not unblock on ctx cancel")
	}
}

func TestPair_CloseUnblocksSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()

	a, b := pipe.NewPair(0, 0, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Send(context.Background(), msg.FromBytes([]byte("x"))) }()
	time.Sleep(50 * time.Millisecond)
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, zmqerr.ErrSocketClosed) {
			t.Fatalf("send err = %v, want ErrSocketClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not unblock on close")
	}
}

func TestPair_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	if err := eng.AttachPipe(a); err != nil {
		t.Fatalf("attach: %v", err)
	}
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

func TestPair_CloseUnblocksRecvBeforeAttach(t *testing.T) {
	t.Parallel()
	eng := socket.NewPairEngine()
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
