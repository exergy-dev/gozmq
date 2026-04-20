package pipe

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/msg"
	"pgregory.net/rapid"
)

// TestRoundTrip_Basic is the smoke test — a Send on A surfaces on B.Recv
// and vice versa.
func TestRoundTrip_Basic(t *testing.T) {
	a, b := NewPair(2, 2, PeerInfo{Endpoint: "b-side"}, PeerInfo{Endpoint: "a-side"})
	defer a.Close()
	defer b.Close()

	ctx := context.Background()

	payload := msg.FromBytes([]byte("hello"))
	if err := a.Send(ctx, payload); err != nil {
		t.Fatalf("a.Send: %v", err)
	}
	got, err := b.Recv(ctx)
	if err != nil {
		t.Fatalf("b.Recv: %v", err)
	}
	if string(got.Bytes()) != "hello" {
		t.Fatalf("got %q, want hello", got.Bytes())
	}

	if a.SendHWM() != 2 || a.RecvHWM() != 2 {
		t.Fatalf("HWM report wrong on A: send=%d recv=%d", a.SendHWM(), a.RecvHWM())
	}
	if a.PeerEndpoint() != "b-side" {
		t.Fatalf("a.PeerEndpoint = %q, want b-side", a.PeerEndpoint())
	}
	if b.PeerEndpoint() != "a-side" {
		t.Fatalf("b.PeerEndpoint = %q, want a-side", b.PeerEndpoint())
	}
}

// TestHWM_Blocks_Send_On_Full asserts that Send blocks once HWM buffered
// messages are in flight, and unblocks within bounded time after Recv.
func TestHWM_Blocks_Send_On_Full(t *testing.T) {
	const hwm = 3
	a, b := NewPair(hwm, hwm, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	ctx := context.Background()

	for i := 0; i < hwm; i++ {
		if err := a.Send(ctx, msg.FromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("a.Send[%d]: %v", i, err)
		}
	}

	// Fourth send must block. Confirm by using a deadline.
	blockCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err := a.Send(blockCtx, msg.FromBytes([]byte{byte(hwm)}))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded on over-HWM send, got %v", err)
	}

	// Drain one; another send must now succeed.
	if _, err := b.Recv(ctx); err != nil {
		t.Fatalf("b.Recv drain: %v", err)
	}
	if err := a.Send(ctx, msg.FromBytes([]byte{byte(hwm)})); err != nil {
		t.Fatalf("a.Send post-drain: %v", err)
	}
}

// TestClose_Unblocks_Send asserts that closing either end wakes a Send
// blocked on a full pipe with io.ErrClosedPipe.
func TestClose_Unblocks_Send(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	ctx := context.Background()
	// Fill the single slot.
	if err := a.Send(ctx, msg.FromBytes([]byte{1})); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- a.Send(ctx, msg.FromBytes([]byte{2})) }()

	// Brief settle; Send should be blocked.
	time.Sleep(5 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Send returned before close: %v", err)
	default:
	}

	if err := b.Close(); err != nil {
		t.Fatalf("b.Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Send err = %v, want io.ErrClosedPipe", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send did not wake within 1s of peer Close")
	}
}

// TestClose_Drains_Then_EOF asserts Recv drains buffered messages before
// signaling EOF after peer close.
func TestClose_Drains_Then_EOF(t *testing.T) {
	a, b := NewPair(4, 4, PeerInfo{}, PeerInfo{})

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := a.Send(ctx, msg.FromBytes([]byte{byte(i)})); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		m, err := b.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv[%d]: %v", i, err)
		}
		if m.Bytes()[0] != byte(i) {
			t.Fatalf("Recv[%d] = %v, want %d", i, m.Bytes(), i)
		}
	}
	if _, err := b.Recv(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after drain, got %v", err)
	}
}

// TestCtx_Cancel_Wakes_Send asserts ctx.Err() propagates out of Send.
func TestCtx_Cancel_Wakes_Send(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	if err := a.Send(context.Background(), msg.FromBytes([]byte{1})); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- a.Send(ctx, msg.FromBytes([]byte{2})) }()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send did not honor ctx.Cancel")
	}
}

func TestTrySend_FastPath(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	endA, ok := a.(*end)
	if !ok {
		t.Fatalf("pipe end type = %T, want *end", a)
	}
	if !endA.TrySend(msg.FromBytes([]byte("one"))) {
		t.Fatal("TrySend on empty pipe = false, want true")
	}
	if endA.TrySend(msg.FromBytes([]byte("two"))) {
		t.Fatal("TrySend on full pipe = true, want false")
	}
	got, err := b.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(got.Bytes()) != "one" {
		t.Fatalf("Recv = %q, want one", got.Bytes())
	}
}

// TestDoubleClose_Idempotent ensures Close is safe to call repeatedly.
func TestDoubleClose_Idempotent(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestClosed_Channel fires once either side closes.
func TestClosed_Channel(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	select {
	case <-a.Closed():
		t.Fatal("Closed() fired before Close call")
	default:
	}
	_ = b.Close()
	select {
	case <-a.Closed():
	case <-time.After(time.Second):
		t.Fatal("Closed() did not fire after peer Close")
	}
}

// TestProperty_HWM_Invariant is the design §10.1 invariant: the number of
// in-flight messages on a channel never exceeds its HWM capacity.
//
// Strategy: drive random interleaved Send/Recv ops and track the pending
// count. Send succeeds only when pending < HWM; Recv succeeds only when
// pending > 0. We use a non-blocking ctx for Send to observe the HWM
// boundary deterministically.
func TestProperty_HWM_Invariant(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		hwm := rapid.IntRange(1, 8).Draw(rt, "hwm")
		ops := rapid.IntRange(1, 200).Draw(rt, "ops")

		a, b := NewPair(hwm, hwm, PeerInfo{}, PeerInfo{})
		defer a.Close()
		defer b.Close()

		pending := 0
		sent := 0
		received := 0

		for i := 0; i < ops; i++ {
			op := rapid.IntRange(0, 1).Draw(rt, "op")
			if op == 0 {
				// Non-blocking try-send (zero-deadline ctx).
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				err := a.Send(ctx, msg.FromBytes([]byte{byte(sent)}))
				cancel()
				if err == nil {
					pending++
					sent++
					if pending > hwm {
						rt.Fatalf("HWM violated: pending=%d hwm=%d", pending, hwm)
					}
				} else if !errors.Is(err, context.DeadlineExceeded) {
					rt.Fatalf("unexpected Send err: %v", err)
				}
			} else {
				// Non-blocking try-recv.
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				m, err := b.Recv(ctx)
				cancel()
				if err == nil {
					pending--
					received++
					if pending < 0 {
						rt.Fatalf("negative pending (%d) after Recv", pending)
					}
					if m.Bytes()[0] != byte(received-1) {
						rt.Fatalf("FIFO violation: got %d want %d", m.Bytes()[0], received-1)
					}
				} else if !errors.Is(err, context.DeadlineExceeded) {
					rt.Fatalf("unexpected Recv err: %v", err)
				}
			}
		}
	})
}

// TestProperty_Bounded_Unblock asserts a blocked Send wakes within a
// bounded window after a Recv creates space — the second design §10.1
// invariant.
func TestProperty_Bounded_Unblock(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		hwm := rapid.IntRange(1, 4).Draw(rt, "hwm")
		a, b := NewPair(hwm, hwm, PeerInfo{}, PeerInfo{})
		defer a.Close()
		defer b.Close()

		ctx := context.Background()
		// Fill.
		for i := 0; i < hwm; i++ {
			if err := a.Send(ctx, msg.FromBytes([]byte{byte(i)})); err != nil {
				rt.Fatalf("fill[%d]: %v", i, err)
			}
		}

		// Launch a blocked Send; measure the gap between Recv and unblock.
		var woke atomic.Int64
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = a.Send(ctx, msg.FromBytes([]byte{42}))
			woke.Store(time.Now().UnixNano())
		}()
		time.Sleep(2 * time.Millisecond) // let goroutine park on send

		freed := time.Now().UnixNano()
		if _, err := b.Recv(ctx); err != nil {
			rt.Fatalf("drain: %v", err)
		}
		wg.Wait()

		gap := time.Duration(woke.Load() - freed)
		if gap < 0 || gap > 100*time.Millisecond {
			rt.Fatalf("Send unblock gap = %v (want ≤100ms)", gap)
		}
	})
}
