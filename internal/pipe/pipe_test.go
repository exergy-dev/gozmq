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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, a.Send(ctx, payload))
	got, err := b.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got.Bytes()))

	// SendHWM/RecvHWM/PeerEndpoint are concrete *end accessors, no
	// longer part of iface.Pipe.
	ae, be := a.(*end), b.(*end)
	require.Equal(t, 2, ae.SendHWM(), "SendHWM")
	require.Equal(t, 2, ae.RecvHWM(), "RecvHWM")
	require.Equal(t, "b-side", ae.PeerEndpoint())
	require.Equal(t, "a-side", be.PeerEndpoint())
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
		require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{byte(i)})), "a.Send[%d]", i)
	}

	// Fourth send must block. Confirm by using a deadline.
	blockCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err := a.Send(blockCtx, msg.FromBytes([]byte{byte(hwm)}))
	require.ErrorIs(t, err, context.DeadlineExceeded, "expected DeadlineExceeded on over-HWM send")

	// Drain one; another send must now succeed.
	_, err = b.Recv(ctx)
	require.NoError(t, err, "b.Recv drain")
	require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{byte(hwm)})), "a.Send post-drain")
}

func TestHWMZero_IsUnbounded(t *testing.T) {
	a, b := NewPair(0, 0, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		sendCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := a.Send(sendCtx, msg.FromBytes([]byte{byte(i)}))
		cancel()
		require.NoError(t, err, "a.Send[%d]", i)
	}

	for i := 0; i < 10000; i++ {
		got, err := b.Recv(ctx)
		require.NoError(t, err, "b.Recv[%d]", i)
		require.Equal(t, byte(i), got.Bytes()[0], "b.Recv[%d]", i)
	}
}

func TestHWMZero_MixedDirection(t *testing.T) {
	a, b := NewPair(0, 1, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{byte(i)})), "unbounded a.Send[%d]", i)
	}

	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("one"))))
	blockCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err := b.Send(blockCtx, msg.FromBytes([]byte("two")))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestClose_Unblocks_Send asserts that closing either end wakes a Send
// blocked on a full pipe with io.ErrClosedPipe.
func TestClose_Unblocks_Send(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	ctx := context.Background()
	// Fill the single slot.
	require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{1})))

	done := make(chan error, 1)
	go func() { done <- a.Send(ctx, msg.FromBytes([]byte{2})) }()

	// Brief settle; Send should be blocked.
	time.Sleep(5 * time.Millisecond)
	select {
	case err := <-done:
		require.Fail(t, "Send returned before close", "%v", err)
	default:
	}

	require.NoError(t, b.Close(), "b.Close")
	select {
	case err := <-done:
		require.ErrorIs(t, err, io.ErrClosedPipe)
	case <-time.After(time.Second):
		require.Fail(t, "Send did not wake within 1s of peer Close")
	}
}

// TestClose_Drains_Then_EOF asserts Recv drains buffered messages before
// signaling EOF after peer close.
func TestClose_Drains_Then_EOF(t *testing.T) {
	a, b := NewPair(4, 4, PeerInfo{}, PeerInfo{})

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{byte(i)})))
	}
	require.NoError(t, a.Close())

	for i := 0; i < 3; i++ {
		m, err := b.Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		require.Equal(t, byte(i), m.Bytes()[0], "Recv[%d]", i)
	}
	_, err := b.Recv(ctx)
	require.ErrorIs(t, err, io.EOF, "expected io.EOF after drain")
}

// TestCtx_Cancel_Wakes_Send asserts ctx.Err() propagates out of Send.
func TestCtx_Cancel_Wakes_Send(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	require.NoError(t, a.Send(context.Background(), msg.FromBytes([]byte{1})))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- a.Send(ctx, msg.FromBytes([]byte{2})) }()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		require.Fail(t, "Send did not honor ctx.Cancel")
	}
}

func TestTrySend_FastPath(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	endA, ok := a.(*end)
	require.True(t, ok, "pipe end type = %T, want *end", a)
	require.True(t, endA.TrySend(msg.FromBytes([]byte("one"))), "TrySend on empty pipe should succeed")
	assert.False(t, endA.TrySend(msg.FromBytes([]byte("two"))), "TrySend on full pipe should fail")
	got, err := b.Recv(context.Background())
	require.NoError(t, err)
	require.Equal(t, "one", string(got.Bytes()))
}

// TestDoubleClose_Idempotent ensures Close is safe to call repeatedly.
func TestDoubleClose_Idempotent(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	require.NoError(t, a.Close())
	require.NoError(t, a.Close())
	require.NoError(t, b.Close())
}

// TestClosed_Channel fires once either side closes.
func TestClosed_Channel(t *testing.T) {
	a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	select {
	case <-a.Closed():
		require.Fail(t, "Closed() fired before Close call")
	default:
	}
	_ = b.Close()
	select {
	case <-a.Closed():
	case <-time.After(time.Second):
		require.Fail(t, "Closed() did not fire after peer Close")
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

// TestHWMZero_CloseDrainsThenEOF is the HWM-0 counterpart of
// TestClose_Drains_Then_EOF: messages parked inside the unbounded relay's
// queue (not yet visible on the out channel) must still be delivered after
// the sender closes, with EOF only after the relay's flush completes.
func TestHWMZero_CloseDrainsThenEOF(t *testing.T) {
	a, b := NewPair(0, 0, PeerInfo{}, PeerInfo{})
	defer b.Close()

	ctx := context.Background()
	const n = 200
	for i := 0; i < n; i++ {
		require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{byte(i)})), "a.Send[%d]", i)
	}
	require.NoError(t, a.Close())

	for i := 0; i < n; i++ {
		m, err := b.Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		require.Equal(t, byte(i), m.Bytes()[0], "Recv[%d]", i)
	}
	_, err := b.Recv(ctx)
	require.ErrorIs(t, err, io.EOF, "expected io.EOF after relay drain")
}

// TestSetRecvReady_ArmsOnSendAndClose verifies the recv-ready gate is
// armed from the data path (every enqueue) and on close, including for
// HWM-0 relay-backed directions — the level-trigger contract the Poller
// readiness fix relies on.
func TestSetRecvReady_ArmsOnSendAndClose(t *testing.T) {
	for _, hwm := range []int{4, 0} {
		t.Run(map[int]string{4: "hwm4", 0: "hwm0"}[hwm], func(t *testing.T) {
			a, b := NewPair(hwm, hwm, PeerInfo{}, PeerInfo{})
			defer a.Close()
			defer b.Close()

			gate := make(chan struct{}, 1)
			b.(*end).SetRecvReady(gate)
			select {
			case <-gate:
				require.Fail(t, "gate armed with no data")
			case <-time.After(20 * time.Millisecond):
			}

			require.NoError(t, a.Send(context.Background(), msg.FromBytes([]byte("x"))))
			select {
			case <-gate:
			case <-time.After(time.Second):
				require.Fail(t, "gate not armed by enqueue")
			}

			// Drain the message, then close: the gate must arm again so a
			// parked poller wakes and observes the close.
			_, err := b.Recv(context.Background())
			require.NoError(t, err)
			select {
			case <-gate:
				// A relay-backed direction may have re-armed during its
				// in→out hop; drain and continue to the close check.
			default:
			}
			require.NoError(t, a.Close())
			select {
			case <-gate:
			case <-time.After(time.Second):
				require.Fail(t, "gate not armed by close")
			}
		})
	}
}

// TestSetRecvReady_LateRegistrationArmsIfPending verifies a gate registered
// after data already arrived is armed immediately (level trigger).
func TestSetRecvReady_LateRegistrationArmsIfPending(t *testing.T) {
	a, b := NewPair(4, 4, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()

	require.NoError(t, a.Send(context.Background(), msg.FromBytes([]byte("x"))))
	gate := make(chan struct{}, 1)
	b.(*end).SetRecvReady(gate)
	select {
	case <-gate:
	case <-time.After(time.Second):
		require.Fail(t, "late-registered gate not armed despite buffered data")
	}
}
