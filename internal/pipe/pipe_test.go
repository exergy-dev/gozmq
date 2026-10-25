package pipe

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

// TestRoundTrip_Basic is the smoke test — a Send on A surfaces on B.Recv
// and vice versa.
func TestRoundTrip_Basic(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(2, 2, PeerInfo{Identity: []byte("b-id")}, PeerInfo{Identity: []byte("a-id")})
		defer a.Close()
		defer b.Close()

		ctx := context.Background()

		payload := msg.FromBytes([]byte("hello"))
		require.NoError(t, a.Send(ctx, payload))
		got, err := b.Recv(ctx)
		require.NoError(t, err)
		require.Equal(t, "hello", string(got.Bytes()))

		require.Equal(t, "b-id", string(a.PeerIdentity()), "A peer identity")
		require.Equal(t, "a-id", string(b.PeerIdentity()), "B peer identity")
	})
}

// TestHWM_Blocks_Send_On_Full asserts that Send blocks once HWM buffered
// messages are in flight, and unblocks within bounded time after Recv.
func TestHWM_Blocks_Send_On_Full(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		const hwm = 3
		a, b := k.newPair(hwm, hwm, PeerInfo{}, PeerInfo{})
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
	})
}

func TestHWMZero_IsUnbounded(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(0, 0, PeerInfo{}, PeerInfo{})
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
	})
}

func TestHWMZero_MixedDirection(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(0, 1, PeerInfo{}, PeerInfo{})
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
	})
}

// TestClose_Unblocks_Send asserts that closing either end wakes a Send
// blocked on a full pipe with io.ErrClosedPipe.
func TestClose_Unblocks_Send(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(1, 1, PeerInfo{}, PeerInfo{})
		defer a.Close()
		defer b.Close()

		ctx := context.Background()
		// Fill the single slot.
		require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{1})))

		err := testutil.RequireUnblocks(t,
			func() error { return a.Send(ctx, msg.FromBytes([]byte{2})) },
			func() { require.NoError(t, b.Close(), "b.Close") })
		require.ErrorIs(t, err, io.ErrClosedPipe)
	})
}

// TestClose_Drains_Then_EOF asserts Recv drains buffered messages before
// signaling EOF after peer close.
func TestClose_Drains_Then_EOF(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(4, 4, PeerInfo{}, PeerInfo{})

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
	})
}

// TestCtx_Cancel_Wakes_Send asserts ctx.Err() propagates out of Send.
func TestCtx_Cancel_Wakes_Send(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(1, 1, PeerInfo{}, PeerInfo{})
		defer a.Close()
		defer b.Close()

		require.NoError(t, a.Send(context.Background(), msg.FromBytes([]byte{1})))

		ctx, cancel := context.WithCancel(context.Background())
		err := testutil.RequireUnblocks(t,
			func() error { return a.Send(ctx, msg.FromBytes([]byte{2})) },
			cancel)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestTrySend_FastPath(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(1, 1, PeerInfo{}, PeerInfo{})
		defer a.Close()
		defer b.Close()

		require.True(t, a.TrySend(msg.FromBytes([]byte("one"))), "TrySend on empty pipe should succeed")
		assert.False(t, a.TrySend(msg.FromBytes([]byte("two"))), "TrySend on full pipe should fail")
		got, err := b.Recv(context.Background())
		require.NoError(t, err)
		require.Equal(t, "one", string(got.Bytes()))
	})
}

// TestDoubleClose_Idempotent ensures Close is safe to call repeatedly.
func TestDoubleClose_Idempotent(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(1, 1, PeerInfo{}, PeerInfo{})
		require.NoError(t, a.Close())
		require.NoError(t, a.Close())
		require.NoError(t, b.Close())
	})
}

// TestClosed_Channel fires once either side closes.
func TestClosed_Channel(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(1, 1, PeerInfo{}, PeerInfo{})
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
	})
}

// TestProperty_HWM_Invariant is the design §10.1 invariant: the number of
// in-flight messages on a channel never exceeds its HWM capacity.
//
// Strategy: for each HWM, drive a seeded random interleaving of TrySend
// and TryRecv and track the pending count. The pipe is deterministic from
// one goroutine, so the boundary is exact: TrySend succeeds iff pending <
// HWM, TryRecv succeeds iff pending > 0, and delivery is FIFO.
func TestProperty_HWM_Invariant(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		rng := rand.New(rand.NewPCG(1, 2))
		const ops = 300
		for hwm := 1; hwm <= 8; hwm++ {
			a, b := k.newPair(hwm, hwm, PeerInfo{}, PeerInfo{})
			pending, sent, received := 0, 0, 0
			for i := 0; i < ops; i++ {
				if rng.IntN(2) == 0 {
					ok := a.TrySend(msg.FromBytes([]byte{byte(sent)}))
					require.Equal(t, pending < hwm, ok, "hwm=%d op=%d: TrySend with %d pending", hwm, i, pending)
					if ok {
						pending++
						sent++
					}
					require.LessOrEqual(t, pending, hwm, "hwm=%d op=%d: HWM violated", hwm, i)
				} else {
					m, ok := b.TryRecv()
					require.Equal(t, pending > 0, ok, "hwm=%d op=%d: TryRecv with %d pending", hwm, i, pending)
					if ok {
						pending--
						received++
						require.GreaterOrEqual(t, pending, 0, "hwm=%d op=%d: negative pending after Recv", hwm, i)
						require.Equal(t, byte(received-1), m.Bytes()[0], "hwm=%d op=%d: FIFO violation", hwm, i)
					}
				}
			}
			require.Equal(t, pending, b.Len(), "hwm=%d: receiver Len disagrees with the model", hwm)
			_ = a.Close()
			_ = b.Close()
		}
	})
}

// TestProperty_Bounded_Unblock asserts a blocked Send wakes within a
// bounded window after a Recv creates space — the second design §10.1
// invariant.
func TestProperty_Bounded_Unblock(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		for hwm := 1; hwm <= 8; hwm++ {
			a, b := k.newPair(hwm, hwm, PeerInfo{}, PeerInfo{})
			ctx := context.Background()
			for i := 0; i < hwm; i++ {
				require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{byte(i)})), "hwm=%d fill[%d]", hwm, i)
			}

			// A Send parked on the full pipe; measure the gap between the Recv
			// that frees a slot and the Send returning.
			var freed, woke atomic.Int64
			err := testutil.RequireUnblocks(t, func() error {
				err := a.Send(ctx, msg.FromBytes([]byte{42}))
				woke.Store(time.Now().UnixNano())
				return err
			}, func() {
				freed.Store(time.Now().UnixNano())
				_, err := b.Recv(ctx)
				require.NoError(t, err, "hwm=%d drain", hwm)
			})
			require.NoError(t, err, "hwm=%d blocked Send", hwm)
			gap := time.Duration(woke.Load() - freed.Load())
			require.GreaterOrEqual(t, gap, time.Duration(0), "hwm=%d: Send woke before the Recv", hwm)
			require.LessOrEqual(t, gap, 100*time.Millisecond, "hwm=%d: Send unblock gap", hwm)
			_ = a.Close()
			_ = b.Close()
		}
	})
}

// TestHWMZero_CloseDrainsThenEOF is the HWM-0 counterpart of
// TestClose_Drains_Then_EOF: messages queued across several chunks are still
// delivered, in order, after the sender
// closes, and EOF comes only after the last one.
func TestHWMZero_CloseDrainsThenEOF(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(0, 0, PeerInfo{}, PeerInfo{})
		defer b.Close()

		ctx := context.Background()
		n := 3*k.span + 7
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
		require.ErrorIs(t, err, io.EOF, "expected io.EOF after the queue drained")
	})
}

// notifyGate registers a notification on e that arms the returned cap-1
// gate, as the socket engines do.
func notifyGate(e *End) chan struct{} {
	gate := make(chan struct{}, 1)
	e.SetNotify(func() {
		select {
		case gate <- struct{}{}:
		default:
		}
	})
	return gate
}

func requireArmed(t *testing.T, gate chan struct{}, what string) {
	t.Helper()
	select {
	case <-gate:
	case <-time.After(time.Second):
		require.Fail(t, "notification did not run", what)
	}
}

func requireNotArmed(t *testing.T, gate chan struct{}, what string) {
	t.Helper()
	select {
	case <-gate:
		require.Fail(t, "unexpected notification", what)
	default:
	}
}

// TestNotify_OnlyWhenIdle pins the idle protocol: a send notifies only
// after the receiver went idle, and only once; a queued message keeps
// MarkIdle from idling; a close notifies an idle receiver. HWM 0 included.
func TestNotify_OnlyWhenIdle(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		for _, hwm := range []int{4, 0} {
			t.Run(map[int]string{4: "hwm4", 0: "hwm0"}[hwm], func(t *testing.T) {
				t.Parallel()
				a, b := k.newPair(hwm, hwm, PeerInfo{}, PeerInfo{})
				defer a.Close()
				defer b.Close()
				ctx := context.Background()
				gate := notifyGate(b)

				require.NoError(t, a.Send(ctx, msg.FromBytes([]byte("1"))))
				requireNotArmed(t, gate, "send to a receiver that is not idle")
				require.True(t, b.MarkIdle(), "queued message")
				require.NoError(t, a.Send(ctx, msg.FromBytes([]byte("2"))))
				requireNotArmed(t, gate, "MarkIdle with a message queued")
				require.Equal(t, 2, b.Len())

				for _, want := range []string{"1", "2"} {
					m, ok := b.RecvOrIdle()
					require.True(t, ok)
					require.Equal(t, want, string(m.Bytes()))
				}
				_, ok := b.RecvOrIdle()
				require.False(t, ok, "empty")
				require.NoError(t, a.Send(ctx, msg.FromBytes([]byte("3"))))
				requireArmed(t, gate, "send to an idle receiver")
				require.True(t, a.TrySend(msg.FromBytes([]byte("4"))))
				requireNotArmed(t, gate, "second send before the receiver looked again")

				for range 2 {
					_, err := b.Recv(ctx)
					require.NoError(t, err)
				}
				require.False(t, b.MarkIdle(), "empty")
				require.NoError(t, a.Close())
				requireArmed(t, gate, "close of an idle receiver")
				require.True(t, b.IsClosed())
			})
		}
	})
}

// TestIdleNotify_NoLostWakeup races senders against a receiver that goes
// idle whenever it finds the pipe empty and then waits for the
// notification alone. A lost wakeup shows up as a timeout. It covers a
// single message (and a close) per fresh pipe, the tightest race, and a
// stream through blocking sends at HWM and the deep HWM-0 queues.
func TestIdleNotify_NoLostWakeup(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		const iters = 10000
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		// drain receives n messages (and, with closed, waits for the close),
		// sleeping only on the notification.
		drain := func(b *End, gate chan struct{}, n int, closed bool) error {
			for got := 0; ; {
				if _, ok := b.RecvOrIdle(); ok {
					got++
					continue
				}
				if got == n && (!closed || b.IsClosed()) {
					return nil
				}
				select {
				case <-gate:
				case <-time.After(5 * time.Second):
					return fmt.Errorf("lost wakeup after %d of %d messages", got, n)
				}
			}
		}

		for _, hwm := range []int{1, 0} {
			t.Run(fmt.Sprintf("single/hwm%d", hwm), func(t *testing.T) {
				for i := range iters {
					a, b := k.newPair(hwm, hwm, PeerInfo{}, PeerInfo{})
					gate := notifyGate(b)
					closeToo := i%2 == 1
					// Both sides spin on go so they collide within nanoseconds.
					var ready, start atomic.Bool
					go func() {
						ready.Store(true)
						for !start.Load() {
						}
						_ = a.Send(ctx, msg.FromBytes([]byte("x")))
						if closeToo {
							_ = a.Close()
						}
					}()
					for !ready.Load() {
						runtime.Gosched()
					}
					start.Store(true)
					require.NoError(t, drain(b, gate, 1, closeToo), "iteration %d", i)
					_ = a.Close()
				}
			})
		}

		for _, hwm := range []int{4, 0} {
			t.Run(fmt.Sprintf("stream/hwm%d", hwm), func(t *testing.T) {
				a, b := k.newPair(hwm, hwm, PeerInfo{}, PeerInfo{})
				defer a.Close()
				gate := notifyGate(b)
				go func() {
					// Bursts of varying size cross chunk boundaries at HWM 0.
					for sent := 0; sent < iters; {
						burst := min(1+sent%(3*k.span), iters-sent)
						for range burst {
							if a.Send(ctx, msg.FromBytes([]byte("x"))) != nil {
								return
							}
						}
						sent += burst
						runtime.Gosched()
					}
				}()
				require.NoError(t, drain(b, gate, iters, false))
			})
		}
	})
}

// TestTryRecv_DrainsWholeQueueAfterClose: TryRecv on either end of a
// closed pipe hands out every queued message in order — including a deep
// HWM-0 queue — and only then reports empty. The session writer's linger
// drain and PAIR's retired-pipe drain rely on this.
func TestTryRecv_DrainsWholeQueueAfterClose(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		for _, hwm := range []int{0, 500} {
			for _, closer := range []string{"sender", "receiver"} {
				a, b := k.newPair(hwm, hwm, PeerInfo{}, PeerInfo{})
				ctx := context.Background()
				n := 500
				if hwm == 0 {
					n = 2*k.span + 3
				}
				for i := 0; i < n; i++ {
					require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{byte(i)})), "hwm=%d Send[%d]", hwm, i)
				}
				if closer == "sender" {
					_ = a.Close()
				} else {
					_ = b.Close()
				}
				for i := 0; i < n; i++ {
					m, ok := b.TryRecv()
					require.True(t, ok, "hwm=%d %s closed: TryRecv[%d]", hwm, closer, i)
					require.Equal(t, byte(i), m.Bytes()[0], "hwm=%d TryRecv[%d]", hwm, i)
				}
				_, ok := b.TryRecv()
				require.False(t, ok, "hwm=%d: empty once drained", hwm)
				_ = a.Close()
			}
		}
	})
}

// TestRecvOr_Abort: RecvOr and SendOr give up with ErrAborted when their
// abort channel fires, leaving the pipe usable.
func TestRecvOr_Abort(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(1, 1, PeerInfo{}, PeerInfo{})
		defer a.Close()
		defer b.Close()
		ctx := context.Background()
		abort := make(chan struct{})
		close(abort)

		_, err := b.RecvOr(ctx, abort)
		require.ErrorIs(t, err, ErrAborted)

		require.NoError(t, a.SendOr(ctx, msg.FromBytes([]byte("x")), abort), "room: abort not consulted")
		require.ErrorIs(t, a.SendOr(ctx, msg.FromBytes([]byte("y")), abort), ErrAborted)

		m, err := b.RecvOr(ctx, abort)
		require.NoError(t, err, "buffered message wins over abort")
		require.Equal(t, "x", string(m.Bytes()))
	})
}

// TestHWMZero_ConcurrentChunkGrowth stresses chunk growth: several senders
// race a receiver on an HWM-0 direction with several chunks of messages.
// Everything must arrive, in per-sender order, and a blocking Recv must
// never be stranded on an empty direction while messages are queued.
func TestHWMZero_ConcurrentChunkGrowth(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		a, b := k.newPair(0, 0, PeerInfo{}, PeerInfo{})
		defer a.Close()
		defer b.Close()
		senders, per := 4, 5*k.span
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		for s := 0; s < senders; s++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < per; i++ {
					if !a.TrySend(msg.FromFrames([]byte{byte(s)}, []byte{byte(i >> 16), byte(i >> 8), byte(i)})) {
						assert.Fail(t, "HWM-0 TrySend refused")
						return
					}
				}
			}()
		}
		next := make([]int, senders)
		for got := 0; got < senders*per; got++ {
			m, err := b.Recv(ctx)
			require.NoError(t, err, "Recv %d stranded", got)
			f := m.Frames()
			s, seq := int(f[0][0]), int(f[1][0])<<16|int(f[1][1])<<8|int(f[1][2])
			require.Equal(t, next[s], seq, "sender %d out of order", s)
			next[s]++
		}
		wg.Wait()
		require.False(t, b.Pending())
	})
}

// TestSendDrained_FiresWhenPeerConsumed: with both ends open, SendDrained
// stays pending while anything we sent is unread (including a deep
// HWM-0 queue), fires once the peer has received it all, and fires when
// either end closes.
func TestSendDrained_FiresWhenPeerConsumed(t *testing.T) {
	forEachKind(t, func(t *testing.T, k kind) {
		t.Parallel()
		ctx := context.Background()
		for _, hwm := range []int{0, 500} {
			a, b := k.newPair(hwm, hwm, PeerInfo{}, PeerInfo{})
			n := 100
			if hwm == 0 {
				n = k.span + 100
			}
			for i := 0; i < n; i++ {
				require.NoError(t, a.Send(ctx, msg.FromBytes([]byte{byte(i)})), "hwm=%d Send[%d]", hwm, i)
			}
			drained := a.SendDrained()
			for i := 0; i < n; i++ {
				select {
				case <-drained:
					require.FailNow(t, "SendDrained fired with messages unread", "hwm=%d read=%d", hwm, i)
				default:
				}
				m, err := b.Recv(ctx)
				require.NoError(t, err)
				require.Equal(t, byte(i), m.Bytes()[0], "hwm=%d Recv[%d]", hwm, i)
			}
			select {
			case <-drained:
			case <-time.After(time.Second):
				require.FailNow(t, "SendDrained did not fire after the peer read everything", "hwm=%d", hwm)
			}
			_ = a.Close()
			_ = b.Close()

			// Nothing consumed, but the peer closes: fires too.
			a, b = k.newPair(hwm, hwm, PeerInfo{}, PeerInfo{})
			require.NoError(t, a.Send(ctx, msg.FromBytes([]byte("x"))))
			drained = a.SendDrained()
			_ = b.Close()
			select {
			case <-drained:
			case <-time.After(time.Second):
				require.FailNow(t, "SendDrained did not fire on peer close", "hwm=%d", hwm)
			}
			_ = a.Close()
		}
	})
}
