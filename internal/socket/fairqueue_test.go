package socket_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/msg"
)

// TestFairQueue_NoGoroutines: attaching, receiving from and detaching 64
// pipes starts no goroutine. Not parallel, so the count is not disturbed.
func TestFairQueue_NoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	fq := newFQ()
	defer fq.Close()
	const n = 64
	engineEnds, peerEnds := makePairs(n, 4, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	for _, p := range engineEnds {
		require.NoError(t, fq.Attach(p))
	}
	ctx := testCtx(t)
	for _, p := range peerEnds {
		require.NoError(t, p.Send(ctx, msg.FromBytes([]byte("x"))))
	}
	for range n {
		_, err := fq.Recv(ctx)
		require.NoError(t, err)
	}
	require.False(t, fq.CanRecv())
	require.LessOrEqual(t, runtime.NumGoroutine(), before, "FairQueue started goroutines")
	for _, p := range engineEnds {
		require.NoError(t, fq.Detach(p))
	}
}

// TestFairQueue_PerPeerFIFOConcurrentReceivers: three receivers share one
// queue; each sees every peer's messages in the order that peer sent them,
// and together they receive everything. One peer exercises the single-pipe
// fast path, four the multi-pipe walk; HWM 4 makes the senders block.
func TestFairQueue_PerPeerFIFOConcurrentReceivers(t *testing.T) {
	t.Parallel()
	for _, peers := range []int{1, 4} {
		t.Run(fmt.Sprintf("peers=%d", peers), func(t *testing.T) {
			fq := newFQ()
			defer fq.Close()
			engineEnds, peerEnds := makePairs(peers, 4, 4)
			defer closePipes(append(engineEnds, peerEnds...)...)
			for _, p := range engineEnds {
				require.NoError(t, fq.Attach(p))
			}
			ctx := testCtx(t)
			const per, receivers = 2000, 3
			for src, p := range peerEnds {
				go func() {
					for i := range per {
						if p.Send(ctx, seqMsg(src, i)) != nil {
							return
						}
					}
				}()
			}

			var total atomic.Int64
			errs := make(chan error, receivers)
			for range receivers {
				go func() {
					last := make([]int, peers)
					for i := range last {
						last[i] = -1
					}
					for total.Add(1) <= int64(peers*per) {
						m, err := fq.Recv(ctx)
						if err != nil {
							errs <- err
							return
						}
						src, i := seqOf(m)
						if i <= last[src] {
							errs <- fmt.Errorf("peer %d: %d after %d", src, i, last[src])
							return
						}
						last[src] = i
					}
					errs <- nil
				}()
			}
			for range receivers {
				require.NoError(t, <-errs)
			}
		})
	}
}

// TestFairQueue_ClosedPipeDrainsWithoutDetach: a peer that queued messages
// and closed is drained in order, and then dropped, without the owner ever
// detaching it, on both receive paths.
func TestFairQueue_ClosedPipeDrainsWithoutDetach(t *testing.T) {
	t.Parallel()
	for _, peers := range []int{1, 2} {
		fq := newFQ()
		engineEnds, peerEnds := makePairs(peers, 8, 8)
		for _, p := range engineEnds {
			require.NoError(t, fq.Attach(p))
		}
		ctx := testCtx(t)
		const n = 5
		for i := range n {
			require.NoError(t, peerEnds[0].Send(ctx, seqMsg(0, i)))
		}
		require.NoError(t, peerEnds[0].Close())
		for i := range n {
			m, err := fq.Recv(ctx)
			require.NoError(t, err, "peers=%d", peers)
			_, got := seqOf(m)
			require.Equal(t, i, got, "peers=%d", peers)
		}
		if peers > 1 {
			require.NoError(t, peerEnds[1].Send(ctx, seqMsg(1, 0)))
			m, err := fq.Recv(ctx)
			require.NoError(t, err)
			src, _ := seqOf(m)
			require.Equal(t, 1, src)
		}
		require.False(t, fq.CanRecv(), "peers=%d: closed pipe drained", peers)
		require.NoError(t, fq.Close())
		closePipes(append(engineEnds, peerEnds...)...)
	}
}

// TestFairQueue_PollerAndRecvShareGate: a receiver blocked in Recv and a
// Poller parked on the gate share one wake-up token. Two messages arrive
// from one peer, which notifies once; both waiters must still be served.
func TestFairQueue_PollerAndRecvShareGate(t *testing.T) {
	t.Parallel()
	for _, peers := range []int{1, 2} {
		t.Run(fmt.Sprintf("peers=%d", peers), func(t *testing.T) {
			gate := make(chan struct{}, 1)
			fq := socket.NewFairQueue(gate)
			defer fq.Close()
			engineEnds, peerEnds := makePairs(peers, 4, 4)
			defer closePipes(append(engineEnds, peerEnds...)...)
			for _, p := range engineEnds {
				require.NoError(t, fq.Attach(p))
			}
			ctx := testCtx(t)

			got := make(chan string, 2)
			go func() { // blocked receiver
				if m, err := fq.Recv(ctx); err == nil {
					got <- "recv"
					m.Release()
				}
			}()
			polled := make(chan struct{})
			go func() { // Poller: park, re-check, hand the token on, receive
				defer close(polled)
				for ctx.Err() == nil {
					if !fq.CanRecv() {
						select {
						case <-gate:
						case <-ctx.Done():
							return
						}
					}
					if fq.CanRecv() {
						select { // Poller.rearm
						case gate <- struct{}{}:
						default:
						}
						if m, err := fq.Recv(ctx); err == nil {
							got <- "poller"
							m.Release()
						}
						return
					}
				}
			}()
			time.Sleep(20 * time.Millisecond) // both park

			for i := range 2 {
				require.NoError(t, peerEnds[0].Send(ctx, seqMsg(0, i)))
			}
			seen := map[string]bool{}
			for range 2 {
				select {
				case who := <-got:
					seen[who] = true
				case <-time.After(2 * time.Second):
					require.Fail(t, "a waiter was stranded", "served: %v", seen)
				}
			}
			require.Equal(t, map[string]bool{"recv": true, "poller": true}, seen)
			<-polled
		})
	}
}

// TestFairQueue_NoLostWakeupStress: many senders race receivers that go
// idle whenever every pipe is empty; nothing may be stranded (a lost
// wakeup shows up as the deadline), with bounded and HWM-0 pipes.
func TestFairQueue_NoLostWakeupStress(t *testing.T) {
	t.Parallel()
	for _, hwm := range []int{4, 0} {
		t.Run(fmt.Sprintf("hwm=%d", hwm), func(t *testing.T) {
			fq := newFQ()
			defer fq.Close()
			const peers, per, receivers = 8, 1250, 2
			engineEnds, peerEnds := makePairs(peers, hwm, hwm)
			defer closePipes(append(engineEnds, peerEnds...)...)
			for _, p := range engineEnds {
				require.NoError(t, fq.Attach(p))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for src, p := range peerEnds {
				go func() {
					for i := range per {
						if p.Send(ctx, seqMsg(src, i)) != nil {
							return
						}
						if i%7 == 0 {
							runtime.Gosched()
						}
					}
				}()
			}
			var total atomic.Int64
			var wg sync.WaitGroup
			for range receivers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for total.Add(1) <= peers*per {
						if _, err := fq.Recv(ctx); err != nil {
							return
						}
					}
				}()
			}
			wg.Wait()
			require.NoError(t, ctx.Err(), "receivers stranded")
		})
	}
}

// TestFairQueue_TryRecvWithSourceAndPending: the non-blocking receive
// reports the source pipe, and Pending turns false once every pipe is
// drained and true again when a message arrives.
func TestFairQueue_TryRecvWithSourceAndPending(t *testing.T) {
	t.Parallel()
	fq := newFQ()
	defer fq.Close()
	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	require.NoError(t, fq.Attach(a))
	_, _, ok := fq.TryRecvWithSource()
	require.False(t, ok)
	require.False(t, fq.Pending())

	require.NoError(t, b.Send(testCtx(t), msg.FromBytes([]byte("x"))))
	require.True(t, fq.Pending(), "the message activates the idle pipe")
	m, src, ok := fq.TryRecvWithSource()
	require.True(t, ok)
	require.Same(t, a, src)
	require.Equal(t, "x", string(m.Bytes()))
	_, _, ok = fq.TryRecvWithSource()
	require.False(t, ok)
	require.False(t, fq.Pending())
}
