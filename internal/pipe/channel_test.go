package pipe

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/msg"
)

// Tests for HWM-0 directions around the queue's chunk size, on both pair
// constructors (HWM 0 on a channel pair is a queue-kind direction).

// TestHWMZero_ChunkBoundaries sends n messages around the chunk size into
// an HWM-0 direction and checks FIFO order, Len and Pending at every stage.
func TestHWMZero_ChunkBoundaries(t *testing.T) {
	t.Parallel()
	forEachKind(t, func(t *testing.T, k kind) {
		for _, n := range []int{1, k.span - 1, k.span, k.span + 1, 2 * k.span, 3*k.span + 5} {
			a, b := k.newPair(0, 0, PeerInfo{}, PeerInfo{})
			for i := range n {
				require.True(t, a.TrySend(seqMsg(i)), "n=%d send %d", n, i)
			}
			require.Equal(t, n, b.Len(), "n=%d", n)
			require.True(t, b.Pending())
			for i := range n {
				m, ok := b.TryRecv()
				require.True(t, ok, "n=%d recv %d", n, i)
				require.Equal(t, i, seqOf(m), "n=%d recv %d", n, i)
			}
			_, ok := b.TryRecv()
			require.False(t, ok)
			require.Zero(t, b.Len())
			require.False(t, b.Pending())
			a.Close()
			b.Close()
		}
	})
}

// TestHWMZero_Interleaved alternates bursts larger than a chunk with
// partial drains, so chunks are allocated, recycled and emptied repeatedly
// while FIFO order holds.
func TestHWMZero_Interleaved(t *testing.T) {
	t.Parallel()
	forEachKind(t, func(t *testing.T, k kind) {
		a, b := k.newPair(0, 0, PeerInfo{}, PeerInfo{})
		defer a.Close()
		defer b.Close()
		next, want := 0, 0
		for round := range 20 {
			burst := k.span/2 + round*k.span/3
			for range burst {
				require.True(t, a.TrySend(seqMsg(next)))
				next++
			}
			drain := burst * 2 / 3
			for range drain {
				m, ok := b.TryRecv()
				require.True(t, ok)
				require.Equal(t, want, seqOf(m))
				want++
			}
			require.Equal(t, next-want, b.Len())
		}
		for want < next {
			m, ok := b.TryRecv()
			require.True(t, ok)
			require.Equal(t, want, seqOf(m))
			want++
		}
		require.False(t, b.Pending())
	})
}

// TestHWMZero_ParkedRecvSeesBurst checks that a receiver blocked on an
// empty direction is woken by a burst of sends.
func TestHWMZero_ParkedRecvSeesBurst(t *testing.T) {
	t.Parallel()
	forEachKind(t, func(t *testing.T, k kind) {
		for range 50 {
			a, b := k.newPair(0, 0, PeerInfo{}, PeerInfo{})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			var wg sync.WaitGroup
			wg.Add(1)
			got := 0
			n := 2*k.span + 3
			go func() {
				defer wg.Done()
				for got < n {
					if _, err := b.Recv(ctx); err != nil {
						return
					}
					got++
				}
			}()
			for i := range n {
				require.True(t, a.TrySend(seqMsg(i)))
			}
			wg.Wait()
			require.Equal(t, n, got)
			cancel()
			a.Close()
			b.Close()
		}
	})
}

// TestChannel_DeferredIsPlainSend: TrySendDeferred on a channel pair wakes
// the receiver at once and Flush has nothing to do.
func TestChannel_DeferredIsPlainSend(t *testing.T) {
	t.Parallel()
	a, b := NewChannelPair(10, 10, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()
	gate := notifyGate(b)
	_, ok := b.RecvOrIdle()
	require.False(t, ok)
	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("x"))))
	requireArmed(t, gate, "TrySendDeferred on a channel pair")
	a.Flush()
	requireNotArmed(t, gate, "Flush on a channel pair")
	m, ok := b.TryRecv()
	require.True(t, ok)
	require.Equal(t, "x", string(m.Bytes()))
}
