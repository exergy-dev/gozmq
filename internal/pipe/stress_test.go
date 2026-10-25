package pipe

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/msg"
)

// stressN picks the message count of a stress test: a fast tier by default,
// a heavy one when the suite is not run with -short.
func stressN(fast, heavy int) int {
	if testing.Short() {
		return fast
	}
	return heavy
}

// TestStress_LostWakeup_1x1: one blocking producer, one blocking consumer,
// random Gosched on both sides to shuffle the interleavings around the
// park/arm protocol. Every message must arrive, in order, before the timeout.
func TestStress_LostWakeup_1x1(t *testing.T) {
	t.Parallel()
	for _, hwm := range []int{1, 7, 0} {
		for _, mode := range []string{"send", "deferred"} {
			t.Run(mode, func(t *testing.T) {
				t.Parallel()
				n := stressN(20000, 300000)
				a, b := NewPair(hwm, hwm, PeerInfo{}, PeerInfo{})
				defer a.Close()
				defer b.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()

				prodErr := make(chan error, 1)
				go func() {
					rng := rand.New(rand.NewPCG(1, uint64(hwm)))
					for i := 0; i < n; i++ {
						if mode == "send" {
							if err := a.Send(ctx, seqMsg(i)); err != nil {
								prodErr <- err
								return
							}
						} else {
							m := seqMsg(i)
							for !a.TrySendDeferred(m) {
								a.Flush() // never wait with a wakeup owed
								runtime.Gosched()
								if ctx.Err() != nil {
									prodErr <- ctx.Err()
									return
								}
							}
							if rng.IntN(8) == 0 {
								a.Flush()
							}
						}
						if rng.IntN(16) == 0 {
							runtime.Gosched()
						}
					}
					a.Flush()
					prodErr <- nil
				}()

				rng := rand.New(rand.NewPCG(2, uint64(hwm)))
				for i := 0; i < n; i++ {
					m, err := b.Recv(ctx)
					require.NoError(t, err, "Recv[%d]", i)
					require.Equal(t, i, seqOf(m), "order")
					if rng.IntN(16) == 0 {
						runtime.Gosched()
					}
				}
				require.NoError(t, <-prodErr)
			})
		}
	}
}

// TestStress_MultiSleeperBaton: several consumers park on one direction;
// bursts of messages arrive while they sleep. The wake baton must hand every
// message to someone: each is received exactly once and nobody stays parked.
func TestStress_MultiSleeperBaton(t *testing.T) {
	t.Parallel()
	const consumers = 8
	n := stressN(5000, 100000)
	a, b := NewPair(0, 0, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seen := make([]atomic.Int32, n)
	var got atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, consumers)
	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				m, err := b.Recv(ctx)
				if err != nil {
					errs <- err
					return
				}
				seen[seqOf(m)].Add(1)
				got.Add(1)
			}
		}()
	}
	for i := 0; i < n; {
		burst := 1 + rand.IntN(40)
		for j := 0; j < burst && i < n; j, i = j+1, i+1 {
			require.NoError(t, a.Send(ctx, seqMsg(i)))
		}
		if rand.IntN(4) == 0 {
			time.Sleep(50 * time.Microsecond) // let the consumers park
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for got.Load() < int64(n) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.EqualValues(t, n, got.Load(), "messages stuck with consumers parked")
	a.Close() // consumers exit with EOF
	wg.Wait()
	close(errs)
	for err := range errs {
		require.ErrorIs(t, err, io.EOF)
	}
	for i := range seen {
		require.EqualValues(t, 1, seen[i].Load(), "message %d", i)
	}
}

// TestStress_CloseWhileParked: consumers and producers parked on a pipe all
// return promptly when it closes.
func TestStress_CloseWhileParked(t *testing.T) {
	t.Parallel()
	for round := 0; round < stressN(20, 200); round++ {
		a, b := NewPair(1, 1, PeerInfo{}, PeerInfo{})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(2)
			go func() {
				defer wg.Done()
				if _, err := a.Recv(ctx); !errors.Is(err, io.EOF) {
					t.Errorf("Recv: %v", err)
				}
			}()
		}
		require.NoError(t, a.Send(ctx, seqMsg(0)))
		// Fill b->a... a->b has one message queued: producers on a park.
		for range 4 {
			go func() {
				defer wg.Done()
				if err := a.Send(ctx, seqMsg(1)); !errors.Is(err, io.ErrClosedPipe) {
					t.Errorf("Send: %v", err)
				}
			}()
		}
		for a.send.wsleepers.Load() < 4 || a.recv.rsleepers.Load() < 4 {
			time.Sleep(100 * time.Microsecond)
			require.NoError(t, ctx.Err())
		}
		if round%2 == 0 {
			a.Close()
		} else {
			b.Close()
		}
		wg.Wait()
		cancel()
		a.Close()
		b.Close()
	}
}

// TestLen_Accuracy: Len follows pushes and pops exactly, across chunk
// boundaries, for bounded and unbounded directions.
func TestLen_Accuracy(t *testing.T) {
	t.Parallel()
	for _, hwm := range []int{0, chunkSize*3 + 5} {
		a, b := NewPair(hwm, hwm, PeerInfo{}, PeerInfo{})
		want := 0
		rng := rand.New(rand.NewPCG(3, 4))
		for step := 0; step < 20000; step++ {
			if rng.IntN(2) == 0 {
				if a.TrySend(seqMsg(step)) {
					want++
				} else {
					require.NotZero(t, hwm)
					require.Equal(t, hwm, want, "refused below hwm")
				}
			} else if _, ok := b.TryRecv(); ok {
				want--
			} else {
				require.Zero(t, want, "empty while Len says otherwise")
			}
			require.Equal(t, want, b.Len(), "step %d", step)
			require.Equal(t, want > 0, b.Pending())
			// Drift the depth so it crosses chunk boundaries both ways.
			if step%5000 == 4999 {
				for range chunkSize + 3 {
					if a.TrySend(seqMsg(step)) {
						want++
					}
				}
			}
		}
		a.Close()
		b.Close()
	}
}

// TestAllocs_SteadyStateZero: once the queue has its first chunk, sending
// and receiving the same message allocates nothing (chunks recycle through
// spare), including across many chunk boundaries.
func TestAllocs_SteadyStateZero(t *testing.T) {
	a, b := NewPair(0, 0, PeerInfo{}, PeerInfo{})
	defer a.Close()
	defer b.Close()
	ctx := context.Background()
	m := msg.FromBytes([]byte("x"))
	// Warm up past a chunk boundary so spare is populated.
	for range chunkSize * 3 {
		require.True(t, a.TrySend(m))
		_, ok := b.TryRecv()
		require.True(t, ok)
	}
	require.Zero(t, testing.AllocsPerRun(chunkSize*4, func() {
		a.TrySend(m)
		b.TryRecv()
	}), "TrySend/TryRecv")
	require.Zero(t, testing.AllocsPerRun(chunkSize*4, func() {
		_ = a.Send(ctx, m)
		_, _ = b.Recv(ctx)
	}), "Send/Recv")
	require.Zero(t, testing.AllocsPerRun(chunkSize*4, func() {
		a.TrySendDeferred(m)
		a.Flush()
		b.TryRecv()
	}), "TrySendDeferred/Flush")
}
