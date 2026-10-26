package pipe

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/exergy-dev/gozmq/msg"
)

// Microbenchmarks for the pipe hot paths. Run pinned and directional:
//
//	GOMAXPROCS=8 taskset -c 0-7 go test ./internal/pipe -run '^$' -bench Pipe_ -count 6 -benchmem

// BenchmarkPipe_Uncontended: one goroutine, TrySend then TryRecv at depth
// 0/1. Measures the per-op cost with no contention at all.
func BenchmarkPipe_Uncontended(b *testing.B) {
	forEachKind(b, func(b *testing.B, k kind) {
		x, y := k.newPair(1000, 1000, nil, nil)
		defer x.Close()
		defer y.Close()
		m := msg.FromBytes([]byte("x"))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !x.TrySend(m) {
				b.Fatal("send")
			}
			if _, ok := y.TryRecv(); !ok {
				b.Fatal("recv")
			}
		}
	})
}

// BenchmarkPipe_SPSC_Stream: one producer goroutine, one consumer, blocking
// Send/Recv, bounded at the default HWM. Use -cpu 2,8.
func BenchmarkPipe_SPSC_Stream(b *testing.B) {
	forEachKind(b, func(b *testing.B, k kind) {
		x, y := k.newPair(1000, 1000, nil, nil)
		defer x.Close()
		defer y.Close()
		ctx := context.Background()
		m := msg.FromBytes([]byte("x"))
		b.ReportAllocs()
		b.ResetTimer()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < b.N; i++ {
				if _, err := y.RecvOr(ctx, nil); err != nil {
					b.Error(err)
					return
				}
			}
		}()
		for i := 0; i < b.N; i++ {
			if err := x.Send(ctx, m); err != nil {
				b.Fatal(err)
			}
		}
		<-done
	})
}

// BenchmarkPipe_SPSC_Deferred: like SPSC_Stream but the producer pushes in
// batches of 64 with TrySendDeferred and one Flush, as the session reader
// does (queue kind only). It spins (Gosched) while the bounded direction is
// full.
func BenchmarkPipe_SPSC_Deferred(b *testing.B) {
	b.Run("queue", func(b *testing.B) {
		const batch = 64
		x, y := NewPair(1000, 1000, nil, nil)
		defer x.Close()
		defer y.Close()
		ctx := context.Background()
		m := msg.FromBytes([]byte("x"))
		b.ReportAllocs()
		b.ResetTimer()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < b.N; i++ {
				if _, err := y.RecvOr(ctx, nil); err != nil {
					b.Error(err)
					return
				}
			}
		}()
		for i := 0; i < b.N; i++ {
			for !x.TrySendDeferred(m) {
				x.Flush()
				runtime.Gosched()
			}
			if i%batch == batch-1 {
				x.Flush()
			}
		}
		x.Flush()
		<-done
	})
}

// BenchmarkPipe_SPSC_PingPong: strict alternation across two directions;
// every Recv parks, so this is dominated by the wakeup path.
func BenchmarkPipe_SPSC_PingPong(b *testing.B) {
	forEachKind(b, func(b *testing.B, k kind) {
		x, y := k.newPair(1000, 1000, nil, nil)
		defer x.Close()
		defer y.Close()
		ctx := context.Background()
		m := msg.FromBytes([]byte("x"))
		b.ReportAllocs()
		b.ResetTimer()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < b.N; i++ {
				r, err := y.RecvOr(ctx, nil)
				if err != nil {
					b.Error(err)
					return
				}
				if err := y.Send(ctx, r); err != nil {
					b.Error(err)
					return
				}
			}
		}()
		for i := 0; i < b.N; i++ {
			if err := x.Send(ctx, m); err != nil {
				b.Fatal(err)
			}
			if _, err := x.RecvOr(ctx, nil); err != nil {
				b.Fatal(err)
			}
		}
		<-done
	})
}

// BenchmarkPipe_MPSC_4: four producers, one consumer.
func BenchmarkPipe_MPSC_4(b *testing.B) {
	forEachKind(b, func(b *testing.B, k kind) {
		const producers = 4
		x, y := k.newPair(1000, 1000, nil, nil)
		defer x.Close()
		defer y.Close()
		ctx := context.Background()
		m := msg.FromBytes([]byte("x"))
		b.ReportAllocs()
		b.ResetTimer()
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			n := b.N / producers
			if p == 0 {
				n += b.N % producers
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					if err := x.Send(ctx, m); err != nil {
						b.Error(err)
						return
					}
				}
			}()
		}
		for i := 0; i < b.N; i++ {
			if _, err := y.RecvOr(ctx, nil); err != nil {
				b.Fatal(err)
			}
		}
		wg.Wait()
	})
}

// BenchmarkPipe_Writer_Batch mimics the session writer: a blocking RecvOr
// for the first message, then up to 31 TryRecv for the rest of the batch.
func BenchmarkPipe_Writer_Batch(b *testing.B) {
	forEachKind(b, func(b *testing.B, k kind) {
		x, y := k.newPair(1000, 1000, nil, nil)
		defer x.Close()
		defer y.Close()
		ctx := context.Background()
		m := msg.FromBytes([]byte("x"))
		b.ReportAllocs()
		b.ResetTimer()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for got := 0; got < b.N; {
				if _, err := y.RecvOr(ctx, nil); err != nil {
					b.Error(err)
					return
				}
				got++
				for n := 1; n < 32 && got < b.N; n++ {
					if _, ok := y.TryRecv(); !ok {
						break
					}
					got++
				}
			}
		}()
		for i := 0; i < b.N; i++ {
			if err := x.Send(ctx, m); err != nil {
				b.Fatal(err)
			}
		}
		<-done
	})
}
