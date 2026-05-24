package msg

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolGetTierSizes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		size    int
		wantCap int
	}{
		{"zero", 0, 256},
		{"under-256", 100, 256},
		{"exact-256", 256, 256},
		{"under-4k", 1000, 4 * 1024},
		{"exact-4k", 4 * 1024, 4 * 1024},
		{"under-64k", 50 * 1024, 64 * 1024},
		{"exact-64k", 64 * 1024, 64 * 1024},
		{"under-1m", 300 * 1024, 1024 * 1024},
		{"exact-1m", 1024 * 1024, 1024 * 1024},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := PoolGet(tc.size)
			require.Equal(t, tc.size, len(b))
			require.GreaterOrEqual(t, cap(b), tc.wantCap)
			// Tier buffers should have exactly tier capacity (not oversized).
			require.Equal(t, tc.wantCap, cap(b))
			PoolPut(b)
		})
	}
}

func TestPoolGetOversizeFallback(t *testing.T) {
	t.Parallel()
	// 2 MiB exceeds the largest tier (1 MiB). The pool falls back to a
	// plain make, so cap == len.
	const oversize = 2 * 1024 * 1024
	b := PoolGet(oversize)
	require.Equal(t, oversize, len(b))
	require.Equal(t, oversize, cap(b), "oversize should not be pooled")
	// Putting an oversize slice must not panic and must not corrupt the pool.
	PoolPut(b)

	// A subsequent Get at the 1 MiB tier must still work.
	next := PoolGet(1024 * 1024)
	require.Equal(t, 1024*1024, cap(next))
	PoolPut(next)
}

func TestPoolPutGetReusesBuffer(t *testing.T) {
	t.Parallel()
	// sync.Pool does not guarantee reuse, but under steady state with no
	// GC pressure the same slice is very likely to come back. We assert
	// "cap preserved" rather than "same underlying array" since identity
	// is not guaranteed.
	b1 := PoolGet(100)
	orig := &b1[:cap(b1)][0]
	PoolPut(b1)
	b2 := PoolGet(100)
	require.Equal(t, 256, cap(b2))
	if &b2[:cap(b2)][0] != orig {
		// Not fatal — sync.Pool may have GC'd the entry — but log so CI
		// operators can notice regressions under low-churn conditions.
		t.Logf("buffer identity not preserved across Put/Get (acceptable per sync.Pool contract)")
	}
	PoolPut(b2)
}

func TestPoolPutIgnoresNonTierSlice(t *testing.T) {
	t.Parallel()
	// A slice whose cap doesn't match any tier must be ignored (not
	// inserted into any pool). We verify by: Put a non-tier slice, then
	// Get — the returned slice must have a tier capacity, not the
	// corrupted cap.
	junk := make([]byte, 200, 777) // cap 777 is not a tier
	PoolPut(junk)
	b := PoolGet(200)
	require.Equal(t, 256, cap(b), "pool corrupted by non-tier Put")
	PoolPut(b)
}

func TestPoolPutNilNoPanic(t *testing.T) {
	t.Parallel()
	require.NotPanics(t, func() { PoolPut(nil) })
}

func TestPoolGetReturnsUsableSlice(t *testing.T) {
	t.Parallel()
	// The returned slice must be writable at every index up to len.
	b := PoolGet(1000)
	for i := range b {
		b[i] = byte(i)
	}
	for i := range b {
		require.Equal(t, byte(i), b[i], "b[%d] mismatch", i)
	}
	PoolPut(b)
}

func TestPoolConcurrentGetPut(t *testing.T) {
	t.Parallel()
	const (
		workers = 16
		iters   = 2000
	)
	sizes := []int{64, 300, 5000, 80 * 1024, 600 * 1024, 2 * 1024 * 1024}
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				n := sizes[(seed+i)%len(sizes)]
				b := PoolGet(n)
				if !assert.Equal(t, n, len(b), "worker %d", seed) {
					return
				}
				// Touch first and last byte to catch cap/len confusion.
				if n > 0 {
					b[0] = byte(seed)
					b[n-1] = byte(i)
				}
				PoolPut(b)
			}
		}(w)
	}
	wg.Wait()
}
