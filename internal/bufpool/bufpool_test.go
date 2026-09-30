package bufpool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTiers(t *testing.T) {
	for _, tc := range []struct{ n, wantCap int }{
		{4<<10 + 1, 8 << 10},
		{8 << 10, 8 << 10},
		{8<<10 + 1, 16 << 10},
		{64 << 10, 64 << 10},
		{1<<20 - 1, 1 << 20},
		{4 << 20, 4 << 20},
		{4<<20 + 1, 4<<20 + 1}, // beyond the top tier: plain make
	} {
		b := Get(tc.n)
		require.Lenf(t, b, tc.n, "Get(%d)", tc.n)
		require.Equalf(t, tc.wantCap, cap(b), "Get(%d)", tc.n)
		Put(b)
	}
}

// TestPutIgnoresForeignBuffers: only exact tier capacities re-enter the
// pool, so a later Get never returns a buffer of the wrong size.
func TestPutIgnoresForeignBuffers(t *testing.T) {
	Put(nil)
	Put(make([]byte, 100))
	Put(make([]byte, 12<<10))
	Put(make([]byte, 8<<20))
	for range 100 {
		b := Get(8 << 10)
		require.Equal(t, 8<<10, cap(b))
		Put(b)
	}
}
