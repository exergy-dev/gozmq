package pipe

import (
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/msg"
)

func seqMsg(i int) *msg.Msg {
	return msg.FromBytes([]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
}

func seqOf(m *msg.Msg) int {
	b := m.Bytes()
	return int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
}

// TestQueue_FIFOAcrossChunks fills and drains the bare queue at sizes that
// straddle chunk boundaries, for bounded and unbounded queues.
func TestQueue_FIFOAcrossChunks(t *testing.T) {
	t.Parallel()
	for _, hwm := range []int{1, chunkSize - 1, chunkSize, chunkSize + 1, 1000, 0} {
		t.Run(fmt.Sprintf("hwm%d", hwm), func(t *testing.T) {
			t.Parallel()
			q := newQueue(hwm)
			n := 3*chunkSize + 7
			if hwm > 0 {
				n = min(n, hwm)
			}
			// Several rounds so the spare chunk is recycled.
			next := 0
			for round := 0; round < 4; round++ {
				for i := 0; i < n; i++ {
					require.True(t, q.push(seqMsg(next+i)), "push %d", i)
				}
				require.Equal(t, n, q.length())
				require.True(t, q.pending())
				if hwm > 0 && n == hwm {
					require.False(t, q.push(seqMsg(-1)), "full")
					require.False(t, q.hasRoom())
				}
				for i := 0; i < n; i++ {
					m, ok := q.pop()
					require.True(t, ok, "pop %d", i)
					require.Equal(t, next+i, seqOf(m))
				}
				next += n
				_, ok := q.pop()
				require.False(t, ok)
				require.Zero(t, q.length())
				require.False(t, q.pending())
			}
		})
	}
}

// TestQueue_SPSC_Concurrent runs one pushing and one popping goroutine
// against the bare queue (spinning), under -race this checks the
// slot/counter publication argument.
func TestQueue_SPSC_Concurrent(t *testing.T) {
	t.Parallel()
	const n = 200000
	for _, hwm := range []int{1, 7, chunkSize, 0} {
		t.Run(fmt.Sprintf("hwm%d", hwm), func(t *testing.T) {
			t.Parallel()
			q := newQueue(hwm)
			deadline := time.Now().Add(30 * time.Second)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					for !q.push(seqMsg(i)) {
						if time.Now().After(deadline) {
							return
						}
					}
				}
			}()
			for i := 0; i < n; i++ {
				var m *msg.Msg
				var ok bool
				for m, ok = q.pop(); !ok; m, ok = q.pop() {
					if time.Now().After(deadline) {
						require.FailNow(t, "timeout", "popped %d of %d", i, n)
					}
				}
				require.Equal(t, i, seqOf(m))
			}
			wg.Wait()
		})
	}
}

// TestQueue_Layout pins the cache-line separation the design relies on:
// producer words, consumer words and the published counters must each sit
// at least 128 bytes apart.
func TestQueue_Layout(t *testing.T) {
	t.Parallel()
	var q queue
	pmu := unsafe.Offsetof(q.pmu)
	pubTail := unsafe.Offsetof(q.pubTail)
	cmu := unsafe.Offsetof(q.cmu)
	pubHead := unsafe.Offsetof(q.pubHead)
	spare := unsafe.Offsetof(q.spares)
	offs := []uintptr{pmu, pubTail, cmu, pubHead, spare}
	for i := 1; i < len(offs); i++ {
		require.GreaterOrEqual(t, offs[i]-offs[i-1], uintptr(128), "group %d too close to %d", i, i-1)
	}
	// The producer-private group is contiguous with pmu; likewise consumer.
	require.Less(t, unsafe.Offsetof(q.cachedHead)-pmu, uintptr(64))
	require.Less(t, unsafe.Offsetof(q.cachedTail)-cmu, uintptr(64))
}
