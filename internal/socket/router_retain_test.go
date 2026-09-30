//go:build go1.24

package socket_test

import (
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/msg"
)

// TestRouter_DetachReleasesPipes: once a peer is detached and its messages
// consumed, the ROUTER holds no reference to its pipe. A retained pipe pins
// its channel buffers, which under peer churn added up to tens of MB when
// detached pipes were cached for identity lookup. Covers direct mode (sole
// peer), forwarder mode, and pipes closed before or after the detach.
func TestRouter_DetachReleasesPipes(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(false)
	defer eng.Close()
	ctx := testCtx(t)

	const n = 64
	var ends []weak.Pointer[pipe.End]
	churn := func() {
		for i := range n {
			a, b := pipe.NewPair(1000, 1000, pipe.PeerInfo{}, pipe.PeerInfo{})
			require.NoError(t, eng.AttachPipe(a))
			require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("x"))))
			_, err := eng.Recv(ctx)
			require.NoError(t, err)
			if i%2 == 0 {
				require.NoError(t, b.Close()) // closed first: detach drains it
			}
			require.NoError(t, eng.DetachPipe(a))
			closePipes(a, b)
			ends = append(ends, weak.Make(a), weak.Make(b))
		}
	}
	churn() // each peer is the sole one: direct mode

	anchor, anchorPeer := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(anchor, anchorPeer)
	require.NoError(t, eng.AttachPipe(anchor))
	churn() // forwarder mode

	// Forwarders exit asynchronously after Detach; give them a moment.
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		live := 0
		for _, w := range ends {
			if w.Value() != nil {
				live++
			}
		}
		if live == 0 {
			return
		}
		if time.Now().After(deadline) {
			require.Zero(t, live, "detached pipe ends still reachable")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
