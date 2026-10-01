package gozmq

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// chanOf lets waitChannels wait on bare channels.
func chanOf(c chan struct{}) <-chan struct{} { return c }

func TestWaitWritersFlushed_LingerTimeoutDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	start := time.Now()
	err := waitChannels(context.Background(), []chan struct{}{make(chan struct{})}, chanOf, 20*time.Millisecond)
	require.NoError(t, err, "waitChannels on linger timeout")
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 15*time.Millisecond, "returned after %v, want ~linger wait", elapsed)
}

func TestWaitWritersFlushed_CtxCancelDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := waitChannels(ctx, []chan struct{}{make(chan struct{})}, chanOf, -1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestContextReleasesClosedSockets is the fast regression guard for a leak
// the soak suite found: NewSocket appended to Context.sockets and nothing
// ever removed a closed socket, so a long-lived Context — the documented
// one-per-process pattern — retained every socket it had ever created,
// along with each socket's config, engine, and engine-owned maps and
// channels. At ~2 KiB apiece that is tens of megabytes per hundred
// thousand connection cycles.
//
// soak/ catches this over minutes; this catches it in milliseconds.
func TestContextReleasesClosedSockets(t *testing.T) {
	c := NewContext()
	defer func() { _ = c.Close() }()

	const n = 50
	for i := 0; i < n; i++ {
		s, err := c.NewSocket(Pair)
		require.NoError(t, err)
		require.NoError(t, s.Close())
	}

	c.mu.Lock()
	retained := len(c.sockets)
	c.mu.Unlock()
	require.Zero(t, retained, "Context retained %d of %d closed sockets", retained, n)
}

// Sockets still open must remain tracked, so Context.Close can tear them
// down. Guards against "fixing" the leak by never tracking at all.
func TestContextTracksOpenSockets(t *testing.T) {
	c := NewContext()
	defer func() { _ = c.Close() }()

	open, err := c.NewSocket(Pair)
	require.NoError(t, err)
	closed, err := c.NewSocket(Pair)
	require.NoError(t, err)
	require.NoError(t, closed.Close())

	c.mu.Lock()
	tracked := slices.Clone(c.sockets)
	c.mu.Unlock()

	require.Len(t, tracked, 1)
	require.Same(t, open, tracked[0])
}
