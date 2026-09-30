package gozmq

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/session"
	"github.com/exergy-dev/gozmq/security"
)

var closedWaitSessionCh = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type waitStubSession struct {
	flushed <-chan struct{}
}

func (s waitStubSession) Start()                      {}
func (s waitStubSession) Stop() error                 { return nil }
func (s waitStubSession) Handshaked() <-chan struct{} { return closedWaitSessionCh }
func (s waitStubSession) HandshakeErr() error         { return nil }
func (s waitStubSession) Pipe() *pipe.End             { return nil }
func (s waitStubSession) Endpoint() string            { return "" }
func (s waitStubSession) Role() security.Role         { return security.RoleClient }
func (s waitStubSession) Done() <-chan struct{}       { return closedWaitSessionCh }
func (s waitStubSession) WriterFlushed() <-chan struct{} {
	return s.flushed
}
func (s waitStubSession) CancelWriter() {}

func TestWaitWritersFlushed_LingerTimeoutDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	start := time.Now()
	err := waitChannels(context.Background(), []session.Session{
		waitStubSession{flushed: make(chan struct{})},
	}, session.Session.WriterFlushed, 20*time.Millisecond)
	require.NoError(t, err, "waitChannels on linger timeout")
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 15*time.Millisecond, "returned after %v, want ~linger wait", elapsed)
}

func TestWaitWritersFlushed_CtxCancelDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := waitChannels(ctx, []session.Session{
		waitStubSession{flushed: make(chan struct{})},
	}, session.Session.WriterFlushed, -1)
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
