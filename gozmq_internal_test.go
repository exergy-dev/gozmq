package gozmq

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
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
func (s waitStubSession) Pipe() iface.Pipe            { return nil }
func (s waitStubSession) Endpoint() string            { return "" }
func (s waitStubSession) Role() iface.Role            { return iface.RoleClient }
func (s waitStubSession) Done() <-chan struct{}       { return closedWaitSessionCh }
func (s waitStubSession) WriterFlushed() <-chan struct{} {
	return s.flushed
}
func (s waitStubSession) CancelWriter() {}

func TestWaitWritersFlushed_LingerTimeoutDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	start := time.Now()
	err := waitWritersFlushed(context.Background(), []iface.Session{
		waitStubSession{flushed: make(chan struct{})},
	}, 20*time.Millisecond)
	require.NoError(t, err, "waitWritersFlushed on linger timeout")
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 15*time.Millisecond, "returned after %v, want ~linger wait", elapsed)
}

func TestWaitWritersFlushed_CtxCancelDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := waitWritersFlushed(ctx, []iface.Session{
		waitStubSession{flushed: make(chan struct{})},
	}, -1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
