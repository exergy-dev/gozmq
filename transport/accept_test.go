package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// listenAll returns one freshly bound listener per built-in scheme, for
// contract tests that must hold across all of them.
func listenAll(t *testing.T) map[string]Listener {
	t.Helper()
	ls := map[string]Listener{}
	var err error
	ls["tcp"], err = NewTCPTransport().Listen(context.Background(), "tcp://127.0.0.1:0")
	require.NoError(t, err)
	ls["ipc"], err = NewIPCTransport().Listen(context.Background(), "ipc://"+tmpSock(t, "all.sock"))
	require.NoError(t, err)
	ls["tls+tcp"], err = mustNewTLS(t, serverTLSConfig()).Listen(context.Background(), "tls+tcp://127.0.0.1:0")
	require.NoError(t, err)
	return ls
}

// TestAccept_ClosedErrorWrapsNetErrClosed pins the terminal-error contract
// documented on Listener.Accept: once the listener is closed, Accept (parked
// or new) returns an error matching net.ErrClosed, which is how the socket
// accept loop tells "stop" from "retry".
func TestAccept_ClosedErrorWrapsNetErrClosed(t *testing.T) {
	for scheme, l := range listenAll(t) {
		t.Run(scheme, func(t *testing.T) {
			errCh := make(chan error, 1)
			go func() {
				_, err := l.Accept(context.Background())
				errCh <- err
			}()
			time.Sleep(20 * time.Millisecond)
			require.NoError(t, l.Close())
			select {
			case err := <-errCh:
				require.ErrorIs(t, err, net.ErrClosed, "parked Accept")
				require.ErrorIs(t, err, zmqerr.ErrEndpointInvalid, "parked Accept")
			case <-time.After(2 * time.Second):
				t.Fatal("Close did not wake Accept")
			}
			_, err := l.Accept(context.Background())
			require.ErrorIs(t, err, net.ErrClosed, "Accept after Close")
		})
	}
}

// TestAccept_CancelledAcceptDoesNotSwallowConn: a ctx-cancelled Accept must
// leave nothing behind that could accept (and discard) the next connection.
// The old poll-in-a-goroutine implementation kept polling for up to one
// interval after returning and closed whatever it caught.
func TestAccept_CancelledAcceptDoesNotSwallowConn(t *testing.T) {
	tr := NewTCPTransport()
	l, err := tr.Listen(context.Background(), "tcp://127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, err := l.Accept(ctx)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded, "iter %d", i)

		c, err := net.Dial("tcp", l.Endpoint()[len("tcp://"):])
		require.NoError(t, err)
		actx, acancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		got, err := l.Accept(actx)
		acancel()
		_ = c.Close()
		require.NoError(t, err, "iter %d: conn dialed after a cancelled Accept was lost", i)
		_ = got.Close()
	}
}

// TestAccept_ConcurrentAcceptsIndependentCtx: cancelling one Accept's ctx
// must not fail or wedge another Accept parked on the same listener.
func TestAccept_ConcurrentAcceptsIndependentCtx(t *testing.T) {
	tr := NewTCPTransport()
	l, err := tr.Listen(context.Background(), "tcp://127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	longCtx := testutil.Ctx(t, 5*time.Second)
	longCh := make(chan error, 1)
	go func() {
		c, err := l.Accept(longCtx)
		if c != nil {
			_ = c.Close()
		}
		longCh <- err
	}()

	shortCtx, cancel := context.WithCancel(context.Background())
	shortCh := make(chan error, 1)
	go func() {
		_, err := l.Accept(shortCtx)
		shortCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-shortCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled Accept did not return")
	}

	c, err := net.Dial("tcp", l.Endpoint()[len("tcp://"):])
	require.NoError(t, err)
	defer c.Close()
	select {
	case err := <-longCh:
		require.NoError(t, err, "uncancelled Accept")
	case <-time.After(2 * time.Second):
		t.Fatal("uncancelled Accept did not pick up the connection")
	}
}
