package gozmq_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/transport"
)

// Regressions for the bind-side accept path: a per-connection Accept error
// must not stop the accept loop, and Bind must not race Close.

// TestBind_TLSBadClientsDoNotStopAccepting sends a silent TCP client and a
// non-TLS client at a tls+tcp bind, then requires a legitimate TLS peer to
// be served promptly. Before lazy TLS handshakes, the garbage client's
// handshake error killed the accept loop and the silent one stalled it for
// the full TLS handshake timeout.
func TestBind_TLSBadClientsDoNotStopAccepting(t *testing.T) {
	tr, err := transport.NewTLSTransport(newSelfSignedTLSConfig(t))
	require.NoError(t, err, "NewTLSTransport")
	zctx := gozmq.NewContext()
	defer zctx.Close()
	zctx.RegisterTransport(tr)

	pull, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err, "new pull")
	defer pull.Close()
	require.NoError(t, pull.Bind("tls+tcp://127.0.0.1:0"), "bind")
	ep := pull.LastEndpoint()
	addr := strings.TrimPrefix(ep, "tls+tcp://")

	silent, err := net.Dial("tcp", addr)
	require.NoError(t, err, "silent dial")
	defer silent.Close()

	garbage, err := net.Dial("tcp", addr)
	require.NoError(t, err, "garbage dial")
	_, _ = garbage.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
	// Wait for the server to reject it (it answers with a TLS alert and
	// closes) so the garbage conn has fully gone through the accept path.
	_ = garbage.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _ = garbage.Read(make([]byte, 64))
	_ = garbage.Close()

	push, err := zctx.NewSocket(gozmq.Push, gozmq.WithLinger(0))
	require.NoError(t, err, "new push")
	defer push.Close()
	require.NoError(t, push.Connect(ep), "connect")

	ctx := testutil.Ctx(t, 3*time.Second)
	require.NoError(t, push.SendBytes(ctx, []byte("hello")), "send")
	b, err := pull.RecvBytes(ctx)
	require.NoError(t, err, "legitimate TLS client not served after bad clients")
	require.Equal(t, "hello", string(b), "recv")
}

const flakyScheme = "flaky+tcp"

// flakyTransport wraps the built-in TCP transport and makes the first
// failures Accept calls on each listener return a per-connection error, the
// way EMFILE or a failed socket option would.
type flakyTransport struct {
	tcp      transport.Transport
	failures int32
}

func (t *flakyTransport) Scheme() string { return flakyScheme }

func (t *flakyTransport) Dial(ctx context.Context, endpoint string) (transport.Conn, error) {
	return t.tcp.Dial(ctx, "tcp://"+strings.TrimPrefix(endpoint, flakyScheme+"://"))
}

func (t *flakyTransport) Listen(ctx context.Context, endpoint string) (transport.Listener, error) {
	l, err := t.tcp.Listen(ctx, "tcp://"+strings.TrimPrefix(endpoint, flakyScheme+"://"))
	if err != nil {
		return nil, err
	}
	fl := &flakyListener{Listener: l}
	fl.remaining.Store(t.failures)
	return fl, nil
}

type flakyListener struct {
	transport.Listener
	remaining atomic.Int32
}

var errInjectedAccept = errors.New("injected accept failure: too many open files")

func (l *flakyListener) Accept(ctx context.Context) (transport.Conn, error) {
	if l.remaining.Add(-1) >= 0 {
		return nil, errInjectedAccept
	}
	return l.Listener.Accept(ctx)
}

func (l *flakyListener) Endpoint() string {
	return flakyScheme + "://" + strings.TrimPrefix(l.Listener.Endpoint(), "tcp://")
}

// TestBind_AcceptErrorDoesNotStopAccepting injects Accept errors that are not
// net.ErrClosed through a registered out-of-tree transport. The accept loop
// must surface them as EventAcceptFailed and keep accepting.
func TestBind_AcceptErrorDoesNotStopAccepting(t *testing.T) {
	zctx := gozmq.NewContext()
	defer zctx.Close()
	zctx.RegisterTransport(&flakyTransport{tcp: transport.NewTCPTransport(), failures: 3})

	pull, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err, "new pull")
	defer pull.Close()
	events := make(chan gozmq.Event, 16)
	require.NoError(t, pull.Monitor(events, gozmq.EventAcceptFailed), "monitor")
	require.NoError(t, pull.Bind(flakyScheme+"://127.0.0.1:0"), "bind")

	push, err := zctx.NewSocket(gozmq.Push, gozmq.WithLinger(0))
	require.NoError(t, err, "new push")
	defer push.Close()
	require.NoError(t, push.Connect(pull.LastEndpoint()), "connect")

	ctx := testutil.Ctx(t, 3*time.Second)
	require.NoError(t, push.SendBytes(ctx, []byte("hi")), "send")
	_, err = pull.RecvBytes(ctx)
	require.NoError(t, err, "peer not served after injected accept errors")
	select {
	case ev := <-events:
		require.Contains(t, ev.Reason, errInjectedAccept.Error(), "EventAcceptFailed reason")
	default:
		t.Fatal("injected accept error was not reported via EventAcceptFailed")
	}
}

// TestBind_RacingCloseReleasesListener races Bind against Close. Whichever
// wins, the listener must end up closed: a Bind that lands after Close's
// listener snapshot used to leak its listener (and accept goroutine), leaving
// the inproc name bound forever.
func TestBind_RacingCloseReleasesListener(t *testing.T) {
	zctx := gozmq.NewContext()
	defer zctx.Close()
	for i := 0; i < 1000; i++ {
		ep := fmt.Sprintf("inproc://bind-close-race-%d", i)
		sock, err := zctx.NewSocket(gozmq.Pull)
		require.NoError(t, err, "new socket")
		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		var bindErr error
		go func() {
			defer wg.Done()
			<-start
			bindErr = sock.Bind(ep)
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = sock.Close()
		}()
		close(start)
		wg.Wait()
		if bindErr != nil {
			require.ErrorIs(t, bindErr, gozmq.ErrSocketClosed, "iter %d: Bind", i)
		}

		again, err := zctx.NewSocket(gozmq.Pull)
		require.NoError(t, err, "new socket")
		err = again.Bind(ep)
		_ = again.Close()
		require.NoError(t, err, "iter %d: rebind after Bind/Close race (bindErr=%v)", i, bindErr)
	}
}
