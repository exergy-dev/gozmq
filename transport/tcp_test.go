package transport

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listenLoopback binds tcp://127.0.0.1:0 and returns the resolved Listener.
func listenLoopback(t *testing.T) (*TCPTransport, *tcpListener) {
	t.Helper()
	tr := NewTCPTransport()
	l, err := tr.Listen(context.Background(), "tcp://127.0.0.1:0")
	require.NoError(t, err, "Listen")
	return tr, l.(*tcpListener)
}

func TestTCP_RoundTrip_Bytes(t *testing.T) {
	tr, l := listenLoopback(t)
	defer l.Close()

	endpoint := l.Endpoint()
	require.True(t, strings.HasPrefix(endpoint, "tcp://127.0.0.1:"), "Endpoint = %q, want tcp://127.0.0.1:<port>", endpoint)

	ctx := testCtx(t, 5*time.Second)

	type acceptResult struct {
		c   net.Conn
		err error
	}
	acceptedCh := make(chan acceptResult, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			acceptedCh <- acceptResult{nil, err}
			return
		}
		acceptedCh <- acceptResult{c.(net.Conn), nil}
	}()

	dial, err := tr.Dial(ctx, endpoint)
	require.NoError(t, err, "Dial")
	defer dial.Close()

	r := <-acceptedCh
	require.NoError(t, r.err, "Accept")
	defer r.c.Close()

	// Both directions.
	want := []byte("hello, world")
	_, err = dial.Write(want)
	require.NoError(t, err, "Write client->server")
	got := make([]byte, len(want))
	_, err = io.ReadFull(r.c, got)
	require.NoError(t, err, "ReadFull server")
	require.Equal(t, want, got)

	want2 := []byte("ack")
	_, err = r.c.Write(want2)
	require.NoError(t, err, "Write server->client")
	got2 := make([]byte, len(want2))
	_, err = io.ReadFull(dial, got2)
	require.NoError(t, err, "ReadFull client")
	require.Equal(t, want2, got2)
}

// TestTCP_Wildcard_And_Ephemeral_Port covers the "tcp://*:0" → resolved
// endpoint flow that Socket.LastEndpoint() relies on.
func TestTCP_Wildcard_And_Ephemeral_Port(t *testing.T) {
	tr := NewTCPTransport()
	l, err := tr.Listen(context.Background(), "tcp://*:0")
	require.NoError(t, err, "Listen")
	defer l.Close()

	ep := l.Endpoint()
	// On all-interfaces with port 0 we expect "tcp://[::]:<port>" or
	// "tcp://0.0.0.0:<port>" depending on the kernel; the dial path can
	// connect via 127.0.0.1 regardless.
	require.True(t, strings.HasPrefix(ep, "tcp://") && !strings.HasSuffix(ep, ":0"),
		"Endpoint after wildcard listen = %q (port unresolved?)", ep)

	// Extract the port from the resolved endpoint.
	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(ep, "tcp://"))
	require.NoError(t, err, "SplitHostPort %q", ep)
	dialEP := "tcp://127.0.0.1:" + portStr

	ctx := testCtx(t, 5*time.Second)

	acceptCh := make(chan struct{}, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			assert.NoError(t, err, "Accept")
			return
		}
		defer c.Close()
		buf := make([]byte, 5)
		_, _ = io.ReadFull(c, buf)
		acceptCh <- struct{}{}
	}()

	c, err := tr.Dial(ctx, dialEP)
	require.NoError(t, err, "Dial(%q)", dialEP)
	defer c.Close()
	_, err = c.Write([]byte("hello"))
	require.NoError(t, err, "Write")
	select {
	case <-acceptCh:
	case <-ctx.Done():
		require.Fail(t, "Accept did not receive bytes within deadline")
	}
}

// TestTCP_Listener_Close_Wakes_Accept is the core "no indefinite hang"
// guarantee.
func TestTCP_Listener_Close_Wakes_Accept(t *testing.T) {
	_, l := listenLoopback(t)

	errCh := make(chan error, 1)
	go func() {
		_, err := l.Accept(context.Background())
		errCh <- err
	}()

	// Brief settle so Accept is parked.
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, l.Close(), "Close")
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, zmqerr.ErrEndpointInvalid)
	case <-time.After(2 * time.Second):
		require.Fail(t, "Accept did not wake within 2s of Close")
	}
}

// TestTCP_Accept_Honors_Ctx asserts ctx cancellation wakes Accept.
func TestTCP_Accept_Honors_Ctx(t *testing.T) {
	_, l := listenLoopback(t)
	defer l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { _, err := l.Accept(ctx); errCh <- err }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		require.Fail(t, "Accept did not honor ctx within 2s")
	}
}

// TestTCP_Dial_Honors_Ctx_Cancellation. Dial to an unroutable address with
// a short deadline; ctx must error within the deadline window.
func TestTCP_Dial_Honors_Ctx_Cancellation(t *testing.T) {
	tr := NewTCPTransport()
	// 240.0.0.1 is reserved; connect attempts hang or RST quickly. Use ctx
	// to bound.
	ctx := testCtx(t, 100*time.Millisecond)
	_, err := tr.Dial(ctx, "tcp://240.0.0.1:9")
	require.Error(t, err, "Dial unexpectedly succeeded")
}

// TestTCP_BadEndpoint covers the parse path.
func TestTCP_BadEndpoint(t *testing.T) {
	tr := NewTCPTransport()
	cases := []string{
		"tcp://*:5555",   // wildcard not allowed for Dial
		"tcp://host",     // missing port
		"tcp://host:0",   // port 0 not allowed for Dial
		"udp://host:1",   // wrong scheme
		"no-scheme-here", // no scheme
	}
	for _, ep := range cases {
		_, err := tr.Dial(context.Background(), ep)
		if assert.Error(t, err, "Dial(%q) succeeded, want error", ep) {
			assert.ErrorIs(t, err, zmqerr.ErrEndpointInvalid, "Dial(%q)", ep)
		}
	}
}

// TestTCP_Conn_Endpoints checks LocalEndpoint / RemoteEndpoint round-trip
// through the iface.Conn adapter.
func TestTCP_Conn_Endpoints(t *testing.T) {
	tr, l := listenLoopback(t)
	defer l.Close()
	endpoint := l.Endpoint()

	ctx := testCtx(t, 5*time.Second)

	type acceptResult struct {
		c   net.Conn
		err error
	}
	acceptedCh := make(chan acceptResult, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			acceptedCh <- acceptResult{nil, err}
			return
		}
		acceptedCh <- acceptResult{c.(net.Conn), nil}
	}()

	dial, err := tr.Dial(ctx, endpoint)
	require.NoError(t, err, "Dial")
	defer dial.Close()

	r := <-acceptedCh
	require.NoError(t, r.err, "Accept")
	defer r.c.Close()

	dc := dial.(interface {
		LocalEndpoint() string
		RemoteEndpoint() string
	})
	assert.True(t, strings.HasPrefix(dc.LocalEndpoint(), "tcp://"), "client.LocalEndpoint = %q", dc.LocalEndpoint())
	assert.True(t, dc.RemoteEndpoint() == endpoint || strings.HasPrefix(dc.RemoteEndpoint(), "tcp://"),
		"client.RemoteEndpoint = %q, want tcp://...", dc.RemoteEndpoint())
}

// TestTCP_NoDelay_KeepAlive_Defaults asserts the design defaults are
// applied. We can't easily probe the kernel state without unsafe syscalls,
// so this is a behavioral smoke that toggling the field surfaces in the
// exposed configuration.
func TestTCP_NoDelay_KeepAlive_Defaults(t *testing.T) {
	tr := NewTCPTransport()
	assert.True(t, tr.KeepAlive, "default KeepAlive should be true")
	assert.True(t, tr.NoDelay, "default NoDelay should be true")
	assert.Equal(t, defaultTCPKeepAliveIdle, tr.KeepAlivePeriod)

	tr.KeepAlive = false
	tr.NoDelay = false
	assert.Negative(t, tr.keepAliveSetting(), "keepAliveSetting should be negative when disabled")
}

// TestTCP_TCP6_Scheme covers the alternate scheme ("tcp6"). We only smoke
// it: bind on the IPv6 loopback, dial back, write a byte. If the host has
// no IPv6 stack we skip rather than fail.
func TestTCP_TCP6_Scheme(t *testing.T) {
	tr := NewTCP6Transport()
	l, err := tr.Listen(context.Background(), "tcp6://[::1]:0")
	if err != nil {
		t.Skipf("tcp6 not available: %v", err)
	}
	defer l.Close()

	ep := l.Endpoint()
	require.True(t, strings.HasPrefix(ep, "tcp6://"), "Endpoint = %q", ep)

	ctx := testCtx(t, 5*time.Second)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := l.Accept(ctx)
		if err != nil {
			assert.NoError(t, err, "Accept")
			return
		}
		defer c.Close()
		_, _ = io.ReadFull(c, make([]byte, 1))
	}()

	c, err := tr.Dial(ctx, ep)
	require.NoError(t, err, "Dial")
	defer c.Close()
	_, err = c.Write([]byte{0x42})
	require.NoError(t, err, "Write")
	wg.Wait()
}
