package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/zmqerr"
)

// listenLoopback binds tcp://127.0.0.1:0 and returns the resolved Listener.
func listenLoopback(t *testing.T) (*TCPTransport, *tcpListener) {
	t.Helper()
	tr := NewTCPTransport()
	l, err := tr.Listen(context.Background(), "tcp://127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	return tr, l.(*tcpListener)
}

func TestTCP_RoundTrip_Bytes(t *testing.T) {
	tr, l := listenLoopback(t)
	defer l.Close()

	endpoint := l.Endpoint()
	if !strings.HasPrefix(endpoint, "tcp://127.0.0.1:") {
		t.Fatalf("Endpoint = %q, want tcp://127.0.0.1:<port>", endpoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer dial.Close()

	r := <-acceptedCh
	if r.err != nil {
		t.Fatalf("Accept: %v", r.err)
	}
	defer r.c.Close()

	// Both directions.
	want := []byte("hello, world")
	if _, err := dial.Write(want); err != nil {
		t.Fatalf("Write client→server: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r.c, got); err != nil {
		t.Fatalf("ReadFull server: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("server read %q, want %q", got, want)
	}

	want2 := []byte("ack")
	if _, err := r.c.Write(want2); err != nil {
		t.Fatalf("Write server→client: %v", err)
	}
	got2 := make([]byte, len(want2))
	if _, err := io.ReadFull(dial, got2); err != nil {
		t.Fatalf("ReadFull client: %v", err)
	}
	if !bytes.Equal(got2, want2) {
		t.Fatalf("client read %q, want %q", got2, want2)
	}
}

// TestTCP_Wildcard_And_Ephemeral_Port covers the "tcp://*:0" → resolved
// endpoint flow that Socket.LastEndpoint() relies on.
func TestTCP_Wildcard_And_Ephemeral_Port(t *testing.T) {
	tr := NewTCPTransport()
	l, err := tr.Listen(context.Background(), "tcp://*:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	ep := l.Endpoint()
	// On all-interfaces with port 0 we expect "tcp://[::]:<port>" or
	// "tcp://0.0.0.0:<port>" depending on the kernel; the dial path can
	// connect via 127.0.0.1 regardless.
	if !strings.HasPrefix(ep, "tcp://") || strings.HasSuffix(ep, ":0") {
		t.Fatalf("Endpoint after wildcard listen = %q (port unresolved?)", ep)
	}

	// Extract the port from the resolved endpoint.
	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(ep, "tcp://"))
	if err != nil {
		t.Fatalf("SplitHostPort %q: %v", ep, err)
	}
	dialEP := "tcp://127.0.0.1:" + portStr

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	acceptCh := make(chan struct{}, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer c.Close()
		buf := make([]byte, 5)
		_, _ = io.ReadFull(c, buf)
		acceptCh <- struct{}{}
	}()

	c, err := tr.Dial(ctx, dialEP)
	if err != nil {
		t.Fatalf("Dial(%q): %v", dialEP, err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	select {
	case <-acceptCh:
	case <-ctx.Done():
		t.Fatal("Accept did not receive bytes within deadline")
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

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, zmqerr.ErrEndpointInvalid) {
			t.Fatalf("Accept after Close err = %v, want wraps ErrEndpointInvalid", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not wake within 2s of Close")
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
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Accept err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not honor ctx within 2s")
	}
}

// TestTCP_Dial_Honors_Ctx_Cancellation. Dial to an unroutable address with
// a short deadline; ctx must error within the deadline window.
func TestTCP_Dial_Honors_Ctx_Cancellation(t *testing.T) {
	tr := NewTCPTransport()
	// 240.0.0.1 is reserved; connect attempts hang or RST quickly. Use ctx
	// to bound.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := tr.Dial(ctx, "tcp://240.0.0.1:9")
	if err == nil {
		t.Fatal("Dial unexpectedly succeeded")
	}
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
		if _, err := tr.Dial(context.Background(), ep); err == nil {
			t.Errorf("Dial(%q) succeeded, want error", ep)
		} else if !errors.Is(err, zmqerr.ErrEndpointInvalid) {
			t.Errorf("Dial(%q) err = %v, want wraps ErrEndpointInvalid", ep, err)
		}
	}
}

// TestTCP_Conn_Endpoints checks LocalEndpoint / RemoteEndpoint round-trip
// through the iface.Conn adapter.
func TestTCP_Conn_Endpoints(t *testing.T) {
	tr, l := listenLoopback(t)
	defer l.Close()
	endpoint := l.Endpoint()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer dial.Close()

	r := <-acceptedCh
	if r.err != nil {
		t.Fatalf("Accept: %v", r.err)
	}
	defer r.c.Close()

	dc := dial.(interface {
		LocalEndpoint() string
		RemoteEndpoint() string
	})
	if !strings.HasPrefix(dc.LocalEndpoint(), "tcp://") {
		t.Errorf("client.LocalEndpoint = %q", dc.LocalEndpoint())
	}
	if dc.RemoteEndpoint() != endpoint && !strings.HasPrefix(dc.RemoteEndpoint(), "tcp://") {
		t.Errorf("client.RemoteEndpoint = %q, want tcp://...", dc.RemoteEndpoint())
	}
}

// TestTCP_NoDelay_KeepAlive_Defaults asserts the design defaults are
// applied. We can't easily probe the kernel state without unsafe syscalls,
// so this is a behavioral smoke that toggling the field surfaces in the
// exposed configuration.
func TestTCP_NoDelay_KeepAlive_Defaults(t *testing.T) {
	tr := NewTCPTransport()
	if !tr.KeepAlive {
		t.Error("default KeepAlive should be true")
	}
	if !tr.NoDelay {
		t.Error("default NoDelay should be true")
	}
	if tr.KeepAlivePeriod != defaultTCPKeepAliveIdle {
		t.Errorf("KeepAlivePeriod = %v, want %v", tr.KeepAlivePeriod, defaultTCPKeepAliveIdle)
	}

	tr.KeepAlive = false
	tr.NoDelay = false
	if tr.keepAliveSetting() >= 0 {
		t.Errorf("keepAliveSetting = %v, want negative when disabled", tr.keepAliveSetting())
	}
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
	if !strings.HasPrefix(ep, "tcp6://") {
		t.Fatalf("Endpoint = %q", ep)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := l.Accept(ctx)
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer c.Close()
		_, _ = io.ReadFull(c, make([]byte, 1))
	}()

	c, err := tr.Dial(ctx, ep)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte{0x42}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	wg.Wait()
}
