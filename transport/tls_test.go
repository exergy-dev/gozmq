package transport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/zmqerr"
)

// --------------------------------------------------------------------------
// Test certificate fixture.
//
// We generate a single self-signed ed25519 cert at init-time and use it as
// both the server cert and the client-trusted root. Two configs are
// derived: serverTLSConfig (with the Certificates populated) and
// clientTLSConfig (with RootCAs pinned to the generated cert, and
// ServerName = "localhost" so SAN validation works when we dial 127.0.0.1
// by hostname).
// --------------------------------------------------------------------------

var (
	testCertDER []byte
	testCertPEM *x509.Certificate
	testCertPriv ed25519.PrivateKey
	testRootPool *x509.CertPool
)

func init() {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic("tls_test init: " + err.Error())
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "gozmq-test",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, pub, priv)
	if err != nil {
		panic("tls_test init: " + err.Error())
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		panic("tls_test init: " + err.Error())
	}
	testCertDER = der
	testCertPEM = cert
	testCertPriv = priv

	pool := x509.NewCertPool()
	pool.AddCert(cert)
	testRootPool = pool
}

func serverTLSConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{testCertDER},
			PrivateKey:  testCertPriv,
			Leaf:        testCertPEM,
		}},
		MinVersion: tls.VersionTLS12,
	}
}

func clientTLSConfig() *tls.Config {
	return &tls.Config{
		RootCAs:    testRootPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}
}

// listenTLSLoopback binds tls+tcp://127.0.0.1:0 with a server-side config
// and returns the ephemeral listener.
func listenTLSLoopback(t *testing.T, cfg *tls.Config) (*TLSTransport, *tlsListener) {
	t.Helper()
	tr, err := NewTLSTransport(cfg)
	if err != nil {
		t.Fatalf("NewTLSTransport: %v", err)
	}
	l, err := tr.Listen(context.Background(), "tls+tcp://127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	return tr, l.(*tlsListener)
}

// mustNewTLS wraps NewTLSTransport for tests that don't care about the error path.
func mustNewTLS(t *testing.T, cfg *tls.Config) *TLSTransport {
	t.Helper()
	tr, err := NewTLSTransport(cfg)
	if err != nil {
		t.Fatalf("NewTLSTransport: %v", err)
	}
	return tr
}

// TestTLS_RoundTrip exercises the happy path: handshake, bidirectional
// write, endpoint reporting.
func TestTLS_RoundTrip(t *testing.T) {
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr
	defer l.Close()

	endpoint := l.Endpoint()
	if !strings.HasPrefix(endpoint, "tls+tcp://127.0.0.1:") {
		t.Fatalf("Endpoint = %q, want tls+tcp://127.0.0.1:<port>", endpoint)
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

	// Dial with a client config that trusts the server cert. Dial through
	// "localhost:<port>" so SAN validation matches.
	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(endpoint, "tls+tcp://"))
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	cliTr := mustNewTLS(t, clientTLSConfig())
	dial, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer dial.Close()

	r := <-acceptedCh
	if r.err != nil {
		t.Fatalf("Accept: %v", r.err)
	}
	defer r.c.Close()

	// Endpoint-string reporting: the Conn LocalEndpoint / RemoteEndpoint
	// must announce tls+tcp, not raw tcp.
	if cli, ok := dial.(*tlsConn); ok {
		if !strings.HasPrefix(cli.RemoteEndpoint(), "tls+tcp://") {
			t.Errorf("client RemoteEndpoint = %q, want tls+tcp prefix", cli.RemoteEndpoint())
		}
		if !strings.HasPrefix(cli.LocalEndpoint(), "tls+tcp://") {
			t.Errorf("client LocalEndpoint = %q, want tls+tcp prefix", cli.LocalEndpoint())
		}
	} else {
		t.Fatalf("client conn type = %T", dial)
	}

	// Bidirectional write/read over the TLS channel.
	want := []byte("ping-over-tls")
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

	want2 := []byte("pong-over-tls")
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

// TestTLS_BadCert asserts that a client dialing with a config that does
// NOT trust the server's cert (e.g. empty RootCAs, InsecureSkipVerify=false)
// fails the handshake rather than silently connecting.
func TestTLS_BadCert(t *testing.T) {
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr
	defer l.Close()

	endpoint := l.Endpoint()
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(endpoint, "tls+tcp://"))

	// Acceptor drains any partial handshakes so the server listener does
	// not leak a goroutine on test exit.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		c, err := l.Accept(ctx)
		if err == nil && c != nil {
			c.Close()
		}
	}()

	// Client config with no trusted roots — handshake must fail.
	badCfg := &tls.Config{
		RootCAs:    x509.NewCertPool(),
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}
	cliTr := mustNewTLS(t, badCfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
	if err == nil {
		c.Close()
		t.Fatal("Dial unexpectedly succeeded with untrusted server cert")
	}
	// We don't assert a specific wrapper; any non-nil error from the TLS
	// handshake demonstrates verification actually ran.
	<-done
}

// TestTLS_AcceptHonorsCtx asserts that ctx cancellation wakes Accept
// promptly when no peer arrives.
func TestTLS_AcceptHonorsCtx(t *testing.T) {
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr
	defer l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := l.Accept(ctx)
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Accept err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not wake within 2s of ctx cancel")
	}
}

// TestTLS_ListenerClose_WakesAccept asserts Close wakes a parked Accept.
func TestTLS_ListenerClose_WakesAccept(t *testing.T) {
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr

	errCh := make(chan error, 1)
	go func() {
		_, err := l.Accept(context.Background())
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, zmqerr.ErrEndpointInvalid) {
			t.Fatalf("Accept err = %v, want wraps ErrEndpointInvalid", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not wake on Close")
	}
}

// TestTLS_NilConfig_Rejected asserts the constructor rejects a nil
// tls.Config rather than returning a half-built transport that would
// fail later at Dial/Listen time.
func TestTLS_NilConfig_Rejected(t *testing.T) {
	tr, err := NewTLSTransport(nil)
	if err == nil {
		t.Fatalf("NewTLSTransport(nil) succeeded, want error")
	}
	if tr != nil {
		t.Errorf("NewTLSTransport(nil) returned non-nil transport %v", tr)
	}
}

// TestTLS_BadEndpoint covers scheme and host-port parse failures.
func TestTLS_BadEndpoint(t *testing.T) {
	tr := mustNewTLS(t, serverTLSConfig())
	cases := []string{
		"tcp://x:1",    // wrong scheme
		"no-scheme",    // no ://
		"tls+tcp://xx", // missing port
	}
	for _, ep := range cases {
		if _, err := tr.Listen(context.Background(), ep); !errors.Is(err, zmqerr.ErrEndpointInvalid) {
			t.Errorf("Listen(%q) err = %v, want wraps ErrEndpointInvalid", ep, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := tr.Dial(ctx, ep)
		cancel()
		if !errors.Is(err, zmqerr.ErrEndpointInvalid) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Dial(%q) err = %v, want wraps ErrEndpointInvalid", ep, err)
		}
	}
}

// TestTLS_ManyConcurrentDials sanity-checks that several clients can
// complete the handshake against one listener without deadlock.
func TestTLS_ManyConcurrentDials(t *testing.T) {
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr
	defer l.Close()

	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(l.Endpoint(), "tls+tcp://"))

	const N = 4
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			c, err := l.Accept(ctx)
			cancel()
			if err != nil {
				t.Errorf("Accept[%d]: %v", i, err)
				return
			}
			c.Close()
		}
	}()

	cliTr := mustNewTLS(t, clientTLSConfig())
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
			if err != nil {
				t.Errorf("Dial: %v", err)
				return
			}
			c.Close()
		}()
	}
	wg.Wait()
}

// TestTLS_ConfigCloneIsolatesFromCaller asserts that the transport
// snapshots the caller's *tls.Config at construction time: subsequent
// mutations to the caller's config object (clearing Certificates,
// dropping MinVersion) must not affect the transport's handshake. This
// protects against a data race between a caller mutating the config and
// the transport reading it mid-handshake.
func TestTLS_ConfigCloneIsolatesFromCaller(t *testing.T) {
	srvCfg := serverTLSConfig()
	// Construct first; THEN mutate the caller's config. If the
	// transport held the user's pointer directly, the subsequent
	// Accept/Handshake would see no Certificates and refuse the
	// handshake. Because we Clone on construction, the mutation is
	// invisible to the transport.
	srvTr, err := NewTLSTransport(srvCfg)
	if err != nil {
		t.Fatalf("NewTLSTransport(server): %v", err)
	}
	// Mutate the ORIGINAL caller-owned config after construction.
	srvCfg.Certificates = nil
	srvCfg.MinVersion = 0

	l, err := srvTr.Listen(context.Background(), "tls+tcp://127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()
	endpoint := l.Endpoint()
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(endpoint, "tls+tcp://"))

	type acceptResult struct {
		c   net.Conn
		err error
	}
	acceptedCh := make(chan acceptResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			acceptedCh <- acceptResult{nil, err}
			return
		}
		acceptedCh <- acceptResult{c.(net.Conn), nil}
	}()

	// Client side — also construct, then mutate the original, prove
	// the handshake still works against the snapshot.
	cliCfg := clientTLSConfig()
	cliTr, err := NewTLSTransport(cliCfg)
	if err != nil {
		t.Fatalf("NewTLSTransport(client): %v", err)
	}
	cliCfg.RootCAs = nil
	cliCfg.ServerName = ""
	cliCfg.MinVersion = 0

	dial, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
	if err != nil {
		t.Fatalf("Dial after caller mutation: %v", err)
	}
	defer dial.Close()

	r := <-acceptedCh
	if r.err != nil {
		t.Fatalf("Accept after caller mutation: %v", r.err)
	}
	defer r.c.Close()

	// Sanity-check: the pre-mutation MinVersion (TLS 1.2) is still in
	// force on the transport's snapshot — a full write/read proves
	// the handshake completed end-to-end.
	want := []byte("isolated")
	if _, err := dial.Write(want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r.c, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read %q, want %q", got, want)
	}
}

// TestTLS_AcceptGoroutineExitsOnCtxCancel asserts that canceling the
// Accept ctx drains the backing net.Listener.Accept goroutine within a
// tight deadline. Before the fix, ctx cancel returned to the caller but
// the accept goroutine stayed blocked in l.l.Accept() forever (no
// deadline was set on the underlying listener). goleak in TestMain now
// catches any residual Accept goroutine; this test also explicitly
// verifies Close is idempotent.
func TestTLS_AcceptGoroutineExitsOnCtxCancel(t *testing.T) {
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := l.Accept(ctx)
		errCh <- err
	}()

	// Give the accept goroutine time to park in l.l.Accept().
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Accept err = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Accept did not return within 500ms of ctx cancel — backing goroutine likely leaked")
	}

	// Close is idempotent: calling it again after the ctx-cancel path
	// already closed the underlying listener must be a no-op rather
	// than a panic or a double-close error surfacing to the caller.
	if err := l.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close (idempotency check): %v", err)
	}
	// goleak in TestMain will catch any stray Accept goroutine at
	// package-test teardown; this test's own return is the trigger.
}
