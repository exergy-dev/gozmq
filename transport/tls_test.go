package transport

import (
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

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	testCertDER  []byte
	testCertPEM  *x509.Certificate
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
	require.NoError(t, err, "NewTLSTransport")
	l, err := tr.Listen(context.Background(), "tls+tcp://127.0.0.1:0")
	require.NoError(t, err, "Listen")
	return tr, l.(*tlsListener)
}

// mustNewTLS wraps NewTLSTransport for tests that don't care about the error path.
func mustNewTLS(t *testing.T, cfg *tls.Config) *TLSTransport {
	t.Helper()
	tr, err := NewTLSTransport(cfg)
	require.NoError(t, err, "NewTLSTransport")
	return tr
}

// TestTLS_RoundTrip exercises the happy path: handshake, bidirectional
// write, endpoint reporting.
func TestTLS_RoundTrip(t *testing.T) {
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr
	defer l.Close()

	endpoint := l.Endpoint()
	require.True(t, strings.HasPrefix(endpoint, "tls+tcp://127.0.0.1:"),
		"Endpoint = %q, want tls+tcp://127.0.0.1:<port>", endpoint)

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

	// Dial with a client config that trusts the server cert. Dial through
	// "localhost:<port>" so SAN validation matches.
	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(endpoint, "tls+tcp://"))
	require.NoError(t, err, "SplitHostPort")
	cliTr := mustNewTLS(t, clientTLSConfig())
	dial, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
	require.NoError(t, err, "Dial")
	defer dial.Close()

	r := <-acceptedCh
	require.NoError(t, r.err, "Accept")
	defer r.c.Close()

	// Endpoint-string reporting: the Conn LocalEndpoint / RemoteEndpoint
	// must announce tls+tcp, not raw tcp.
	cli, ok := dial.(*tlsConn)
	require.True(t, ok, "client conn type = %T", dial)
	assert.True(t, strings.HasPrefix(cli.RemoteEndpoint(), "tls+tcp://"),
		"client RemoteEndpoint = %q, want tls+tcp prefix", cli.RemoteEndpoint())
	assert.True(t, strings.HasPrefix(cli.LocalEndpoint(), "tls+tcp://"),
		"client LocalEndpoint = %q, want tls+tcp prefix", cli.LocalEndpoint())

	// Bidirectional write/read over the TLS channel.
	want := []byte("ping-over-tls")
	_, err = dial.Write(want)
	require.NoError(t, err, "Write client->server")
	got := make([]byte, len(want))
	_, err = io.ReadFull(r.c, got)
	require.NoError(t, err, "ReadFull server")
	require.Equal(t, want, got)

	want2 := []byte("pong-over-tls")
	_, err = r.c.Write(want2)
	require.NoError(t, err, "Write server->client")
	got2 := make([]byte, len(want2))
	_, err = io.ReadFull(dial, got2)
	require.NoError(t, err, "ReadFull client")
	require.Equal(t, want2, got2)
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
	ctx := testCtx(t, 2*time.Second)
	c, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
	if err == nil {
		c.Close()
	}
	require.Error(t, err, "Dial unexpectedly succeeded with untrusted server cert")
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
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		require.Fail(t, "Accept did not wake within 2s of ctx cancel")
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
	require.NoError(t, l.Close())
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, zmqerr.ErrEndpointInvalid)
	case <-time.After(2 * time.Second):
		require.Fail(t, "Accept did not wake on Close")
	}
}

// TestTLS_NilConfig_Rejected asserts the constructor rejects a nil
// tls.Config rather than returning a half-built transport that would
// fail later at Dial/Listen time.
func TestTLS_NilConfig_Rejected(t *testing.T) {
	tr, err := NewTLSTransport(nil)
	require.Error(t, err, "NewTLSTransport(nil) succeeded, want error")
	assert.Nil(t, tr, "NewTLSTransport(nil) returned non-nil transport")
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
		_, err := tr.Listen(context.Background(), ep)
		assert.ErrorIs(t, err, zmqerr.ErrEndpointInvalid, "Listen(%q)", ep)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, dialErr := tr.Dial(ctx, ep)
		cancel()
		if !errors.Is(dialErr, zmqerr.ErrEndpointInvalid) && !errors.Is(dialErr, context.DeadlineExceeded) {
			assert.Failf(t, "unexpected Dial error", "Dial(%q) err = %v, want wraps ErrEndpointInvalid", ep, dialErr)
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
				assert.NoError(t, err, "Accept[%d]", i)
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
				assert.NoError(t, err, "Dial")
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
	require.NoError(t, err, "NewTLSTransport(server)")
	// Mutate the ORIGINAL caller-owned config after construction.
	srvCfg.Certificates = nil
	srvCfg.MinVersion = 0

	l, err := srvTr.Listen(context.Background(), "tls+tcp://127.0.0.1:0")
	require.NoError(t, err, "Listen")
	defer l.Close()
	endpoint := l.Endpoint()
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(endpoint, "tls+tcp://"))

	type acceptResult struct {
		c   net.Conn
		err error
	}
	acceptedCh := make(chan acceptResult, 1)
	ctx := testCtx(t, 5*time.Second)
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
	require.NoError(t, err, "NewTLSTransport(client)")
	cliCfg.RootCAs = nil
	cliCfg.ServerName = ""
	cliCfg.MinVersion = 0

	dial, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
	require.NoError(t, err, "Dial after caller mutation")
	defer dial.Close()

	r := <-acceptedCh
	require.NoError(t, r.err, "Accept after caller mutation")
	defer r.c.Close()

	// Sanity-check: the pre-mutation MinVersion (TLS 1.2) is still in
	// force on the transport's snapshot — a full write/read proves
	// the handshake completed end-to-end.
	want := []byte("isolated")
	_, err = dial.Write(want)
	require.NoError(t, err, "Write")
	got := make([]byte, len(want))
	_, err = io.ReadFull(r.c, got)
	require.NoError(t, err, "ReadFull")
	require.Equal(t, want, got)
}

// TestTLS_MinVersionFloor asserts the constructor's TLS-version policy:
// a zero MinVersion is upgraded to TLS 1.2 on the stored clone, and any
// explicit MinVersion below 1.2 is rejected outright.
func TestTLS_MinVersionFloor(t *testing.T) {
	t.Run("zero-defaults-to-1.2", func(t *testing.T) {
		cfg := serverTLSConfig()
		cfg.MinVersion = 0
		tr := mustNewTLS(t, cfg)
		require.Equal(t, uint16(tls.VersionTLS12), tr.Config.MinVersion)
	})
	t.Run("below-1.2-rejected", func(t *testing.T) {
		for _, v := range []uint16{tls.VersionTLS10, tls.VersionTLS11} {
			cfg := serverTLSConfig()
			cfg.MinVersion = v
			tr, err := NewTLSTransport(cfg)
			require.Error(t, err, "MinVersion %#04x accepted", v)
			assert.Nil(t, tr)
		}
	})
	t.Run("1.3-preserved", func(t *testing.T) {
		cfg := serverTLSConfig()
		cfg.MinVersion = tls.VersionTLS13
		tr := mustNewTLS(t, cfg)
		require.Equal(t, uint16(tls.VersionTLS13), tr.Config.MinVersion)
	})
}

// TestTLS_CtxCanceledAcceptLeavesListenerUsable asserts that a ctx-cancelled
// Accept does NOT poison the listener: a subsequent Accept with a fresh ctx
// must still hand out connections. (An earlier implementation closed the
// underlying listener on ctx cancel, permanently bricking the endpoint
// after one timed-out Accept.)
func TestTLS_CtxCanceledAcceptLeavesListenerUsable(t *testing.T) {
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr
	defer l.Close()

	// First Accept: cancelled with no peer in sight.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err := l.Accept(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "first Accept")

	// Let the cancelled Accept's poll goroutine drain (it lingers up to one
	// tcpAcceptPollInterval and would otherwise accept-and-discard a conn
	// that lands in that window).
	time.Sleep(50 * time.Millisecond)

	// Second Accept on the same listener must still work end-to-end.
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(l.Endpoint(), "tls+tcp://"))
	acceptCtx := testCtx(t, 5*time.Second)
	type acceptResult struct {
		c   net.Conn
		err error
	}
	acceptedCh := make(chan acceptResult, 1)
	go func() {
		c, err := l.Accept(acceptCtx)
		if err != nil {
			acceptedCh <- acceptResult{nil, err}
			return
		}
		acceptedCh <- acceptResult{c.(net.Conn), nil}
	}()

	cliTr := mustNewTLS(t, clientTLSConfig())
	dial, err := cliTr.Dial(acceptCtx, "tls+tcp://localhost:"+portStr)
	require.NoError(t, err, "Dial after cancelled Accept")
	defer dial.Close()

	r := <-acceptedCh
	require.NoError(t, r.err, "Accept after cancelled Accept")
	require.NoError(t, r.c.Close())
}

// TestTLS_SilentClientDoesNotBlockAccept asserts that a peer which connects
// at the TCP level but never starts the TLS handshake cannot wedge the
// listener: the handshake deadline trips, that Accept returns an error, and
// the next Accept picks up a legitimate client.
func TestTLS_SilentClientDoesNotBlockAccept(t *testing.T) {
	cfg := serverTLSConfig()
	tr := mustNewTLS(t, cfg)
	tr.HandshakeTimeout = 250 * time.Millisecond
	lIface, err := tr.Listen(context.Background(), "tls+tcp://127.0.0.1:0")
	require.NoError(t, err, "Listen")
	l := lIface.(*tlsListener)
	defer l.Close()

	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(l.Endpoint(), "tls+tcp://"))

	// Silent client: raw TCP connect, no TLS bytes ever.
	silent, err := net.Dial("tcp", "127.0.0.1:"+portStr)
	require.NoError(t, err, "silent raw dial")
	defer silent.Close()

	// Give the silent connection time to land in the accept queue first.
	time.Sleep(50 * time.Millisecond)

	// Legitimate client dials concurrently; its handshake completes once
	// the accept loop reaches it.
	dialErrCh := make(chan error, 1)
	var dialConn net.Conn
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cliTr := mustNewTLS(t, clientTLSConfig())
		c, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
		if err == nil {
			dialConn = c.(net.Conn)
		}
		dialErrCh <- err
	}()

	// Accept until the legitimate client lands. The silent conn costs one
	// Accept call a handshake-timeout error; it must not cost more.
	var got net.Conn
	deadline := time.Now().Add(5 * time.Second)
	for got == nil && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		c, err := l.Accept(ctx)
		cancel()
		if err == nil {
			got = c.(net.Conn)
		}
	}
	require.NotNil(t, got, "no legitimate Accept within 5s — silent client blocked the listener")
	defer got.Close()
	require.NoError(t, <-dialErrCh, "legitimate Dial")
	if dialConn != nil {
		defer dialConn.Close()
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
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "Accept did not return within 500ms of ctx cancel — backing goroutine likely leaked")
	}

	// Close is idempotent: calling it again after the ctx-cancel path
	// already closed the underlying listener must be a no-op rather
	// than a panic or a double-close error surfacing to the caller.
	require.NoError(t, l.Close(), "first Close")
	require.NoError(t, l.Close(), "second Close (idempotency check)")
	// goleak in TestMain will catch any stray Accept goroutine at
	// package-test teardown; this test's own return is the trigger.
}
