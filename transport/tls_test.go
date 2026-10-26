package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/testutil"
)

// Test certificate fixture.
//
// We generate a single self-signed ed25519 cert at init-time and use it as
// both the server cert and the client-trusted root. Two configs are
// derived: serverTLSConfig (with the Certificates populated) and
// clientTLSConfig (with RootCAs pinned to the generated cert, and
// ServerName = "localhost" so SAN validation works when we dial 127.0.0.1
// by hostname).

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
func listenTLSLoopback(t *testing.T, cfg *tls.Config) (*TLSTransport, Listener) {
	t.Helper()
	tr, err := NewTLSTransport(cfg)
	require.NoError(t, err, "NewTLSTransport")
	l, err := tr.Listen(context.Background(), "tls+tcp://127.0.0.1:0")
	require.NoError(t, err, "Listen")
	return tr, l
}

// handshakePair completes the lazy TLS handshake on both ends concurrently,
// as two sessions exchanging ZMTP greetings would. Sequential first I/O on a
// single goroutine would deadlock: each side's handshake needs the other.
func handshakePair(ctx context.Context, t *testing.T, cli, srv Conn) {
	t.Helper()
	errCh := testutil.Async(func() error { return tlsOf(srv).HandshakeContext(ctx) })
	require.NoError(t, tlsOf(cli).HandshakeContext(ctx), "client handshake")
	require.NoError(t, <-errCh, "server handshake")
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
	t.Parallel()
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr
	defer l.Close()

	endpoint := l.Endpoint()
	require.True(t, strings.HasPrefix(endpoint, "tls+tcp://127.0.0.1:"),
		"Endpoint = %q, want tls+tcp://127.0.0.1:<port>", endpoint)

	ctx := testutil.Ctx(t, 5*time.Second)

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
	handshakePair(ctx, t, dial, r.c.(Conn))

	// Endpoint-string reporting: the Conn LocalEndpoint / RemoteEndpoint
	// must announce tls+tcp, not raw tcp.
	cli, ok := dial.(*conn)
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

// TestTLS_BadCert asserts that a client with a config that does NOT trust
// the server's cert (empty RootCAs, InsecureSkipVerify=false) fails the
// handshake rather than silently connecting. The handshake is lazy, so the
// failure surfaces on the first I/O after Dial.
func TestTLS_BadCert(t *testing.T) {
	t.Parallel()
	_, l := listenTLSLoopback(t, serverTLSConfig())
	defer l.Close()

	endpoint := l.Endpoint()
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(endpoint, "tls+tcp://"))

	// Acceptor drives the server side of the handshake so the client's
	// verification actually runs.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		c, err := l.Accept(ctx)
		if err == nil && c != nil {
			_ = c.SetDeadline(time.Now().Add(2 * time.Second))
			_, _ = c.Read(make([]byte, 1))
			c.Close()
		}
	}()

	badCfg := &tls.Config{
		RootCAs:    x509.NewCertPool(),
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}
	cliTr := mustNewTLS(t, badCfg)
	ctx := testutil.Ctx(t, 2*time.Second)
	c, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
	if err == nil {
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		_, err = c.Write([]byte("x"))
		c.Close()
	}
	var certErr *tls.CertificateVerificationError
	require.ErrorAs(t, err, &certErr, "untrusted server cert was not rejected")
	<-done
}

// TestTLS_NilConfig_Rejected asserts the constructor rejects a nil
// tls.Config rather than returning a half-built transport that would
// fail later at Dial/Listen time.
func TestTLS_NilConfig_Rejected(t *testing.T) {
	t.Parallel()
	tr, err := NewTLSTransport(nil)
	require.Error(t, err, "NewTLSTransport(nil) succeeded, want error")
	assert.Nil(t, tr, "NewTLSTransport(nil) returned non-nil transport")
}

// TestTLS_ManyConcurrentDials sanity-checks that several clients can
// complete the handshake against one listener without deadlock.
func TestTLS_ManyConcurrentDials(t *testing.T) {
	t.Parallel()
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
			// Handshakes run per connection, off the accept path.
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				hctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				assert.NoError(t, tlsOf(c).HandshakeContext(hctx), "server handshake")
			}()
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
			assert.NoError(t, tlsOf(c).HandshakeContext(ctx), "client handshake")
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
	t.Parallel()
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
	ctx := testutil.Ctx(t, 5*time.Second)
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
	handshakePair(ctx, t, dial, r.c.(Conn))

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
	t.Parallel()
	t.Run("zero-defaults-to-1.2", func(t *testing.T) {
		t.Parallel()
		cfg := serverTLSConfig()
		cfg.MinVersion = 0
		tr := mustNewTLS(t, cfg)
		require.Equal(t, uint16(tls.VersionTLS12), tr.Config.MinVersion)
	})
	t.Run("below-1.2-rejected", func(t *testing.T) {
		t.Parallel()
		for _, v := range []uint16{tls.VersionTLS10, tls.VersionTLS11} {
			cfg := serverTLSConfig()
			cfg.MinVersion = v
			tr, err := NewTLSTransport(cfg)
			require.Error(t, err, "MinVersion %#04x accepted", v)
			assert.Nil(t, tr)
		}
	})
	t.Run("1.3-preserved", func(t *testing.T) {
		t.Parallel()
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
	t.Parallel()
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr
	defer l.Close()

	// First Accept: cancelled with no peer in sight.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err := l.Accept(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "first Accept")

	// Second Accept on the same listener must still work end-to-end.
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(l.Endpoint(), "tls+tcp://"))
	acceptCtx := testutil.Ctx(t, 5*time.Second)
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

// TestTLS_SilentClientsDoNotBlockAccept asserts that peers which connect at
// the TCP level but never speak TLS cannot stall Accept: the handshake is
// deferred to the connection's first I/O (bounded there by the session's
// handshake deadline), so Accept hands back every connection immediately
// and a legitimate client queued behind N silent ones is served at once.
func TestTLS_SilentClientsDoNotBlockAccept(t *testing.T) {
	t.Parallel()
	_, l := listenTLSLoopback(t, serverTLSConfig())
	defer l.Close()
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(l.Endpoint(), "tls+tcp://"))

	const silentN = 4
	for i := 0; i < silentN; i++ {
		silent, err := net.Dial("tcp", "127.0.0.1:"+portStr)
		require.NoError(t, err, "silent raw dial")
		defer silent.Close()
	}
	// Let the silent connections land in the accept queue first.
	time.Sleep(50 * time.Millisecond)

	cliErr := make(chan error, 1)
	go func() {
		cliTr := mustNewTLS(t, clientTLSConfig())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		dial, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
		if err != nil {
			cliErr <- err
			return
		}
		defer dial.Close()
		_ = dial.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := dial.Write([]byte("hello")); err != nil {
			cliErr <- err
			return
		}
		_, err = io.ReadFull(dial, make([]byte, 5))
		cliErr <- err
	}()

	// Every Accept must return promptly: silentN silent conns, then ours.
	// Serve each on its own goroutine, as the socket layer does.
	actx := testutil.Ctx(t, time.Second)
	var conns []Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i <= silentN; i++ {
		c, err := l.Accept(actx)
		require.NoError(t, err, "Accept %d stalled behind silent clients", i)
		conns = append(conns, c)
		go func(c Conn) {
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			buf := make([]byte, 5)
			if _, err := io.ReadFull(c, buf); err == nil {
				_, _ = c.Write(buf)
			}
		}(c)
	}
	require.NoError(t, testutil.RequireRecv(t, cliErr, 5*time.Second, "legitimate client not served"),
		"legitimate client round trip")
}

// TestTLS_DialDoesNotBlockOnSilentServer: a server that accepts TCP but never
// speaks TLS must not park Dial. The reconnector dials with its long-lived
// ctx, so a Dial-time handshake would hang it indefinitely; instead Dial
// returns at once and the handshake is bounded by the conn deadline.
func TestTLS_DialDoesNotBlockOnSilentServer(t *testing.T) {
	t.Parallel()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer raw.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		defer close(accepted)
		if c, err := raw.Accept(); err == nil {
			accepted <- c
		}
	}()
	defer func() {
		if c := <-accepted; c != nil {
			c.Close()
		}
	}()
	_, portStr, _ := net.SplitHostPort(raw.Addr().String())

	cliTr := mustNewTLS(t, clientTLSConfig())
	ctx := testutil.Ctx(t, time.Second)
	start := time.Now()
	c, err := cliTr.Dial(ctx, "tls+tcp://localhost:"+portStr)
	require.NoError(t, err, "Dial blocked on the TLS handshake")
	defer c.Close()
	require.Less(t, time.Since(start), 500*time.Millisecond, "Dial blocked on the TLS handshake")

	// The deferred handshake honours the conn deadline, which is how the
	// session layer's handshake timeout bounds it.
	require.NoError(t, c.SetDeadline(time.Now().Add(100*time.Millisecond)))
	_, err = c.Write([]byte("x"))
	var ne net.Error
	require.ErrorAs(t, err, &ne)
	require.True(t, ne.Timeout(), "want timeout, got %v", err)
}

// TestTLS_AcceptGoroutineExitsOnCtxCancel asserts that canceling the
// Accept ctx drains the backing net.Listener.Accept goroutine within a
// tight deadline. Before the fix, ctx cancel returned to the caller but
// the accept goroutine stayed blocked in l.l.Accept() forever (no
// deadline was set on the underlying listener). goleak in TestMain now
// catches any residual Accept goroutine; this test also explicitly
// verifies Close is idempotent.
func TestTLS_AcceptGoroutineExitsOnCtxCancel(t *testing.T) {
	t.Parallel()
	srvTr, l := listenTLSLoopback(t, serverTLSConfig())
	_ = srvTr

	ctx, cancel := context.WithCancel(context.Background())
	errCh := testutil.Async(func() error {
		_, err := l.Accept(ctx)
		return err
	})

	// Give the accept goroutine time to park in l.l.Accept().
	time.Sleep(100 * time.Millisecond)
	cancel()

	require.ErrorIs(t, testutil.RequireRecv(t, errCh, 500*time.Millisecond,
		"Accept did not return within 500ms of ctx cancel — backing goroutine likely leaked"), context.Canceled)

	// Close is idempotent: calling it again after the ctx-cancel path
	// already closed the underlying listener must be a no-op rather
	// than a panic or a double-close error surfacing to the caller.
	require.NoError(t, l.Close(), "first Close")
	require.NoError(t, l.Close(), "second Close (idempotency check)")
	// goleak in TestMain will catch any stray Accept goroutine at
	// package-test teardown; this test's own return is the trigger.
}

// tlsOf returns the *tls.Conn under a Conn the TLS transport produced.
func tlsOf(c Conn) *tls.Conn { return c.(*conn).Conn.(*tls.Conn) }
