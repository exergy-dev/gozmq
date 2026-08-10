package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/exergy-dev/gozmq/internal/iface"
)

// defaultTLSHandshakeTimeout bounds the server-side TLS handshake performed
// inside Accept. A client that connects at the TCP level but never speaks
// TLS would otherwise pin an Accept call indefinitely.
const defaultTLSHandshakeTimeout = 10 * time.Second

// tlsScheme is the URL scheme this transport claims. Per design §7.4 we
// spell it "tls+tcp" so the presence of TLS is explicit at the endpoint
// level, and the base-transport ("tcp") remains discoverable.
const tlsScheme = "tls+tcp"

// TLSTransport wraps a base TCPTransport with crypto/tls. Dial runs the
// TLS client handshake after the underlying TCP connect; Listen returns a
// listener whose Accept wraps each raw conn with tls.Server and completes
// the server-side handshake before handing the conn back. Users supply a
// *tls.Config at construction time — we do not synthesize one.
//
// Scheme: "tls+tcp". Endpoints look like "tls+tcp://host:port"; wildcards
// and ephemeral-port semantics follow TCP.
//
// Registration: the default transport registry (transport.New) does NOT
// register a TLSTransport — there is no reasonable default tls.Config.
// Callers install via (*gozmq.Context).RegisterTransport(
// transport.NewTLSTransport(cfg)).
//
// Concurrency: NewTLSTransport deep-copies the caller's *tls.Config via
// Config.Clone at construction and uses the snapshot for every subsequent
// Dial/Accept. Mutations to the caller's original config after construction
// do NOT affect the transport. This rules out a data race between a caller
// mutating Certificates/MinVersion/etc. and a concurrent Dial/Accept that
// reads those fields mid-handshake.
type TLSTransport struct {
	// Config is the *tls.Config applied on Dial and Listen. It is a clone
	// of the config passed to NewTLSTransport — it is owned by the
	// transport and callers MUST NOT mutate it after construction.
	// Mutating the caller's original config has no effect because the
	// transport holds its own copy.
	Config *tls.Config

	// HandshakeTimeout caps how long Accept spends on the server-side TLS
	// handshake for a single accepted connection. Zero selects the 10s
	// default. A peer that exceeds the cap has its connection closed and
	// the Accept call returns a timeout error; the listener remains
	// usable, so one stalling client cannot wedge the endpoint.
	HandshakeTimeout time.Duration

	// base carries the TCP-level knobs (KeepAlive, NoDelay). We reuse
	// TCPTransport to pick up its ctx-aware Dial/Accept without duplicating
	// the net.Dialer / net.ListenConfig wiring.
	base *TCPTransport
}

// NewTLSTransport returns a TLSTransport configured with cfg. cfg is
// required — passing nil returns an error rather than a transport whose
// Dial/Listen would fail later.
//
// cfg is deep-copied via (*tls.Config).Clone and the copy is stored on the
// returned transport. This isolates the transport from any subsequent
// mutations to the caller's config object — concurrent reads on the
// transport's snapshot during handshake are race-free. Cloning once at
// construction (rather than lazily per Dial/Accept) avoids a data race
// between a caller mutating their original cfg and the transport's clone
// reading it.
//
// MinVersion floor: a zero cfg.MinVersion is upgraded to TLS 1.2 on the
// stored clone, and any explicit MinVersion below TLS 1.2 is rejected with
// an error — TLS 1.0/1.1 are deprecated (RFC 8996) and have no place under
// a security-oriented transport.
func NewTLSTransport(cfg *tls.Config) (*TLSTransport, error) {
	if cfg == nil {
		return nil, fmt.Errorf("tls: config required")
	}
	cfg = cfg.Clone()
	switch {
	case cfg.MinVersion == 0:
		cfg.MinVersion = tls.VersionTLS12
	case cfg.MinVersion < tls.VersionTLS12:
		return nil, fmt.Errorf("tls: MinVersion %#04x is below TLS 1.2 (deprecated by RFC 8996); refusing", cfg.MinVersion)
	}
	return &TLSTransport{
		Config: cfg,
		base:   NewTCPTransport(),
	}, nil
}

// handshakeTimeout resolves the effective per-connection handshake cap.
func (t *TLSTransport) handshakeTimeout() time.Duration {
	if t.HandshakeTimeout > 0 {
		return t.HandshakeTimeout
	}
	return defaultTLSHandshakeTimeout
}

// Scheme reports "tls+tcp".
func (t *TLSTransport) Scheme() string { return tlsScheme }

// Dial connects to endpoint over TCP and runs the TLS client handshake.
// Honors ctx via net.Dialer.DialContext for the TCP handshake and
// tls.Conn.HandshakeContext for the TLS handshake.
func (t *TLSTransport) Dial(ctx context.Context, endpoint string) (iface.Conn, error) {
	addr, err := t.parseDialAddress(endpoint)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{KeepAlive: t.base.keepAliveSetting()}
	raw, err := d.DialContext(ctx, t.base.Network, addr)
	if err != nil {
		return nil, err
	}
	if err := t.base.applySocketOptions(raw); err != nil {
		_ = raw.Close()
		return nil, err
	}
	// t.Config is a construction-time private snapshot — safe for
	// concurrent readers. But when the caller did not pre-set
	// ServerName we need to inject the dial host for SAN validation.
	// That tweak is per-dial, so we must clone-from-stored *here* to
	// avoid mutating the shared snapshot (which is concurrently read
	// by other Dials and by the Accept path's tls.Server). This is
	// the only remaining lazy clone — all other config reads go
	// straight to t.Config without copying.
	cfg := t.Config
	if cfg.ServerName == "" {
		cfg = cfg.Clone()
		if host, _, err := net.SplitHostPort(addr); err == nil {
			cfg.ServerName = host
		}
	}
	tc := tls.Client(raw, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = tc.Close()
		return nil, err
	}
	return newTLSConn(tc), nil
}

// Listen binds endpoint on TCP and wraps the resulting listener in a
// tlsListener whose Accept wraps each raw conn with tls.Server and runs
// the server-side handshake eagerly (Go's tls.Conn would otherwise run it
// lazily on first I/O), so Accept never hands back an unhandshaken
// connection.
func (t *TLSTransport) Listen(_ context.Context, endpoint string) (iface.Listener, error) {
	addr, err := t.parseListenAddress(endpoint)
	if err != nil {
		return nil, err
	}
	lc := net.ListenConfig{KeepAlive: t.base.keepAliveSetting()}
	raw, err := lc.Listen(context.Background(), t.base.Network, addr)
	if err != nil {
		return nil, err
	}
	return newTLSListener(raw, t), nil
}

func (t *TLSTransport) parseDialAddress(endpoint string) (string, error) {
	return parseTCPEndpoint(endpoint, tlsScheme, true)
}

func (t *TLSTransport) parseListenAddress(endpoint string) (string, error) {
	return parseTCPEndpoint(endpoint, tlsScheme, false)
}

// --------------------------------------------------------------------------
// tlsConn — iface.Conn over a *tls.Conn.
// --------------------------------------------------------------------------

// tlsConn reports its endpoints as tls+tcp://… rather than the raw tcp
// form of the underlying connection. Everything else delegates to the
// embedded *tls.Conn.
type tlsConn struct {
	*tls.Conn
}

func newTLSConn(c *tls.Conn) *tlsConn { return &tlsConn{Conn: c} }

func (c *tlsConn) LocalEndpoint() string {
	return formatTCPEndpoint(tlsScheme, c.LocalAddr())
}

func (c *tlsConn) RemoteEndpoint() string {
	return formatTCPEndpoint(tlsScheme, c.RemoteAddr())
}

func (c *tlsConn) WriteBuffers(buffers *net.Buffers) (int64, error) {
	return buffers.WriteTo(c.Conn)
}

// --------------------------------------------------------------------------
// tlsListener — iface.Listener that performs the TLS handshake inside
// Accept, honoring ctx/deadline and Close-wakes-Accept semantics.
// --------------------------------------------------------------------------

// tlsListener owns a raw net.Listener and wraps every accepted net.Conn
// with tls.Server; the handshake runs eagerly inside Accept so a bad
// client cert fails here rather than at first Read. Accept follows the
// tcp/ipc poll-with-deadline pattern (accept.go): ctx cancellation and
// Close both wake a parked Accept within bounded latency, and a
// ctx-cancelled Accept leaves the listener fully usable for later
// Accept calls. Each handshake is capped by the transport's
// HandshakeTimeout so a connected-but-silent peer cannot pin Accept
// beyond that bound.
type tlsListener struct {
	l        net.Listener
	t        *TLSTransport
	endpoint string
	listenerCloseState
}

func newTLSListener(l net.Listener, t *TLSTransport) *tlsListener {
	return &tlsListener{
		l:                  l,
		t:                  t,
		endpoint:           formatTCPEndpoint(tlsScheme, l.Addr()),
		listenerCloseState: newListenerCloseState(),
	}
}

func (l *tlsListener) Endpoint() string { return l.endpoint }

func (l *tlsListener) Accept(ctx context.Context) (iface.Conn, error) {
	c, err := acceptWithDeadline(
		ctx,
		l.l,
		l.closeCh,
		l.closed.Load,
		tcpAcceptPollInterval,
		func(t time.Time) error {
			if tl, ok := l.l.(*net.TCPListener); ok {
				return tl.SetDeadline(t)
			}
			return nil
		},
		func(c net.Conn) (net.Conn, error) {
			if err := l.t.base.applySocketOptions(c); err != nil {
				return nil, err
			}
			// Run the server-side handshake inside Accept so a failure is
			// reported here, not at first Read. A hard deadline on the raw
			// conn caps a connected-but-silent peer at HandshakeTimeout;
			// HandshakeContext additionally aborts promptly on ctx cancel.
			// On error acceptWithDeadline closes the raw conn for us.
			// t.Config is a construction-time clone so handing the pointer
			// directly is safe; tls.Server does not mutate it.
			if err := c.SetDeadline(time.Now().Add(l.t.handshakeTimeout())); err != nil {
				return nil, err
			}
			tc := tls.Server(c, l.t.Config)
			if err := tc.HandshakeContext(ctx); err != nil {
				return nil, err
			}
			// Clear the handshake deadline; traffic-phase deadlines are the
			// session layer's business.
			if err := tc.SetDeadline(time.Time{}); err != nil {
				return nil, err
			}
			return tc, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return newTLSConn(c.(*tls.Conn)), nil
}

func (l *tlsListener) Close() error {
	return l.closeWith(l.l.Close)
}

// Compile-time interface checks.
var (
	_ iface.Transport = (*TLSTransport)(nil)
	_ iface.Listener  = (*tlsListener)(nil)
	_ iface.Conn      = (*tlsConn)(nil)
)
