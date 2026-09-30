package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
)

// tlsScheme is the URL scheme this transport claims. Per design §7.4 we
// spell it "tls+tcp" so the presence of TLS is explicit at the endpoint
// level, and the base-transport ("tcp") remains discoverable.
const tlsScheme = "tls+tcp"

// TLSTransport wraps a base TCPTransport with crypto/tls. Dial wraps the
// TCP connection with tls.Client and Accept wraps each accepted one with
// tls.Server. Neither runs the TLS handshake: it runs on the connection's
// first Read or Write, which in a gozmq socket is the ZMTP handshake. That
// keeps it per-connection (a silent or garbage peer cannot stall the accept
// loop or park a reconnector) and bounds it by the socket's
// WithHandshakeTimeout, which the session applies via Conn.SetDeadline.
// Direct users of Dial/Accept should likewise set a deadline before the
// first I/O. Users supply a *tls.Config at construction time — we do not
// synthesize one.
//
// Scheme: "tls+tcp". Endpoints look like "tls+tcp://host:port"; wildcards
// and ephemeral-port semantics follow TCP.
//
// Registration: a new gozmq.Context does NOT preregister
// a TLSTransport — there is no reasonable default tls.Config.
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

// Scheme reports "tls+tcp".
func (t *TLSTransport) Scheme() string { return tlsScheme }

// Dial connects to endpoint over TCP (honoring ctx) and returns a TLS client
// conn whose handshake runs on first I/O; see TLSTransport.
func (t *TLSTransport) Dial(ctx context.Context, endpoint string) (Conn, error) {
	addr, err := t.parseDialAddress(endpoint)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{KeepAlive: t.base.keepAliveSetting()}
	raw, err := d.DialContext(ctx, t.base.Network, addr)
	if err != nil {
		return nil, err
	}
	t.base.applySocketOptions(raw)
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
	return newTLSConn(tls.Client(raw, cfg)), nil
}

// Listen binds endpoint on TCP. Accept wraps each connection with
// tls.Server without handshaking; see TLSTransport.
func (t *TLSTransport) Listen(ctx context.Context, endpoint string) (Listener, error) {
	addr, err := t.parseListenAddress(endpoint)
	if err != nil {
		return nil, err
	}
	lc := net.ListenConfig{KeepAlive: t.base.keepAliveSetting()}
	raw, err := lc.Listen(ctx, t.base.Network, addr)
	if err != nil {
		return nil, err
	}
	return newNetListener(raw, formatTCPEndpoint(tlsScheme, raw.Addr()), func(c net.Conn) Conn {
		t.base.applySocketOptions(c)
		// t.Config is a construction-time clone; tls.Server does not mutate it.
		return newTLSConn(tls.Server(c, t.Config))
	}), nil
}

func (t *TLSTransport) parseDialAddress(endpoint string) (string, error) {
	return parseTCPEndpoint(endpoint, tlsScheme, true)
}

func (t *TLSTransport) parseListenAddress(endpoint string) (string, error) {
	return parseTCPEndpoint(endpoint, tlsScheme, false)
}

// --------------------------------------------------------------------------
// tlsConn — Conn over a *tls.Conn.
// --------------------------------------------------------------------------

// tlsConn reports its endpoints as tls+tcp://… rather than the raw tcp
// form of the underlying connection. Everything else delegates to the
// embedded *tls.Conn — including SetDeadline, which sets the deadline on the
// underlying TCP conn and therefore also bounds the lazy handshake.
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

// Compile-time interface checks.
var (
	_ Transport = (*TLSTransport)(nil)
	_ Conn      = (*tlsConn)(nil)
)
