package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
)

// tlsScheme is the URL scheme this transport claims. We
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
// Concurrency: NewTLSTransport clones the caller's *tls.Config once and
// uses the clone for every Dial/Accept, so later mutations of the original
// do not affect the transport or race with a handshake.
type TLSTransport struct {
	// Config is the clone of the config passed to NewTLSTransport; it is
	// owned by the transport and must not be mutated after construction.
	Config *tls.Config

	// base carries the TCP-level knobs and the dial/listen wiring.
	base *TCPTransport
}

// NewTLSTransport returns a TLSTransport configured with a clone of cfg.
// cfg is required; nil is an error rather than a transport whose
// Dial/Listen would fail later.
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
	addr, err := parseTCPEndpoint(endpoint, tlsScheme, true)
	if err != nil {
		return nil, err
	}
	raw, err := t.base.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	// Without a ServerName the dial host is used for SAN validation; that
	// is per dial, so it goes on a clone rather than the shared Config.
	cfg := t.Config
	if cfg.ServerName == "" {
		cfg = cfg.Clone()
		if host, _, err := net.SplitHostPort(addr); err == nil {
			cfg.ServerName = host
		}
	}
	return t.base.wrap(tls.Client(raw, cfg), tlsScheme), nil
}

// Listen binds endpoint on TCP. Accept wraps each connection with
// tls.Server without handshaking; see TLSTransport.
func (t *TLSTransport) Listen(ctx context.Context, endpoint string) (Listener, error) {
	addr, err := parseTCPEndpoint(endpoint, tlsScheme, false)
	if err != nil {
		return nil, err
	}
	raw, err := t.base.listen(ctx, addr)
	if err != nil {
		return nil, err
	}
	return newNetListener(raw, formatTCPEndpoint(tlsScheme, raw.Addr()), func(c net.Conn) Conn {
		return t.base.wrap(tls.Server(c, t.Config), tlsScheme)
	}), nil
}

// Compile-time interface checks.
var _ Transport = (*TLSTransport)(nil)
