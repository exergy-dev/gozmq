package transport

import (
	"context"
	"net"
	"time"
)

// Defaults that match design §7.1 and the socketConfig defaults in
// options.go. Higher layers may override on a per-Transport basis by
// constructing TCPTransport directly and toggling the exported fields
// before registering it on a Context.
const (
	defaultTCPKeepAlive     = true
	defaultTCPKeepAliveIdle = 30 * time.Second
	defaultTCPNoDelay       = true
)

// TCPTransport implements Transport over the standard library's
// net.Dial / net.Listen. The exported fields are configuration knobs the
// higher layers may flip before registering the transport on a Context;
// they take effect for every subsequent Dial / Accept on this transport.
//
// Concurrency: Dial / Listen are safe for concurrent use; the configuration
// fields should only be mutated before the transport is registered.
type TCPTransport struct {
	// Network is the net.Dial network ("tcp", "tcp4", "tcp6"). Defaults to
	// "tcp" which selects v4 or v6 per the OS resolver.
	Network string
	// scheme is the URL scheme ("tcp" or "tcp6") this transport claims.
	scheme string

	// KeepAlive toggles SO_KEEPALIVE; KeepAlivePeriod sets the idle period.
	KeepAlive       bool
	KeepAlivePeriod time.Duration
	// NoDelay toggles TCP_NODELAY (Nagle disable).
	NoDelay bool
	// ReadBufferSize / WriteBufferSize set SO_RCVBUF / SO_SNDBUF on every
	// dialed and accepted connection. Zero (the default) leaves the OS
	// sizing alone; on Linux, setting either disables the kernel's buffer
	// autotuning for that direction, so only set them to pin a size.
	ReadBufferSize  int
	WriteBufferSize int
}

// newTCP builds a TCPTransport for the given network ("tcp" or "tcp6") with
// the design's defaults (keepalive on, nodelay on). The scheme equals the
// network for both variants.
func newTCP(network string) *TCPTransport {
	return &TCPTransport{
		Network:         network,
		scheme:          network,
		KeepAlive:       defaultTCPKeepAlive,
		KeepAlivePeriod: defaultTCPKeepAliveIdle,
		NoDelay:         defaultTCPNoDelay,
	}
}

// NewTCPTransport returns a TCPTransport that selects IPv4 or IPv6 per the OS
// resolver ("tcp").
func NewTCPTransport() *TCPTransport { return newTCP("tcp") }

// NewTCP6Transport returns a TCPTransport that forces IPv6 ("tcp6").
func NewTCP6Transport() *TCPTransport { return newTCP("tcp6") }

// Scheme reports "tcp" or "tcp6".
func (t *TCPTransport) Scheme() string { return t.scheme }

// Dial honors ctx via net.Dialer.DialContext.
func (t *TCPTransport) Dial(ctx context.Context, endpoint string) (Conn, error) {
	addr, err := t.parseAddress(endpoint)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{
		KeepAlive: t.keepAliveSetting(),
	}
	c, err := d.DialContext(ctx, t.Network, addr)
	if err != nil {
		return nil, err
	}
	t.applySocketOptions(c)
	return newTCPConn(c, t.scheme), nil
}

// Listen binds endpoint. For "tcp://*:port" the wildcard expands to
// 0.0.0.0 (or :: for tcp6). Endpoint() on the returned Listener reports the
// resolved address — important for ephemeral-port (port 0 or "*")
// bindings. ctx bounds host-name resolution.
func (t *TCPTransport) Listen(ctx context.Context, endpoint string) (Listener, error) {
	addr, err := t.parseListenAddress(endpoint)
	if err != nil {
		return nil, err
	}
	lc := net.ListenConfig{
		KeepAlive: t.keepAliveSetting(),
	}
	l, err := lc.Listen(ctx, t.Network, addr)
	if err != nil {
		return nil, err
	}
	return newNetListener(l, formatTCPEndpoint(t.scheme, l.Addr()), func(c net.Conn) Conn {
		t.applySocketOptions(c)
		return newTCPConn(c, t.scheme)
	}), nil
}

func (t *TCPTransport) keepAliveSetting() time.Duration {
	if !t.KeepAlive {
		return -1 // net.Dialer / net.ListenConfig: negative disables
	}
	if t.KeepAlivePeriod <= 0 {
		return defaultTCPKeepAliveIdle
	}
	return t.KeepAlivePeriod
}

// applySocketOptions sets per-connection knobs the Dialer/ListenConfig do
// not surface (TCP_NODELAY and socket buffer sizes). Keepalive is NOT set
// here: net.Dialer.KeepAlive (Dial) and net.ListenConfig.KeepAlive (Accept)
// — both fed by keepAliveSetting() — already enable SO_KEEPALIVE and set the
// idle period on dialed and accepted connections alike.
//
// Best-effort: these are tuning knobs, and a connection that could not take
// one (e.g. the peer already reset it) is still usable or will fail on its
// own. Failing Dial/Accept over them would turn a tuning miss into an
// outage.
func (t *TCPTransport) applySocketOptions(c net.Conn) {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tc.SetNoDelay(t.NoDelay)
	if t.ReadBufferSize > 0 {
		_ = tc.SetReadBuffer(t.ReadBufferSize)
	}
	if t.WriteBufferSize > 0 {
		_ = tc.SetWriteBuffer(t.WriteBufferSize)
	}
}

func (t *TCPTransport) parseAddress(endpoint string) (string, error) {
	return parseTCPEndpoint(endpoint, t.scheme, true)
}

func (t *TCPTransport) parseListenAddress(endpoint string) (string, error) {
	return parseTCPEndpoint(endpoint, t.scheme, false)
}

// --------------------------------------------------------------------------
// tcpConn — Conn adapter over net.Conn.
// --------------------------------------------------------------------------

// tcpConn wraps a *net.TCPConn (or any net.Conn) and exposes ZMQ-style
// endpoint strings via LocalEndpoint / RemoteEndpoint.
type tcpConn struct {
	net.Conn
	scheme string
}

func newTCPConn(c net.Conn, scheme string) *tcpConn {
	return &tcpConn{Conn: c, scheme: scheme}
}

func (c *tcpConn) LocalEndpoint() string {
	return formatTCPEndpoint(c.scheme, c.LocalAddr())
}

func (c *tcpConn) RemoteEndpoint() string {
	return formatTCPEndpoint(c.scheme, c.RemoteAddr())
}

func (c *tcpConn) WriteBuffers(buffers *net.Buffers) (int64, error) {
	return buffers.WriteTo(c.Conn)
}

func formatTCPEndpoint(scheme string, a net.Addr) string {
	if a == nil {
		return ""
	}
	return scheme + "://" + a.String()
}
