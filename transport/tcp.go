package transport

import (
	"context"
	"net"
	"time"
)

// Defaults. Higher layers override them by constructing a TCPTransport,
// setting the exported fields and registering it on a Context.
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
	addr, err := parseTCPEndpoint(endpoint, t.scheme, true)
	if err != nil {
		return nil, err
	}
	c, err := t.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return t.wrap(c, t.scheme), nil
}

// dial and listen are the ctx-aware net.Dialer / net.ListenConfig wiring,
// shared with TLSTransport.
func (t *TCPTransport) dial(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{KeepAlive: t.keepAliveSetting()}
	return d.DialContext(ctx, t.Network, addr)
}

func (t *TCPTransport) listen(ctx context.Context, addr string) (net.Listener, error) {
	lc := net.ListenConfig{KeepAlive: t.keepAliveSetting()}
	return lc.Listen(ctx, t.Network, addr)
}

// wrap applies the socket options and adapts c to Conn with scheme://
// endpoint strings.
func (t *TCPTransport) wrap(c net.Conn, scheme string) *conn {
	t.applySocketOptions(c)
	return &conn{Conn: c, scheme: scheme}
}

// Listen binds endpoint. For "tcp://*:port" the wildcard expands to
// 0.0.0.0 (or :: for tcp6). Endpoint() on the returned Listener reports the
// resolved address — important for ephemeral-port (port 0 or "*")
// bindings. ctx bounds host-name resolution.
func (t *TCPTransport) Listen(ctx context.Context, endpoint string) (Listener, error) {
	addr, err := parseTCPEndpoint(endpoint, t.scheme, false)
	if err != nil {
		return nil, err
	}
	l, err := t.listen(ctx, addr)
	if err != nil {
		return nil, err
	}
	return newNetListener(l, formatTCPEndpoint(t.scheme, l.Addr()), func(c net.Conn) Conn { return t.wrap(c, t.scheme) }), nil
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

// conn adapts a net.Conn (TCP, Unix or TLS) to Conn. IPC fixes the
// endpoint strings at construction; TCP and TLS format them from the
// addresses on demand (they are rarely asked for, so this costs nothing
// per connection). WriteBuffers is writev where the underlying conn
// supports it.
type conn struct {
	net.Conn
	scheme        string // TCP/TLS: formats the endpoints from the addresses
	local, remote string // IPC: fixed endpoint strings
}

func (c *conn) LocalEndpoint() string {
	if c.scheme != "" {
		return formatTCPEndpoint(c.scheme, c.LocalAddr())
	}
	return c.local
}

func (c *conn) RemoteEndpoint() string {
	if c.scheme != "" {
		return formatTCPEndpoint(c.scheme, c.RemoteAddr())
	}
	return c.remote
}

func (c *conn) WriteBuffers(buffers *net.Buffers) (int64, error) { return buffers.WriteTo(c.Conn) }

func formatTCPEndpoint(scheme string, a net.Addr) string {
	if a == nil {
		return ""
	}
	return scheme + "://" + a.String()
}
