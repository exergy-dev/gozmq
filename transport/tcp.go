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
	c, _, err := t.dialRaw(ctx, endpoint, t.scheme)
	if err != nil {
		return nil, err
	}
	return newConn(c, t.scheme), nil
}

// dialRaw dials a scheme:// endpoint with the ctx-aware net.Dialer and the
// socket options applied, returning the connection and the host:port it
// dialed. TLSTransport wraps the result.
func (t *TCPTransport) dialRaw(ctx context.Context, endpoint, scheme string) (net.Conn, string, error) {
	addr, err := parseTCPEndpoint(endpoint, scheme, true)
	if err != nil {
		return nil, "", err
	}
	d := net.Dialer{KeepAlive: t.keepAliveSetting()}
	c, err := d.DialContext(ctx, t.Network, addr)
	if err != nil {
		return nil, "", err
	}
	t.applySocketOptions(c)
	return c, addr, nil
}

// listenRaw binds a scheme:// endpoint with the ctx-aware net.ListenConfig.
// wrap adapts each accepted connection, after the socket options are
// applied to it; TLSTransport's wrap runs tls.Server.
func (t *TCPTransport) listenRaw(ctx context.Context, endpoint, scheme string, wrap func(net.Conn) net.Conn) (Listener, error) {
	addr, err := parseTCPEndpoint(endpoint, scheme, false)
	if err != nil {
		return nil, err
	}
	lc := net.ListenConfig{KeepAlive: t.keepAliveSetting()}
	l, err := lc.Listen(ctx, t.Network, addr)
	if err != nil {
		return nil, err
	}
	return newNetListener(l, formatTCPEndpoint(scheme, l.Addr()), func(c net.Conn) Conn {
		t.applySocketOptions(c)
		return newConn(wrap(c), scheme)
	}), nil
}

// Listen binds endpoint. For "tcp://*:port" the wildcard expands to
// 0.0.0.0 (or :: for tcp6). Endpoint() on the returned Listener reports the
// resolved address — important for ephemeral-port (port 0 or "*")
// bindings. ctx bounds host-name resolution.
func (t *TCPTransport) Listen(ctx context.Context, endpoint string) (Listener, error) {
	return t.listenRaw(ctx, endpoint, t.scheme, func(c net.Conn) net.Conn { return c })
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

// conn adapts a net.Conn (TCP, Unix or TLS) to Conn with fixed endpoint
// strings. WriteBuffers is writev on a TCP or Unix conn; any other conn
// (TLS) gets the batch as one Write, since net.Buffers.WriteTo would issue
// one Write per buffer and a *tls.Conn turns each into its own record and
// syscall.
type conn struct {
	net.Conn
	local, remote string
	vectored      bool   // the net.Conn is a *net.TCPConn or *net.UnixConn
	wbuf          []byte // coalescing buffer when !vectored, reused
}

// newConn adapts c, a TCP or TLS connection, with scheme:// endpoints.
func newConn(c net.Conn, scheme string) *conn {
	return &conn{
		Conn:     c,
		local:    formatTCPEndpoint(scheme, c.LocalAddr()),
		remote:   formatTCPEndpoint(scheme, c.RemoteAddr()),
		vectored: isVectored(c),
	}
}

func isVectored(c net.Conn) bool {
	switch c.(type) {
	case *net.TCPConn, *net.UnixConn:
		return true
	}
	return false
}

func (c *conn) LocalEndpoint() string  { return c.local }
func (c *conn) RemoteEndpoint() string { return c.remote }

// maxRetainedWriteBuf bounds the coalescing buffer kept between writes.
const maxRetainedWriteBuf = 1 << 20

func (c *conn) WriteBuffers(buffers *net.Buffers) (int64, error) {
	if len(*buffers) == 1 {
		// One buffer (a fully coalesced batch) is a plain write(2): no
		// iovec setup for a vectored conn, no wbuf copy for TLS. Consume
		// the slice header as WriteTo would.
		b := (*buffers)[0]
		*buffers = (*buffers)[1:]
		n, err := c.Write(b)
		return int64(n), err
	}
	if c.vectored {
		return buffers.WriteTo(c.Conn)
	}
	c.wbuf = c.wbuf[:0]
	for _, b := range *buffers {
		c.wbuf = append(c.wbuf, b...)
	}
	n, err := c.Write(c.wbuf)
	*buffers = (*buffers)[len(*buffers):] // consumed, as WriteTo would
	if cap(c.wbuf) > maxRetainedWriteBuf {
		c.wbuf = nil
	}
	return int64(n), err
}

func formatTCPEndpoint(scheme string, a net.Addr) string {
	if a == nil {
		return ""
	}
	return scheme + "://" + a.String()
}
