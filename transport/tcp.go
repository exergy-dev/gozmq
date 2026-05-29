package transport

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rkm/gozmq/internal/iface"
)

// Defaults that match design §7.1 and the socketConfig defaults in
// options.go. Higher layers may override on a per-Transport basis by
// constructing TCPTransport directly and toggling the exported fields
// before installing into a Registry.
const (
	defaultTCPKeepAlive       = true
	defaultTCPKeepAliveIdle   = 30 * time.Second
	defaultTCPNoDelay         = true
	tcpAcceptPollInterval     = 5 * time.Millisecond
	tcpDialContextDefaultWait = 0 // unlimited; ctx supplies the deadline
	defaultTCPReadBuffer      = 8 << 20
	defaultTCPWriteBuffer     = 8 << 20
)

// TCPTransport implements iface.Transport over the standard library's
// net.Dial / net.Listen. The exported fields are configuration knobs the
// higher layers may flip before installing the transport into a Registry;
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
func (t *TCPTransport) Dial(ctx context.Context, endpoint string) (iface.Conn, error) {
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
	if err := t.applySocketOptions(c); err != nil {
		_ = c.Close()
		return nil, err
	}
	return newTCPConn(c, t.scheme), nil
}

// Listen binds endpoint. For "tcp://*:port" the wildcard expands to
// 0.0.0.0 (or :: for tcp6). Endpoint() on the returned Listener reports the
// resolved address — important for ephemeral-port (port 0) bindings.
func (t *TCPTransport) Listen(_ context.Context, endpoint string) (iface.Listener, error) {
	addr, err := t.parseListenAddress(endpoint)
	if err != nil {
		return nil, err
	}
	lc := net.ListenConfig{
		KeepAlive: t.keepAliveSetting(),
	}
	// Listen does not strictly need a Context, but the iface lets callers
	// pass one; honoring it via ListenConfig.Listen keeps the door open if
	// we add resolver hooks later.
	l, err := lc.Listen(context.Background(), t.Network, addr)
	if err != nil {
		return nil, err
	}
	return newTCPListener(l, t), nil
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

// applySocketOptions tweaks per-connection settings the dialer does not
// surface (TCP_NODELAY).
func (t *TCPTransport) applySocketOptions(c net.Conn) error {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return nil
	}
	if err := tc.SetNoDelay(t.NoDelay); err != nil {
		return err
	}
	if err := tc.SetReadBuffer(defaultTCPReadBuffer); err != nil {
		return err
	}
	if err := tc.SetWriteBuffer(defaultTCPWriteBuffer); err != nil {
		return err
	}
	if t.KeepAlive {
		if err := tc.SetKeepAlive(true); err != nil {
			return err
		}
		if t.KeepAlivePeriod > 0 {
			_ = tc.SetKeepAlivePeriod(t.KeepAlivePeriod)
		}
	}
	return nil
}

func (t *TCPTransport) parseAddress(endpoint string) (string, error) {
	return parseTCPEndpoint(endpoint, t.scheme, true)
}

func (t *TCPTransport) parseListenAddress(endpoint string) (string, error) {
	return parseTCPEndpoint(endpoint, t.scheme, false)
}

// --------------------------------------------------------------------------
// tcpConn — iface.Conn adapter over net.Conn.
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

// SetLinger forwards to the underlying *net.TCPConn so the session layer
// can arrange a graceful close that waits for the kernel send buffer to
// flush (avoids losing last-moment data when the process exits shortly
// after Close). Returns nil silently for non-TCP connections.
func (c *tcpConn) SetLinger(sec int) error {
	if tc, ok := c.Conn.(*net.TCPConn); ok {
		return tc.SetLinger(sec)
	}
	return nil
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

// --------------------------------------------------------------------------
// tcpListener — iface.Listener adapter over net.Listener with ctx-aware
// Accept and Close-wakes-Accept semantics.
// --------------------------------------------------------------------------

// tcpListener implements iface.Listener over a net.Listener. Accept honors
// ctx via a poll-with-deadline pattern: we set a short read deadline on the
// underlying listener (when supported) so that a concurrent Close or a ctx
// cancellation trips Accept within bounded latency. On Close, in-flight
// Accepts return a wrapped ErrEndpointInvalid (i.e. "this endpoint is no
// longer accepting") rather than blocking indefinitely.
type tcpListener struct {
	l        net.Listener
	t        *TCPTransport
	endpoint string

	closed atomic.Bool
	// closeCh fires when Close runs; pollers listen for it.
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newTCPListener(l net.Listener, t *TCPTransport) *tcpListener {
	return &tcpListener{
		l:        l,
		t:        t,
		endpoint: formatTCPEndpoint(t.scheme, l.Addr()),
		closeCh:  make(chan struct{}),
	}
}

func (l *tcpListener) Endpoint() string { return l.endpoint }

func (l *tcpListener) Accept(ctx context.Context) (iface.Conn, error) {
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
			if err := l.t.applySocketOptions(c); err != nil {
				return nil, err
			}
			return c, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return newTCPConn(c, l.t.scheme), nil
}

func (l *tcpListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		l.closed.Store(true)
		close(l.closeCh)
		err = l.l.Close()
	})
	return err
}
