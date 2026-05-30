package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/rkm/gozmq/internal/iface"
)

// tlsScheme is the URL scheme this transport claims. Per design §7.4 we
// spell it "tls+tcp" so the presence of TLS is explicit at the endpoint
// level, and the base-transport ("tcp") remains discoverable.
const tlsScheme = "tls+tcp"

// TLSTransport wraps a base TCPTransport with crypto/tls. Dial runs the
// TLS client handshake after the underlying TCP connect; Listen wraps the
// underlying net.Listener with tls.NewListener so every Accept returns an
// already-wrapped *tls.Conn. Users supply a *tls.Config at construction
// time — we do not synthesize one.
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
func NewTLSTransport(cfg *tls.Config) (*TLSTransport, error) {
	if cfg == nil {
		return nil, fmt.Errorf("tls: config required")
	}
	return &TLSTransport{
		Config: cfg.Clone(),
		base:   NewTCPTransport(),
	}, nil
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

// Listen binds endpoint on TCP and wraps the resulting listener with
// tls.NewListener so Accept yields *tls.Conn values that have completed
// the server-side handshake on first I/O (Go's tls.Conn runs the
// handshake lazily; our tlsListener forces it eagerly inside Accept so
// Accept never hands back an unhandshaken connection).
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
// client cert fails here rather than at first Read. Accept is ctx-aware:
// ctx cancel closes the underlying listener, causing the blocked
// Accept goroutine to return with net.ErrClosed and exit immediately.
type tlsListener struct {
	l        net.Listener
	t        *TLSTransport
	endpoint string

	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newTLSListener(l net.Listener, t *TLSTransport) *tlsListener {
	return &tlsListener{
		l:        l,
		t:        t,
		endpoint: formatTCPEndpoint(tlsScheme, l.Addr()),
		closeCh:  make(chan struct{}),
	}
}

func (l *tlsListener) Endpoint() string { return l.endpoint }

func (l *tlsListener) Accept(ctx context.Context) (iface.Conn, error) {
	if l.closed.Load() {
		return nil, listenerClosedErr()
	}

	type acceptResult struct {
		c   net.Conn
		err error
	}
	resCh := make(chan acceptResult, 1)
	// Accept goroutine: a bare l.l.Accept() blocks until either a peer
	// arrives or the underlying listener is closed. Closing the listener
	// (via closeListener below) unblocks the call with net.ErrClosed, which
	// we detect to exit cleanly. This avoids the earlier poll-with-deadline
	// pattern that could linger for up to tcpAcceptPollInterval after ctx
	// cancel.
	go func() {
		c, err := l.l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || l.closed.Load() {
				resCh <- acceptResult{nil, listenerClosedErr()}
				return
			}
			resCh <- acceptResult{nil, err}
			return
		}
		if err := l.t.base.applySocketOptions(c); err != nil {
			_ = c.Close()
			resCh <- acceptResult{nil, err}
			return
		}
		resCh <- acceptResult{c, nil}
	}()

	select {
	case r := <-resCh:
		if r.err != nil {
			return nil, r.err
		}
		// Run the server-side handshake inside Accept so a failure is
		// reported here, not at first Read. Bound by ctx for timely
		// cancellation and ctx-aware peer verification. t.Config is a
		// construction-time clone so handing the pointer directly is safe;
		// tls.Server does not mutate it.
		tc := tls.Server(r.c, l.t.Config)
		if err := tc.HandshakeContext(ctx); err != nil {
			_ = tc.Close()
			// Distinguish "ctx tripped" from "bad cert" so callers can
			// react appropriately. errors.Is on context.Canceled works
			// because HandshakeContext surfaces ctx errors verbatim.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, ctx.Err()
			}
			return nil, err
		}
		return newTLSConn(tc), nil
	case <-ctx.Done():
		// Ctx cancel: close the underlying listener so the blocked
		// Accept goroutine returns with net.ErrClosed and exits rather
		// than lingering past our return. Close is idempotent via
		// closeOnce so a caller Close on the same listener is still
		// safe. The tradeoff: a ctx-cancelled Accept leaves the listener
		// unusable for further Accepts. Callers who want a re-usable
		// listener should scope ctx to the listener's lifetime, not to a
		// single Accept attempt.
		l.closeListener()
		go func() {
			r := <-resCh
			if r.c != nil {
				_ = r.c.Close()
			}
		}()
		return nil, ctx.Err()
	case <-l.closeCh:
		go func() {
			r := <-resCh
			if r.c != nil {
				_ = r.c.Close()
			}
		}()
		return nil, listenerClosedErr()
	}
}

// closeListener closes the underlying net.Listener exactly once. Used by
// both Close (explicit) and Accept (on ctx cancel) to unblock a parked
// l.l.Accept call. Safe to call any number of times; net.Listener.Close
// returning a non-nil error on the second call is swallowed via sync.Once.
func (l *tlsListener) closeListener() {
	l.closeOnce.Do(func() {
		l.closed.Store(true)
		close(l.closeCh)
		// net.Listener.Close per the Go docs unblocks any concurrent
		// Accept calls. We intentionally discard the error: the only
		// interesting failure here is "already closed" which sync.Once
		// already prevents, and any other error is not actionable at
		// this point (the listener is torn down regardless).
		_ = l.l.Close()
	})
}

func (l *tlsListener) Close() error {
	l.closeListener()
	return nil
}

// Compile-time interface checks.
var (
	_ iface.Transport = (*TLSTransport)(nil)
	_ iface.Listener  = (*tlsListener)(nil)
	_ iface.Conn      = (*tlsConn)(nil)
)
