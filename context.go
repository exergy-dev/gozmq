package gozmq

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/gozmq/security"
	"github.com/exergy-dev/gozmq/transport"
)

// Context is the top-level resource owner. A Context holds the scheme to
// transport table, the inproc namespace shared by all inproc sockets on
// this Context, and the optional ZAP handler. Contexts share nothing with
// each other.
//
// The zero value is NOT usable; construct via NewContext.
type Context struct {
	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc

	trMu       sync.RWMutex
	transports map[string]transport.Transport // by Scheme()
	zapHandler security.ZAPHandler

	inproc inprocRegistry

	mu      sync.Mutex
	sockets []*Socket
}

// ContextOption configures a Context at construction.
type ContextOption interface{ applyContext(*Context) }

type contextOptionFunc func(*Context)

func (f contextOptionFunc) applyContext(c *Context) { f(c) }

// WithZAPHandler installs a context-level RFC 27 ZAP handler. It authorizes
// the peer on the server side of every PLAIN and CURVE handshake, whether that
// socket binds or connects (the PLAIN/CURVE role comes from the options, not
// the transport direction), and on the accepting side of NULL handshakes on
// sockets with a [WithZAPDomain]. It is called directly in each handshake
// goroutine; a rejection fails the handshake with an error wrapping the
// handler's own. A nil handler removes it.
func WithZAPHandler(handler security.ZAPHandler) ContextOption {
	return contextOptionFunc(func(c *Context) { c.zapHandler = handler })
}

// NewContext creates a new Context. Call Close to release resources.
func NewContext(opts ...ContextOption) *Context {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Context{
		ctx:        ctx,
		cancel:     cancel,
		transports: make(map[string]transport.Transport),
		inproc: inprocRegistry{
			bound:   make(map[string]*Socket),
			names:   make(map[*Socket][]string),
			pending: make(map[string][]*inprocLink),
		},
	}
	// TLS (tls+tcp) is deliberately not preregistered: there is no
	// reasonable default *tls.Config. inproc is built into the Context,
	// not a transport.
	for _, t := range []transport.Transport{
		transport.NewTCPTransport(),
		transport.NewTCP6Transport(),
		transport.NewIPCTransport(),
	} {
		c.transports[t.Scheme()] = t
	}
	for _, o := range opts {
		o.applyContext(c)
	}
	return c
}

// Ctx returns the Context's cancellation context. Blocking API calls that
// accept a context.Context honor this root cancellation.
func (c *Context) Ctx() context.Context { return c.ctx }

// RegisterTransport installs t in the Context's transport registry. The
// transport's Scheme() determines which endpoint prefix it handles;
// registering a transport for an already-registered scheme overwrites the
// prior entry.
//
// A Context starts with tcp, tcp6 and ipc already registered, so this is
// needed in two cases. inproc:// is not a transport: the Context joins
// inproc sockets directly, and registering a transport whose Scheme is
// "inproc" panics (a programmer error).
//
// The first is a built-in transport that needs caller-supplied
// configuration. TLS is the main one — there is no reasonable default
// *tls.Config, so tls+tcp is not preregistered:
//
//	tr, err := transport.NewTLSTransport(tlsCfg)
//	if err != nil {
//		return err
//	}
//	ctx := gozmq.NewContext()
//	ctx.RegisterTransport(tr)
//	sock, _ := ctx.NewSocket(gozmq.Router)
//	_ = sock.Bind("tls+tcp://0.0.0.0:5555")
//
// It also re-registers a built-in whose exported knobs you want to change —
// for example TCP keepalive or socket buffer sizes:
//
//	tcp := transport.NewTCPTransport()
//	tcp.KeepAlivePeriod = 10 * time.Second
//	ctx.RegisterTransport(tcp) // replaces the default tcp:// entry
//
// The second is a scheme gozmq does not ship. [transport.Transport] is a
// public interface naming only standard-library types, so an out-of-tree
// carrier (QUIC, WebSocket, a fault-injecting test harness) can be plugged
// in without forking. Everything above the transport — sessions, ZMTP
// framing, security mechanisms, socket patterns — is unaffected.
//
// Safe to call concurrently with NewSocket; existing sockets consult the
// registry at Bind / Connect time, so a socket constructed before
// RegisterTransport sees the updated scheme set as long as its Bind /
// Connect call races after Register.
func (c *Context) RegisterTransport(t transport.Transport) {
	if t.Scheme() == "inproc" {
		panic("gozmq: inproc is built into the Context and cannot be registered as a transport")
	}
	c.trMu.Lock()
	defer c.trMu.Unlock()
	c.transports[t.Scheme()] = t
}

// lookupTransport returns the transport registered for endpoint's scheme.
func (c *Context) lookupTransport(endpoint string) (transport.Transport, error) {
	scheme, _, ok := strings.Cut(endpoint, "://")
	if !ok || scheme == "" {
		return nil, fmt.Errorf("%w: %q (missing scheme://)", ErrEndpointInvalid, endpoint)
	}
	c.trMu.RLock()
	t, ok := c.transports[scheme]
	c.trMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTransportUnknown, scheme)
	}
	return t, nil
}

// removeSocket drops c's reference to a socket that has finished closing,
// so a long-lived Context does not accumulate dead sockets. Called from
// (*Socket).CloseCtx after teardown completes.
//
// A no-op when the socket is not present, which is the normal case during
// Context.Close: it detaches the whole slice up front and then closes each
// socket, so the removal finds nothing left to do.
func (c *Context) removeSocket(s *Socket) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i := slices.Index(c.sockets, s); i >= 0 {
		c.sockets = slices.Delete(c.sockets, i, i+1)
	}
}

// Close cancels the Context and tears down every socket created on it.
// It is idempotent; subsequent Close calls return nil. Subsequent
// NewSocket calls return ErrContextClosed.
func (c *Context) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.mu.Lock()
	socks := slices.Clone(c.sockets)
	c.sockets = nil
	c.mu.Unlock()
	for _, s := range socks {
		_ = s.Close()
	}
	c.cancel()
	return nil
}
