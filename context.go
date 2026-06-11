package gozmq

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/security"
	"github.com/rkm/gozmq/transport"
)

// Context is the top-level resource owner. A Context holds the transport
// registry (including the inproc name table shared by all inproc sockets
// on this Context) and the defaults sockets inherit.
//
// The zero value is NOT usable; construct via NewContext.
type Context struct {
	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc

	registry *transport.Registry
	zapAuth  iface.ZAPAuthorizer

	mu      sync.Mutex
	sockets []*Socket
}

// ContextOption configures a Context at construction.
type ContextOption interface{ applyContext(*Context) }

type contextOptionFunc func(*Context)

func (f contextOptionFunc) applyContext(c *Context) { f(c) }

// WithZapHandler installs a context-level RFC 27 ZAP handler used by
// server-side PLAIN and CURVE handshakes.
func WithZapHandler(handler *security.ZapHandler) ContextOption {
	return contextOptionFunc(func(c *Context) {
		if handler == nil {
			c.zapAuth = nil
			return
		}
		c.zapAuth = func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			req := &security.ZapRequest{
				Domain:      domain,
				Address:     address,
				Mechanism:   mech,
				Credentials: credentials,
			}
			resp, err := security.DecodeZapResponse(handler.ServeRequest(security.EncodeZapRequest(req)))
			if err != nil {
				return "", nil, fmt.Errorf("gozmq: decode ZAP response: %w", err)
			}
			switch resp.StatusCode {
			case security.ZapStatusOK:
				return resp.UserID, resp.Metadata, nil
			case security.ZapStatusTempError:
				return "", nil, fmt.Errorf("gozmq: ZAP temporary failure: %s: %w", resp.StatusText, security.ErrZapTemporary)
			case security.ZapStatusClientError:
				return "", nil, fmt.Errorf("gozmq: ZAP rejected %s peer: %s: %w", mech, resp.StatusText, zmqerr.ErrAuthFailed)
			case security.ZapStatusServerError:
				return "", nil, fmt.Errorf("gozmq: ZAP server error: %s", resp.StatusText)
			default:
				return "", nil, fmt.Errorf("gozmq: unknown ZAP status code %q", resp.StatusCode)
			}
		}
	})
}

// NewContext creates a new Context. Call Close to release resources.
func NewContext(opts ...ContextOption) *Context {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Context{
		ctx:      ctx,
		cancel:   cancel,
		registry: transport.New(),
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
// This is the supported extension point for transports whose defaults
// cannot be baked into transport.New() — most notably TLS, which needs a
// caller-supplied *tls.Config. Typical usage:
//
//	ctx := gozmq.NewContext()
//	ctx.RegisterTransport(transport.NewTLSTransport(tlsCfg))
//	sock, _ := ctx.NewSocket(gozmq.Router)
//	_ = sock.Bind("tls+tcp://0.0.0.0:5555")
//
// Safe to call concurrently with NewSocket; existing sockets consult the
// registry at Bind / Connect time, so a socket constructed before
// RegisterTransport sees the updated scheme set as long as its Bind /
// Connect call races after Register.
//
// The parameter type iface.Transport is internal by design: transports are
// an intentional first-party closed set (see package transport). This method
// exists to inject configuration into the built-in transports — chiefly TLS,
// which needs a caller-supplied *tls.Config — not as a plugin boundary for
// arbitrary third-party transports.
func (c *Context) RegisterTransport(t iface.Transport) {
	c.registry.Register(t)
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
