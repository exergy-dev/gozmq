// Package transport provides concrete iface.Transport implementations for
// the schemes the gozmq library understands: tcp, tcp6, and inproc.
//
// Each transport turns an endpoint string into Dial / Listen operations and
// adapts the underlying primitive (a net.Conn for TCP, an in-memory pipe for
// inproc) to the iface.Conn / iface.Listener contracts. Higher layers reach
// transports exclusively through the iface.Registry interface and never
// import this package directly.
package transport

import (
	"fmt"
	"strings"
	"sync"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// registry is the goroutine-safe scheme → Transport map that satisfies
// iface.Registry. The zero value is NOT usable; construct via New or
// NewRegistry.
type registry struct {
	mu sync.RWMutex
	m  map[string]iface.Transport
}

// NewRegistry returns an empty registry. Callers must Register transports
// before Lookup will succeed.
func NewRegistry() iface.Registry {
	return &registry{m: make(map[string]iface.Transport)}
}

// New returns a registry preloaded with the standard transports: tcp,
// tcp6, inproc, and ipc. Defaults match the design doc:
//   - TCP: keepalive on, nodelay on (design §7.1).
//   - inproc: 500ms connect-before-bind queue window (design §7.3).
//   - ipc: no FilePerm override (design §7.2).
//
// TLS (tls+tcp) is deliberately NOT registered here because there is no
// reasonable default *tls.Config — callers install it themselves via
// (*gozmq.Context).RegisterTransport(transport.NewTLSTransport(cfg)).
func New() iface.Registry {
	r := NewRegistry()
	r.Register(NewTCPTransport())
	r.Register(NewTCP6Transport())
	r.Register(NewInprocTransport(0)) // 0 → use default window
	r.Register(NewIPCTransport())
	return r
}

func (r *registry) Register(t iface.Transport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[t.Scheme()] = t
}

func (r *registry) Lookup(endpoint string) (iface.Transport, error) {
	scheme, _, err := splitEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	t, ok := r.m[scheme]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", zmqerr.ErrTransportUnknown, scheme)
	}
	return t, nil
}

// splitEndpoint parses "scheme://address" into its parts. Returns
// ErrEndpointInvalid if the separator is missing.
func splitEndpoint(endpoint string) (scheme, address string, err error) {
	const sep = "://"
	i := strings.Index(endpoint, sep)
	if i <= 0 {
		return "", "", fmt.Errorf("%w: %q (missing scheme://)", zmqerr.ErrEndpointInvalid, endpoint)
	}
	return endpoint[:i], endpoint[i+len(sep):], nil
}
