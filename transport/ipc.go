package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// ipcScheme is the URL scheme this transport claims. Matches libzmq.
const ipcScheme = "ipc"

// ipcAcceptPollInterval mirrors the TCP accept poll (tcpAcceptPollInterval)
// so ctx cancellation / Close unblock Accept within the same bounded latency
// on every net-based transport.
const ipcAcceptPollInterval = 5 * time.Millisecond

// IPCTransport implements iface.Transport over Unix domain sockets,
// matching libzmq's "ipc" scheme (design §7.2). Endpoint syntax:
//
//	ipc:///path/to/file.sock   — filesystem-namespace socket
//	ipc://@name                — abstract-namespace socket (Linux only)
//
// The abstract namespace uses a leading NUL byte in the sun_path of the
// sockaddr; in Go we spell that by prefixing the path with "\x00". We
// translate the "@" sigil to that form on Dial/Listen and back to "@" in
// LocalEndpoint/RemoteEndpoint so callers see the original syntax.
//
// Listener Close removes the socket file — but only when the file is in
// the filesystem namespace AND we created it via Listen. Abstract
// sockets have no inode to remove.
//
// Concurrency: all exported methods are safe for concurrent use. Exported
// configuration fields should only be mutated before the transport is
// registered.
type IPCTransport struct {
	// FilePerm, when non-zero, is chmod'd onto the socket file immediately
	// after Listen. Ignored for abstract-namespace bindings (no file).
	FilePerm os.FileMode
}

// NewIPCTransport returns an IPCTransport with default (libzmq-compatible)
// settings: no file-permission override.
func NewIPCTransport() *IPCTransport { return &IPCTransport{} }

// Scheme reports "ipc".
func (t *IPCTransport) Scheme() string { return ipcScheme }

// Dial connects to endpoint. Respects ctx via net.Dialer.DialContext; the
// dialer resolves the abstract-namespace sigil before handing to the OS.
func (t *IPCTransport) Dial(ctx context.Context, endpoint string) (iface.Conn, error) {
	addr, displayAddr, abstract, err := parseIPCAddress(endpoint)
	if err != nil {
		return nil, err
	}
	if abstract && runtime.GOOS != "linux" {
		return nil, fmt.Errorf("%w: abstract-namespace ipc is Linux-only", zmqerr.ErrEndpointInvalid)
	}
	d := net.Dialer{}
	c, err := d.DialContext(ctx, "unix", addr)
	if err != nil {
		return nil, err
	}
	return newIPCConn(c, "ipc://"+displayAddr, "ipc://"+displayAddr), nil
}

// Listen binds endpoint. For filesystem paths we create the socket inode
// and (if FilePerm is set) chmod it. For abstract namespaces we skip the
// chmod path entirely.
func (t *IPCTransport) Listen(_ context.Context, endpoint string) (iface.Listener, error) {
	addr, displayAddr, abstract, err := parseIPCAddress(endpoint)
	if err != nil {
		return nil, err
	}
	if abstract && runtime.GOOS != "linux" {
		return nil, fmt.Errorf("%w: abstract-namespace ipc is Linux-only", zmqerr.ErrEndpointInvalid)
	}
	l, err := net.Listen("unix", addr)
	if err != nil {
		return nil, err
	}
	// Chmod only makes sense for the filesystem namespace.
	if !abstract && t.FilePerm != 0 {
		if err := os.Chmod(addr, t.FilePerm); err != nil {
			_ = l.Close()
			_ = os.Remove(addr)
			return nil, fmt.Errorf("%w: chmod socket: %s", zmqerr.ErrEndpointInvalid, err.Error())
		}
	}
	return newIPCListener(l, t, addr, displayAddr, abstract), nil
}

// parseIPCAddress decodes an ipc endpoint. Returns:
//   - addr: the net.Listen / net.Dial address, with "@name" translated to
//     "\x00name" (the Linux abstract-namespace spelling).
//   - displayAddr: the original user-facing form ("/path" or "@name").
//   - abstract: true for the abstract namespace.
func parseIPCAddress(endpoint string) (addr, displayAddr string, abstract bool, err error) {
	scheme, raw, err := splitEndpoint(endpoint)
	if err != nil {
		return "", "", false, err
	}
	if scheme != ipcScheme {
		return "", "", false, fmt.Errorf("%w: scheme %q passed to ipc transport", zmqerr.ErrEndpointInvalid, scheme)
	}
	if raw == "" {
		return "", "", false, fmt.Errorf("%w: empty ipc path", zmqerr.ErrEndpointInvalid)
	}
	if raw[0] == '@' {
		name := raw[1:]
		if name == "" {
			return "", "", false, fmt.Errorf("%w: empty abstract-namespace name", zmqerr.ErrEndpointInvalid)
		}
		// Linux spells the abstract namespace with a leading NUL byte.
		return "\x00" + name, "@" + name, true, nil
	}
	return raw, raw, false, nil
}

// --------------------------------------------------------------------------
// ipcListener — iface.Listener with ctx-aware Accept and socket-file
// cleanup on Close.
// --------------------------------------------------------------------------

// ipcListener follows the tcpListener poll pattern. Close removes the
// socket file (filesystem namespace only) and unblocks pending Accept.
type ipcListener struct {
	l        net.Listener
	t        *IPCTransport
	path     string // the raw address we passed to net.Listen (for Remove)
	display  string // "@name" or "/path" for endpoint-string reporting
	abstract bool
	endpoint string
	listenerCloseState
}

func newIPCListener(l net.Listener, t *IPCTransport, path, display string, abstract bool) *ipcListener {
	return &ipcListener{
		l:                  l,
		t:                  t,
		path:               path,
		display:            display,
		abstract:           abstract,
		endpoint:           "ipc://" + display,
		listenerCloseState: newListenerCloseState(),
	}
}

func (l *ipcListener) Endpoint() string { return l.endpoint }

func (l *ipcListener) Accept(ctx context.Context) (iface.Conn, error) {
	c, err := acceptWithDeadline(
		ctx,
		l.l,
		l.closeCh,
		l.closed.Load,
		ipcAcceptPollInterval,
		func(t time.Time) error {
			if ul, ok := l.l.(*net.UnixListener); ok {
				return ul.SetDeadline(t)
			}
			return nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return newIPCConn(c, l.endpoint, l.endpoint), nil
}

// Close stops accepting and removes the socket file. Filesystem sockets
// created by net.Listen leave a stale inode behind; we clean it up so
// repeated bind/unbind cycles do not accumulate cruft. Abstract sockets
// have no inode and need no cleanup. Idempotent.
func (l *ipcListener) Close() error {
	return l.closeWith(func() error {
		// net.UnixListener.Close already removes the socket file by
		// default when SetUnlinkOnClose(true) is set (Go default). We
		// still call os.Remove defensively below in case the listener
		// was constructed by some future path that disables it, and to
		// guarantee our contract ("listener close removes the socket
		// file if we created it").
		firstErr := l.l.Close()
		if !l.abstract {
			if err := os.Remove(l.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		return firstErr
	})
}

// --------------------------------------------------------------------------
// ipcConn — iface.Conn over a net.Conn (Unix socket).
// --------------------------------------------------------------------------

// ipcConn wraps a net.Conn and reports ipc:// endpoints. Unlike inproc
// we are riding on a real stream, so the embedded Conn's Read/Write
// already honor deadlines and ctx-via-deadline patterns.
type ipcConn struct {
	net.Conn
	local, remote string
}

func newIPCConn(c net.Conn, local, remote string) *ipcConn {
	return &ipcConn{Conn: c, local: local, remote: remote}
}

func (c *ipcConn) LocalEndpoint() string  { return c.local }
func (c *ipcConn) RemoteEndpoint() string { return c.remote }

func (c *ipcConn) WriteBuffers(buffers *net.Buffers) (int64, error) {
	return buffers.WriteTo(c.Conn)
}

// Compile-time interface checks.
var (
	_ iface.Transport = (*IPCTransport)(nil)
	_ iface.Listener  = (*ipcListener)(nil)
	_ iface.Conn      = (*ipcConn)(nil)
)
