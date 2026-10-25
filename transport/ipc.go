package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"syscall"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// ipcScheme is the URL scheme this transport claims. Matches libzmq.
const ipcScheme = "ipc"

// IPCTransport implements Transport over Unix domain sockets,
// matching libzmq's "ipc" scheme. Endpoint syntax:
//
//	ipc:///path/to/file.sock   — filesystem-namespace socket
//	ipc://@name                — abstract-namespace socket (Linux only)
//
// The path is handed to the OS as written: on Linux, Go's syscall layer
// already maps a leading '@' to the abstract namespace's leading NUL byte
// (and back again in reported addresses).
//
// Listener Close removes the socket file it created (net.UnixListener's
// unlink-on-close). Abstract sockets have no inode to remove. A socket file
// left behind by a crashed process is reclaimed on Listen once a probe dial
// shows nothing is listening on it.
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

// Dial connects to endpoint. Respects ctx via net.Dialer.DialContext.
func (t *IPCTransport) Dial(ctx context.Context, endpoint string) (Conn, error) {
	path, _, err := parseIPCAddress(endpoint)
	if err != nil {
		return nil, err
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	return newIPCConn(c, "ipc://"+path), nil
}

// newIPCConn adapts c, a Unix socket, with the same endpoint on both sides
// (a Unix socket has no useful peer address).
func newIPCConn(c net.Conn, endpoint string) *conn {
	return &conn{Conn: c, local: endpoint, remote: endpoint, vectored: true}
}

// Listen binds endpoint. For filesystem paths we create the socket inode
// and (if FilePerm is set) chmod it. For abstract namespaces we skip the
// chmod path entirely.
func (t *IPCTransport) Listen(ctx context.Context, endpoint string) (Listener, error) {
	path, abstract, err := parseIPCAddress(endpoint)
	if err != nil {
		return nil, err
	}
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "unix", path)
	if err != nil && !abstract && errors.Is(err, syscall.EADDRINUSE) && reclaimStaleSocket(ctx, path) {
		l, err = lc.Listen(ctx, "unix", path)
	}
	if err != nil {
		return nil, err
	}
	// Chmod only makes sense for the filesystem namespace.
	if !abstract && t.FilePerm != 0 {
		if err := os.Chmod(path, t.FilePerm); err != nil {
			_ = l.Close() // unlinks the file
			return nil, fmt.Errorf("%w: chmod socket: %w", zmqerr.ErrEndpointInvalid, err)
		}
	}
	endpoint = "ipc://" + path
	return newNetListener(l, endpoint, func(c net.Conn) Conn { return newIPCConn(c, endpoint) }), nil
}

// reclaimStaleSocket removes the socket file at path if nothing is listening
// on it (a leftover from a process that died without unlinking), reporting
// whether it did. Only a socket whose probe dial is refused is removed: a
// live listener, a regular file, or an inconclusive probe is left alone.
func reclaimStaleSocket(ctx context.Context, path string) bool {
	fi, err := os.Lstat(path)
	if err != nil || fi.Mode().Type() != os.ModeSocket {
		return false
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", path)
	if err == nil {
		_ = c.Close()
		return false
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return false
	}
	err = os.Remove(path)
	return err == nil || errors.Is(err, os.ErrNotExist)
}

// parseIPCAddress decodes an ipc endpoint into the path handed to the OS
// ("/path" or "@name") and whether it names the abstract namespace.
func parseIPCAddress(endpoint string) (path string, abstract bool, err error) {
	raw, err := splitEndpoint(endpoint, ipcScheme)
	if err != nil {
		return "", false, err
	}
	if raw == "" {
		return "", false, fmt.Errorf("%w: empty ipc path", zmqerr.ErrEndpointInvalid)
	}
	if raw[0] == '@' {
		switch {
		case len(raw) == 1:
			return "", false, fmt.Errorf("%w: empty abstract-namespace name", zmqerr.ErrEndpointInvalid)
		case runtime.GOOS != "linux":
			return "", false, fmt.Errorf("%w: abstract-namespace ipc is Linux-only", zmqerr.ErrEndpointInvalid)
		}
		return raw, true, nil
	}
	return raw, false, nil
}

// Compile-time interface checks.
var _ Transport = (*IPCTransport)(nil)
