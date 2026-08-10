package gozmq_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/transport"
)

// This file is the executable form of the claim made in the docs for
// transport.Transport and (*Context).RegisterTransport: the transport
// contracts are implementable from outside the module. countingTransport
// below uses nothing but the standard library and the public transport
// package, exactly as third-party code would, and carries real ZMTP traffic
// (greeting, NULL handshake, REQ/REP round trip) end to end.
//
// If the transport interfaces ever regress to naming an internal/ type, this
// file stops compiling.

const customScheme = "count+tcp"

// countingTransport wraps TCP and counts the bytes crossing it, proving both
// that a foreign scheme can be registered and that it sits underneath the
// full ZMTP stack rather than beside it.
type countingTransport struct {
	read    atomic.Int64
	written atomic.Int64
}

func (t *countingTransport) Scheme() string { return customScheme }

// hostPort strips the scheme so the embedded net dialer/listener can use it.
func hostPort(endpoint string) (string, error) {
	addr, ok := strings.CutPrefix(endpoint, customScheme+"://")
	if !ok {
		return "", fmt.Errorf("countingTransport: bad endpoint %q", endpoint)
	}
	return strings.Replace(addr, "*", "", 1), nil
}

func (t *countingTransport) Dial(ctx context.Context, endpoint string) (transport.Conn, error) {
	addr, err := hostPort(endpoint)
	if err != nil {
		return nil, err
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: c, tr: t}, nil
}

func (t *countingTransport) Listen(_ context.Context, endpoint string) (transport.Listener, error) {
	addr, err := hostPort(endpoint)
	if err != nil {
		return nil, err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &countingListener{Listener: l, tr: t}, nil
}

type countingConn struct {
	net.Conn
	tr *countingTransport
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.tr.read.Add(int64(n))
	return n, err
}

func (c *countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.tr.written.Add(int64(n))
	return n, err
}

func (c *countingConn) LocalEndpoint() string {
	return customScheme + "://" + c.Conn.LocalAddr().String()
}

func (c *countingConn) RemoteEndpoint() string {
	return customScheme + "://" + c.Conn.RemoteAddr().String()
}

type countingListener struct {
	net.Listener
	tr *countingTransport
}

// Accept honors ctx by closing the listener on cancellation, which unblocks
// the pending net.Listener.Accept.
func (l *countingListener) Accept(ctx context.Context) (transport.Conn, error) {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = l.Listener.Close()
		case <-done:
		}
	}()
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: c, tr: l.tr}, nil
}

func (l *countingListener) Endpoint() string {
	return customScheme + "://" + l.Listener.Addr().String()
}

func (l *countingListener) Close() error { return l.Listener.Close() }

// Compile-time proof that the public interfaces are satisfiable by code that
// imports only the standard library and the public transport package.
var (
	_ transport.Transport = (*countingTransport)(nil)
	_ transport.Conn      = (*countingConn)(nil)
	_ transport.Listener  = (*countingListener)(nil)
)

func TestRegisterOutOfTreeTransport(t *testing.T) {
	tr := &countingTransport{}

	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()
	zctx.RegisterTransport(tr)

	rep, err := zctx.NewSocket(gozmq.Rep)
	require.NoError(t, err)
	defer func() { _ = rep.Close() }()
	require.NoError(t, rep.Bind(customScheme+"://127.0.0.1:0"))

	req, err := zctx.NewSocket(gozmq.Req)
	require.NoError(t, err)
	defer func() { _ = req.Close() }()
	require.NoError(t, req.Connect(rep.LastEndpoint()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, req.SendBytes(ctx, []byte("ping")))

	got, err := rep.RecvBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, "ping", string(got))

	require.NoError(t, rep.SendBytes(ctx, []byte("pong")))

	reply, err := req.RecvBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, "pong", string(reply))

	// The ZMTP greeting alone is 64 bytes per direction, so any nonzero
	// count here confirms the whole stack rode over the custom transport
	// rather than falling back to the built-in tcp:// entry.
	require.Greater(t, tr.written.Load(), int64(64), "custom transport carried no bytes")
	require.Greater(t, tr.read.Load(), int64(64), "custom transport received no bytes")
}

// TestRegisterTransportOverridesBuiltin covers the other documented use of
// RegisterTransport: replacing a preregistered scheme with a reconfigured
// instance of the same built-in.
func TestRegisterTransportOverridesBuiltin(t *testing.T) {
	tcp := transport.NewTCPTransport()
	tcp.KeepAlivePeriod = 10 * time.Second
	tcp.ReadBufferSize = 1 << 20

	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()
	zctx.RegisterTransport(tcp)

	pull, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err)
	defer func() { _ = pull.Close() }()
	require.NoError(t, pull.Bind("tcp://127.0.0.1:0"))

	push, err := zctx.NewSocket(gozmq.Push)
	require.NoError(t, err)
	defer func() { _ = push.Close() }()
	require.NoError(t, push.Connect(pull.LastEndpoint()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, push.SendBytes(ctx, []byte("payload")))
	got, err := pull.RecvBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, "payload", string(got))
}
