package gozmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq"
	"github.com/stretchr/testify/require"
)

// newSocketPair creates a Context with a bound server and connected client of
// the given type over the specified transport (e.g. "tcp://127.0.0.1:0" or
// "inproc://test"). Cleanup is registered via t.Cleanup.
func newSocketPair(t *testing.T, typ gozmq.SocketType, transport string, opts ...gozmq.Option) (*gozmq.Context, *gozmq.Socket, *gozmq.Socket) {
	t.Helper()
	ctx := gozmq.NewContext()
	t.Cleanup(func() { _ = ctx.Close() })
	server, err := ctx.NewSocket(typ, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	require.NoError(t, server.Bind(transport))
	client, err := ctx.NewSocket(typ, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Connect(server.LastEndpoint()))
	return ctx, server, client
}

// newSocketPairTypes creates a Context with a bound server of serverType and a
// connected client of clientType over the specified transport. Server and
// client receive independent option lists. Cleanup is registered via
// t.Cleanup.
func newSocketPairTypes(
	t *testing.T,
	serverType, clientType gozmq.SocketType,
	transport string,
	serverOpts, clientOpts []gozmq.Option,
) (*gozmq.Context, *gozmq.Socket, *gozmq.Socket) {
	t.Helper()
	ctx := gozmq.NewContext()
	t.Cleanup(func() { _ = ctx.Close() })
	server, err := ctx.NewSocket(serverType, serverOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	require.NoError(t, server.Bind(transport))
	client, err := ctx.NewSocket(clientType, clientOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Connect(server.LastEndpoint()))
	return ctx, server, client
}

// testCtx returns a context.Context with a 5-second timeout suitable for
// most test operations. The caller must defer cancel().
func testCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}
