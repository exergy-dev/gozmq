package transport

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// TestTCP_BufferDefaultsKeepOSAutotuning: setting SO_RCVBUF/SO_SNDBUF pins
// the size and disables Linux's receive/send buffer autotuning, so the
// default must leave them alone.
func TestTCP_BufferDefaultsKeepOSAutotuning(t *testing.T) {
	t.Parallel()
	for _, tr := range []*TCPTransport{NewTCPTransport(), NewTCP6Transport()} {
		require.Zero(t, tr.ReadBufferSize, "%s ReadBufferSize", tr.Scheme())
		require.Zero(t, tr.WriteBufferSize, "%s WriteBufferSize", tr.Scheme())
	}
}

// TestTCP_WildcardPortListens: "tcp://*:*" (libzmq's "any interface, any
// port") binds an ephemeral port.
func TestTCP_WildcardPortListens(t *testing.T) {
	t.Parallel()
	addr, err := parseTCPEndpoint("tcp://*:*", "tcp", false)
	require.NoError(t, err)
	require.Equal(t, ":0", addr)

	l, err := NewTCPTransport().Listen(context.Background(), "tcp://127.0.0.1:*")
	require.NoError(t, err)
	defer l.Close()
	require.True(t, strings.HasPrefix(l.Endpoint(), "tcp://127.0.0.1:"), l.Endpoint())
	require.NotEqual(t, "tcp://127.0.0.1:0", l.Endpoint())

	_, err = parseTCPEndpoint("tcp://127.0.0.1:*", "tcp", true)
	require.ErrorIs(t, err, zmqerr.ErrEndpointInvalid, "wildcard port on Dial")
}
