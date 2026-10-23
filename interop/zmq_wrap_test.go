//go:build interop

package interop

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWrap_PairSmoke exercises the CGO wrapper on its own: two libzmq PAIR
// sockets round-trip a multipart message over TCP, and setsockopt rejects a
// bogus option code. If this fails every other test in the package is
// suspect.
func TestWrap_PairSmoke(t *testing.T) {
	t.Parallel()
	ctx := NewCtx()
	t.Cleanup(func() { _ = ctx.Close() })
	server := newLibEnd(t, ctx, ZMQ_PAIR)
	require.NoError(t, server.s.Bind("tcp://127.0.0.1:0"))
	ep, err := server.s.LastEndpoint()
	require.NoError(t, err)
	require.NotEmpty(t, ep)

	client := newLibEnd(t, ctx, ZMQ_PAIR)
	require.NoError(t, client.s.Connect(ep))
	require.Error(t, client.s.SetOption(999999, 1), "bogus option code")

	want := [][]byte{[]byte("hello"), []byte("world")}
	client.send(want...)
	requireFrames(t, want, server.recv())
	server.send([]byte("ack"))
	requireFrames(t, [][]byte{[]byte("ack")}, client.recv())
}
