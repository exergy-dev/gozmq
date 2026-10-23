//go:build interop

package interop

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/security"
)

// startLibzmqZAPHandler serves exactly one ZAP request on ctx's
// inproc://zeromq.zap.01 with handler. It returns the channel that reports
// the outcome and the REP socket, which callers close before expecting a
// libzmq server to finish disconnecting (see link.closeLib).
func startLibzmqZAPHandler(t *testing.T, ctx *Ctx, handler security.ZAPHandler) (<-chan error, *Sock) {
	t.Helper()
	sock := ctx.NewSocket(ZMQ_REP)
	require.NoError(t, sock.Bind("inproc://zeromq.zap.01"), "libzmq ZAP bind")
	t.Cleanup(func() { _ = sock.Close() })

	done := make(chan error, 1)
	go func() {
		req, err := sock.Recv()
		if err != nil {
			done <- err
			return
		}
		done <- sock.Send(handler.ServeRequest(req))
	}()
	return done, sock
}
