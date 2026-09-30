//go:build interop

package interop

import (
	"testing"

	"github.com/exergy-dev/gozmq/security"
)

func startLibzmqZAPHandler(t *testing.T, ctx *Ctx, handler security.ZAPHandler) <-chan error {
	t.Helper()

	sock := ctx.NewSocket(ZMQ_REP)
	if err := sock.Bind("inproc://zeromq.zap.01"); err != nil {
		t.Fatalf("libzmq ZAP bind: %v", err)
	}
	t.Cleanup(func() { _ = sock.Close() })

	done := make(chan error, 1)
	go func() {
		req, err := sock.Recv()
		if err != nil {
			done <- err
			return
		}
		if err := sock.Send(handler.ServeRequest(req)); err != nil {
			done <- err
			return
		}
		done <- nil
	}()
	return done
}
