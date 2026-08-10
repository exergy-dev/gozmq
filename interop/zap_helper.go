//go:build interop

package interop

import (
	"testing"

	"github.com/exergy-dev/gozmq/security"
)

type zapDecisionFunc func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error)

func (f zapDecisionFunc) Authenticate(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
	return f(domain, address, mech, credentials)
}

func startLibzmqZAPHandler(t *testing.T, ctx *Ctx, decide zapDecisionFunc) <-chan error {
	t.Helper()

	handler := security.NewZapHandler(decide)
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
