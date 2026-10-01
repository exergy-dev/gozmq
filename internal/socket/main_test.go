package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

func TestMain(m *testing.M) { testutil.VerifyTestMain(m) }

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func mustAttach(t *testing.T, eng socket.Engine, pipes ...*pipe.End) {
	t.Helper()
	for _, p := range pipes {
		require.NoError(t, eng.AttachPipe(p))
	}
}

// newFQ and newLB build a FairQueue / LoadBalancer with a gate of their
// own, for tests that do not watch it.
func newFQ() *socket.FairQueue { return socket.NewFairQueue(make(chan struct{}, 1)) }

func newLB() *socket.LoadBalancer { return socket.NewLoadBalancer(make(chan struct{}, 1)) }
