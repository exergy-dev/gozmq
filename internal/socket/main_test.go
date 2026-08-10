package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) { testutil.VerifyTestMain(m) }

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func mustAttach(t *testing.T, eng iface.SocketEngine, pipes ...iface.Pipe) {
	t.Helper()
	for _, p := range pipes {
		require.NoError(t, eng.AttachPipe(p))
	}
}
