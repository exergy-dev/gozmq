package transport

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq/internal/testutil"
)

// TestMain enforces goroutine-leak-free Test runs across the whole package.
// Every Listener / Conn must be Close'd in defer; any leak is a real bug
// in this layer because the transport owns the long-lived goroutines that
// poll Accept and shuttle bytes.
func TestMain(m *testing.M) { testutil.VerifyTestMain(m) }

// testCtx returns a context.Context that is cancelled when t finishes.
func testCtx(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}
