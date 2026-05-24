package session

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/testutil"
)

func TestMain(m *testing.M) { testutil.VerifyTestMain(m) }

// testCtx returns a context.Context that is cancelled when t finishes.
func testCtx(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}
