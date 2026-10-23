package transport

import (
	"testing"

	"github.com/exergy-dev/gozmq/internal/testutil"
)

// TestMain enforces goroutine-leak-free Test runs across the whole package.
// Every Listener / Conn must be Close'd in defer; any leak is a real bug
// in this layer because the transport owns the long-lived goroutines that
// poll Accept and shuttle bytes.
func TestMain(m *testing.M) { testutil.VerifyMain(m) }
