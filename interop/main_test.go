//go:build interop

package interop

import (
	"testing"

	"github.com/exergy-dev/gozmq/internal/testutil"
)

// TestMain runs the package's tests under goleak via the shared helper so
// any gozmq-side goroutine leak from a failed Socket.Close surfaces here
// too (design §10.5 — "all tests run with -race; goroutine leak detection
// via goleak in every test's TestMain").
//
// Note on CGO + goleak: libzmq spawns its own pthreads for the I/O
// reactor(s) (`zmq::io_thread_t`, `zmq::reaper_t`), but those are C
// threads outside the Go runtime and therefore invisible to goleak — no
// whitelist needed. The only Go goroutines in this package are the ones
// the tests spawn themselves (Recv fan-outs in push_pull_test.go) and
// they all join before the test returns. If a future test introduces a
// goroutine blocked in a cgocall across the TestMain boundary, add an
// explicit goleak.IgnoreTopFunction here and document WHICH function and
// WHY — per the contract in §10.5.
func TestMain(m *testing.M) { testutil.VerifyTestMain(m) }
