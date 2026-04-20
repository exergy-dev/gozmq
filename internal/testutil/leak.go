// Package testutil provides shared test helpers. The goroutine-leak
// wrapper is the most load-bearing: every package's TestMain should call
// VerifyTestMain so Socket.Close and Session.Stop are covered by leak
// assertions end-to-end (design §10.5).
package testutil

import (
	"testing"

	"go.uber.org/goleak"
)

// VerifyTestMain runs m.Run then asserts no goroutines leaked. Intended as
// a one-liner from a package's TestMain:
//
//	func TestMain(m *testing.M) { testutil.VerifyTestMain(m) }
//
// Common framework goroutines (race detector, testing scheduler) are
// whitelisted via goleak's defaults; add package-specific ignores via
// goleak.IgnoreTopFunction in a wrapper if needed.
func VerifyTestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
