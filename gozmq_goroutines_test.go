package gozmq_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
)

// Goroutine budgets for an idle, established link, measured per connecting
// socket including its session on both ends. Tighten these as engine and
// session goroutines are removed; never raise them.
const (
	maxGoroutinesPerTCPConn    = 10
	maxGoroutinesPerInprocLink = 2
)

const goroutineTestLinks = 16

func TestGoroutinesPerTCPConnection(t *testing.T) {
	perLink := goroutinesPerLink(t, "tcp://127.0.0.1:0")
	t.Logf("goroutines per TCP connection: %.2f (budget %d)", perLink, maxGoroutinesPerTCPConn)
	require.LessOrEqual(t, perLink, float64(maxGoroutinesPerTCPConn))
}

func TestGoroutinesPerInprocLink(t *testing.T) {
	perLink := goroutinesPerLink(t, "inproc://goroutines-per-link")
	t.Logf("goroutines per inproc link: %.2f (budget %d)", perLink, maxGoroutinesPerInprocLink)
	require.LessOrEqual(t, perLink, float64(maxGoroutinesPerInprocLink))
}

// goroutinesPerLink binds a PULL on endpoint, then connects
// goroutineTestLinks PUSH sockets to it, each delivering one message to
// prove its link is established. It returns the settled goroutine delta
// (taken from after the Bind) divided by the number of links.
func goroutinesPerLink(t *testing.T, endpoint string) float64 {
	t.Helper()
	ctx, pull := newTestSocket(t, gozmq.Pull)
	require.NoError(t, pull.Bind(endpoint))
	before := settledGoroutines(t)

	callCtx := testCtx(t)
	for i := 0; i < goroutineTestLinks; i++ {
		push, err := ctx.NewSocket(gozmq.Push)
		require.NoError(t, err)
		t.Cleanup(func() { _ = push.Close() })
		require.NoError(t, push.Connect(pull.LastEndpoint()))
		require.NoError(t, push.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})))
		m, err := pull.RecvMsg(callCtx)
		require.NoError(t, err)
		m.Release()
	}

	after := settledGoroutines(t)
	return float64(after-before) / goroutineTestLinks
}

// settledGoroutines polls runtime.NumGoroutine until it reads the same
// value several times in a row, so goroutines still starting or exiting
// don't skew the count.
func settledGoroutines(t *testing.T) int {
	t.Helper()
	const stableReads = 10
	deadline := time.Now().Add(5 * time.Second)
	last, same := runtime.NumGoroutine(), 0
	for same < stableReads {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count did not settle (last %d)", last)
		}
		time.Sleep(5 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == last {
			same++
		} else {
			last, same = n, 0
		}
	}
	return last
}
