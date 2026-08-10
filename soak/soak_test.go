//go:build soak

// Package soak holds the long-running stability tests. They are behind the
// `soak` build tag because they measure resource drift over minutes, which
// is far too slow for the PR path; the nightly workflow runs them.
//
//	go test -tags soak -run TestSoak -v ./soak/...
//	GOZMQ_SOAK_DURATION=2m go test -tags soak -run TestSoak -v ./soak/...
//
// What they are for: the ordinary suite proves the library is correct on a
// fresh Context. It does not prove that ten thousand connect/disconnect
// cycles leave no goroutines, no unreclaimed pool buffers, and no growing
// maps behind — and for a networking library meant to sit in a long-lived
// server process, that is the failure mode that actually reaches users.
// Each test below churns hard, then asserts goroutine count and heap have
// returned to their steady-state baseline.
package soak

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

// defaultDuration keeps an unconfigured local run short enough to be
// useful interactively. Nightly CI sets GOZMQ_SOAK_DURATION=10m.
const defaultDuration = 30 * time.Second

func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := os.Getenv("GOZMQ_SOAK_DURATION")
	if raw == "" {
		return defaultDuration
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("GOZMQ_SOAK_DURATION=%q: %v", raw, err)
	}
	return d
}

// resourceSample is a point-in-time reading of the two things that drift
// when teardown is incomplete.
type resourceSample struct {
	goroutines int
	heapBytes  uint64
}

// sampleResources forces a GC (twice — the first may only queue finalizers,
// and *Msg reclamation has a finalizer safety net) and then reads the
// counters. The sleep gives goroutines that were signalled to exit a chance
// to actually be descheduled, so the count is not spuriously high.
func sampleResources() resourceSample {
	for range 3 {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return resourceSample{
		goroutines: runtime.NumGoroutine(),
		heapBytes:  ms.HeapAlloc,
	}
}

// assertNoDrift compares a post-churn sample against the baseline taken
// after warm-up. Both bounds are generous: the point is to catch unbounded
// growth (a leak per connection), not to police normal allocator slack.
func assertNoDrift(t *testing.T, base, after resourceSample, iterations int) {
	t.Helper()

	const (
		goroutineSlack = 16
		heapSlackBytes = 32 << 20
	)

	t.Logf("baseline: %d goroutines, %.1f MiB heap", base.goroutines, mib(base.heapBytes))
	t.Logf("after %d iterations: %d goroutines, %.1f MiB heap",
		iterations, after.goroutines, mib(after.heapBytes))

	if grew := after.goroutines - base.goroutines; grew > goroutineSlack {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		t.Errorf("goroutine leak: %d over baseline after %d iterations (slack %d)\n\n%s",
			grew, iterations, goroutineSlack, buf)
	}

	if after.heapBytes > base.heapBytes+heapSlackBytes {
		t.Errorf("heap growth: %.1f MiB over baseline after %d iterations (slack %d MiB)",
			mib(after.heapBytes-base.heapBytes), iterations, heapSlackBytes>>20)
	}
}

func mib(b uint64) float64 { return float64(b) / (1 << 20) }

// TestSoakConnectChurn is the core case: a stable server with clients
// connecting, exchanging traffic, and disconnecting as fast as they can.
// Each cycle builds and tears down a full session — TCP dial, ZMTP
// greeting, NULL handshake, pipe attach, then teardown — so anything the
// session layer forgets to release accumulates visibly.
func TestSoakConnectChurn(t *testing.T) {
	duration := soakDuration(t)
	t.Logf("churning connections for %s", duration)

	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	server, err := zctx.NewSocket(gozmq.Router)
	if err != nil {
		t.Fatalf("new ROUTER: %v", err)
	}
	defer func() { _ = server.Close() }()
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	endpoint := server.LastEndpoint()

	srvCtx, stopServer := context.WithCancel(context.Background())
	var srvWG sync.WaitGroup
	srvWG.Add(1)
	go func() {
		defer srvWG.Done()
		for {
			m, err := server.RecvMsg(srvCtx)
			if err != nil {
				return
			}
			// Echo back to the sender: frame 0 is the ROUTER identity.
			err = server.SendMsg(srvCtx, gozmq.NewMsgFromFrames(m.Frames()...))
			m.Release()
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		stopServer()
		srvWG.Wait()
	}()

	// Warm up before sampling: the first few connections allocate pool
	// tiers and grow the runtime's stacks, which is one-time cost, not drift.
	for range 20 {
		if err := oneClientCycle(zctx, endpoint); err != nil {
			t.Fatalf("warm-up cycle: %v", err)
		}
	}
	base := sampleResources()

	deadline := time.Now().Add(duration)
	iterations := 0
	for time.Now().Before(deadline) {
		if err := oneClientCycle(zctx, endpoint); err != nil {
			t.Fatalf("cycle %d: %v", iterations, err)
		}
		iterations++
	}

	assertNoDrift(t, base, sampleResources(), iterations)
}

// oneClientCycle is a complete client lifetime: construct, connect, one
// round trip, close.
func oneClientCycle(zctx *gozmq.Context, endpoint string) error {
	sock, err := zctx.NewSocket(gozmq.Dealer)
	if err != nil {
		return fmt.Errorf("new DEALER: %w", err)
	}
	defer func() { _ = sock.Close() }()

	if err := sock.Connect(endpoint); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := sock.SendBytes(ctx, []byte("ping")); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	m, err := sock.RecvMsg(ctx)
	if err != nil {
		return fmt.Errorf("recv: %w", err)
	}
	m.Release()
	return nil
}

// TestSoakPubSubChurn covers the other churn axis: subscribers coming and
// going against a steady publisher. This exercises the subscription
// refcount tables and the PUB fan-out peer snapshot, neither of which is
// touched by connect churn alone.
func TestSoakPubSubChurn(t *testing.T) {
	duration := soakDuration(t)
	t.Logf("churning subscribers for %s", duration)

	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	pub, err := zctx.NewSocket(gozmq.Pub)
	if err != nil {
		t.Fatalf("new PUB: %v", err)
	}
	defer func() { _ = pub.Close() }()
	if err := pub.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	endpoint := pub.LastEndpoint()

	// A publisher that never stops, so subscribers always attach to a
	// socket mid-fan-out rather than an idle one.
	pubCtx, stopPub := context.WithCancel(context.Background())
	var pubWG sync.WaitGroup
	pubWG.Add(1)
	go func() {
		defer pubWG.Done()
		payload := []byte("topic.soak|" + string(make([]byte, 512)))
		for pubCtx.Err() == nil {
			if err := pub.SendBytes(pubCtx, payload); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	defer func() {
		stopPub()
		pubWG.Wait()
	}()

	for range 20 {
		oneSubscriberCycle(zctx, endpoint)
	}
	base := sampleResources()

	deadline := time.Now().Add(duration)
	iterations := 0
	for time.Now().Before(deadline) {
		oneSubscriberCycle(zctx, endpoint)
		iterations++
	}

	assertNoDrift(t, base, sampleResources(), iterations)
}

// oneSubscriberCycle attaches a subscriber, waits briefly for at most one
// message, and detaches. It intentionally does not require delivery: PUB
// drops for peers whose subscription has not propagated, and that race is
// the realistic case being soaked, not a failure.
func oneSubscriberCycle(zctx *gozmq.Context, endpoint string) {
	sock, err := zctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte("topic.")))
	if err != nil {
		return
	}
	defer func() { _ = sock.Close() }()
	if err := sock.Connect(endpoint); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if m, err := sock.RecvMsg(ctx); err == nil {
		m.Release()
	}
}

// TestSoakAbortedHandshakes covers the hostile path: peers that connect and
// vanish part-way through the ZMTP handshake. A server that only cleans up
// after a *successful* handshake leaks a session per aborted connection,
// which is trivially reachable by anything scanning the port.
func TestSoakAbortedHandshakes(t *testing.T) {
	duration := soakDuration(t)
	t.Logf("aborting handshakes for %s", duration)

	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	server, err := zctx.NewSocket(gozmq.Rep, gozmq.WithHandshakeTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatalf("new REP: %v", err)
	}
	defer func() { _ = server.Close() }()
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	endpoint := server.LastEndpoint()

	for range 20 {
		abortMidHandshake(endpoint)
	}
	base := sampleResources()

	deadline := time.Now().Add(duration)
	iterations := 0
	for time.Now().Before(deadline) {
		abortMidHandshake(endpoint)
		iterations++
	}

	// Aborted sessions are reaped by the handshake timeout, so give the
	// longest outstanding one time to fire before sampling.
	time.Sleep(time.Second)
	assertNoDrift(t, base, sampleResources(), iterations)
}

// abortMidHandshake dials the raw TCP port, sends a partial ZMTP greeting,
// then hangs up. The signature is valid enough for the server to accept the
// connection and commit to a handshake, but the greeting never completes.
func abortMidHandshake(endpoint string) {
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.Dial("tcp", strings.TrimPrefix(endpoint, "tcp://"))
	if err != nil {
		return
	}
	_, _ = conn.Write([]byte{0xFF, 0, 0, 0, 0, 0, 0, 0, 1, 0x7F, 3})
	_ = conn.Close()
}
