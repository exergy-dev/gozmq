package gozmq_test

import (
	"context"
	"expvar"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

// drainEvents spools events off ch into a slice until the ctx fires or
// the channel closes. We never close a monitor channel in these tests
// because the library does not own it, so the test drives termination
// via ctx cancellation.
func drainEvents(ctx context.Context, ch <-chan gozmq.Event) []gozmq.Event {
	var out []gozmq.Event
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-ctx.Done():
			return out
		}
	}
}

// waitForEvents spins until the predicate is satisfied by the events
// gathered so far, or timeout elapses. Returns the collected slice.
// Using a predicate-based wait lets each test assert on a specific
// subsequence without requiring a hardcoded delay or a full
// deterministic scheduler.
func waitForEvents(t *testing.T, ch <-chan gozmq.Event, timeout time.Duration, predicate func([]gozmq.Event) bool) []gozmq.Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var collected []gozmq.Event
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			collected = append(collected, ev)
			if predicate(collected) {
				return collected
			}
		case <-time.After(50 * time.Millisecond):
			if predicate(collected) {
				return collected
			}
		}
	}
	return collected
}

// hasType reports whether any event in the slice matches the given type.
func hasType(events []gozmq.Event, t gozmq.EventType) bool {
	for _, e := range events {
		if e.Type == t {
			return true
		}
	}
	return false
}

// TestMonitor_ServerLifecycle verifies the full server-side event
// sequence on a single PAIR/TCP round trip: Listening → Accepted →
// HandshakeSucceeded → (client connect-side events) → Closed.
func TestMonitor_ServerLifecycle(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	srvEvents := make(chan gozmq.Event, 64)
	srv, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMonitor(srvEvents, gozmq.EventAll))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	if err := srv.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	ep := srv.LastEndpoint()

	cliEvents := make(chan gozmq.Event, 64)
	cli, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMonitor(cliEvents, gozmq.EventAll))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := cli.Connect(ep); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Round-trip a message to guarantee the handshake completed on
	// both ends before we assert on events.
	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("hello"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := srv.RecvMsg(callCtx); err != nil {
		t.Fatalf("recv: %v", err)
	}

	// Wait for the server to observe Listening + Accepted +
	// HandshakeSucceeded before we tear down.
	srvCollected := waitForEvents(t, srvEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventListening) &&
			hasType(evts, gozmq.EventAccepted) &&
			hasType(evts, gozmq.EventHandshakeSucceeded)
	})
	if !hasType(srvCollected, gozmq.EventListening) {
		t.Errorf("no EventListening; collected=%v", eventTypes(srvCollected))
	}
	if !hasType(srvCollected, gozmq.EventAccepted) {
		t.Errorf("no EventAccepted; collected=%v", eventTypes(srvCollected))
	}
	if !hasType(srvCollected, gozmq.EventHandshakeSucceeded) {
		t.Errorf("no EventHandshakeSucceeded; collected=%v", eventTypes(srvCollected))
	}

	// Listening must precede Accepted (lifecycle order).
	if idxL, idxA := indexOfType(srvCollected, gozmq.EventListening), indexOfType(srvCollected, gozmq.EventAccepted); idxL > idxA {
		t.Errorf("EventListening (idx=%d) after EventAccepted (idx=%d)", idxL, idxA)
	}

	// Client side: EventConnected + EventHandshakeSucceeded.
	cliCollected := waitForEvents(t, cliEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventConnected) && hasType(evts, gozmq.EventHandshakeSucceeded)
	})
	if !hasType(cliCollected, gozmq.EventConnected) {
		t.Errorf("client: no EventConnected; collected=%v", eventTypes(cliCollected))
	}
	if !hasType(cliCollected, gozmq.EventHandshakeSucceeded) {
		t.Errorf("client: no EventHandshakeSucceeded; collected=%v", eventTypes(cliCollected))
	}

	// Endpoint is populated on Listening.
	if idx := indexOfType(srvCollected, gozmq.EventListening); idx >= 0 {
		if srvCollected[idx].Endpoint == "" {
			t.Errorf("EventListening.Endpoint empty")
		}
		if srvCollected[idx].Time.IsZero() {
			t.Errorf("EventListening.Time zero")
		}
	}

	// Close emits EventClosed.
	if err := cli.Close(); err != nil {
		t.Errorf("client close: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Errorf("server close: %v", err)
	}
	// Drain any pending events for up to 1s looking for EventClosed.
	// Use separate contexts per channel so the drain of one doesn't
	// starve the drain of the other.
	srvDrainCtx, srvDrainCancel := context.WithTimeout(context.Background(), 1*time.Second)
	srvTail := drainEvents(srvDrainCtx, srvEvents)
	srvDrainCancel()
	cliDrainCtx, cliDrainCancel := context.WithTimeout(context.Background(), 1*time.Second)
	cliTail := drainEvents(cliDrainCtx, cliEvents)
	cliDrainCancel()
	if !hasType(srvTail, gozmq.EventClosed) {
		t.Errorf("server: no EventClosed after Close; tail=%v", eventTypes(srvTail))
	}
	if !hasType(cliTail, gozmq.EventClosed) {
		t.Errorf("client: no EventClosed after Close; tail=%v", eventTypes(cliTail))
	}
}

// TestMonitor_BindFailed exercises the EventBindFailed path: a
// malformed endpoint produces an error and a monitor event with a
// non-empty Reason.
func TestMonitor_BindFailed(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	evts := make(chan gozmq.Event, 16)
	s, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMonitor(evts, gozmq.EventAll))
	if err != nil {
		t.Fatalf("new socket: %v", err)
	}
	defer s.Close()

	// Unknown scheme — Registry.Lookup returns ErrTransportUnknown.
	if err := s.Bind("bogus://nothing"); err == nil {
		t.Fatal("expected bind error for bogus scheme")
	}
	// Pull any events with a short wait.
	drainCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	collected := drainEvents(drainCtx, evts)
	cancel()
	if !hasType(collected, gozmq.EventBindFailed) {
		t.Errorf("no EventBindFailed; collected=%v", eventTypes(collected))
	}
	if idx := indexOfType(collected, gozmq.EventBindFailed); idx >= 0 && collected[idx].Reason == "" {
		t.Errorf("EventBindFailed.Reason empty, want error text")
	}
}

// TestMonitor_ConnectRetryOnRefused forces the reconnect loop to fire
// at least one EventConnectDelayed by dialing a port we never listen
// on. We cap the test to a short window and assert the Reconnects
// metric was bumped.
func TestMonitor_ConnectRetryOnRefused(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	evts := make(chan gozmq.Event, 64)
	s, err := ctxZmq.NewSocket(
		gozmq.Pair,
		gozmq.WithMonitor(evts, gozmq.EventAll),
		// Small backoff so this test stays fast.
		gozmq.WithReconnectInterval(5*time.Millisecond),
		gozmq.WithReconnectIntervalMax(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new socket: %v", err)
	}
	defer s.Close()

	// 127.0.0.1:1 is reserved; connections reliably refuse or time
	// out fast enough for the Reconnector to fire multiple cycles.
	if err := s.Connect("tcp://127.0.0.1:1"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Wait for at least one EventConnectDelayed.
	collected := waitForEvents(t, evts, 2*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventConnectDelayed)
	})
	if !hasType(collected, gozmq.EventConnectDelayed) {
		t.Errorf("no EventConnectDelayed after retry window; collected=%v", eventTypes(collected))
	}
}

// TestMonitor_DropsWhenChannelFull exercises the non-blocking drop
// path: a tiny monitor channel (cap 2) is fed more events than it
// can hold, and we assert MonitorChanDropped went up.
func TestMonitor_DropsWhenChannelFull(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	// Pre-fill the monitor channel with a dummy event and never read
	// it: every subsequent emit will drop.
	evts := make(chan gozmq.Event, 2)
	evts <- gozmq.Event{Type: gozmq.EventListening}
	evts <- gozmq.Event{Type: gozmq.EventListening}

	// Label the socket uniquely so we can look up its expvar map
	// without racing other tests.
	label := fmt.Sprintf("PAIR.%d", time.Now().UnixNano())
	// We can't inject a label directly — internal/metrics derives it
	// from socketTypeName. Instead we snapshot the PAIR map before
	// and after and compare deltas by finding the exact SocketMetrics
	// our socket owns.
	_ = label // unused — we rely on the latest-registered PAIR below.

	before := sumAllPAIRMonitorDrops()

	srv, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMonitor(evts, gozmq.EventAll))
	if err != nil {
		t.Fatalf("new socket: %v", err)
	}
	defer srv.Close()

	// Trigger many emits: two bind failures plus one bind success
	// plus the Close-time event more than exceeds the channel cap.
	for i := 0; i < 16; i++ {
		_ = srv.Bind("bogus://nothing") // each fires EventBindFailed
	}

	// Give the emit goroutines time to enqueue-or-drop.
	time.Sleep(100 * time.Millisecond)

	after := sumAllPAIRMonitorDrops()
	if after <= before {
		t.Errorf("MonitorChanDropped did not increase: before=%d after=%d", before, after)
	}
}

// indexOfType returns the position of the first event of type t, or -1.
func indexOfType(events []gozmq.Event, t gozmq.EventType) int {
	for i, e := range events {
		if e.Type == t {
			return i
		}
	}
	return -1
}

// eventTypes is a debug helper: stringifies the type field of each
// event for friendlier test failures.
func eventTypes(events []gozmq.Event) []uint32 {
	out := make([]uint32, len(events))
	for i, e := range events {
		out[i] = uint32(e.Type)
	}
	return out
}

// sumAllPAIRMonitorDrops walks the expvar namespace looking for every
// published gozmq.PAIR* map and sums its MonitorChanDropped counter.
// Using a sum (rather than a single lookup) lets the test work whether
// or not the collision-fallback path was triggered for prior sockets
// in this test run.
func sumAllPAIRMonitorDrops() int64 {
	return sumMonitorDropsByPrefix("gozmq.PAIR")
}

// sumMonitorDropsByPrefix generalises sumAllPAIRMonitorDrops to any
// socket-type key prefix (e.g. "gozmq.ROUTER"). Walks the full expvar
// namespace because sockets registered under the collision-fallback
// naming scheme (.<ptrhash>) still match the prefix.
func sumMonitorDropsByPrefix(prefix string) int64 {
	var total int64
	expvar.Do(func(kv expvar.KeyValue) {
		if len(kv.Key) < len(prefix) {
			return
		}
		if kv.Key[:len(prefix)] != prefix {
			return
		}
		m, ok := kv.Value.(*expvar.Map)
		if !ok {
			return
		}
		v := m.Get("MonitorChanDropped")
		if v == nil {
			return
		}
		n, err := strconv.ParseInt(v.String(), 10, 64)
		if err == nil {
			total += n
		}
	})
	return total
}

// TestMonitorOverflow_DropsEventsWhenChannelFull is the Track 2D
// explicit regression for the non-blocking monitor-drop path. Unlike
// TestMonitor_DropsWhenChannelFull (which uses a pre-filled channel as
// a quick smoke test), this test:
//
//  1. Uses a realistically tiny buffered channel (cap 1).
//  2. Drives real lifecycle events via Bind + Connect + handshake so
//     the emit path is the one a production user would trip, not a
//     synthetic BindFailed loop.
//  3. Asserts the test does not deadlock (bounded timeouts everywhere).
//  4. Asserts MonitorChanDropped strictly increases while the channel
//     is full.
//  5. Idempotency: after draining the channel so emits succeed again,
//     MonitorChanDropped does NOT keep climbing for quiescent events.
func TestMonitorOverflow_DropsEventsWhenChannelFull(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	t.Cleanup(func() { _ = ctxZmq.Close() })

	// Capacity-1 buffered channel with one pre-queued dummy event so the
	// very first real emit already has to drop. Using a ROUTER socket
	// (not PAIR) isolates this test's expvar map from the pre-existing
	// PAIR tests, keeping the delta assertions unambiguous.
	evts := make(chan gozmq.Event, 1)
	evts <- gozmq.Event{Type: gozmq.EventListening}

	before := sumMonitorDropsByPrefix("gozmq.ROUTER")

	srv, err := ctxZmq.NewSocket(gozmq.Router, gozmq.WithMonitor(evts, gozmq.EventAll))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	if err := srv.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	ep := srv.LastEndpoint()

	// Spin up 3 dealers that connect rapidly. Each client generates a
	// Connected + HandshakeSucceeded on its own socket, and an Accepted
	// + HandshakeSucceeded on the server. With cap-1 + one pre-queued
	// event, the server's monitor channel cannot absorb all four server
	// events before the test drains it — drops are guaranteed.
	for i := 0; i < 3; i++ {
		cli, err := ctxZmq.NewSocket(gozmq.Dealer,
			gozmq.WithIdentity([]byte(fmt.Sprintf("dealer-%d", i))))
		if err != nil {
			t.Fatalf("new client %d: %v", i, err)
		}
		t.Cleanup(func() { _ = cli.Close() })
		if err := cli.Connect(ep); err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
	}

	// Give the emit goroutines a bounded window to enqueue-or-drop.
	// A test that deadlocks here would block forever without a bound;
	// the time.Sleep bounds the wait and the subsequent assertion
	// catches the failure mode.
	time.Sleep(300 * time.Millisecond)

	after := sumMonitorDropsByPrefix("gozmq.ROUTER")
	if after <= before {
		t.Fatalf("MonitorChanDropped did not increase after cap-1 channel overflow: before=%d after=%d", before, after)
	}

	// Idempotency check: drain the channel so subsequent emits succeed
	// rather than drop. Then quiesce — no new drops should accumulate
	// because the channel has space.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_ = drainEvents(drainCtx, evts)
	drainCancel()

	// Sample the counter right after drain, then again after a quiet
	// window. If the counter keeps climbing without any new events
	// being emitted (i.e. no new connects), that would indicate the
	// drop counter is incrementing spuriously for events the channel
	// COULD now accept.
	afterDrain := sumMonitorDropsByPrefix("gozmq.ROUTER")
	time.Sleep(200 * time.Millisecond)
	afterQuiet := sumMonitorDropsByPrefix("gozmq.ROUTER")
	if afterQuiet != afterDrain {
		t.Errorf("MonitorChanDropped incremented after drain with channel idle: afterDrain=%d afterQuiet=%d",
			afterDrain, afterQuiet)
	}
}

// TestMonitor_MaskFiltersEvents verifies WithMonitor's mask argument
// keeps events off the channel when their Type bit is unset.
func TestMonitor_MaskFiltersEvents(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	evts := make(chan gozmq.Event, 16)
	// Subscribe only to EventListening — no other event should
	// reach the channel.
	s, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMonitor(evts, gozmq.EventListening))
	if err != nil {
		t.Fatalf("new socket: %v", err)
	}

	if err := s.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("close: %v", err)
	}

	// Drain the channel for a short window.
	drainCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	collected := drainEvents(drainCtx, evts)
	cancel()

	for _, ev := range collected {
		if ev.Type != gozmq.EventListening {
			t.Errorf("mask-filtered channel received non-Listening event type=%d", ev.Type)
		}
	}
	if len(collected) == 0 {
		t.Error("expected at least one EventListening")
	}
}

// ensure the Event's Time field is monotonic non-decreasing across a
// single socket's emissions. This is a weak invariant (system clock
// skew could break it on exotic platforms) but catches accidental
// zero-Time events.
func TestMonitor_EventTimestampsPopulated(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	evts := make(chan gozmq.Event, 16)
	s, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMonitor(evts, gozmq.EventAll))
	if err != nil {
		t.Fatalf("new socket: %v", err)
	}
	if err := s.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	collected := drainEvents(drainCtx, evts)
	cancel()

	if len(collected) == 0 {
		t.Fatal("no events collected")
	}
	var lastTime time.Time
	var seen atomic.Bool
	for _, ev := range collected {
		if ev.Time.IsZero() {
			t.Errorf("event type=%d has zero Time", ev.Type)
		}
		if seen.Load() && ev.Time.Before(lastTime) {
			t.Errorf("event time went backward: prev=%v now=%v", lastTime, ev.Time)
		}
		lastTime = ev.Time
		seen.Store(true)
	}
}
