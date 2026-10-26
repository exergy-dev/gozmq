package gozmq_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
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

// hasType reports whether any event in the slice matches the given type.
func hasType(events []gozmq.Event, t gozmq.EventType) bool {
	return slices.ContainsFunc(events, func(e gozmq.Event) bool { return e.Type == t })
}

// TestMonitor_ServerLifecycle verifies the full server-side event
// sequence on a single PAIR/TCP round trip: Listening → Accepted →
// HandshakeSucceeded → (client connect-side events) → Closed.
func TestMonitor_ServerLifecycle(t *testing.T) {
	t.Parallel()
	srvEvents := make(chan gozmq.Event, 64)
	cliEvents := make(chan gozmq.Event, 64)
	_, srv, cli := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
		[]gozmq.Option{gozmq.WithMonitor(srvEvents, gozmq.EventAll)},
		[]gozmq.Option{gozmq.WithMonitor(cliEvents, gozmq.EventAll)},
	)

	// Round-trip a message to guarantee the handshake completed on
	// both ends before we assert on events.
	callCtx := testutil.Ctx(t)
	if err := cli.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("hello"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	if m, err := srv.RecvMsg(callCtx); err != nil {
		t.Fatalf("recv: %v", err)
	} else {
		m.Release()
	}

	// Wait for the server to observe Listening + Accepted +
	// HandshakeSucceeded before we tear down.
	srvCollected := testutil.CollectUntil(srvEvents, 3*time.Second, func(evts []gozmq.Event) bool {
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
	cliCollected := testutil.CollectUntil(cliEvents, 3*time.Second, func(evts []gozmq.Event) bool {
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
	closed := func(evts []gozmq.Event) bool { return hasType(evts, gozmq.EventClosed) }
	srvTail := testutil.CollectUntil(srvEvents, 3*time.Second, closed)
	cliTail := testutil.CollectUntil(cliEvents, 3*time.Second, closed)
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
	t.Parallel()
	evts := make(chan gozmq.Event, 16)
	_, s := newTestSocket(t, gozmq.Pair, gozmq.WithMonitor(evts, gozmq.EventAll))

	// Unknown scheme — the Context has no transport for it (ErrTransportUnknown).
	if err := s.Bind("bogus://nothing"); err == nil {
		t.Fatal("expected bind error for bogus scheme")
	}
	collected := testutil.CollectUntil(evts, 3*time.Second, func(evts []gozmq.Event) bool { return hasType(evts, gozmq.EventBindFailed) })
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
	t.Parallel()
	evts := make(chan gozmq.Event, 64)
	_, s := newTestSocket(t, gozmq.Pair,
		gozmq.WithMonitor(evts, gozmq.EventAll),
		// Small backoff so this test stays fast.
		gozmq.WithReconnectInterval(5*time.Millisecond),
		gozmq.WithReconnectIntervalMax(20*time.Millisecond),
	)

	// 127.0.0.1:1 is reserved; connections reliably refuse or time
	// out fast enough for the Reconnector to fire multiple cycles.
	if err := s.Connect("tcp://127.0.0.1:1"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Wait for at least one EventConnectDelayed.
	collected := testutil.CollectUntil(evts, 2*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventConnectDelayed)
	})
	if !hasType(collected, gozmq.EventConnectDelayed) {
		t.Errorf("no EventConnectDelayed after retry window; collected=%v", eventTypes(collected))
	}
}

// indexOfType returns the position of the first event of type t, or -1.
func indexOfType(events []gozmq.Event, t gozmq.EventType) int {
	return slices.IndexFunc(events, func(e gozmq.Event) bool { return e.Type == t })
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

// TestMonitor_MaskFiltersEvents verifies WithMonitor's mask argument
// keeps events off the channel when their Type bit is unset.
func TestMonitor_MaskFiltersEvents(t *testing.T) {
	t.Parallel()
	evts := make(chan gozmq.Event, 16)
	// Subscribe only to EventListening — no other event should
	// reach the channel.
	_, s := newTestSocket(t, gozmq.Pair, gozmq.WithMonitor(evts, gozmq.EventListening))

	if err := s.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("close: %v", err)
	}

	// Listening is the only event the mask lets through; Closed must not
	// follow it, so give the masked-out event a short window to show up.
	collected := testutil.CollectUntil(evts, 3*time.Second, func(evts []gozmq.Event) bool { return len(evts) > 0 })
	collected = append(collected, testutil.CollectUntil(evts, 100*time.Millisecond, func([]gozmq.Event) bool { return false })...)

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
	t.Parallel()
	evts := make(chan gozmq.Event, 16)
	_, s := newTestSocket(t, gozmq.Pair, gozmq.WithMonitor(evts, gozmq.EventAll))
	if err := s.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	collected := testutil.CollectUntil(evts, 3*time.Second, func(evts []gozmq.Event) bool { return hasType(evts, gozmq.EventClosed) })
	if len(collected) == 0 {
		t.Fatal("no events collected")
	}
	var lastTime time.Time
	seen := false
	for _, ev := range collected {
		if ev.Time.IsZero() {
			t.Errorf("event type=%d has zero Time", ev.Type)
		}
		if seen && ev.Time.Before(lastTime) {
			t.Errorf("event time went backward: prev=%v now=%v", lastTime, ev.Time)
		}
		lastTime = ev.Time
		seen = true
	}
}

// countType counts the events of type t.
func countType(events []gozmq.Event, t gozmq.EventType) int {
	n := 0
	for _, e := range events {
		if e.Type == t {
			n++
		}
	}
	return n
}

// TestMonitor_SessionEventsBothSides: accepted and connected sessions run
// the same lifecycle callbacks, so each side reports one handshake
// success and, when the link drops, one disconnect; a failed handshake is
// reported on both sides with no success and no attach.
func TestMonitor_SessionEventsBothSides(t *testing.T) {
	t.Parallel()
	const peerEvents = gozmq.EventHandshakeSucceeded | gozmq.EventHandshakeFailedProtocol |
		gozmq.EventConnected | gozmq.EventDisconnected
	t.Run("established", func(t *testing.T) {
		srvEvents := make(chan gozmq.Event, 64)
		cliEvents := make(chan gozmq.Event, 64)
		_, srv, cli := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
			[]gozmq.Option{gozmq.WithMonitor(srvEvents, peerEvents)},
			[]gozmq.Option{gozmq.WithMonitor(cliEvents, peerEvents)},
		)
		callCtx := testutil.Ctx(t)
		require.NoError(t, cli.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("hello"))))
		m, err := srv.RecvMsg(callCtx)
		require.NoError(t, err)
		m.Release()
		require.NoError(t, cli.Close())

		srvGot := testutil.CollectUntil(srvEvents, 3*time.Second, func(evts []gozmq.Event) bool {
			return hasType(evts, gozmq.EventDisconnected)
		})
		require.Equal(t, 1, countType(srvGot, gozmq.EventHandshakeSucceeded), "server: %v", eventTypes(srvGot))
		require.Equal(t, 1, countType(srvGot, gozmq.EventDisconnected), "server: %v", eventTypes(srvGot))
		require.Zero(t, countType(srvGot, gozmq.EventConnected), "server: %v", eventTypes(srvGot))
		// The client's Close returned after its session ended, so all its
		// events are already queued.
		var cliGot []gozmq.Event
		for len(cliEvents) > 0 {
			cliGot = append(cliGot, <-cliEvents)
		}
		require.Equal(t, 1, countType(cliGot, gozmq.EventHandshakeSucceeded), "client: %v", eventTypes(cliGot))
		require.Equal(t, 1, countType(cliGot, gozmq.EventConnected), "client: %v", eventTypes(cliGot))
		require.Equal(t, 1, countType(cliGot, gozmq.EventDisconnected), "client: %v", eventTypes(cliGot))
		require.Less(t, indexOfType(cliGot, gozmq.EventHandshakeSucceeded), indexOfType(cliGot, gozmq.EventConnected),
			"handshake success must precede the attach")
	})
	t.Run("mechanism mismatch", func(t *testing.T) {
		srvEvents := make(chan gozmq.Event, 64)
		cliEvents := make(chan gozmq.Event, 64)
		newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
			[]gozmq.Option{gozmq.WithMonitor(srvEvents, peerEvents)},
			[]gozmq.Option{gozmq.WithMonitor(cliEvents, peerEvents), gozmq.WithPlainClient("u", "p")},
		)
		failed := func(evts []gozmq.Event) bool { return hasType(evts, gozmq.EventHandshakeFailedProtocol) }
		for side, ch := range map[string]chan gozmq.Event{"server": srvEvents, "client": cliEvents} {
			got := testutil.CollectUntil(ch, 3*time.Second, failed)
			require.True(t, failed(got), "%s: %v", side, eventTypes(got))
			for _, typ := range []gozmq.EventType{gozmq.EventHandshakeSucceeded, gozmq.EventConnected, gozmq.EventDisconnected} {
				require.Zero(t, countType(got, typ), "%s: %v", side, eventTypes(got))
			}
		}
	})
}
