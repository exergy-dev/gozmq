package gozmq_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

// TestSubscribe_RefcountedBehavior verifies that SUB's Subscribe/Unsubscribe
// maintain a refcount per prefix — two Subscribe calls require two matching
// Unsubscribe calls before the filter is actually removed and the upstream
// CANCEL is forwarded.
//
// Shape:
//
//  1. PUB bind / SUB connect on inproc.
//  2. SUB.Subscribe("a") twice.
//  3. PUB.Send("a:1") — must be received (refcount 2).
//  4. SUB.Unsubscribe("a") once. PUB.Send("a:2") — must STILL be received
//     (refcount 1 ≠ 0 → filter intact, upstream not yet cancelled).
//  5. SUB.Unsubscribe("a") again. PUB.Send("a:3") — must NOT be received
//     within a bounded timeout (refcount 0 → filter removed, CANCEL
//     forwarded to PUB so PUB drops messages too).
func TestSubscribe_RefcountedBehavior(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	t.Cleanup(func() { _ = ctxZmq.Close() })

	endpoint := fmt.Sprintf("inproc://sub-refcount-%d", time.Now().UnixNano())

	pub, err := ctxZmq.NewSocket(gozmq.Pub)
	if err != nil {
		t.Fatalf("new pub: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	if err := pub.Bind(endpoint); err != nil {
		t.Fatalf("pub bind: %v", err)
	}

	sub, err := ctxZmq.NewSocket(gozmq.Sub)
	if err != nil {
		t.Fatalf("new sub: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	// Two Subscribe calls for the same prefix — refcount goes 0 → 1 → 2.
	if err := sub.Subscribe([]byte("a")); err != nil {
		t.Fatalf("subscribe 1: %v", err)
	}
	if err := sub.Subscribe([]byte("a")); err != nil {
		t.Fatalf("subscribe 2: %v", err)
	}

	if err := sub.Connect(endpoint); err != nil {
		t.Fatalf("sub connect: %v", err)
	}

	// Slow-joiner: give the SUBSCRIBE command time to reach the PUB so
	// it starts forwarding to this peer.
	time.Sleep(150 * time.Millisecond)

	// Step 3: both refs held — should receive.
	sendAndExpect(t, pub, sub, "a:1", true)

	// Step 4: Unsubscribe once. Refcount 2 → 1 — filter intact locally,
	// no CANCEL yet upstream.
	if err := sub.Unsubscribe([]byte("a")); err != nil {
		t.Fatalf("unsubscribe 1: %v", err)
	}
	sendAndExpect(t, pub, sub, "a:2", true)

	// Step 5: Unsubscribe again. Refcount 1 → 0 — filter removed, CANCEL
	// forwarded to PUB. PUB.Send still enqueues but PUB drops before the
	// wire. SUB receives nothing within the bounded timeout.
	if err := sub.Unsubscribe([]byte("a")); err != nil {
		t.Fatalf("unsubscribe 2: %v", err)
	}
	// Let the CANCEL propagate upstream before we send.
	time.Sleep(100 * time.Millisecond)
	sendAndExpect(t, pub, sub, "a:3", false)
}

// sendAndExpect emits a single topic-prefixed message on pub and either
// expects it to arrive on sub (want=true) or confirms it does not within
// a bounded 500ms window (want=false). Fails the test on mismatch.
func sendAndExpect(t *testing.T, pub, sub *gozmq.Socket, payload string, want bool) {
	t.Helper()
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer sendCancel()
	if err := pub.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte(payload))); err != nil {
		t.Fatalf("pub send %q: %v", payload, err)
	}
	recvCtx, recvCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer recvCancel()
	got, err := sub.RecvMsg(recvCtx)
	if want {
		if err != nil {
			t.Fatalf("expected to receive %q, got err=%v", payload, err)
		}
		if string(got.Bytes()) != payload {
			t.Fatalf("received %q want %q", got.Bytes(), payload)
		}
	} else {
		if err == nil {
			t.Fatalf("expected NOT to receive %q (filter removed), but got %q", payload, got.Bytes())
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			// Closed pipes / context cancellations are acceptable
			// non-delivery outcomes too; log but don't fail.
			t.Logf("non-delivery confirmed via %v", err)
		}
	}
}

// TestRouterMandatory_UnroutableReturnsError verifies the two modes of
// ROUTER's unknown-identity Send handling:
//
//	Case A (default, mandatory=false): Send(unknown) succeeds silently,
//	  returning nil. The test bounds the call so a regression to
//	  blocking-instead-of-dropping surfaces as a timeout failure.
//	Case B (WithRouterMandatory(true)): Send(unknown) returns
//	  ErrRouteUnknown per errors.Is.
func TestRouterMandatory_UnroutableReturnsError(t *testing.T) {
	t.Run("DefaultDropsSilently", func(t *testing.T) {
		ctxZmq := gozmq.NewContext()
		t.Cleanup(func() { _ = ctxZmq.Close() })

		router, err := ctxZmq.NewSocket(gozmq.Router)
		if err != nil {
			t.Fatalf("new router: %v", err)
		}
		t.Cleanup(func() { _ = router.Close() })

		// No peers attached — every identity is unknown.
		sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		msg := gozmq.NewMsgFromFrames([]byte("nobody-home"), []byte("payload"))
		err = router.SendMsg(sendCtx, msg)
		if err != nil {
			t.Fatalf("expected silent drop (nil), got %v", err)
		}
	})

	t.Run("MandatoryReturnsErrRouteUnknown", func(t *testing.T) {
		ctxZmq := gozmq.NewContext()
		t.Cleanup(func() { _ = ctxZmq.Close() })

		router, err := ctxZmq.NewSocket(gozmq.Router, gozmq.WithRouterMandatory(true))
		if err != nil {
			t.Fatalf("new router: %v", err)
		}
		t.Cleanup(func() { _ = router.Close() })

		sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		msg := gozmq.NewMsgFromFrames([]byte("nobody-home"), []byte("payload"))
		err = router.SendMsg(sendCtx, msg)
		if err == nil {
			t.Fatal("expected ErrRouteUnknown with WithRouterMandatory(true), got nil")
		}
		if !errors.Is(err, gozmq.ErrRouteUnknown) {
			t.Fatalf("expected errors.Is(err, ErrRouteUnknown), got %v", err)
		}
	})
}

// TestReq_FSMViolations exercises the REQ socket's strict lock-step FSM:
//   - Send-after-Send without Recv: ErrFSMViolation.
//   - Recv without prior Send: ErrFSMViolation.
//   - Full Send→Recv→Send→Recv round-trips: no errors.
//
// The REQ↔REP pairing uses inproc for deterministic scheduling.
func TestReq_FSMViolations(t *testing.T) {
	t.Run("SendTwiceWithoutRecv", func(t *testing.T) {
		ctxZmq := gozmq.NewContext()
		t.Cleanup(func() { _ = ctxZmq.Close() })

		endpoint := fmt.Sprintf("inproc://req-fsm-double-send-%d", time.Now().UnixNano())

		rep, err := ctxZmq.NewSocket(gozmq.Rep)
		if err != nil {
			t.Fatalf("new rep: %v", err)
		}
		t.Cleanup(func() { _ = rep.Close() })
		if err := rep.Bind(endpoint); err != nil {
			t.Fatalf("rep bind: %v", err)
		}

		req, err := ctxZmq.NewSocket(gozmq.Req)
		if err != nil {
			t.Fatalf("new req: %v", err)
		}
		t.Cleanup(func() { _ = req.Close() })
		if err := req.Connect(endpoint); err != nil {
			t.Fatalf("req connect: %v", err)
		}

		sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// First send transitions state canSend → canRecv.
		if err := req.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte("first"))); err != nil {
			t.Fatalf("first send: %v", err)
		}

		// Drain the REP side so the REP engine is ready when we retry,
		// but we deliberately do NOT call req.RecvMsg — we want to test
		// the second Send against the still-canRecv state.
		if _, err := rep.RecvMsg(sendCtx); err != nil {
			t.Fatalf("rep recv first: %v", err)
		}

		// Second send without a Recv: FSM violation.
		err = req.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte("second")))
		if err == nil {
			t.Fatal("expected ErrFSMViolation on second REQ send without Recv, got nil")
		}
		if !errors.Is(err, gozmq.ErrFSMViolation) {
			t.Fatalf("expected errors.Is(err, ErrFSMViolation), got %v", err)
		}
	})

	t.Run("RecvWithoutSend", func(t *testing.T) {
		ctxZmq := gozmq.NewContext()
		t.Cleanup(func() { _ = ctxZmq.Close() })

		// No peer needed — the FSM check fires before any transport
		// interaction.
		req, err := ctxZmq.NewSocket(gozmq.Req)
		if err != nil {
			t.Fatalf("new req: %v", err)
		}
		t.Cleanup(func() { _ = req.Close() })

		recvCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err = req.RecvMsg(recvCtx)
		if err == nil {
			t.Fatal("expected ErrFSMViolation on REQ Recv without prior Send, got nil")
		}
		if !errors.Is(err, gozmq.ErrFSMViolation) {
			t.Fatalf("expected errors.Is(err, ErrFSMViolation), got %v", err)
		}
	})

	t.Run("HappyPathRoundTrip", func(t *testing.T) {
		ctxZmq := gozmq.NewContext()
		t.Cleanup(func() { _ = ctxZmq.Close() })

		endpoint := fmt.Sprintf("inproc://req-fsm-roundtrip-%d", time.Now().UnixNano())

		rep, err := ctxZmq.NewSocket(gozmq.Rep)
		if err != nil {
			t.Fatalf("new rep: %v", err)
		}
		t.Cleanup(func() { _ = rep.Close() })
		if err := rep.Bind(endpoint); err != nil {
			t.Fatalf("rep bind: %v", err)
		}

		req, err := ctxZmq.NewSocket(gozmq.Req)
		if err != nil {
			t.Fatalf("new req: %v", err)
		}
		t.Cleanup(func() { _ = req.Close() })
		if err := req.Connect(endpoint); err != nil {
			t.Fatalf("req connect: %v", err)
		}

		callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		for i := 0; i < 2; i++ {
			payload := []byte(fmt.Sprintf("req-%d", i))
			if err := req.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
				t.Fatalf("req send %d: %v", i, err)
			}
			got, err := rep.RecvMsg(callCtx)
			if err != nil {
				t.Fatalf("rep recv %d: %v", i, err)
			}
			if string(got.Bytes()) != string(payload) {
				t.Fatalf("rep got %q want %q", got.Bytes(), payload)
			}
			reply := []byte(fmt.Sprintf("rep-%d", i))
			if err := rep.SendMsg(callCtx, gozmq.NewMsgFromBytes(reply)); err != nil {
				t.Fatalf("rep send %d: %v", i, err)
			}
			back, err := req.RecvMsg(callCtx)
			if err != nil {
				t.Fatalf("req recv %d: %v", i, err)
			}
			if string(back.Bytes()) != string(reply) {
				t.Fatalf("req got %q want %q", back.Bytes(), reply)
			}
		}
	})
}
