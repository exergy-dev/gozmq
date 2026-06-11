package gozmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rkm/gozmq"
	"github.com/stretchr/testify/require"
)

// TestSubscribe_RefcountedBehavior verifies that SUB's Subscribe/Unsubscribe
// maintain a refcount per prefix — two Subscribe calls require two matching
// Unsubscribe calls before the filter is actually removed and the upstream
// CANCEL is forwarded.
//
// Shape:
//
//  1. PUB bind / SUB connect on inproc, with TWO Subscribe("a") calls
//     (refcount 0 → 1 → 2) and the subscription propagation awaited by
//     newPubSubPair.
//  2. PUB.Send("a:1") — must be received (refcount 2).
//  3. SUB.Unsubscribe("a") once. PUB.Send("a:2") — must STILL be received
//     (refcount 1 ≠ 0 → filter intact, upstream not yet cancelled).
//  4. SUB.Unsubscribe("a") again. PUB.Send("a:3") — must NOT be received
//     within a bounded timeout (refcount 0 → filter removed locally and
//     CANCEL forwarded to PUB).
func TestSubscribe_RefcountedBehavior(t *testing.T) {
	endpoint := fmt.Sprintf("inproc://sub-refcount-%d", time.Now().UnixNano())
	// The same prefix twice — refcount goes 0 → 1 → 2.
	_, pub, sub := newPubSubPair(t, endpoint, [][]byte{[]byte("a"), []byte("a")}, nil, nil)

	// Step 2: both refs held — should receive.
	sendAndExpect(t, pub, sub, "a:1", true)

	// Step 3: Unsubscribe once. Refcount 2 → 1 — filter intact locally,
	// no CANCEL yet upstream.
	require.NoError(t, sub.Unsubscribe([]byte("a")), "unsubscribe 1")
	sendAndExpect(t, pub, sub, "a:2", true)

	// Step 4: Unsubscribe again. Refcount 1 → 0 — the local filter is
	// removed immediately (so delivery stops regardless of when the
	// upstream CANCEL lands at the PUB). SUB receives nothing within the
	// bounded timeout.
	require.NoError(t, sub.Unsubscribe([]byte("a")), "unsubscribe 2")
	sendAndExpect(t, pub, sub, "a:3", false)
}

// sendAndExpect emits a single topic-prefixed message on pub and either
// expects it to arrive on sub (want=true) or confirms it does not within
// a bounded 500ms window (want=false). Fails the test on mismatch.
func sendAndExpect(t *testing.T, pub, sub *gozmq.Socket, payload string, want bool) {
	t.Helper()
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer sendCancel()
	require.NoError(t, pub.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte(payload))), "pub send %q", payload)
	recvCtx, recvCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer recvCancel()
	got, err := sub.RecvMsg(recvCtx)
	if want {
		require.NoError(t, err, "expected to receive %q", payload)
		require.Equal(t, payload, string(got.Bytes()))
		got.Release()
		return
	}
	if err == nil {
		require.Fail(t, "expected NOT to receive", "payload %q (filter removed), but got %q", payload, got.Bytes())
	}
	// Non-delivery must surface as the recv context timing out — any
	// other error (closed pipe, FSM violation, ...) is a real failure.
	require.ErrorIs(t, err, context.DeadlineExceeded, "non-delivery of %q", payload)
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
		// No peers attached — every identity is unknown.
		_, router := newTestSocket(t, gozmq.Router)

		sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		msg := gozmq.NewMsgFromFrames([]byte("nobody-home"), []byte("payload"))
		require.NoError(t, router.SendMsg(sendCtx, msg), "expected silent drop (nil)")
	})

	t.Run("MandatoryReturnsErrRouteUnknown", func(t *testing.T) {
		_, router := newTestSocket(t, gozmq.Router, gozmq.WithRouterMandatory(true))

		sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		msg := gozmq.NewMsgFromFrames([]byte("nobody-home"), []byte("payload"))
		err := router.SendMsg(sendCtx, msg)
		require.Error(t, err, "expected ErrRouteUnknown with WithRouterMandatory(true)")
		require.ErrorIs(t, err, gozmq.ErrRouteUnknown)
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
		endpoint := fmt.Sprintf("inproc://req-fsm-double-send-%d", time.Now().UnixNano())
		_, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, endpoint, nil, nil)

		sendCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// First send transitions state canSend → canRecv.
		require.NoError(t, req.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte("first"))), "first send")

		// Drain the REP side so the REP engine is ready when we retry,
		// but we deliberately do NOT call req.RecvMsg — we want to test
		// the second Send against the still-canRecv state.
		first, err := rep.RecvMsg(sendCtx)
		require.NoError(t, err, "rep recv first")
		first.Release()

		// Second send without a Recv: FSM violation.
		err = req.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte("second")))
		require.Error(t, err, "expected ErrFSMViolation on second REQ send without Recv")
		require.ErrorIs(t, err, gozmq.ErrFSMViolation)
	})

	t.Run("RecvWithoutSend", func(t *testing.T) {
		// No peer needed — the FSM check fires before any transport
		// interaction.
		_, req := newTestSocket(t, gozmq.Req)

		recvCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := req.RecvMsg(recvCtx)
		require.Error(t, err, "expected ErrFSMViolation on REQ Recv without prior Send")
		require.ErrorIs(t, err, gozmq.ErrFSMViolation)
	})

	t.Run("HappyPathRoundTrip", func(t *testing.T) {
		endpoint := fmt.Sprintf("inproc://req-fsm-roundtrip-%d", time.Now().UnixNano())
		_, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, endpoint, nil, nil)

		callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		for i := 0; i < 2; i++ {
			payload := []byte(fmt.Sprintf("req-%d", i))
			require.NoError(t, req.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)), "req send %d", i)
			got, err := rep.RecvMsg(callCtx)
			require.NoError(t, err, "rep recv %d", i)
			require.Equal(t, string(payload), string(got.Bytes()))
			got.Release()
			reply := []byte(fmt.Sprintf("rep-%d", i))
			require.NoError(t, rep.SendMsg(callCtx, gozmq.NewMsgFromBytes(reply)), "rep send %d", i)
			back, err := req.RecvMsg(callCtx)
			require.NoError(t, err, "req recv %d", i)
			require.Equal(t, string(reply), string(back.Bytes()))
			back.Release()
		}
	})
}
