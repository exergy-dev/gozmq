package gozmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
)

// TestReq_FSMViolations exercises the REQ socket's strict lock-step FSM:
//   - Send-after-Send without Recv: ErrFSMViolation.
//   - Recv without prior Send: ErrFSMViolation.
//   - Full Send→Recv→Send→Recv round-trips: no errors.
//
// The REQ↔REP pairing uses inproc for deterministic scheduling.
func TestReq_FSMViolations(t *testing.T) {
	t.Parallel()
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
