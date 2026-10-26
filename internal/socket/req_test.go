package socket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// TestReq_PrependsEmptyDelimiter asserts that REQ.Send wraps the caller's
// body with a leading empty delimiter frame.
func TestReq_PrependsEmptyDelimiter(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("hello"))), "send")
	got, err := b.RecvOr(ctx, nil)
	require.NoError(t, err, "peer recv")
	require.Equal(t, 2, got.NumFrames(), "frames (empty + body)")
	require.Empty(t, got.Frames()[0], "leading frame should be empty delimiter")
	require.Equal(t, []byte("hello"), got.Frames()[1])
}

// TestReq_StripsEmptyDelimiterOnRecv asserts that REQ.Recv strips the
// leading empty frame the REP peer prepends to its reply.
func TestReq_StripsEmptyDelimiterOnRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	// First send so the FSM moves to canRecv with a sticky pipe.
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))), "send")
	// Drain the request from the peer side so we keep pipe clean.
	_, err := b.RecvOr(ctx, nil)
	require.NoError(t, err, "drain request")

	// Peer-side REP-shape reply: [empty, "reply"].
	reply := msg.FromFrames([]byte{}, []byte("reply"))
	require.NoError(t, b.Send(ctx, reply), "peer send")
	got, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, 1, got.NumFrames())
	require.Equal(t, []byte("reply"), got.Bytes())
}

// TestReq_DoubleSendFSMViolation asserts that calling Send twice
// without a matching Recv returns ErrFSMViolation.
func TestReq_DoubleSendFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("1"))), "first send")
	err := eng.Send(ctx, msg.FromBytes([]byte("2")))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "second send")
}

// TestReq_ConcurrentSendsOneWins: the canSend check and the transition
// must be atomic, so of several concurrent Sends exactly one puts a
// request in flight and the rest fail with ErrFSMViolation. The pipe
// starts full so every Send that passes the check parks, widening the
// race window.
func TestReq_ConcurrentSendsOneWins(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	require.NoError(t, a.Send(context.Background(), msg.FromBytes([]byte("fill"))))
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	const n = 8
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			<-start
			errs <- eng.Send(ctx, msg.FromBytes([]byte("q")))
		}()
	}
	close(start)
	// Give every Send time to get past the state check before the peer
	// frees pipe capacity.
	time.Sleep(50 * time.Millisecond)

	// Collect results while the peer drains, so the winner (and, without
	// the atomic reservation, any extra in-flight requests) can land.
	var ok, fsm, requests int
	deadline := time.After(time.Second)
	for ok+fsm < n {
		rctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		if m, err := b.RecvOr(rctx, nil); err == nil && m.NumFrames() == 2 {
			requests++
		}
		cancel()
		for drained := false; !drained; {
			select {
			case err := <-errs:
				switch {
				case err == nil:
					ok++
				case errors.Is(err, zmqerr.ErrFSMViolation):
					fsm++
				default:
					require.Failf(t, "unexpected send error", "%v", err)
				}
			default:
				drained = true
			}
		}
		select {
		case <-deadline:
			require.Failf(t, "sends did not finish", "ok=%d fsm=%d", ok, fsm)
		default:
		}
	}
	require.Equal(t, 1, ok, "exactly one Send should win")
	require.Equal(t, n-1, fsm)
	require.LessOrEqual(t, requests, 1, "only one request may be in flight")
}

// TestReq_ConcurrentRecvFSMViolation: a second Recv while one is already
// awaiting the reply fails fast instead of racing it for the reply.
func TestReq_ConcurrentRecvFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))))
	_, err := b.RecvOr(ctx, nil)
	require.NoError(t, err)

	rc1 := testutil.AsyncErr(func() (*msg.Msg, error) { return eng.Recv(ctx) })
	time.Sleep(20 * time.Millisecond)
	_, err = eng.Recv(ctx)
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "concurrent recv")

	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("a"))))
	r := testutil.RequireRecv(t, rc1, 5*time.Second, "first recv did not get the reply")
	require.NoError(t, r.Err, "first recv failed")
	require.Equal(t, "a", string(r.V.Bytes()))
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q2"))), "FSM back to canSend")
}

// TestReq_SendCtxCancelRestoresCanSend: a Send abandoned by its context
// must release the in-flight reservation so the next Send can proceed.
func TestReq_SendCtxCancelRestoresCanSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	require.NoError(t, a.Send(context.Background(), msg.FromBytes([]byte("fill"))))
	mustAttach(t, eng, a)

	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, eng.Send(sctx, msg.FromBytes([]byte("q"))), context.DeadlineExceeded)

	ctx := testutil.Ctx(t)
	_, err := b.RecvOr(ctx, nil) // drain the filler
	require.NoError(t, err)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))))
}

// TestReq_RecvBeforeSendFSMViolation asserts that Recv without a prior
// Send returns ErrFSMViolation.
func TestReq_RecvBeforeSendFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	_, err := eng.Recv(context.Background())
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation)
}

// TestReq_DoubleRecvFSMViolation asserts that a second Recv after one
// successful round-trip, without an intervening Send, returns FSM
// violation.
func TestReq_DoubleRecvFSMViolation(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))), "send")
	// Drain request; peer replies.
	_, err := b.RecvOr(ctx, nil)
	require.NoError(t, err, "drain")
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("a"))), "peer send")
	_, err = eng.Recv(ctx)
	require.NoError(t, err, "first recv")
	// A second Recv with no Send between must fail FSM.
	_, err = eng.Recv(ctx)
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "second recv")
}

// TestReq_Rep_RoundTrip_Direct wires two engines (REQ + REP) directly via
// a pipe pair (no session layer) and asserts a request/reply alternation
// survives multiple rounds.
func TestReq_Rep_RoundTrip_Direct(t *testing.T) {
	t.Parallel()
	reqEng := socket.NewReqEngine()
	repEng := socket.NewRepEngine()
	defer reqEng.Close()
	defer repEng.Close()

	// Pipe pair: one end attached to REQ, the other to REP.
	reqSide, repSide := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(reqSide, repSide)
	mustAttach(t, reqEng, reqSide)
	mustAttach(t, repEng, repSide)

	ctx := testutil.Ctx(t)

	const rounds = 4
	for i := 0; i < rounds; i++ {
		body := []byte{byte('A' + i)}
		// REQ → REP
		errCh := testutil.Async(func() error { return reqEng.Send(ctx, msg.FromBytes(body)) })

		got, err := repEng.Recv(ctx)
		require.NoError(t, err, "rep recv %d", i)
		require.NoError(t, <-errCh, "req send %d", i)
		require.Equal(t, 1, got.NumFrames())
		require.Equal(t, body, got.Bytes())

		// REP → REQ
		reply := append([]byte("r:"), body...)
		require.NoError(t, repEng.Send(ctx, msg.FromBytes(reply)), "rep send %d", i)
		gotReply, err := reqEng.Recv(ctx)
		require.NoError(t, err, "req recv %d", i)
		require.Equal(t, 1, gotReply.NumFrames())
		require.Equal(t, reply, gotReply.Bytes())
	}
}

// TestReq_Close_UnblocksRecv asserts Close unblocks a parked Recv.
func TestReq_Close_UnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()

	a, b := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := context.Background()
	// Send to move to canRecv, then drain so the peer side is idle.
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))), "send")
	_, err := b.RecvOr(ctx, nil)
	require.NoError(t, err, "drain")

	err = testutil.RequireUnblocks(t, func() error {
		_, err := eng.Recv(ctx)
		return err
	}, func() { require.NoError(t, eng.Close(), "close") })
	require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
}

// TestReq_Send_WaitsForPeer asserts that Send parks when no peer is
// attached and wakes when one arrives.
func TestReq_Send_WaitsForPeer(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	ctx := testutil.Ctx(t)
	a, b := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(a, b)

	err := testutil.RequireUnblocks(t,
		func() error { return eng.Send(ctx, msg.FromBytes([]byte("q"))) },
		func() { mustAttach(t, eng, a) })
	require.NoError(t, err, "send")
	// Drain the request so the pipe doesn't hold a message at close.
	_, err = b.RecvOr(ctx, nil)
	require.NoError(t, err, "drain")
}

// TestReq_RecvCtxErrorKeepsAwaitingReply pins the libzmq EAGAIN semantic:
// a Recv that fails with a context error must NOT flip the FSM or drop
// the sticky pipe — a retried Recv still gets the in-flight reply.
func TestReq_RecvCtxErrorKeepsAwaitingReply(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q"))), "send")
	_, err := b.RecvOr(ctx, nil)
	require.NoError(t, err, "drain request")

	// Recv times out before the reply exists.
	shortCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, err = eng.Recv(shortCtx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "first recv")

	// The reply lands late; a retried Recv must deliver it (previously
	// the failed Recv reset the FSM to canSend and this returned
	// ErrFSMViolation / lost the reply).
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("late-but-mine"))), "peer reply")
	got, err := eng.Recv(ctx)
	require.NoError(t, err, "retried recv")
	require.Equal(t, "late-but-mine", string(got.Bytes()))
}

// TestReq_StaleReplyDiscardedOnNextSend: a reply buffered on a pipe from
// an exchange the FSM already abandoned must not be delivered as the
// answer to a later request — Send drains the chosen pipe first.
func TestReq_StaleReplyDiscardedOnNextSend(t *testing.T) {
	t.Parallel()
	eng := socket.NewReqEngine()
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q1"))), "send q1")
	_, err := b.RecvOr(ctx, nil)
	require.NoError(t, err, "drain q1")

	// Malformed reply (no empty delimiter) aborts the exchange and flips
	// the FSM back to canSend.
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("garbage"))), "malformed reply")
	_, err = eng.Recv(ctx)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "recv malformed")

	// The "real" reply to q1 arrives after the abort: it is now stale.
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("stale"))), "stale reply")

	// Next request must discard the stale reply at Send time...
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("q2"))), "send q2")
	got, err := b.RecvOr(ctx, nil)
	require.NoError(t, err, "drain q2")
	require.Equal(t, "q2", string(got.Frames()[1]))
	// ...so the reply Recv returns q2's answer, not the stale one.
	require.NoError(t, b.Send(ctx, msg.FromFrames([]byte{}, []byte("fresh"))), "fresh reply")
	reply, err := eng.Recv(ctx)
	require.NoError(t, err, "recv fresh")
	require.Equal(t, "fresh", string(reply.Bytes()), "stale reply delivered for a later request")
}
