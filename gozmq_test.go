package gozmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/security/curve"
)

func TestMain(m *testing.M) { testutil.VerifyMain(m) }

// TestPairRoundTrip proves that two PAIR sockets can round-trip a multipart
// and a single-frame Msg, -race clean and with no goroutine leaks, over both
// the TCP session path and the inproc direct-pipe path.
func TestPairRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{"tcp", "tcp://127.0.0.1:0"},
		{"inproc", "inproc://pair-roundtrip"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, server, client := newSocketPair(t, gozmq.Pair, tc.endpoint)
			callCtx := testutil.Ctx(t)

			// Client → server, multipart.
			req := gozmq.NewMsgFromFrames([]byte("hello"), []byte("world"))
			require.NoError(t, client.SendMsg(callCtx, req), "client send")
			got, err := server.RecvMsg(callCtx)
			require.NoError(t, err, "server recv")
			require.Equal(t, [][]byte{[]byte("hello"), []byte("world")}, got.Frames())
			got.Release()

			// Server → client, single frame.
			require.NoError(t, server.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("ack"))), "server send")
			ack, err := client.RecvMsg(callCtx)
			require.NoError(t, err, "client recv")
			require.Equal(t, "ack", string(ack.Bytes()))
			ack.Release()
		})
	}
}

// TestPushPull_TCP exercises the A5 load-balance/fair-queue primitives end
// to end through real sessions and transport.
func TestPushPull_TCP(t *testing.T) {
	t.Parallel()
	_, puller, pusher := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)

	callCtx := testutil.Ctx(t)

	for i := 0; i < 5; i++ {
		require.NoError(t, pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})), "push[%d]", i)
	}
	for i := 0; i < 5; i++ {
		m, err := puller.RecvMsg(callCtx)
		require.NoError(t, err, "pull[%d]", i)
		require.Equal(t, byte(i), m.Bytes()[0], "pull[%d]: order violated", i)
		m.Release()
	}
}

// TestPushSendRejected ensures PUSH.Recv returns ErrFSMViolation — a
// smoke-test that the typed sentinels propagate correctly through the
// public API.
func TestPushSendRejected(t *testing.T) {
	t.Parallel()
	_, s := newTestSocket(t, gozmq.Push)

	_, err := s.RecvMsg(context.Background())
	require.ErrorIs(t, err, gozmq.ErrFSMViolation)
}

// TestReqRep_TCP exercises the REQ/REP lock-step FSM over a real session
// and transport.
func TestReqRep_TCP(t *testing.T) {
	t.Parallel()
	_, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, "tcp://127.0.0.1:0", nil, nil)

	callCtx := testutil.Ctx(t)

	for i := 0; i < 3; i++ {
		require.NoError(t, req.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})), "req send[%d]", i)
		got, err := rep.RecvMsg(callCtx)
		require.NoError(t, err, "rep recv[%d]", i)
		require.Equal(t, byte(i), got.Bytes()[0], "rep got")
		got.Release()
		reply := []byte{byte(i) * 10}
		require.NoError(t, rep.SendMsg(callCtx, gozmq.NewMsgFromBytes(reply)), "rep send[%d]", i)
		back, err := req.RecvMsg(callCtx)
		require.NoError(t, err, "req recv[%d]", i)
		require.Equal(t, reply[0], back.Bytes()[0], "req got")
		back.Release()
	}
}

// TestPubSub_Inproc exercises PUB/SUB with topic filtering over inproc.
func TestPubSub_Inproc(t *testing.T) {
	t.Parallel()
	_, pub, sub := newPubSubPair(t, "inproc://pubsub-test", [][]byte{[]byte("alpha")}, nil, nil)

	callCtx := testutil.Ctx(t)

	// Send one matching and one non-matching message. The filter must
	// drop "beta" before it reaches RecvMsg.
	for _, body := range []string{"alphaOne", "betaTwo", "alphaThree"} {
		require.NoError(t, pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte(body))), "pub send %s", body)
	}

	for _, want := range []string{"alphaOne", "alphaThree"} {
		got, err := sub.RecvMsg(callCtx)
		require.NoError(t, err, "sub recv %s", want)
		require.Equal(t, want, string(got.Bytes()), "sub got")
		got.Release()
	}
}

// TestPubSub_BinaryPayloadLeadingSubByte is a regression test for a SUB
// receive-path bug: single-frame data whose first byte was 0x00 or 0x01
// (the SUBSCRIBE/CANCEL marker bytes) was silently dropped because Recv
// classified subscriptions by sniffing the payload. A SUB's peer is a PUB
// that only ever sends data, so such payloads must be delivered intact.
func TestPubSub_BinaryPayloadLeadingSubByte(t *testing.T) {
	t.Parallel()
	// Match-all subscription: every published message should be delivered.
	_, pub, sub := newPubSubPair(t, "inproc://pubsub-binary", [][]byte{{}}, nil, nil)

	callCtx := testutil.Ctx(t)

	// Payloads beginning with the subscription marker bytes (0x00/0x01)
	// plus a control case. Before the fix the first two were dropped.
	want := [][]byte{
		{0x00, 0x42, 0x43},
		{0x01, 0x42, 0x43},
		{'A', 0x00, 0x01},
	}
	for _, p := range want {
		require.NoError(t, pub.SendMsg(callCtx, gozmq.NewMsgFromBytes(p)), "pub send %x", p)
	}
	for i, w := range want {
		got, err := sub.RecvMsg(callCtx)
		require.NoError(t, err, "sub recv %d", i)
		require.Equal(t, w, got.Bytes(), "payload %d", i)
		got.Release()
	}
}

// TestRouterDealer_TCP exercises ROUTER identity-based routing.
func TestRouterDealer_TCP(t *testing.T) {
	t.Parallel()
	_, router, dealer := newSocketPairTypes(
		t, gozmq.Router, gozmq.Dealer, "tcp://127.0.0.1:0",
		nil, []gozmq.Option{gozmq.WithIdentity([]byte("worker-1"))},
	)

	callCtx := testutil.Ctx(t)

	// Dealer → Router (Router sees identity frame prepended).
	require.NoError(t, dealer.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("req-payload"))))
	got, err := router.RecvMsg(callCtx)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("worker-1"), []byte("req-payload")}, got.Frames(), "router frames: want identity + body")
	got.Release()

	// Router → Dealer (use same identity in leading frame).
	require.NoError(t, router.SendMsg(callCtx, gozmq.NewMsgFromFrames(
		[]byte("worker-1"), []byte("reply-payload"),
	)))
	reply, err := dealer.RecvMsg(callCtx)
	require.NoError(t, err)
	require.Equal(t, "reply-payload", string(reply.Bytes()), "dealer got")
	reply.Release()
}

// TestPlainAuth_Success: a PLAIN client with correct credentials handshakes
// with a PLAIN server and then round-trips.
func TestPlainAuth_Success(t *testing.T) {
	t.Parallel()
	_, server, client := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
		[]gozmq.Option{gozmq.WithPlainAuth(func(u, p string) bool {
			return u == "alice" && p == "hunter2"
		})},
		[]gozmq.Option{gozmq.WithPlainClient("alice", "hunter2")},
	)

	callCtx := testutil.Ctx(t)
	require.NoError(t, client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("secret"))))
	got, err := server.RecvMsg(callCtx)
	require.NoError(t, err)
	require.Equal(t, "secret", string(got.Bytes()))
	got.Release()
}

// TestCurveAuth_Success verifies that a CURVE client with the correct
// server key completes the handshake and round-trips.
func TestCurveAuth_Success(t *testing.T) {
	t.Parallel()
	serverPub, serverSec, err := curve.GenerateKeyPair()
	require.NoError(t, err)
	clientPub, clientSec, err := curve.GenerateKeyPair()
	require.NoError(t, err)

	_, server, client := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
		[]gozmq.Option{gozmq.WithCurveServer(serverSec[:])},
		[]gozmq.Option{gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:])},
	)

	callCtx := testutil.Ctx(t)
	require.NoError(t, client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("curve-secret"))), "send")
	got, err := server.RecvMsg(callCtx)
	require.NoError(t, err, "recv")
	require.Equal(t, "curve-secret", string(got.Bytes()))
	got.Release()
}

// TestContextClosed_BlocksNewSocket is a smoke test on teardown ordering.
func TestContextClosed_BlocksNewSocket(t *testing.T) {
	t.Parallel()
	ctxZmq := gozmq.NewContext()
	require.NoError(t, ctxZmq.Close(), "close")
	_, err := ctxZmq.NewSocket(gozmq.Pair)
	require.ErrorIs(t, err, gozmq.ErrContextClosed)
}

// TestMaxMessageSize_SendBoundary covers the synchronous send-side
// MaxMessageSize contract across patterns and boundary positions:
//
//   - At exactly the limit the message round-trips.
//   - One byte over, SendMsg rejects synchronously with an error wrapping
//     ErrMalformedFrame (one case also asserts the return is prompt — no
//     monitor channel or session round-trip is involved), the session is
//     NOT torn down, and a subsequent at-limit send still round-trips.
func TestMaxMessageSize_SendBoundary(t *testing.T) {
	t.Parallel()
	const limit = 1024
	opts := []gozmq.Option{gozmq.WithMaxMessageSize(limit)}

	cases := []struct {
		name         string
		server       gozmq.SocketType
		client       gozmq.SocketType
		endpoint     string
		overLimit    bool
		assertPrompt bool // assert the oversize rejection returns synchronously
	}{
		{"push-pull/at-limit", gozmq.Pull, gozmq.Push, "inproc://maxmsg-pushpull-at", false, false},
		{"push-pull/over-limit", gozmq.Pull, gozmq.Push, "inproc://maxmsg-pushpull-over", true, false},
		{"pair/at-limit", gozmq.Pair, gozmq.Pair, "inproc://maxmsg-pair-at", false, false},
		{"pair/over-limit", gozmq.Pair, gozmq.Pair, "inproc://maxmsg-pair-over", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, server, client := newSocketPairTypes(t, tc.server, tc.client, tc.endpoint, opts, opts)
			callCtx := testutil.Ctx(t)

			if tc.overLimit {
				big := make([]byte, limit+1)
				start := time.Now()
				err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes(big))
				elapsed := time.Since(start)
				require.ErrorIs(t, err, gozmq.ErrMalformedFrame, "oversize SendMsg")
				if tc.assertPrompt {
					require.LessOrEqual(t, elapsed, 500*time.Millisecond, "oversize SendMsg took %v — expected synchronous return", elapsed)
				}
			}

			// At-limit payload round-trips — and, in the over-limit cases,
			// proves the rejected send did not tear down the session.
			payload := make([]byte, limit)
			for i := range payload {
				payload[i] = byte(i & 0xff)
			}
			require.NoError(t, client.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)), "at-limit SendMsg")
			got, err := server.RecvMsg(callCtx)
			require.NoError(t, err, "recv at limit")
			require.Equal(t, payload, got.Bytes(), "recv size = %d want %d (payload mismatch)", len(got.Bytes()), limit)
			got.Release()
		})
	}
}

// TestSendMsg_CollectsMultipartSizeCorrectly: WithMaxMessageSize bounds the
// whole logical message on both sides. The send check sums the frames (as
// the receiving session does for a MORE chain), so a multipart message whose
// total exceeds the cap is rejected synchronously even when every frame is
// under it, and one whose total fits is delivered.
func TestSendMsg_CollectsMultipartSizeCorrectly(t *testing.T) {
	t.Parallel()
	const limit = 1024
	_, srv, cli := newSocketPair(t, gozmq.Pair, "inproc://sendmsg-multipart", gozmq.WithMaxMessageSize(limit))

	callCtx := testutil.Ctx(t)

	// Case A1: two frames whose sum is exactly the limit — accepted.
	a := make([]byte, limit/2)
	b := make([]byte, limit/2)
	require.NoError(t, cli.SendMsg(callCtx, gozmq.NewMsgFromFrames(a, b)), "multipart (sum == limit) SendMsg")
	got, err := srv.RecvMsg(callCtx)
	require.NoError(t, err, "recv multipart")
	require.Equal(t, [][]byte{a, b}, got.Frames(), "want 2x%d", limit/2)
	got.Release()

	// Case A2: two frames each under the limit but summing over it —
	// rejected up front, so a gozmq receiver never tears the session down
	// for a message a gozmq sender accepted.
	err = cli.SendMsg(callCtx, gozmq.NewMsgFromFrames(make([]byte, limit), make([]byte, 1)))
	require.ErrorIs(t, err, gozmq.ErrMalformedFrame, "multipart (sum > limit) SendMsg")

	// Case B: one frame > limit — rejected with ErrMalformedFrame.
	small := []byte("ok")
	bigFrame := make([]byte, limit+1)
	err = cli.SendMsg(callCtx, gozmq.NewMsgFromFrames(small, bigFrame))
	require.ErrorIs(t, err, gozmq.ErrMalformedFrame, "multipart w/ oversize frame")
}

// TestCloseCtx_CancellationBreaksLingerDrain verifies that under
// WithLinger(-1) (infinite) a stalled outbound queue would previously
// block Close forever; CloseCtx honors ctx cancellation and returns
// ctx.Err() after the linger wait is interrupted, while still completing
// the rest of teardown.
//
// The reliable reproduction is a PAIR with a peer whose recv side is
// saturated AND WithLinger(infinite). We configure recv+send HWMs to 1
// and pre-fill past the transport-level pipe buffer, so the drain path
// blocks until the deadline interrupts it.
func TestCloseCtx_CancellationBreaksLingerDrain(t *testing.T) {
	t.Parallel()
	// receiver accepts the wiring but never RecvMsgs payload. With a tiny
	// recvHWM its inbound pipe fills quickly and blocks further writes.
	_, receiver, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"inproc://closectx-cancel-linger",
		[]gozmq.Option{gozmq.WithRecvHWM(1)},
		[]gozmq.Option{gozmq.WithLinger(-1) /* infinite */, gozmq.WithSendHWM(1)},
	)

	// Settle: a reverse round-trip (receiver → sender) proves both ends
	// are attached without a fixed sleep.
	callCtx := testutil.Ctx(t)
	require.NoError(t, receiver.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("sync"))), "settle send")
	m, err := sender.RecvMsg(callCtx)
	require.NoError(t, err, "settle recv")
	m.Release()

	// Pre-fill: enqueue enough messages that the transport pipe +
	// receiver pipe (HWM 1) + buffers all saturate. Once saturated, a
	// further SendMsg blocks; we use a short per-call deadline so the
	// pre-fill step itself never blocks the test. The exact count that
	// queues vs. blocks is implementation-dependent — we just need
	// SOMETHING pending when Close runs.
	for i := 0; i < 256; i++ {
		sCtx, sCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := sender.SendMsg(sCtx, gozmq.NewMsgFromBytes([]byte{byte(i)}))
		sCancel()
		if err != nil {
			// pipe full / deadline hit — we're saturated; proceed.
			break
		}
	}

	// CloseCtx with a short deadline must return ~immediately after the
	// deadline fires, not wait forever.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer closeCancel()

	start := time.Now()
	err = sender.CloseCtx(closeCtx)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded, "CloseCtx (elapsed=%v)", elapsed)
	// Generous bound: the linger wait should be interrupted within a
	// few hundred ms of the deadline, not hours.
	require.LessOrEqual(t, elapsed, 5*time.Second, "CloseCtx took %v after ctx cancel — did not honor ctx.Done", elapsed)

	// Idempotent: second CloseCtx returns nil.
	require.NoError(t, sender.CloseCtx(context.Background()), "second CloseCtx")
}

// TestCloseCtx_NormalDrainStillWorks confirms that CloseCtx with a
// bounded linger and a responsive peer returns nil (no ctx error): once
// the peer has drained every queued message, the close path has nothing
// left to linger on.
func TestCloseCtx_NormalDrainStillWorks(t *testing.T) {
	t.Parallel()
	_, receiver, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"inproc://closectx-normal-drain",
		nil,
		[]gozmq.Option{gozmq.WithLinger(2 * time.Second)},
	)

	callCtx := testutil.Ctx(t)
	const N = 4
	for i := 0; i < N; i++ {
		require.NoError(t, sender.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})), "send %d", i)
	}
	// Drain everything BEFORE closing — this is the "responsive peer"
	// scenario, not the "drain at Close" scenario, and waiting for the
	// last receive replaces any settle sleep.
	for i := 0; i < N; i++ {
		m, err := receiver.RecvMsg(callCtx)
		require.NoError(t, err, "recv %d", i)
		require.Equal(t, []byte{byte(i)}, m.Bytes(), "recv %d: unexpected payload", i)
		m.Release()
	}

	require.NoError(t, sender.CloseCtx(context.Background()), "CloseCtx")
}

// TestClose_DelegatesToCloseCtx confirms that Close uses the same teardown
// path as CloseCtx with a background context.
func TestClose_DelegatesToCloseCtx(t *testing.T) {
	t.Parallel()
	_, _, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"inproc://close-delegates",
		nil,
		[]gozmq.Option{gozmq.WithLinger(200 * time.Millisecond)},
	)

	require.NoError(t, sender.Close(), "Close()")
	// Idempotent contract: repeated Close returns nil, like Context.Close.
	require.NoError(t, sender.Close(), "second Close()")
}
