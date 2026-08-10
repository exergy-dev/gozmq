package gozmq_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/security/curve"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// TestPairRoundTrip proves that two PAIR sockets can round-trip a multipart
// and a single-frame Msg, -race clean and with no goroutine leaks, over both
// the TCP session path and the inproc direct-pipe path.
func TestPairRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{"tcp", "tcp://127.0.0.1:0"},
		{"inproc", "inproc://pair-roundtrip"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, server, client := newSocketPair(t, gozmq.Pair, tc.endpoint)
			callCtx := testCtx(t)

			// Client → server, multipart.
			req := gozmq.NewMsgFromFrames([]byte("hello"), []byte("world"))
			if err := client.SendMsg(callCtx, req); err != nil {
				t.Fatalf("client send: %v", err)
			}
			got, err := server.RecvMsg(callCtx)
			if err != nil {
				t.Fatalf("server recv: %v", err)
			}
			if got.NumFrames() != 2 || string(got.Frames()[0]) != "hello" || string(got.Frames()[1]) != "world" {
				t.Fatalf("server got %v frames=%v", got.NumFrames(), got.Frames())
			}
			got.Release()

			// Server → client, single frame.
			if err := server.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("ack"))); err != nil {
				t.Fatalf("server send: %v", err)
			}
			ack, err := client.RecvMsg(callCtx)
			if err != nil {
				t.Fatalf("client recv: %v", err)
			}
			if string(ack.Bytes()) != "ack" {
				t.Fatalf("client got %q want ack", ack.Bytes())
			}
			ack.Release()
		})
	}
}

// TestPushPull_TCP exercises the A5 load-balance/fair-queue primitives end
// to end through real sessions and transport.
func TestPushPull_TCP(t *testing.T) {
	_, puller, pusher := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)

	callCtx := testCtx(t)

	for i := 0; i < 5; i++ {
		if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("push[%d]: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		m, err := puller.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("pull[%d]: %v", i, err)
		}
		if m.Bytes()[0] != byte(i) {
			t.Fatalf("pull[%d] = %d (order violated)", i, m.Bytes()[0])
		}
		m.Release()
	}
}

// TestPushSendRejected ensures PUSH.Recv returns ErrFSMViolation — a
// smoke-test that the typed sentinels propagate correctly through the
// public API.
func TestPushSendRejected(t *testing.T) {
	_, s := newTestSocket(t, gozmq.Push)

	_, err := s.RecvMsg(context.Background())
	if !errors.Is(err, gozmq.ErrFSMViolation) {
		t.Fatalf("expected ErrFSMViolation, got %v", err)
	}
}

// TestReqRep_TCP exercises the REQ/REP lock-step FSM over a real session
// and transport.
func TestReqRep_TCP(t *testing.T) {
	_, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, "tcp://127.0.0.1:0", nil, nil)

	callCtx := testCtx(t)

	for i := 0; i < 3; i++ {
		if err := req.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("req send[%d]: %v", i, err)
		}
		got, err := rep.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("rep recv[%d]: %v", i, err)
		}
		if got.Bytes()[0] != byte(i) {
			t.Fatalf("rep got %d want %d", got.Bytes()[0], i)
		}
		got.Release()
		reply := []byte{byte(i) * 10}
		if err := rep.SendMsg(callCtx, gozmq.NewMsgFromBytes(reply)); err != nil {
			t.Fatalf("rep send[%d]: %v", i, err)
		}
		back, err := req.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("req recv[%d]: %v", i, err)
		}
		if back.Bytes()[0] != reply[0] {
			t.Fatalf("req got %d want %d", back.Bytes()[0], reply[0])
		}
		back.Release()
	}
}

// TestPubSub_Inproc exercises PUB/SUB with topic filtering over inproc.
func TestPubSub_Inproc(t *testing.T) {
	_, pub, sub := newPubSubPair(t, "inproc://pubsub-test", [][]byte{[]byte("alpha")}, nil, nil)

	callCtx := testCtx(t)

	// Send one matching and one non-matching message. The filter must
	// drop "beta" before it reaches RecvMsg.
	if err := pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("alphaOne"))); err != nil {
		t.Fatalf("pub send alpha: %v", err)
	}
	if err := pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("betaTwo"))); err != nil {
		t.Fatalf("pub send beta: %v", err)
	}
	if err := pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("alphaThree"))); err != nil {
		t.Fatalf("pub send alpha: %v", err)
	}

	for _, want := range []string{"alphaOne", "alphaThree"} {
		got, err := sub.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("sub recv %s: %v", want, err)
		}
		if string(got.Bytes()) != want {
			t.Fatalf("sub got %q want %q", got.Bytes(), want)
		}
		got.Release()
	}
}

// TestPubSub_BinaryPayloadLeadingSubByte is a regression test for a SUB
// receive-path bug: single-frame data whose first byte was 0x00 or 0x01
// (the SUBSCRIBE/CANCEL marker bytes) was silently dropped because Recv
// classified subscriptions by sniffing the payload. A SUB's peer is a PUB
// that only ever sends data, so such payloads must be delivered intact.
func TestPubSub_BinaryPayloadLeadingSubByte(t *testing.T) {
	// Match-all subscription: every published message should be delivered.
	_, pub, sub := newPubSubPair(t, "inproc://pubsub-binary", [][]byte{{}}, nil, nil)

	callCtx := testCtx(t)

	// Payloads beginning with the subscription marker bytes (0x00/0x01)
	// plus a control case. Before the fix the first two were dropped.
	want := [][]byte{
		{0x00, 0x42, 0x43},
		{0x01, 0x42, 0x43},
		{'A', 0x00, 0x01},
	}
	for _, p := range want {
		if err := pub.SendMsg(callCtx, gozmq.NewMsgFromBytes(p)); err != nil {
			t.Fatalf("pub send %x: %v", p, err)
		}
	}
	for i, w := range want {
		got, err := sub.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("sub recv %d: %v", i, err)
		}
		if !bytes.Equal(got.Bytes(), w) {
			t.Fatalf("payload %d: got %x want %x", i, got.Bytes(), w)
		}
		got.Release()
	}
}

// TestRouterDealer_TCP exercises ROUTER identity-based routing.
func TestRouterDealer_TCP(t *testing.T) {
	_, router, dealer := newSocketPairTypes(
		t, gozmq.Router, gozmq.Dealer, "tcp://127.0.0.1:0",
		nil, []gozmq.Option{gozmq.WithIdentity([]byte("worker-1"))},
	)

	callCtx := testCtx(t)

	// Dealer → Router (Router sees identity frame prepended).
	if err := dealer.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("req-payload"))); err != nil {
		t.Fatal(err)
	}
	got, err := router.RecvMsg(callCtx)
	if err != nil {
		t.Fatal(err)
	}
	if got.NumFrames() != 2 {
		t.Fatalf("router frames = %d want 2 (identity + body), got %v", got.NumFrames(), got.Frames())
	}
	if string(got.Frames()[0]) != "worker-1" {
		t.Fatalf("router identity %q want worker-1", got.Frames()[0])
	}
	if string(got.Frames()[1]) != "req-payload" {
		t.Fatalf("router body %q", got.Frames()[1])
	}
	got.Release()

	// Router → Dealer (use same identity in leading frame).
	if err := router.SendMsg(callCtx, gozmq.NewMsgFromFrames(
		[]byte("worker-1"), []byte("reply-payload"),
	)); err != nil {
		t.Fatal(err)
	}
	reply, err := dealer.RecvMsg(callCtx)
	if err != nil {
		t.Fatal(err)
	}
	if string(reply.Bytes()) != "reply-payload" {
		t.Fatalf("dealer got %q", reply.Bytes())
	}
	reply.Release()
}

// TestPlainAuth_Success: a PLAIN client with correct credentials handshakes
// with a PLAIN server and then round-trips.
func TestPlainAuth_Success(t *testing.T) {
	_, server, client := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
		[]gozmq.Option{gozmq.WithPlainAuth(func(u, p string) bool {
			return u == "alice" && p == "hunter2"
		})},
		[]gozmq.Option{gozmq.WithPlainClient("alice", "hunter2")},
	)

	callCtx := testCtx(t)
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("secret"))); err != nil {
		t.Fatal(err)
	}
	got, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Bytes()) != "secret" {
		t.Fatalf("got %q", got.Bytes())
	}
	got.Release()
}

// TestCurveAuth_Success verifies that a CURVE client with the correct
// server key completes the handshake and round-trips.
func TestCurveAuth_Success(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	_, server, client := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
		[]gozmq.Option{gozmq.WithCurveServer(serverSec[:])},
		[]gozmq.Option{gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:])},
	)

	callCtx := testCtx(t)
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("curve-secret"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if string(got.Bytes()) != "curve-secret" {
		t.Fatalf("got %q", got.Bytes())
	}
	got.Release()
}

// TestContextClosed_BlocksNewSocket is a smoke test on teardown ordering.
func TestContextClosed_BlocksNewSocket(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	if err := ctxZmq.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := ctxZmq.NewSocket(gozmq.Pair); !errors.Is(err, gozmq.ErrContextClosed) {
		t.Fatalf("expected ErrContextClosed, got %v", err)
	}
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
			callCtx := testCtx(t)

			if tc.overLimit {
				big := make([]byte, limit+1)
				start := time.Now()
				err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes(big))
				elapsed := time.Since(start)
				if !errors.Is(err, gozmq.ErrMalformedFrame) {
					t.Fatalf("oversize SendMsg err = %v, want ErrMalformedFrame", err)
				}
				if tc.assertPrompt && elapsed > 500*time.Millisecond {
					t.Fatalf("oversize SendMsg took %v — expected synchronous return", elapsed)
				}
			}

			// At-limit payload round-trips — and, in the over-limit cases,
			// proves the rejected send did not tear down the session.
			payload := make([]byte, limit)
			for i := range payload {
				payload[i] = byte(i & 0xff)
			}
			if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
				t.Fatalf("at-limit SendMsg: %v", err)
			}
			got, err := server.RecvMsg(callCtx)
			if err != nil {
				t.Fatalf("recv at limit: %v", err)
			}
			if !bytes.Equal(got.Bytes(), payload) {
				t.Fatalf("recv size = %d want %d (payload mismatch)", len(got.Bytes()), limit)
			}
			got.Release()
		})
	}
}

// TestSendMsg_CollectsMultipartSizeCorrectly exercises the per-frame
// semantics of the synchronous check: it mirrors the codec's per-frame
// body-length enforcement (zmtp/framing.go WriteFrame),
// so a multipart message where every frame is <= MaxMessageSize passes
// even if the sum exceeds the cap; a multipart message with any single
// frame > MaxMessageSize is rejected.
func TestSendMsg_CollectsMultipartSizeCorrectly(t *testing.T) {
	const limit = 1024
	_, srv, cli := newSocketPair(t, gozmq.Pair, "inproc://sendmsg-multipart", gozmq.WithMaxMessageSize(limit))

	callCtx := testCtx(t)

	// Case A: two <=limit frames (sum > limit) — accepted.
	// Each frame is limit bytes, total 2*limit, but the codec/SendMsg
	// check is per-frame so this round-trips successfully.
	a := make([]byte, limit)
	b := make([]byte, limit)
	if err := cli.SendMsg(callCtx, gozmq.NewMsgFromFrames(a, b)); err != nil {
		t.Fatalf("multipart (per-frame <= limit) SendMsg: %v", err)
	}
	got, err := srv.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("recv multipart: %v", err)
	}
	if got.NumFrames() != 2 || len(got.Frames()[0]) != limit || len(got.Frames()[1]) != limit {
		t.Fatalf("recv frames = %v, want 2x%d", got.Frames(), limit)
	}
	got.Release()

	// Case B: one frame > limit — rejected with ErrMalformedFrame.
	small := []byte("ok")
	bigFrame := make([]byte, limit+1)
	err = cli.SendMsg(callCtx, gozmq.NewMsgFromFrames(small, bigFrame))
	if !errors.Is(err, gozmq.ErrMalformedFrame) {
		t.Fatalf("multipart w/ oversize frame err = %v, want ErrMalformedFrame", err)
	}
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
	// receiver accepts the wiring but never RecvMsgs payload. With a tiny
	// recvHWM its inbound pipe fills quickly and blocks further writes.
	_, receiver, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"inproc://closectx-cancel-linger",
		[]gozmq.Option{gozmq.WithRecvHWM(1)},
		[]gozmq.Option{gozmq.WithLinger(-1) /* infinite */, gozmq.WithSendHWM(1)},
	)

	// Settle: a reverse round-trip (receiver → sender) proves both ends
	// are attached without a fixed sleep.
	callCtx := testCtx(t)
	if err := receiver.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("sync"))); err != nil {
		t.Fatalf("settle send: %v", err)
	}
	if m, err := sender.RecvMsg(callCtx); err != nil {
		t.Fatalf("settle recv: %v", err)
	} else {
		m.Release()
	}

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
	err := sender.CloseCtx(closeCtx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseCtx err = %v, want context.DeadlineExceeded (elapsed=%v)", err, elapsed)
	}
	// Generous bound: the linger wait should be interrupted within a
	// few hundred ms of the deadline, not hours.
	if elapsed > 5*time.Second {
		t.Fatalf("CloseCtx took %v after ctx cancel — did not honor ctx.Done", elapsed)
	}

	// Idempotent: second CloseCtx returns nil.
	if err2 := sender.CloseCtx(context.Background()); err2 != nil {
		t.Fatalf("second CloseCtx err = %v, want nil", err2)
	}
}

// TestCloseCtx_NormalDrainStillWorks confirms that CloseCtx with a
// bounded linger and a responsive peer returns nil (no ctx error): once
// the peer has drained every queued message, the close path has nothing
// left to linger on.
func TestCloseCtx_NormalDrainStillWorks(t *testing.T) {
	_, receiver, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"inproc://closectx-normal-drain",
		nil,
		[]gozmq.Option{gozmq.WithLinger(2 * time.Second)},
	)

	callCtx := testCtx(t)
	const N = 4
	for i := 0; i < N; i++ {
		if err := sender.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	// Drain everything BEFORE closing — this is the "responsive peer"
	// scenario, not the "drain at Close" scenario, and waiting for the
	// last receive replaces any settle sleep.
	for i := 0; i < N; i++ {
		m, err := receiver.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if len(m.Bytes()) != 1 || m.Bytes()[0] != byte(i) {
			t.Fatalf("recv %d: unexpected payload %x", i, m.Bytes())
		}
		m.Release()
	}

	if err := sender.CloseCtx(context.Background()); err != nil {
		t.Fatalf("CloseCtx err = %v, want nil", err)
	}
}

// TestClose_DelegatesToCloseCtx confirms that Close uses the same teardown
// path as CloseCtx with a background context.
func TestClose_DelegatesToCloseCtx(t *testing.T) {
	_, _, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"inproc://close-delegates",
		nil,
		[]gozmq.Option{gozmq.WithLinger(200 * time.Millisecond)},
	)

	if err := sender.Close(); err != nil {
		t.Fatalf("Close() err = %v, want nil", err)
	}
	// Idempotent contract: repeated Close returns nil, like Context.Close.
	if err := sender.Close(); err != nil {
		t.Fatalf("second Close() err = %v, want nil", err)
	}
}
