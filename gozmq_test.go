package gozmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq"
	"github.com/rkm/gozmq/security/curve"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// TestPairRoundTrip_TCP proves that two PAIR sockets can round-trip a
// multipart Msg over TCP with -race clean and no goroutine leaks.
func TestPairRoundTrip_TCP(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	server, err := ctxZmq.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer server.Close()
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	ep := server.LastEndpoint()
	if ep == "" {
		t.Fatal("LastEndpoint empty after bind")
	}

	client, err := ctxZmq.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()
	if err := client.Connect(ep); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
}

// TestPairRoundTrip_Inproc is the same convergence test over inproc —
// exercises the inproc transport registry and pipe-backed Conn.
func TestPairRoundTrip_Inproc(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	server, err := ctxZmq.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer server.Close()
	if err := server.Bind("inproc://pair-ping-test"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	client, err := ctxZmq.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()
	if err := client.Connect("inproc://pair-ping-test"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := gozmq.NewMsgFromFrames([]byte("ping"), []byte("payload-bytes"))
	if err := client.SendMsg(callCtx, req); err != nil {
		t.Fatalf("client send: %v", err)
	}
	got, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("server recv: %v", err)
	}
	if got.NumFrames() != 2 || string(got.Frames()[0]) != "ping" || string(got.Frames()[1]) != "payload-bytes" {
		t.Fatalf("server frames: %v", got.Frames())
	}

	if err := server.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("pong"))); err != nil {
		t.Fatalf("server send: %v", err)
	}
	reply, err := client.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("client recv: %v", err)
	}
	if string(reply.Bytes()) != "pong" {
		t.Fatalf("client reply %q want pong", reply.Bytes())
	}
}

// TestPushPull_TCP exercises the A5 load-balance/fair-queue primitives end
// to end through real sessions and transport.
func TestPushPull_TCP(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	puller, err := ctxZmq.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatalf("new pull: %v", err)
	}
	defer puller.Close()
	if err := puller.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("bind: %v", err)
	}

	pusher, err := ctxZmq.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatalf("new push: %v", err)
	}
	defer pusher.Close()
	if err := pusher.Connect(puller.LastEndpoint()); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	}
}

// TestPushSendRejected ensures PUSH.Recv returns ErrFSMViolation — a
// smoke-test that the typed sentinels propagate correctly through the
// public API.
func TestPushSendRejected(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	s, err := ctxZmq.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.RecvMsg(context.Background())
	if !errors.Is(err, gozmq.ErrFSMViolation) {
		t.Fatalf("expected ErrFSMViolation, got %v", err)
	}
}

// TestReqRep_TCP exercises the REQ/REP lock-step FSM over a real session
// and transport.
func TestReqRep_TCP(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	rep, err := ctxZmq.NewSocket(gozmq.Rep)
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Close()
	if err := rep.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	req, err := ctxZmq.NewSocket(gozmq.Req)
	if err != nil {
		t.Fatal(err)
	}
	defer req.Close()
	if err := req.Connect(rep.LastEndpoint()); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	}
}

// TestPubSub_Inproc exercises PUB/SUB with topic filtering over inproc.
func TestPubSub_Inproc(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	pub, err := ctxZmq.NewSocket(gozmq.Pub)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Bind("inproc://pubsub-test"); err != nil {
		t.Fatal(err)
	}

	sub, err := ctxZmq.NewSocket(gozmq.Sub)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.Subscribe([]byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if err := sub.Connect("inproc://pubsub-test"); err != nil {
		t.Fatal(err)
	}

	// Give handshake + subscription forwarding a window to settle before
	// publishing (libzmq-style slow-joiner pattern).
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	}
}

// TestRouterDealer_TCP exercises ROUTER identity-based routing.
func TestRouterDealer_TCP(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	router, err := ctxZmq.NewSocket(gozmq.Router)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	if err := router.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	dealer, err := ctxZmq.NewSocket(gozmq.Dealer, gozmq.WithIdentity([]byte("worker-1")))
	if err != nil {
		t.Fatal(err)
	}
	defer dealer.Close()
	if err := dealer.Connect(router.LastEndpoint()); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
}

// TestPlainAuth_Success: a PLAIN client with correct credentials handshakes
// with a PLAIN server and then round-trips.
func TestPlainAuth_Success(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	server, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithPlainAuth(func(u, p string) bool {
		return u == "alice" && p == "hunter2"
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithPlainClient("alice", "hunter2"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Connect(server.LastEndpoint()); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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
}

// TestCurveAuth_Success verifies that a CURVE client with the correct
// server key completes the handshake and round-trips.
func TestCurveAuth_Success(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	server, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithCurveServer(serverSec[:]))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Connect(server.LastEndpoint()); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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

// TestMaxMessageSize_AtLimit verifies that a message of exactly
// MaxMessageSize bytes round-trips successfully through a PUSH/PULL pair.
// Uses a small custom limit so the test doesn't allocate the 64 MiB default.
func TestMaxMessageSize_AtLimit(t *testing.T) {
	const limit = 1024
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	puller, err := ctxZmq.NewSocket(gozmq.Pull, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new pull: %v", err)
	}
	defer puller.Close()
	endpoint := "inproc://maxmsg-at-limit"
	if err := puller.Bind(endpoint); err != nil {
		t.Fatalf("bind: %v", err)
	}

	pusher, err := ctxZmq.NewSocket(gozmq.Push, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new push: %v", err)
	}
	defer pusher.Close()
	if err := pusher.Connect(endpoint); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := make([]byte, limit)
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}
	if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
		t.Fatalf("send at limit: %v", err)
	}
	got, err := puller.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("recv at limit: %v", err)
	}
	if len(got.Bytes()) != limit {
		t.Fatalf("recv size = %d want %d", len(got.Bytes()), limit)
	}
}

// TestMaxMessageSize_OverLimit verifies that a message of MaxMessageSize+1
// bytes is rejected synchronously by SendMsg with an error wrapping
// ErrMalformedFrame, and that the failed send does not tear down the
// session — a subsequent in-limit SendMsg succeeds on the same socket.
//
// Oversize messages are rejected synchronously by SendMsg with
// ErrMalformedFrame so callers don't need a monitor to observe the error.
func TestMaxMessageSize_OverLimit(t *testing.T) {
	const limit = 1024
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	puller, err := ctxZmq.NewSocket(gozmq.Pull, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new pull: %v", err)
	}
	defer puller.Close()
	endpoint := "inproc://maxmsg-over-limit"
	if err := puller.Bind(endpoint); err != nil {
		t.Fatalf("bind: %v", err)
	}

	pusher, err := ctxZmq.NewSocket(gozmq.Push, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new push: %v", err)
	}
	defer pusher.Close()
	if err := pusher.Connect(endpoint); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Oversize send: synchronous ErrMalformedFrame, no session teardown.
	big := make([]byte, limit+1)
	if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes(big)); !errors.Is(err, gozmq.ErrMalformedFrame) {
		t.Fatalf("oversize SendMsg err = %v, want ErrMalformedFrame", err)
	}

	// Session still healthy: an in-limit send round-trips.
	ok := make([]byte, limit)
	for i := range ok {
		ok[i] = byte(i & 0xff)
	}
	if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes(ok)); err != nil {
		t.Fatalf("in-limit SendMsg after oversize: %v", err)
	}
	got, err := puller.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("recv after oversize: %v", err)
	}
	if len(got.Bytes()) != limit {
		t.Fatalf("recv size = %d want %d", len(got.Bytes()), limit)
	}
}

// TestSendMsg_OversizeSynchronousError verifies the synchronous size
// check: SendMsg with a payload exceeding MaxMessageSize returns
// immediately with an error wrapping ErrMalformedFrame, without needing
// a monitor channel and without tearing down the session. A subsequent
// in-limit SendMsg must succeed on the same socket.
func TestSendMsg_OversizeSynchronousError(t *testing.T) {
	const limit = 1024
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	srv, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new pair (srv): %v", err)
	}
	defer srv.Close()
	endpoint := "inproc://sendmsg-oversize-sync"
	if err := srv.Bind(endpoint); err != nil {
		t.Fatalf("bind: %v", err)
	}

	cli, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new pair (cli): %v", err)
	}
	defer cli.Close()
	if err := cli.Connect(endpoint); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Fast path: synchronous rejection. The call should return well
	// under the ctx deadline (in practice, immediately).
	start := time.Now()
	big := make([]byte, limit+1)
	err = cli.SendMsg(callCtx, gozmq.NewMsgFromBytes(big))
	if !errors.Is(err, gozmq.ErrMalformedFrame) {
		t.Fatalf("oversize SendMsg err = %v, want ErrMalformedFrame", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("oversize SendMsg took %v — expected synchronous return", elapsed)
	}

	// Session still healthy: in-limit send round-trips.
	payload := []byte("still-alive")
	if err := cli.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
		t.Fatalf("post-oversize in-limit SendMsg: %v", err)
	}
	got, err := srv.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("recv after oversize: %v", err)
	}
	if string(got.Bytes()) != string(payload) {
		t.Fatalf("recv payload = %q, want %q", got.Bytes(), payload)
	}
}

// TestSendMsg_AtLimitBoundary confirms the synchronous check does not
// reject payloads of exactly MaxMessageSize bytes.
func TestSendMsg_AtLimitBoundary(t *testing.T) {
	const limit = 1024
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	srv, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new pair (srv): %v", err)
	}
	defer srv.Close()
	endpoint := "inproc://sendmsg-at-limit-boundary"
	if err := srv.Bind(endpoint); err != nil {
		t.Fatalf("bind: %v", err)
	}
	cli, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new pair (cli): %v", err)
	}
	defer cli.Close()
	if err := cli.Connect(endpoint); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	payload := make([]byte, limit)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := cli.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
		t.Fatalf("at-limit SendMsg: %v", err)
	}
	got, err := srv.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("recv at limit: %v", err)
	}
	if len(got.Bytes()) != limit {
		t.Fatalf("recv size = %d want %d", len(got.Bytes()), limit)
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
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	srv, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new pair (srv): %v", err)
	}
	defer srv.Close()
	endpoint := "inproc://sendmsg-multipart"
	if err := srv.Bind(endpoint); err != nil {
		t.Fatalf("bind: %v", err)
	}
	cli, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithMaxMessageSize(limit))
	if err != nil {
		t.Fatalf("new pair (cli): %v", err)
	}
	defer cli.Close()
	if err := cli.Connect(endpoint); err != nil {
		t.Fatalf("connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

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

	// Case B: one frame > limit — rejected with ErrMalformedFrame.
	small := []byte("ok")
	bigFrame := make([]byte, limit+1)
	err = cli.SendMsg(callCtx, gozmq.NewMsgFromFrames(small, bigFrame))
	if !errors.Is(err, gozmq.ErrMalformedFrame) {
		t.Fatalf("multipart w/ oversize frame err = %v, want ErrMalformedFrame", err)
	}
}

// TestCloseCtx_CancellationBreaksLingerDrain verifies that under
// WithLinger(time.Hour) (effectively infinite for a test),
// a stalled outbound queue would previously block Close for an hour; the
// new CloseCtx honors ctx cancellation and returns ctx.Err() after the
// linger wait is interrupted, while still completing the rest of
// teardown.
//
// To force a genuinely-stalled drain we use a PUSH socket connected to
// nothing: PUSH blocks at the engine's Send until a peer attaches, so
// SendMsg calls would deadlock. Instead we rely on the fact that with
// WithLinger(time.Hour) and zero active sessions, waitWritersFlushed
// returns nil immediately — which does not exercise the bug. The
// reliable reproduction is: a PAIR with a peer whose recv side is
// saturated AND WithLinger(infinite). We configure recv+send HWMs to
// 1 and pre-fill past the transport-level pipe buffer (HWM 64 for
// inproc), so the writer's drain path blocks on conn.Write.
func TestCloseCtx_CancellationBreaksLingerDrain(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	// A receiver that accepts the handshake but never RecvMsgs. With
	// a tiny recvHWM, its engine-side pipe fills after 1 buffered
	// message and blocks further writes on the transport pipe.
	receiver, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithRecvHWM(1))
	if err != nil {
		t.Fatalf("new receiver: %v", err)
	}
	defer receiver.Close()
	endpoint := "inproc://closectx-cancel-linger"
	if err := receiver.Bind(endpoint); err != nil {
		t.Fatalf("bind: %v", err)
	}

	sender, err := ctxZmq.NewSocket(
		gozmq.Pair,
		gozmq.WithLinger(-1), // infinite
		gozmq.WithSendHWM(1),
	)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if err := sender.Connect(endpoint); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Give the handshake time to complete so the session is attached.
	time.Sleep(100 * time.Millisecond)

	// Pre-fill: enqueue enough messages that the transport pipe (HWM 64)
	// + receiver engine pipe (HWM 1) + session/writer buffers all
	// saturate. Once saturated, a further SendMsg will block at the
	// engine level; we use a short per-call deadline so we don't block
	// the test on the pre-fill step. The exact count that queues vs.
	// blocks is implementation-dependent — we just need SOMETHING
	// pending when Close runs.
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

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseCtx err = %v, want context.DeadlineExceeded (elapsed=%v)", err, elapsed)
	}
	// Generous bound: the linger wait should be interrupted within a
	// few hundred ms of the deadline, not hours.
	if elapsed > 5*time.Second {
		t.Fatalf("CloseCtx took %v after ctx cancel — did not honor ctx.Done", elapsed)
	}

	// Idempotent: second CloseCtx returns ErrSocketClosed.
	if err2 := sender.CloseCtx(context.Background()); !errors.Is(err2, gozmq.ErrSocketClosed) {
		t.Fatalf("second CloseCtx err = %v, want ErrSocketClosed", err2)
	}
}

// TestCloseCtx_NormalDrainStillWorks confirms that CloseCtx with a
// bounded linger and a responsive peer returns nil (no ctx error) and
// that queued messages arrive at the peer before the drain completes.
func TestCloseCtx_NormalDrainStillWorks(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	receiver, err := ctxZmq.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("new receiver: %v", err)
	}
	defer receiver.Close()
	endpoint := "inproc://closectx-normal-drain"
	if err := receiver.Bind(endpoint); err != nil {
		t.Fatalf("bind: %v", err)
	}

	sender, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithLinger(2*time.Second))
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if err := sender.Connect(endpoint); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Start receiver FIRST and actively drain so sender's writer never
	// backs up — the drain path is exercised by the writerCtx cancel
	// seeing no residual queue (WriterFlushed fires promptly) while
	// the writer is still fully functional.
	const N = 4
	recvDone := make(chan struct{})
	recvErr := make(chan error, 1)
	gotCount := make(chan int, 1)
	go func() {
		defer close(recvDone)
		recvCtx, recvCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer recvCancel()
		seen := 0
		for seen < N {
			m, err := receiver.RecvMsg(recvCtx)
			if err != nil {
				recvErr <- err
				break
			}
			if len(m.Bytes()) != 1 || m.Bytes()[0] != byte(seen) {
				recvErr <- errors.New("unexpected payload")
				break
			}
			seen++
		}
		gotCount <- seen
	}()

	// Give the handshake time to complete.
	time.Sleep(100 * time.Millisecond)

	sendCtx, sendCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer sendCancel()
	for i := 0; i < N; i++ {
		if err := sender.SendMsg(sendCtx, gozmq.NewMsgFromBytes([]byte{byte(i)})); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	// Give messages time to flow through before closing — this is the
	// "responsive peer" scenario, not the "drain at Close" scenario.
	// CloseCtx(Background()) should return nil, not leak, and not surface
	// a context error. This does not mandate that queued messages always
	// arrive; that is the separate WithLinger contract.
	time.Sleep(200 * time.Millisecond)

	if err := sender.CloseCtx(context.Background()); err != nil {
		t.Fatalf("CloseCtx err = %v, want nil", err)
	}

	<-recvDone
	select {
	case err := <-recvErr:
		t.Fatalf("receiver: %v", err)
	default:
	}
	if got := <-gotCount; got != N {
		t.Fatalf("receiver got %d/%d messages", got, N)
	}
}

// TestClose_DelegatesToCloseCtx confirms that Close uses the same teardown
// path as CloseCtx with a background context.
func TestClose_DelegatesToCloseCtx(t *testing.T) {
	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	receiver, err := ctxZmq.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("new receiver: %v", err)
	}
	defer receiver.Close()
	endpoint := "inproc://close-delegates"
	if err := receiver.Bind(endpoint); err != nil {
		t.Fatalf("bind: %v", err)
	}
	sender, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithLinger(200*time.Millisecond))
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if err := sender.Connect(endpoint); err != nil {
		t.Fatalf("connect: %v", err)
	}

	if err := sender.Close(); err != nil {
		t.Fatalf("Close() err = %v, want nil", err)
	}
	// Idempotent contract preserved.
	if err := sender.Close(); !errors.Is(err, gozmq.ErrSocketClosed) {
		t.Fatalf("second Close() err = %v, want ErrSocketClosed", err)
	}
}
