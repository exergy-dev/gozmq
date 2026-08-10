//go:build interop

package interop

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

// TestDealerRouter_GozmqDealer_LibzmqRouter: gozmq DEALER configured with
// identity "worker-1" connects to a libzmq ROUTER bind. The ROUTER must see
// "worker-1" as the leading envelope frame.
func TestDealerRouter_GozmqDealer_LibzmqRouter(t *testing.T) {
	libCtx := newLibCtx(t)
	router := libCtx.NewSocket(ZMQ_ROUTER)
	t.Cleanup(func() { _ = router.Close() })
	if err := router.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq router bind: %v", err)
	}
	ep, err := router.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	dealer, err := gCtx.NewSocket(gozmq.Dealer, gozmq.WithIdentity([]byte("worker-1")))
	if err != nil {
		t.Fatalf("gozmq dealer new: %v", err)
	}
	t.Cleanup(func() { _ = dealer.Close() })
	if err := dealer.Connect(ep); err != nil {
		t.Fatalf("gozmq dealer connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// DEALER → ROUTER: ROUTER sees [identity, body...].
	body := []byte("dealer-payload")
	if err := dealer.SendMsg(callCtx, gozmq.NewMsgFromBytes(body)); err != nil {
		t.Fatalf("gozmq dealer send: %v", err)
	}
	got, err := router.Recv()
	if err != nil {
		t.Fatalf("libzmq router recv: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("router frames = %d, want 2 (identity + body): %v", len(got), got)
	}
	if string(got[0]) != "worker-1" {
		t.Fatalf("router identity = %q, want worker-1", got[0])
	}
	if string(got[1]) != string(body) {
		t.Fatalf("router body = %q, want %q", got[1], body)
	}

	// ROUTER → DEALER: ROUTER prepends identity, DEALER strips it.
	reply := []byte("router-reply")
	if err := router.Send([][]byte{[]byte("worker-1"), reply}); err != nil {
		t.Fatalf("libzmq router send: %v", err)
	}
	back, err := dealer.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq dealer recv: %v", err)
	}
	assertFramesEqual(t, back.Frames(), [][]byte{reply})
}

// TestDealerRouter_LibzmqDealer_GozmqRouter: libzmq DEALER with ZMQ_ROUTING_ID
// "worker-2" connects to a gozmq ROUTER bind. The gozmq ROUTER must surface
// "worker-2" as the leading envelope frame.
func TestDealerRouter_LibzmqDealer_GozmqRouter(t *testing.T) {
	gCtx := newGozmqCtx(t)
	router, err := gCtx.NewSocket(gozmq.Router)
	if err != nil {
		t.Fatalf("gozmq router new: %v", err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if err := router.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq router bind: %v", err)
	}
	ep := router.LastEndpoint()
	if ep == "" {
		t.Fatal("gozmq LastEndpoint empty after bind")
	}

	libCtx := newLibCtx(t)
	dealer := libCtx.NewSocket(ZMQ_DEALER)
	t.Cleanup(func() { _ = dealer.Close() })
	// ZMQ_ROUTING_ID is exposed by zmq_wrap.go via C.ZMQ_IDENTITY (same
	// option code; ZMQ_ROUTING_ID is the modern alias). Set it *before*
	// Connect so it is advertised in the ZMTP handshake Identity metadata.
	if err := dealer.SetOption(ZMQ_ROUTING_ID, []byte("worker-2")); err != nil {
		t.Fatalf("libzmq dealer set ROUTING_ID: %v", err)
	}
	if err := dealer.Connect(ep); err != nil {
		t.Fatalf("libzmq dealer connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// DEALER → ROUTER.
	body := []byte("payload-from-worker-2")
	if err := dealer.Send([][]byte{body}); err != nil {
		t.Fatalf("libzmq dealer send: %v", err)
	}
	got, err := router.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq router recv: %v", err)
	}
	if got.NumFrames() != 2 {
		t.Fatalf("router frames = %d, want 2 (identity + body): %v", got.NumFrames(), got.Frames())
	}
	if string(got.Frames()[0]) != "worker-2" {
		t.Fatalf("router identity = %q, want worker-2", got.Frames()[0])
	}
	if string(got.Frames()[1]) != string(body) {
		t.Fatalf("router body = %q, want %q", got.Frames()[1], body)
	}

	// ROUTER → DEALER.
	reply := []byte("ack-to-worker-2")
	if err := router.SendMsg(callCtx, gozmq.NewMsgFromFrames(
		[]byte("worker-2"), reply,
	)); err != nil {
		t.Fatalf("gozmq router send: %v", err)
	}
	back, err := dealer.Recv()
	if err != nil {
		t.Fatalf("libzmq dealer recv: %v", err)
	}
	assertFramesEqual(t, back, [][]byte{reply})
}
