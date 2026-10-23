//go:build interop

package interop

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

func TestRouterMandatory_GozmqRouter_LibzmqDealer(t *testing.T) {
	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	router, err := gCtx.NewSocket(gozmq.Router,
		gozmq.WithRouterMandatory(true),
		gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq router new: %v", err)
	}
	t.Cleanup(func() { _ = router.Close() })
	if err := router.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq router bind: %v", err)
	}

	libCtx := newLibCtx(t)
	dealer := libCtx.NewSocket(ZMQ_DEALER)
	t.Cleanup(func() { _ = dealer.Close() })
	if err := dealer.SetOption(ZMQ_ROUTING_ID, []byte("known-peer")); err != nil {
		t.Fatalf("libzmq dealer set ROUTING_ID: %v", err)
	}
	if err := dealer.Connect(router.LastEndpoint()); err != nil {
		t.Fatalf("libzmq dealer connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake event = %+v, want success", ev)
	}

	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	err = router.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte("unknown-peer"), []byte("no-route")))
	if err == nil {
		t.Fatal("gozmq router send unexpectedly succeeded")
	}
	if !errors.Is(err, gozmq.ErrRouteUnknown) {
		t.Fatalf("router send err = %v, want ErrRouteUnknown", err)
	}
}

func TestRouterMandatory_LibzmqRouter_GozmqDealer(t *testing.T) {
	libCtx := newLibCtx(t)
	router := libCtx.NewSocket(ZMQ_ROUTER)
	t.Cleanup(func() { _ = router.Close() })
	if err := router.SetOption(ZMQ_ROUTER_MANDATORY, 1); err != nil {
		t.Fatalf("libzmq set ROUTER_MANDATORY: %v", err)
	}
	if err := router.SetOption(ZMQ_SNDTIMEO, 250); err != nil {
		t.Fatalf("libzmq set SNDTIMEO: %v", err)
	}
	if err := router.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq router bind: %v", err)
	}
	ep, err := router.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	dealer, err := gCtx.NewSocket(gozmq.Dealer, gozmq.WithIdentity([]byte("known-peer")))
	if err != nil {
		t.Fatalf("gozmq dealer new: %v", err)
	}
	t.Cleanup(func() { _ = dealer.Close() })
	if err := dealer.Connect(ep); err != nil {
		t.Fatalf("gozmq dealer connect: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	err = router.Send([][]byte{[]byte("unknown-peer"), []byte("no-route")})
	if err == nil {
		t.Fatal("libzmq router send unexpectedly succeeded")
	}
}

func TestXPubVerbose_GozmqXPub_LibzmqXSub(t *testing.T) {
	gCtx := newGozmqCtx(t)
	xpub, err := gCtx.NewSocket(gozmq.XPub, gozmq.WithXPubVerbose(true))
	if err != nil {
		t.Fatalf("gozmq xpub new: %v", err)
	}
	t.Cleanup(func() { _ = xpub.Close() })
	if err := xpub.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq xpub bind: %v", err)
	}

	libCtx := newLibCtx(t)
	xsub := libCtx.NewSocket(ZMQ_XSUB)
	t.Cleanup(func() { _ = xsub.Close() })
	if err := xsub.Connect(xpub.LastEndpoint()); err != nil {
		t.Fatalf("libzmq xsub connect: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	sub := testutil.SubscribeFrame(true, []byte("alpha"))
	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if err := xsub.Send([][]byte{sub}); err != nil {
			t.Fatalf("libzmq xsub send subscribe #%d: %v", i+1, err)
		}
		msg, err := xpub.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("gozmq xpub recv subscribe #%d: %v", i+1, err)
		}
		assertFramesEqual(t, msg.Frames(), [][]byte{sub})
	}
}

func TestXPubVerbose_LibzmqXPub_GozmqXSub(t *testing.T) {
	libCtx := newLibCtx(t)
	xpub := libCtx.NewSocket(ZMQ_XPUB)
	t.Cleanup(func() { _ = xpub.Close() })
	if err := xpub.SetOption(ZMQ_XPUB_VERBOSE, 1); err != nil {
		t.Fatalf("libzmq set XPUB_VERBOSE: %v", err)
	}
	if err := xpub.SetOption(ZMQ_RCVTIMEO, 1000); err != nil {
		t.Fatalf("libzmq set RCVTIMEO: %v", err)
	}
	if err := xpub.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq xpub bind: %v", err)
	}
	ep, err := xpub.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	xsub, err := gCtx.NewSocket(gozmq.XSub)
	if err != nil {
		t.Fatalf("gozmq xsub new: %v", err)
	}
	t.Cleanup(func() { _ = xsub.Close() })
	if err := xsub.Connect(ep); err != nil {
		t.Fatalf("gozmq xsub connect: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub := testutil.SubscribeFrame(true, []byte("alpha"))
	for i := 0; i < 2; i++ {
		if err := xsub.SendMsg(callCtx, gozmq.NewMsgFromBytes(sub)); err != nil {
			t.Fatalf("gozmq xsub send subscribe #%d: %v", i+1, err)
		}
		msg, err := xpub.Recv()
		if err != nil {
			t.Fatalf("libzmq xpub recv subscribe #%d: %v", i+1, err)
		}
		assertFramesEqual(t, msg, [][]byte{sub})
	}
}
