//go:build interop

package interop

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

// subscribeFrame builds the ZMTP synthetic subscribe frame that XSUB sends
// to its XPUB peer: a single frame whose first byte is 0x01 (subscribe) or
// 0x00 (unsubscribe), followed by the topic prefix.
func subscribeFrame(subscribe bool, topic []byte) []byte {
	flag := byte(0x00)
	if subscribe {
		flag = 0x01
	}
	buf := make([]byte, 0, 1+len(topic))
	buf = append(buf, flag)
	buf = append(buf, topic...)
	return buf
}

// TestXPubXSub_GozmqXPub_LibzmqXSub: gozmq XPUB binds, libzmq XSUB connects
// and sends a synthetic subscribe frame for "alpha". The gozmq XPUB surfaces
// that subscription on its Recv path (verifying the SUBSCRIBE reaches the
// app) and then publishes; only matching topics reach the libzmq XSUB.
func TestXPubXSub_GozmqXPub_LibzmqXSub(t *testing.T) {
	gCtx := newGozmqCtx(t)
	xpub, err := gCtx.NewSocket(gozmq.XPub)
	if err != nil {
		t.Fatalf("gozmq xpub new: %v", err)
	}
	t.Cleanup(func() { _ = xpub.Close() })
	if err := xpub.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq xpub bind: %v", err)
	}
	ep := xpub.LastEndpoint()
	if ep == "" {
		t.Fatal("gozmq LastEndpoint empty")
	}

	libCtx := newLibCtx(t)
	xsub := libCtx.NewSocket(ZMQ_XSUB)
	t.Cleanup(func() { _ = xsub.Close() })
	if err := xsub.Connect(ep); err != nil {
		t.Fatalf("libzmq xsub connect: %v", err)
	}

	// Small beat so the ZMTP handshake completes before XSUB sends its
	// subscribe frame — libzmq queues pre-handshake sends, but letting the
	// pipe come up makes the subscription immediately visible to XPUB.
	time.Sleep(50 * time.Millisecond)

	// XSUB sends its subscribe as a user-frame: [0x01, "alpha"].
	if err := xsub.Send([][]byte{subscribeFrame(true, []byte("alpha"))}); err != nil {
		t.Fatalf("libzmq xsub send subscribe: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// gozmq XPUB surfaces the subscription to the application.
	surfaced, err := xpub.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq xpub recv subscription: %v", err)
	}
	if surfaced.NumFrames() != 1 {
		t.Fatalf("xpub surfaced frames = %d, want 1", surfaced.NumFrames())
	}
	body := surfaced.Frames()[0]
	if len(body) != 1+len("alpha") || body[0] != 0x01 || string(body[1:]) != "alpha" {
		t.Fatalf("xpub surfaced body = %v, want 0x01+alpha", body)
	}

	// Now XPUB publishes; the subscription tree is already updated so
	// "alpha*" reaches the XSUB while "beta*" is filtered at the XPUB.
	for _, payload := range []string{"alphaOne", "betaTwo", "alphaThree"} {
		if err := xpub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte(payload))); err != nil {
			t.Fatalf("xpub send %s: %v", payload, err)
		}
	}

	for _, want := range []string{"alphaOne", "alphaThree"} {
		got, err := xsub.Recv()
		if err != nil {
			t.Fatalf("libzmq xsub recv %s: %v", want, err)
		}
		if len(got) != 1 || string(got[0]) != want {
			t.Fatalf("libzmq xsub got %v, want %q", got, want)
		}
	}
}

// TestXPubXSub_LibzmqXPub_GozmqXSub: libzmq XPUB binds, gozmq XSUB connects.
// gozmq sends a synthetic subscribe frame to the libzmq XPUB, which drives
// its per-peer filter; then libzmq XPUB publishes and only "alpha*" reaches
// the gozmq XSUB.
func TestXPubXSub_LibzmqXPub_GozmqXSub(t *testing.T) {
	libCtx := newLibCtx(t)
	xpub := libCtx.NewSocket(ZMQ_XPUB)
	t.Cleanup(func() { _ = xpub.Close() })
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

	// Handshake beat.
	time.Sleep(50 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// gozmq XSUB sends a synthetic SUBSCRIBE message to drive the libzmq
	// XPUB's subscription tree.
	if err := xsub.SendMsg(callCtx, gozmq.NewMsgFromBytes(subscribeFrame(true, []byte("alpha")))); err != nil {
		t.Fatalf("gozmq xsub send subscribe: %v", err)
	}

	// libzmq XPUB surfaces the subscription to the application; drain it so
	// it does not collide with the data-read loop below. (XPUB by default
	// deduplicates, so this is exactly one frame.)
	sub, err := xpub.Recv()
	if err != nil {
		t.Fatalf("libzmq xpub recv subscription: %v", err)
	}
	if len(sub) != 1 || sub[0][0] != 0x01 || string(sub[0][1:]) != "alpha" {
		t.Fatalf("libzmq xpub surfaced sub = %v, want 0x01+alpha", sub)
	}

	// Give the subscription a beat to fully bind to the per-peer filter
	// before publishing (the libzmq API surfacing and the filter update are
	// not strictly ordered across threads).
	time.Sleep(50 * time.Millisecond)

	for _, payload := range []string{"alphaOne", "betaTwo", "alphaThree"} {
		if err := xpub.Send([][]byte{[]byte(payload)}); err != nil {
			t.Fatalf("libzmq xpub send %s: %v", payload, err)
		}
	}

	for _, want := range []string{"alphaOne", "alphaThree"} {
		got, err := xsub.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("gozmq xsub recv %s: %v", want, err)
		}
		if string(got.Bytes()) != want {
			t.Fatalf("gozmq xsub got %q, want %q", got.Bytes(), want)
		}
	}
}
