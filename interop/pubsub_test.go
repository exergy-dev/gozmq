//go:build interop

package interop

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

// TestPubSub_GozmqPub_LibzmqSub: gozmq PUB binds, libzmq SUB connects and
// subscribes to "alpha". Publishes one matching, one non-matching, one
// matching — the SUB must receive only the two "alpha*" messages.
func TestPubSub_GozmqPub_LibzmqSub(t *testing.T) {
	gCtx := newGozmqCtx(t)
	pub, err := gCtx.NewSocket(gozmq.Pub)
	if err != nil {
		t.Fatalf("gozmq pub new: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	if err := pub.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq pub bind: %v", err)
	}
	ep := pub.LastEndpoint()
	if ep == "" {
		t.Fatal("gozmq LastEndpoint empty after bind")
	}

	libCtx := newLibCtx(t)
	sub := libCtx.NewSocket(ZMQ_SUB)
	t.Cleanup(func() { _ = sub.Close() })
	if err := sub.Subscribe([]byte("alpha")); err != nil {
		t.Fatalf("libzmq sub subscribe: %v", err)
	}
	if err := sub.Connect(ep); err != nil {
		t.Fatalf("libzmq sub connect: %v", err)
	}

	// Slow-joiner: let the ZMTP handshake finish and libzmq's subscription
	// forward propagate to the gozmq PUB's filter tree before publishing.
	time.Sleep(200 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, payload := range []string{"alphaOne", "betaTwo", "alphaThree"} {
		if err := pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte(payload))); err != nil {
			t.Fatalf("gozmq pub send %s: %v", payload, err)
		}
	}

	for _, want := range []string{"alphaOne", "alphaThree"} {
		got, err := sub.Recv()
		if err != nil {
			t.Fatalf("libzmq sub recv %s: %v", want, err)
		}
		if len(got) != 1 || string(got[0]) != want {
			t.Fatalf("libzmq sub got %v, want %q", got, want)
		}
	}
}

// TestPubSub_LibzmqPub_GozmqSub: libzmq PUB binds, gozmq SUB connects,
// subscribes to "alpha". Same filtering guarantee, reverse direction.
func TestPubSub_LibzmqPub_GozmqSub(t *testing.T) {
	libCtx := newLibCtx(t)
	pub := libCtx.NewSocket(ZMQ_PUB)
	t.Cleanup(func() { _ = pub.Close() })
	if err := pub.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq pub bind: %v", err)
	}
	ep, err := pub.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	sub, err := gCtx.NewSocket(gozmq.Sub)
	if err != nil {
		t.Fatalf("gozmq sub new: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	if err := sub.Subscribe([]byte("alpha")); err != nil {
		t.Fatalf("gozmq sub subscribe: %v", err)
	}
	if err := sub.Connect(ep); err != nil {
		t.Fatalf("gozmq sub connect: %v", err)
	}

	// Slow-joiner: allow the SUBSCRIBE command to reach the libzmq PUB
	// before it publishes, otherwise early matching messages are dropped.
	time.Sleep(200 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, payload := range []string{"alphaOne", "betaTwo", "alphaThree"} {
		if err := pub.Send([][]byte{[]byte(payload)}); err != nil {
			t.Fatalf("libzmq pub send %s: %v", payload, err)
		}
	}

	for _, want := range []string{"alphaOne", "alphaThree"} {
		got, err := sub.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("gozmq sub recv %s: %v", want, err)
		}
		if string(got.Bytes()) != want {
			t.Fatalf("gozmq sub got %q, want %q", got.Bytes(), want)
		}
	}
}
