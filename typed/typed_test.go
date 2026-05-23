package typed_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq"
	"github.com/rkm/gozmq/typed"
	"go.uber.org/goleak"
)

// jsonCodec is a trivial Codec used only for tests. Keeping the package
// free of built-in codecs is intentional; we define one here to exercise
// roundtrips.
type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (jsonCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// failingCodec returns a fixed error from Marshal and panics if Unmarshal
// is called — used to verify marshal-error propagation.
type failingCodec struct{ err error }

func (f failingCodec) Marshal(any) ([]byte, error) { return nil, f.err }
func (failingCodec) Unmarshal([]byte, any) error   { panic("not expected") }

type payload struct {
	N    int    `json:"n"`
	Name string `json:"name"`
}

func TestSenderReceiver_PushPull(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatal(err)
	}
	defer push.Close()
	if err := push.Bind("inproc://typed-pushpull"); err != nil {
		t.Fatal(err)
	}

	pull, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	if err := pull.Connect("inproc://typed-pushpull"); err != nil {
		t.Fatal(err)
	}

	sender := typed.NewSender[payload](push, jsonCodec{})
	receiver := typed.NewReceiver[payload](pull, jsonCodec{})

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := payload{N: 42, Name: "answer"}
	if err := sender.Send(callCtx, want); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := receiver.Recv(callCtx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestPublisherSubscriber_TopicRoundtrip(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pubSock, err := zctx.NewSocket(gozmq.Pub)
	if err != nil {
		t.Fatal(err)
	}
	defer pubSock.Close()
	if err := pubSock.Bind("inproc://typed-pubsub"); err != nil {
		t.Fatal(err)
	}

	subSock, err := zctx.NewSocket(gozmq.Sub)
	if err != nil {
		t.Fatal(err)
	}
	defer subSock.Close()

	pub := typed.NewPublisher[payload](pubSock, jsonCodec{})
	sub := typed.NewSubscriber[payload](subSock, jsonCodec{})

	if err := sub.Subscribe("orders."); err != nil {
		t.Fatal(err)
	}
	if err := subSock.Connect("inproc://typed-pubsub"); err != nil {
		t.Fatal(err)
	}

	// Slow-joiner: allow subscription to propagate before publishing.
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Matching and non-matching topics; the filter must drop the latter.
	if err := pub.Send(callCtx, "orders.new", payload{N: 1, Name: "widget"}); err != nil {
		t.Fatalf("pub orders.new: %v", err)
	}
	if err := pub.Send(callCtx, "metrics.cpu", payload{N: 99, Name: "cpu"}); err != nil {
		t.Fatalf("pub metrics.cpu: %v", err)
	}
	if err := pub.Send(callCtx, "orders.cancel", payload{N: 2, Name: "gadget"}); err != nil {
		t.Fatalf("pub orders.cancel: %v", err)
	}

	wants := []typed.Message[payload]{
		{Topic: "orders.new", Value: payload{N: 1, Name: "widget"}},
		{Topic: "orders.cancel", Value: payload{N: 2, Name: "gadget"}},
	}
	for _, want := range wants {
		got, err := sub.Recv(callCtx)
		if err != nil {
			t.Fatalf("recv %s: %v", want.Topic, err)
		}
		if got != want {
			t.Fatalf("got %+v want %+v", got, want)
		}
	}
}

func TestSender_MarshalErrorPropagates(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatal(err)
	}
	defer push.Close()
	if err := push.Bind("inproc://typed-marshal-err"); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("boom")
	sender := typed.NewSender[payload](push, failingCodec{err: boom})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sender.Send(ctx, payload{}); !errors.Is(err, boom) {
		t.Fatalf("want errors.Is boom, got %v", err)
	}
}

func TestReceiver_WrongFrameCount(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatal(err)
	}
	defer push.Close()
	if err := push.Bind("inproc://typed-badframes"); err != nil {
		t.Fatal(err)
	}

	pull, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	if err := pull.Connect("inproc://typed-badframes"); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send a two-frame message via the raw socket; the single-frame
	// Receiver must surface ErrFrameCount.
	if err := push.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte("a"), []byte("b"))); err != nil {
		t.Fatalf("raw send: %v", err)
	}

	receiver := typed.NewReceiver[payload](pull, jsonCodec{})
	_, err = receiver.Recv(callCtx)
	if !errors.Is(err, typed.ErrFrameCount) {
		t.Fatalf("want ErrFrameCount, got %v", err)
	}
}

func TestReceiver_ContextCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pull, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	if err := pull.Bind("inproc://typed-cancel"); err != nil {
		t.Fatal(err)
	}

	receiver := typed.NewReceiver[payload](pull, jsonCodec{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = receiver.Recv(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}
