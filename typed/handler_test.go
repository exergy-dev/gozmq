package typed_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rkm/gozmq"
	"github.com/rkm/gozmq/typed"
	"go.uber.org/goleak"
)

func TestReceiver_Handle_Roundtrip(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatal(err)
	}
	defer push.Close()
	if err := push.Bind("inproc://typed-handle-pushpull"); err != nil {
		t.Fatal(err)
	}

	pull, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	if err := pull.Connect("inproc://typed-handle-pushpull"); err != nil {
		t.Fatal(err)
	}

	sender := typed.NewSender[payload](push, jsonCodec{})
	receiver := typed.NewReceiver[payload](pull, jsonCodec{})

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := []payload{
		{N: 1, Name: "a"},
		{N: 2, Name: "b"},
		{N: 3, Name: "c"},
	}
	for _, p := range want {
		if err := sender.Send(callCtx, p); err != nil {
			t.Fatalf("send %+v: %v", p, err)
		}
	}

	loopCtx, loopCancel := context.WithCancel(callCtx)
	defer loopCancel()

	var got []payload
	var mu sync.Mutex
	done := make(chan error, 1)
	go func() {
		done <- receiver.Handle(loopCtx, func(_ context.Context, v payload) error {
			mu.Lock()
			got = append(got, v)
			if len(got) == len(want) {
				loopCancel()
			}
			mu.Unlock()
			return nil
		})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Handle returned %v, want Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("got %d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("msg %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestSubscriber_Handle_TopicRoundtrip(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pubSock, err := zctx.NewSocket(gozmq.Pub)
	if err != nil {
		t.Fatal(err)
	}
	defer pubSock.Close()
	if err := pubSock.Bind("inproc://typed-handle-pubsub"); err != nil {
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
	if err := subSock.Connect("inproc://typed-handle-pubsub"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pub.Send(callCtx, "orders.new", payload{N: 1, Name: "w"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Send(callCtx, "metrics.cpu", payload{N: 99, Name: "cpu"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Send(callCtx, "orders.cancel", payload{N: 2, Name: "g"}); err != nil {
		t.Fatal(err)
	}

	loopCtx, loopCancel := context.WithCancel(callCtx)
	defer loopCancel()

	var got []typed.Message[payload]
	var mu sync.Mutex
	done := make(chan error, 1)
	go func() {
		done <- sub.Handle(loopCtx, func(_ context.Context, m typed.Message[payload]) error {
			mu.Lock()
			got = append(got, m)
			if len(got) == 2 {
				loopCancel()
			}
			mu.Unlock()
			return nil
		})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Handle returned %v, want Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return")
	}

	wants := []typed.Message[payload]{
		{Topic: "orders.new", Value: payload{N: 1, Name: "w"}},
		{Topic: "orders.cancel", Value: payload{N: 2, Name: "g"}},
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(wants) {
		t.Fatalf("got %d want %d", len(got), len(wants))
	}
	for i := range wants {
		if got[i] != wants[i] {
			t.Fatalf("msg %d: got %+v want %+v", i, got[i], wants[i])
		}
	}
}

func TestHandle_StopOnHandlerError(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatal(err)
	}
	defer push.Close()
	if err := push.Bind("inproc://typed-handle-stoperr"); err != nil {
		t.Fatal(err)
	}
	pull, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	if err := pull.Connect("inproc://typed-handle-stoperr"); err != nil {
		t.Fatal(err)
	}

	sender := typed.NewSender[payload](push, jsonCodec{})
	receiver := typed.NewReceiver[payload](pull, jsonCodec{})

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sender.Send(callCtx, payload{N: 1}); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("boom")
	err = receiver.Handle(callCtx,
		func(_ context.Context, _ payload) error { return boom },
		typed.WithErrorHandler(func(e error) error { return e }),
	)
	if !errors.Is(err, boom) {
		t.Fatalf("want errors.Is boom, got %v", err)
	}
	if !errors.Is(err, typed.ErrHandler) {
		t.Fatalf("want errors.Is ErrHandler, got %v", err)
	}
}

func TestHandle_ClassifyDecodeVsHandler(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatal(err)
	}
	defer push.Close()
	if err := push.Bind("inproc://typed-handle-classify"); err != nil {
		t.Fatal(err)
	}
	pull, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	if err := pull.Connect("inproc://typed-handle-classify"); err != nil {
		t.Fatal(err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Frame 1: malformed JSON → decode error.
	if err := push.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("not json"))); err != nil {
		t.Fatal(err)
	}
	// Frame 2: valid JSON → handler gets it and returns an error.
	sender := typed.NewSender[payload](push, jsonCodec{})
	if err := sender.Send(callCtx, payload{N: 7}); err != nil {
		t.Fatal(err)
	}

	receiver := typed.NewReceiver[payload](pull, jsonCodec{})

	var kinds []string
	boom := errors.New("boom")
	err = receiver.Handle(callCtx,
		func(_ context.Context, _ payload) error { return boom },
		typed.WithErrorHandler(func(e error) error {
			switch {
			case errors.Is(e, typed.ErrDecode):
				kinds = append(kinds, "decode")
				return nil // keep going
			case errors.Is(e, typed.ErrHandler):
				kinds = append(kinds, "handler")
				return e // stop
			default:
				return e
			}
		}),
	)
	if !errors.Is(err, typed.ErrHandler) || !errors.Is(err, boom) {
		t.Fatalf("want wrapped ErrHandler+boom, got %v", err)
	}
	if len(kinds) != 2 || kinds[0] != "decode" || kinds[1] != "handler" {
		t.Fatalf("classify order: got %v", kinds)
	}
}

func TestHandle_SocketClosedIsFatal(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pull, err := zctx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatal(err)
	}
	if err := pull.Bind("inproc://typed-handle-closed"); err != nil {
		t.Fatal(err)
	}

	receiver := typed.NewReceiver[payload](pull, jsonCodec{})

	done := make(chan error, 1)
	go func() {
		done <- receiver.Handle(context.Background(),
			func(_ context.Context, _ payload) error { return nil },
			// Error handler that would continue on socket error if we let it.
			typed.WithErrorHandler(func(error) error { return nil }),
		)
	}()

	time.Sleep(50 * time.Millisecond)
	if err := pull.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, gozmq.ErrSocketClosed) {
			t.Fatalf("want ErrSocketClosed, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return after socket close")
	}
}
