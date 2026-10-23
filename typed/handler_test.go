package typed_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/typed"
)

func TestReceiver_Handle_Roundtrip(t *testing.T) {
	t.Parallel()

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push, gozmq.WithLinger(0))
	require.NoError(t, err)
	defer push.Close()
	require.NoError(t, push.Bind("inproc://typed-handle-pushpull"))

	pull, err := zctx.NewSocket(gozmq.Pull, gozmq.WithLinger(0))
	require.NoError(t, err)
	defer pull.Close()
	require.NoError(t, pull.Connect("inproc://typed-handle-pushpull"))

	sender := typed.NewSender[payload](push, typed.JSONCodec{})
	receiver := typed.NewReceiver[payload](pull, typed.JSONCodec{})

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := []payload{
		{N: 1, Name: "a"},
		{N: 2, Name: "b"},
		{N: 3, Name: "c"},
	}
	for _, p := range want {
		require.NoError(t, sender.Send(callCtx, p), "send %+v", p)
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
		require.ErrorIs(t, err, context.Canceled, "Handle return error")
	case <-time.After(5 * time.Second):
		require.Fail(t, "Handle did not return")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i], got[i], "msg %d", i)
	}
}

func TestSubscriber_Handle_TopicRoundtrip(t *testing.T) {
	t.Parallel()

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pubSock, err := zctx.NewSocket(gozmq.Pub, gozmq.WithLinger(0))
	require.NoError(t, err)
	defer pubSock.Close()
	require.NoError(t, pubSock.Bind("inproc://typed-handle-pubsub"))

	subSock, err := zctx.NewSocket(gozmq.Sub, gozmq.WithLinger(0))
	require.NoError(t, err)
	defer subSock.Close()

	pub := typed.NewPublisher[payload](pubSock, typed.JSONCodec{})
	sub := typed.NewSubscriber[payload](subSock, typed.JSONCodec{})
	require.NoError(t, sub.Subscribe("orders."))
	require.NoError(t, subSock.Connect("inproc://typed-handle-pubsub"))
	syncSub(t, pubSock, subSock, "orders.")

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, pub.Send(callCtx, "orders.new", payload{N: 1, Name: "w"}))
	require.NoError(t, pub.Send(callCtx, "metrics.cpu", payload{N: 99, Name: "cpu"}))
	require.NoError(t, pub.Send(callCtx, "orders.cancel", payload{N: 2, Name: "g"}))

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
		require.ErrorIs(t, err, context.Canceled, "Handle return error")
	case <-time.After(5 * time.Second):
		require.Fail(t, "Handle did not return")
	}

	wants := []typed.Message[payload]{
		{Topic: "orders.new", Value: payload{N: 1, Name: "w"}},
		{Topic: "orders.cancel", Value: payload{N: 2, Name: "g"}},
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, len(wants))
	for i := range wants {
		require.Equal(t, wants[i], got[i], "msg %d", i)
	}
}

func TestHandle_StopOnHandlerError(t *testing.T) {
	t.Parallel()

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push, gozmq.WithLinger(0))
	require.NoError(t, err)
	defer push.Close()
	require.NoError(t, push.Bind("inproc://typed-handle-stoperr"))
	pull, err := zctx.NewSocket(gozmq.Pull, gozmq.WithLinger(0))
	require.NoError(t, err)
	defer pull.Close()
	require.NoError(t, pull.Connect("inproc://typed-handle-stoperr"))

	sender := typed.NewSender[payload](push, typed.JSONCodec{})
	receiver := typed.NewReceiver[payload](pull, typed.JSONCodec{})

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, sender.Send(callCtx, payload{N: 1}))

	boom := errors.New("boom")
	err = receiver.Handle(callCtx,
		func(_ context.Context, _ payload) error { return boom },
		typed.WithErrorHandler(func(e error) error { return e }),
	)
	require.ErrorIs(t, err, boom)
	require.ErrorIs(t, err, typed.ErrHandler)
}

func TestHandle_ClassifyDecodeVsHandler(t *testing.T) {
	t.Parallel()

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push, gozmq.WithLinger(0))
	require.NoError(t, err)
	defer push.Close()
	require.NoError(t, push.Bind("inproc://typed-handle-classify"))
	pull, err := zctx.NewSocket(gozmq.Pull, gozmq.WithLinger(0))
	require.NoError(t, err)
	defer pull.Close()
	require.NoError(t, pull.Connect("inproc://typed-handle-classify"))

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Frame 1: malformed JSON → decode error.
	require.NoError(t, push.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("not json"))))
	// Frame 2: valid JSON → handler gets it and returns an error.
	sender := typed.NewSender[payload](push, typed.JSONCodec{})
	require.NoError(t, sender.Send(callCtx, payload{N: 7}))

	receiver := typed.NewReceiver[payload](pull, typed.JSONCodec{})

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
	require.ErrorIs(t, err, typed.ErrHandler)
	require.ErrorIs(t, err, boom)
	require.Equal(t, []string{"decode", "handler"}, kinds, "classify order")
}

func TestHandle_SocketClosedIsFatal(t *testing.T) {
	t.Parallel()

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pull, err := zctx.NewSocket(gozmq.Pull, gozmq.WithLinger(0))
	require.NoError(t, err)
	require.NoError(t, pull.Bind("inproc://typed-handle-closed"))

	receiver := typed.NewReceiver[payload](pull, typed.JSONCodec{})

	err = testutil.RequireUnblocks(t, func() error {
		return receiver.Handle(context.Background(),
			func(_ context.Context, _ payload) error { return nil },
			// Error handler that would continue on socket error if we let it.
			typed.WithErrorHandler(func(error) error { return nil }),
		)
	}, func() { require.NoError(t, pull.Close(), "close") })
	require.ErrorIs(t, err, gozmq.ErrSocketClosed)
}
