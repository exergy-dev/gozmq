package typed_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/rkm/gozmq"
	"github.com/rkm/gozmq/typed"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	defer push.Close()
	require.NoError(t, push.Bind("inproc://typed-pushpull"))

	pull, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err)
	defer pull.Close()
	require.NoError(t, pull.Connect("inproc://typed-pushpull"))

	sender := typed.NewSender[payload](push, jsonCodec{})
	receiver := typed.NewReceiver[payload](pull, jsonCodec{})

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := payload{N: 42, Name: "answer"}
	require.NoError(t, sender.Send(callCtx, want), "send")
	got, err := receiver.Recv(callCtx)
	require.NoError(t, err, "recv")
	require.Equal(t, want, got)
}

func TestPublisherSubscriber_TopicRoundtrip(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pubSock, err := zctx.NewSocket(gozmq.Pub)
	require.NoError(t, err)
	defer pubSock.Close()
	require.NoError(t, pubSock.Bind("inproc://typed-pubsub"))

	subSock, err := zctx.NewSocket(gozmq.Sub)
	require.NoError(t, err)
	defer subSock.Close()

	pub := typed.NewPublisher[payload](pubSock, jsonCodec{})
	sub := typed.NewSubscriber[payload](subSock, jsonCodec{})

	require.NoError(t, sub.Subscribe("orders."))
	require.NoError(t, subSock.Connect("inproc://typed-pubsub"))

	// Slow-joiner: allow subscription to propagate before publishing.
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Matching and non-matching topics; the filter must drop the latter.
	require.NoError(t, pub.Send(callCtx, "orders.new", payload{N: 1, Name: "widget"}), "pub orders.new")
	require.NoError(t, pub.Send(callCtx, "metrics.cpu", payload{N: 99, Name: "cpu"}), "pub metrics.cpu")
	require.NoError(t, pub.Send(callCtx, "orders.cancel", payload{N: 2, Name: "gadget"}), "pub orders.cancel")

	wants := []typed.Message[payload]{
		{Topic: "orders.new", Value: payload{N: 1, Name: "widget"}},
		{Topic: "orders.cancel", Value: payload{N: 2, Name: "gadget"}},
	}
	for _, want := range wants {
		got, err := sub.Recv(callCtx)
		require.NoError(t, err, "recv %s", want.Topic)
		require.Equal(t, want, got)
	}
}

func TestSender_MarshalErrorPropagates(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push)
	require.NoError(t, err)
	defer push.Close()
	require.NoError(t, push.Bind("inproc://typed-marshal-err"))

	boom := errors.New("boom")
	sender := typed.NewSender[payload](push, failingCodec{err: boom})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, sender.Send(ctx, payload{}), boom)
}

func TestReceiver_WrongFrameCount(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push)
	require.NoError(t, err)
	defer push.Close()
	require.NoError(t, push.Bind("inproc://typed-badframes"))

	pull, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err)
	defer pull.Close()
	require.NoError(t, pull.Connect("inproc://typed-badframes"))

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send a two-frame message via the raw socket; the single-frame
	// Receiver must surface ErrFrameCount.
	require.NoError(t, push.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte("a"), []byte("b"))), "raw send")

	receiver := typed.NewReceiver[payload](pull, jsonCodec{})
	_, err = receiver.Recv(callCtx)
	require.ErrorIs(t, err, typed.ErrFrameCount)
}

func TestReceiver_ContextCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pull, err := zctx.NewSocket(gozmq.Pull)
	require.NoError(t, err)
	defer pull.Close()
	require.NoError(t, pull.Bind("inproc://typed-cancel"))

	receiver := typed.NewReceiver[payload](pull, jsonCodec{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = receiver.Recv(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestSubscriber_Unsubscribe verifies the Subscriber's filter management
// end to end: after Unsubscribe the topic is no longer delivered. The
// sentinel "zz." subscription is issued AFTER the unsubscribe, and both
// control messages travel the same ordered upstream path — so once a
// "zz." message arrives, the "orders." unsubscribe is provably in effect
// at the publisher.
func TestSubscriber_Unsubscribe(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pubSock, err := zctx.NewSocket(gozmq.Pub)
	require.NoError(t, err)
	defer pubSock.Close()
	require.NoError(t, pubSock.Bind("inproc://typed-unsub"))

	subSock, err := zctx.NewSocket(gozmq.Sub)
	require.NoError(t, err)
	defer subSock.Close()

	pub := typed.NewPublisher[payload](pubSock, jsonCodec{})
	sub := typed.NewSubscriber[payload](subSock, jsonCodec{})

	require.NoError(t, sub.Subscribe("orders."))
	require.NoError(t, subSock.Connect("inproc://typed-unsub"))

	// Probe until the initial subscription has propagated.
	recvProbe := func(topic string) bool {
		for i := 0; i < 100; i++ {
			require.NoError(t, pub.Send(context.Background(), topic, payload{N: i}))
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			_, err := sub.Recv(ctx)
			cancel()
			if err == nil {
				return true
			}
		}
		return false
	}
	require.True(t, recvProbe("orders.probe"), "initial subscription never propagated")

	require.NoError(t, sub.Unsubscribe("orders."))
	require.NoError(t, sub.Subscribe("zz."))
	require.True(t, recvProbe("zz.probe"), "sentinel subscription never propagated")

	// Drain any extra in-flight probes.
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := sub.Recv(ctx)
		cancel()
		if err != nil {
			break
		}
	}

	// "orders." is unsubscribed: only the sentinel must arrive.
	require.NoError(t, pub.Send(context.Background(), "orders.gone", payload{N: 1}))
	require.NoError(t, pub.Send(context.Background(), "zz.fin", payload{N: 2}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := sub.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, "zz.fin", got.Topic, "unsubscribed topic leaked through the filter")
}
