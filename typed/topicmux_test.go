package typed_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rkm/gozmq"
	"github.com/rkm/gozmq/typed"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type order struct {
	ID int `json:"id"`
}

type alert struct {
	Severity string `json:"severity"`
}

func newPubSubPair(t *testing.T, endpoint string) (*gozmq.Context, *gozmq.Socket, *gozmq.Socket) {
	t.Helper()
	zctx := gozmq.NewContext()
	pubSock, err := zctx.NewSocket(gozmq.Pub)
	require.NoError(t, err)
	require.NoError(t, pubSock.Bind(endpoint))
	subSock, err := zctx.NewSocket(gozmq.Sub)
	require.NoError(t, err)
	return zctx, pubSock, subSock
}

func TestTopicMux_DispatchesHeterogeneousTypes(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-hetero")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewTopicMux(subSock, jsonCodec{})
	var (
		mu       sync.Mutex
		orders   []order
		alerts   []alert
		received int
	)
	done := make(chan struct{})
	require.NoError(t, typed.Register[order](router, "orders.", func(_ context.Context, m typed.Message[order]) error {
		mu.Lock()
		orders = append(orders, m.Value)
		received++
		if received == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}))
	require.NoError(t, typed.Register[alert](router, "alerts.", func(_ context.Context, m typed.Message[alert]) error {
		mu.Lock()
		alerts = append(alerts, m.Value)
		received++
		if received == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}))
	require.NoError(t, subSock.Connect("inproc://typed-router-hetero"))
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	orderPub := typed.NewPublisher[order](pubSock, jsonCodec{})
	alertPub := typed.NewPublisher[alert](pubSock, jsonCodec{})
	require.NoError(t, orderPub.Send(callCtx, "orders.created", order{ID: 1}))
	require.NoError(t, alertPub.Send(callCtx, "alerts.disk", alert{Severity: "warn"}))
	require.NoError(t, orderPub.Send(callCtx, "orders.cancelled", order{ID: 2}))

	loopCtx, loopCancel := context.WithCancel(callCtx)
	defer loopCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- router.Run(loopCtx) }()

	select {
	case <-done:
		loopCancel()
	case <-time.After(3 * time.Second):
		require.Fail(t, "did not receive all expected messages")
	}

	select {
	case err := <-runErr:
		require.ErrorIs(t, err, context.Canceled, "Run return error")
	case <-time.After(3 * time.Second):
		require.Fail(t, "Run did not return after cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, orders, 2)
	require.Equal(t, 1, orders[0].ID)
	require.Equal(t, 2, orders[1].ID)
	require.Len(t, alerts, 1)
	require.Equal(t, "warn", alerts[0].Severity)
}

func TestTopicMux_LongestPrefixWins(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-longest")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewTopicMux(subSock, jsonCodec{})

	var (
		mu     sync.Mutex
		winner string
	)
	done := make(chan struct{})
	// Register shorter first; the longest-match rule must still pick the longer.
	require.NoError(t, typed.Register[order](router, "orders.", func(_ context.Context, _ typed.Message[order]) error {
		mu.Lock()
		winner = "orders."
		close(done)
		mu.Unlock()
		return nil
	}))
	require.NoError(t, typed.Register[order](router, "orders.priority.", func(_ context.Context, _ typed.Message[order]) error {
		mu.Lock()
		winner = "orders.priority."
		close(done)
		mu.Unlock()
		return nil
	}))

	require.NoError(t, subSock.Connect("inproc://typed-router-longest"))
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pub := typed.NewPublisher[order](pubSock, jsonCodec{})
	require.NoError(t, pub.Send(callCtx, "orders.priority.urgent", order{ID: 7}))

	loopCtx, loopCancel := context.WithCancel(callCtx)
	defer loopCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- router.Run(loopCtx) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		require.Fail(t, "no dispatch")
	}
	loopCancel()
	<-runErr

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "orders.priority.", winner)
}

func TestTopicMux_RegisterAfterRunReturnsError(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-after-run")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewTopicMux(subSock, jsonCodec{})
	require.NoError(t, subSock.Connect("inproc://typed-router-after-run"))

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- router.Run(runCtx) }()

	// Give Run a moment to set started.
	time.Sleep(50 * time.Millisecond)

	err := typed.Register[order](router, "orders.", func(context.Context, typed.Message[order]) error { return nil })
	require.ErrorIs(t, err, typed.ErrTopicMuxRunning, "Register after Run")
	require.ErrorIs(t, router.Unregister("orders."), typed.ErrTopicMuxRunning, "Unregister after Run")

	runCancel()
	<-runErr

	// Once Run returns the latch resets: registration is allowed again.
	require.NoError(t, typed.Register[order](router, "more.",
		func(context.Context, typed.Message[order]) error { return nil }),
		"Register after Run returned")
}

func TestTopicMux_DuplicateTopicRejected(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-dup")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewTopicMux(subSock, jsonCodec{})
	require.NoError(t, typed.Register[order](router, "orders.", func(context.Context, typed.Message[order]) error { return nil }))
	err := typed.Register[order](router, "orders.", func(context.Context, typed.Message[order]) error { return nil })
	require.ErrorIs(t, err, typed.ErrTopicAlreadyRegistered)
}

func TestTopicMux_UnmatchedTopicSurfacesErrNoTopicMatch(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-nomatch")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewTopicMux(subSock, jsonCodec{})
	// Subscribe via the router to "orders.", but then also subscribe the raw
	// socket to "metrics." so the SUB filter will pass a topic the router
	// has no handler for.
	require.NoError(t, typed.Register[order](router, "orders.", func(context.Context, typed.Message[order]) error { return nil }))
	require.NoError(t, subSock.Subscribe([]byte("metrics.")))
	require.NoError(t, subSock.Connect("inproc://typed-router-nomatch"))
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metricsPub := typed.NewPublisher[order](pubSock, jsonCodec{})
	require.NoError(t, metricsPub.Send(callCtx, "metrics.cpu", order{ID: 99}))

	err := router.Run(callCtx, typed.WithErrorHandler(func(e error) error {
		if errors.Is(e, typed.ErrNoTopicMatch) {
			return e // stop
		}
		return nil
	}))
	require.ErrorIs(t, err, typed.ErrNoTopicMatch)
}
