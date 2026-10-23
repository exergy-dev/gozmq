package typed_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/typed"
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
	pubSock, err := zctx.NewSocket(gozmq.Pub, gozmq.WithLinger(0))
	require.NoError(t, err)
	require.NoError(t, pubSock.Bind(endpoint))
	subSock, err := zctx.NewSocket(gozmq.Sub, gozmq.WithLinger(0))
	require.NoError(t, err)
	return zctx, pubSock, subSock
}

func TestTopicMux_DispatchesHeterogeneousTypes(t *testing.T) {
	t.Parallel()

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
	syncSub(t, pubSock, subSock, "orders.")
	syncSub(t, pubSock, subSock, "alerts.")

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
	t.Parallel()

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
	syncSub(t, pubSock, subSock, "orders.priority.")

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
	t.Parallel()

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

	// Run latches started asynchronously; wait for it rather than sleep.
	require.Eventually(t, func() bool {
		err := typed.Register[order](router, "orders.", func(context.Context, typed.Message[order]) error { return nil })
		return errors.Is(err, typed.ErrTopicMuxRunning)
	}, 5*time.Second, time.Millisecond, "Register after Run")
	require.ErrorIs(t, router.Unregister("orders."), typed.ErrTopicMuxRunning, "Unregister after Run")

	runCancel()
	<-runErr

	// Once Run returns the latch resets: registration is allowed again.
	require.NoError(t, typed.Register[order](router, "more.",
		func(context.Context, typed.Message[order]) error { return nil }),
		"Register after Run returned")
}

func TestTopicMux_DuplicateTopicRejected(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	syncSub(t, pubSock, subSock, "metrics.")

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

// A second Run while one is executing must be rejected, not run alongside
// it (and must not clear the first Run's latch when it returns).
func TestTopicMux_ConcurrentRunRejected(t *testing.T) {
	t.Parallel()

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-concurrent-run")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewTopicMux(subSock, jsonCodec{})
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- router.Run(runCtx) }()
	require.Eventually(t, func() bool {
		return errors.Is(router.Unregister("x."), typed.ErrTopicMuxRunning)
	}, 5*time.Second, time.Millisecond, "first Run never started")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, router.Run(ctx), typed.ErrTopicMuxRunning, "second Run")
	require.ErrorIs(t, router.Unregister("x."), typed.ErrTopicMuxRunning,
		"rejected Run must leave the first Run's latch set")

	runCancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
}

// Register racing a starting Run must either land before Run snapshots the
// handlers or be rejected, never mutate the table Run is reading. Run under
// -race: messages already queued make Run consult the table immediately.
func TestTopicMux_RegisterRacesRun(t *testing.T) {
	t.Parallel()

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-register-race")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewTopicMux(subSock, jsonCodec{})
	var got atomic.Int32
	require.NoError(t, typed.Register[order](router, "a.", func(context.Context, typed.Message[order]) error {
		got.Add(1)
		return nil
	}))
	require.NoError(t, subSock.Connect("inproc://typed-router-register-race"))
	syncSub(t, pubSock, subSock, "a.")

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pub := typed.NewPublisher[order](pubSock, jsonCodec{})
	const n = 50
	for i := range n {
		require.NoError(t, pub.Send(callCtx, "a.x", order{ID: i}))
	}

	regDone := make(chan struct{})
	go func() {
		defer close(regDone)
		for i := 0; ; i++ {
			err := typed.Register[order](router, fmt.Sprintf("b%d.", i),
				func(context.Context, typed.Message[order]) error { return nil })
			if errors.Is(err, typed.ErrTopicMuxRunning) {
				return
			}
			assert.NoError(t, err)
		}
	}()
	loopCtx, loopCancel := context.WithCancel(callCtx)
	defer loopCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- router.Run(loopCtx) }()

	<-regDone
	require.Eventually(t, func() bool { return got.Load() == n }, 5*time.Second, time.Millisecond)
	loopCancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
}
