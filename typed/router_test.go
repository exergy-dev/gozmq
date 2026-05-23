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
	if err != nil {
		t.Fatal(err)
	}
	if err := pubSock.Bind(endpoint); err != nil {
		t.Fatal(err)
	}
	subSock, err := zctx.NewSocket(gozmq.Sub)
	if err != nil {
		t.Fatal(err)
	}
	return zctx, pubSock, subSock
}

func TestRouter_DispatchesHeterogeneousTypes(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-hetero")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewRouter(subSock, jsonCodec{})
	var (
		mu       sync.Mutex
		orders   []order
		alerts   []alert
		received int
	)
	done := make(chan struct{})
	if err := typed.Register[order](router, "orders.", func(_ context.Context, m typed.Message[order]) error {
		mu.Lock()
		orders = append(orders, m.Value)
		received++
		if received == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := typed.Register[alert](router, "alerts.", func(_ context.Context, m typed.Message[alert]) error {
		mu.Lock()
		alerts = append(alerts, m.Value)
		received++
		if received == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := subSock.Connect("inproc://typed-router-hetero"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	orderPub := typed.NewPublisher[order](pubSock, jsonCodec{})
	alertPub := typed.NewPublisher[alert](pubSock, jsonCodec{})
	if err := orderPub.Send(callCtx, "orders.created", order{ID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := alertPub.Send(callCtx, "alerts.disk", alert{Severity: "warn"}); err != nil {
		t.Fatal(err)
	}
	if err := orderPub.Send(callCtx, "orders.cancelled", order{ID: 2}); err != nil {
		t.Fatal(err)
	}

	loopCtx, loopCancel := context.WithCancel(callCtx)
	defer loopCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- router.Run(loopCtx) }()

	select {
	case <-done:
		loopCancel()
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive all expected messages")
	}

	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(orders) != 2 || orders[0].ID != 1 || orders[1].ID != 2 {
		t.Fatalf("orders: got %+v", orders)
	}
	if len(alerts) != 1 || alerts[0].Severity != "warn" {
		t.Fatalf("alerts: got %+v", alerts)
	}
}

func TestRouter_LongestPrefixWins(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-longest")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewRouter(subSock, jsonCodec{})

	var (
		mu     sync.Mutex
		winner string
	)
	done := make(chan struct{})
	// Register shorter first; the longest-match rule must still pick the longer.
	if err := typed.Register[order](router, "orders.", func(_ context.Context, _ typed.Message[order]) error {
		mu.Lock()
		winner = "orders."
		close(done)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := typed.Register[order](router, "orders.priority.", func(_ context.Context, _ typed.Message[order]) error {
		mu.Lock()
		winner = "orders.priority."
		close(done)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := subSock.Connect("inproc://typed-router-longest"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pub := typed.NewPublisher[order](pubSock, jsonCodec{})
	if err := pub.Send(callCtx, "orders.priority.urgent", order{ID: 7}); err != nil {
		t.Fatal(err)
	}

	loopCtx, loopCancel := context.WithCancel(callCtx)
	defer loopCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- router.Run(loopCtx) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("no dispatch")
	}
	loopCancel()
	<-runErr

	mu.Lock()
	defer mu.Unlock()
	if winner != "orders.priority." {
		t.Fatalf("winner = %q, want orders.priority.", winner)
	}
}

func TestRouter_RegisterAfterRunReturnsError(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-after-run")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewRouter(subSock, jsonCodec{})
	if err := subSock.Connect("inproc://typed-router-after-run"); err != nil {
		t.Fatal(err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- router.Run(runCtx) }()

	// Give Run a moment to set started.
	time.Sleep(50 * time.Millisecond)

	err := typed.Register[order](router, "orders.", func(context.Context, typed.Message[order]) error { return nil })
	if !errors.Is(err, typed.ErrRouterRunning) {
		t.Fatalf("Register after Run: got %v want ErrRouterRunning", err)
	}
	if err := router.Unregister("orders."); !errors.Is(err, typed.ErrRouterRunning) {
		t.Fatalf("Unregister after Run: got %v want ErrRouterRunning", err)
	}

	runCancel()
	<-runErr
}

func TestRouter_DuplicateTopicRejected(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-dup")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewRouter(subSock, jsonCodec{})
	if err := typed.Register[order](router, "orders.", func(context.Context, typed.Message[order]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	err := typed.Register[order](router, "orders.", func(context.Context, typed.Message[order]) error { return nil })
	if !errors.Is(err, typed.ErrTopicAlreadyRegistered) {
		t.Fatalf("got %v want ErrTopicAlreadyRegistered", err)
	}
}

func TestRouter_UnmatchedTopicSurfacesErrNoRouterMatch(t *testing.T) {
	defer goleak.VerifyNone(t)

	zctx, pubSock, subSock := newPubSubPair(t, "inproc://typed-router-nomatch")
	defer zctx.Close()
	defer pubSock.Close()
	defer subSock.Close()

	router := typed.NewRouter(subSock, jsonCodec{})
	// Subscribe via the router to "orders.", but then also subscribe the raw
	// socket to "metrics." so the SUB filter will pass a topic the router
	// has no handler for.
	if err := typed.Register[order](router, "orders.", func(context.Context, typed.Message[order]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := subSock.Subscribe([]byte("metrics.")); err != nil {
		t.Fatal(err)
	}
	if err := subSock.Connect("inproc://typed-router-nomatch"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metricsPub := typed.NewPublisher[order](pubSock, jsonCodec{})
	if err := metricsPub.Send(callCtx, "metrics.cpu", order{ID: 99}); err != nil {
		t.Fatal(err)
	}

	err := router.Run(callCtx, typed.WithErrorHandler(func(e error) error {
		if errors.Is(e, typed.ErrNoRouterMatch) {
			return e // stop
		}
		return nil
	}))
	if !errors.Is(err, typed.ErrNoRouterMatch) {
		t.Fatalf("want ErrNoRouterMatch, got %v", err)
	}
}
