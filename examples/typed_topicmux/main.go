// Command typed_topicmux demonstrates the typed.TopicMux: one SUB socket
// receives two different struct types on different topic prefixes,
// and the mux dispatches each message to a typed handler.
//
//	go run ./examples/typed_topicmux
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/typed"
	"github.com/exergy-dev/gozmq/typed/codecjson"
)

type Order struct {
	ID   int    `json:"id"`
	Item string `json:"item"`
}

type Alert struct {
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

func main() {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	pubSock, err := zctx.NewSocket(gozmq.Pub)
	check(err, "new PUB")
	defer pubSock.Close()
	check(pubSock.Bind("tcp://127.0.0.1:0"), "bind PUB")

	subSock, err := zctx.NewSocket(gozmq.Sub)
	check(err, "new SUB")
	defer subSock.Close()

	codec := codecjson.Codec{}
	mux := typed.NewTopicMux(subSock, codec)

	var wg sync.WaitGroup
	wg.Add(3) // expect 2 orders + 1 alert

	check(typed.Register[Order](mux, "orders.", func(_ context.Context, m typed.Message[Order]) error {
		fmt.Printf("order  (%s): %+v\n", m.Topic, m.Value)
		wg.Done()
		return nil
	}), "register orders.")
	check(typed.Register[Alert](mux, "alerts.", func(_ context.Context, m typed.Message[Alert]) error {
		fmt.Printf("alert  (%s): %+v\n", m.Topic, m.Value)
		wg.Done()
		return nil
	}), "register alerts.")

	check(subSock.Connect(pubSock.LastEndpoint()), "connect SUB")
	time.Sleep(150 * time.Millisecond) // slow-joiner window

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runCtx, runCancel := context.WithCancel(callCtx)
	runErr := make(chan error, 1)
	go func() { runErr <- mux.Run(runCtx) }()

	orderPub := typed.NewPublisher[Order](pubSock, codec)
	alertPub := typed.NewPublisher[Alert](pubSock, codec)
	check(orderPub.Send(callCtx, "orders.created", Order{ID: 1, Item: "widget"}), "publish order 1")
	check(alertPub.Send(callCtx, "alerts.disk", Alert{Severity: "warn", Message: "disk 85%"}), "publish alert")
	check(orderPub.Send(callCtx, "orders.cancelled", Order{ID: 2, Item: "gadget"}), "publish order 2")

	waitWithTimeout(&wg, 3*time.Second)
	runCancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("mux.Run: %v", err)
	}
}

func waitWithTimeout(wg *sync.WaitGroup, d time.Duration) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		log.Fatal("timed out waiting for messages")
	}
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
