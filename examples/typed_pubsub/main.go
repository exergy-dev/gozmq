// Command typed_pubsub demonstrates the gozmq/typed wrapper: a publisher
// sends strongly-typed Go structs on a named topic, and a subscriber
// decodes them back into the same type via a pluggable Codec.
//
//	go run ./examples/typed_pubsub
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rkm/gozmq"
	"github.com/rkm/gozmq/typed"
	"github.com/rkm/gozmq/typed/codecjson"
)

// Event is the application-level payload.
type Event struct {
	ID       int    `json:"id"`
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
	pub := typed.NewPublisher[Event](pubSock, codec)
	sub := typed.NewSubscriber[Event](subSock, codec)

	check(sub.Subscribe("alerts."), "subscribe alerts.")
	check(subSock.Connect(pubSock.LastEndpoint()), "connect SUB")

	// Slow-joiner window: subscription forwarding is asynchronous.
	time.Sleep(150 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := []struct {
		topic string
		event Event
	}{
		{"alerts.disk", Event{ID: 1, Severity: "warn", Message: "disk 85% full"}},
		{"metrics.cpu", Event{ID: 2, Severity: "info", Message: "cpu=72"}}, // filtered out
		{"alerts.net", Event{ID: 3, Severity: "crit", Message: "link down"}},
	}
	for _, e := range events {
		check(pub.Send(callCtx, e.topic, e.event), "publish "+e.topic)
		fmt.Printf("published topic=%q event=%+v\n", e.topic, e.event)
	}

	for i := 0; i < 2; i++ {
		got, err := sub.Recv(callCtx)
		check(err, "recv")
		fmt.Printf("received topic=%q event=%+v\n", got.Topic, got.Value)
	}
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
