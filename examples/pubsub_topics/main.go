// Command pubsub_topics demonstrates PUB/SUB topic filtering.
//
//	go run ./examples/pubsub_topics
//	go run ./examples/pubsub_topics -topic weather
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/exergy-dev/gozmq"
)

func main() {
	topic := flag.String("topic", "alerts", "topic prefix to subscribe to")
	flag.Parse()

	zctx := gozmq.NewContext()
	defer zctx.Close()

	pub, err := zctx.NewSocket(gozmq.Pub)
	check(err, "new PUB")
	defer pub.Close()
	check(pub.Bind("tcp://127.0.0.1:0"), "bind PUB")

	sub, err := zctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte(*topic)))
	check(err, "new SUB")
	defer sub.Close()
	check(sub.Connect(pub.LastEndpoint()), "connect SUB")

	// SUB subscriptions are propagated asynchronously, so give the connection
	// a small slow-joiner window before publishing.
	time.Sleep(150 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messages := []string{
		*topic + ":disk-full",
		"metrics:cpu=72",
		*topic + ":service-recovered",
	}
	for _, body := range messages {
		check(pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte(body))), "publish")
		fmt.Printf("published %q\n", body)
	}

	for i := 0; i < 2; i++ {
		msg, err := sub.RecvMsg(callCtx)
		check(err, "subscriber recv")
		fmt.Printf("subscriber received %q\n", msg.Bytes())
	}

	check(sub.Subscribe([]byte("metrics")), "subscribe metrics")
	time.Sleep(100 * time.Millisecond)
	check(pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("metrics:mem=41"))), "publish metrics")
	msg, err := sub.RecvMsg(callCtx)
	check(err, "recv metrics")
	fmt.Printf("subscriber received after Subscribe: %q\n", msg.Bytes())

	check(sub.Unsubscribe([]byte("metrics")), "unsubscribe metrics")
	fmt.Println("unsubscribed from metrics")
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
