// Command router_dealer_workers demonstrates ROUTER/DEALER identity routing.
//
//	go run ./examples/router_dealer_workers
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rkm/gozmq"
)

func main() {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	router, err := zctx.NewSocket(gozmq.Router, gozmq.WithRouterMandatory(true))
	check(err, "new ROUTER")
	defer router.Close()
	check(router.Bind("tcp://127.0.0.1:0"), "bind ROUTER")

	for _, id := range []string{"worker-a", "worker-b"} {
		dealer, err := zctx.NewSocket(gozmq.Dealer, gozmq.WithIdentity([]byte(id)))
		check(err, "new DEALER")
		defer dealer.Close()
		check(dealer.Connect(router.LastEndpoint()), "connect DEALER")
		go dealerWorker(id, dealer)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var identities [][]byte
	for len(identities) < 2 {
		msg, err := router.RecvMsg(callCtx)
		check(err, "router recv ready")
		frames := msg.Frames()
		if len(frames) != 2 {
			log.Fatalf("ready frame count = %d, want identity + body", len(frames))
		}
		identity := append([]byte(nil), frames[0]...)
		identities = append(identities, identity)
		fmt.Printf("router saw %s send %q\n", identity, frames[1])
	}

	for i, id := range identities {
		body := fmt.Sprintf("work-%d", i+1)
		check(router.SendMsg(callCtx, gozmq.NewMsgFromFrames(id, []byte(body))), "route work")
		fmt.Printf("router sent %q to %s\n", body, id)
	}

	for range identities {
		msg, err := router.RecvMsg(callCtx)
		check(err, "router recv result")
		frames := msg.Frames()
		if len(frames) != 2 {
			log.Fatalf("result frame count = %d, want identity + body", len(frames))
		}
		fmt.Printf("router received %q from %s\n", frames[1], frames[0])
	}
}

func dealerWorker(id string, sock *gozmq.Socket) {
	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	check(sock.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("ready"))), id+" send ready")
	job, err := sock.RecvMsg(callCtx)
	check(err, id+" recv job")
	body := fmt.Sprintf("%s done by %s", job.Bytes(), id)
	check(sock.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte(body))), id+" send result")
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
