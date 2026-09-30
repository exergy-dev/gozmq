// Command reqrep_service demonstrates a small REQ/REP service loop.
//
//	go run ./examples/reqrep_service
//	go run ./examples/reqrep_service -requests 5
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
	requests := flag.Int("requests", 3, "number of requests to send")
	flag.Parse()

	zctx := gozmq.NewContext()
	defer zctx.Close()

	rep, err := zctx.NewSocket(gozmq.Rep)
	check(err, "new REP")
	defer rep.Close()
	check(rep.Bind("tcp://127.0.0.1:0"), "bind REP")

	req, err := zctx.NewSocket(gozmq.Req)
	check(err, "new REQ")
	defer req.Close()
	check(req.Connect(rep.LastEndpoint()), "connect REQ")

	done := make(chan error, 1)
	go func() {
		callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := 1; i <= *requests; i++ {
			msg, err := rep.RecvMsg(callCtx)
			if err != nil {
				done <- fmt.Errorf("service recv: %w", err)
				return
			}
			body := string(msg.Bytes())
			fmt.Printf("service received %q\n", body)
			reply := fmt.Sprintf("reply(%s)", body)
			if err := rep.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte(reply))); err != nil {
				done <- fmt.Errorf("service send: %w", err)
				return
			}
		}
		done <- nil
	}()

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 1; i <= *requests; i++ {
		body := fmt.Sprintf("request-%d", i)
		check(req.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte(body))), "client send")
		reply, err := req.RecvMsg(callCtx)
		check(err, "client recv")
		fmt.Printf("client got %q\n", reply.Bytes())
	}

	check(<-done, "service loop")
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
