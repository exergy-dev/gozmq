// Command pair_ping is the M1 end-to-end demo: two PAIR sockets round-trip
// a multipart message over TCP (default) or inproc. Runs a server + client
// in the same process.
//
//	go run ./examples/pair_ping                  # TCP loopback, ephemeral port
//	go run ./examples/pair_ping -transport inproc -endpoint inproc://demo
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/rkm/gozmq"
)

func main() {
	transport := flag.String("transport", "tcp", "transport scheme: tcp or inproc")
	endpoint := flag.String("endpoint", "", "explicit endpoint (overrides -transport)")
	flag.Parse()

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	server, err := ctxZmq.NewSocket(gozmq.Pair)
	check(err, "new server")
	defer server.Close()

	bindEP := *endpoint
	if bindEP == "" {
		switch *transport {
		case "tcp":
			bindEP = "tcp://127.0.0.1:0"
		case "inproc":
			bindEP = "inproc://pair-ping-demo"
		default:
			log.Fatalf("unknown transport %q", *transport)
		}
	}
	check(server.Bind(bindEP), "bind")
	boundEP := server.LastEndpoint()
	if boundEP == "" {
		boundEP = bindEP
	}
	fmt.Printf("server bound on %s\n", boundEP)

	client, err := ctxZmq.NewSocket(gozmq.Pair)
	check(err, "new client")
	defer client.Close()
	check(client.Connect(boundEP), "connect")

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	check(client.SendMsg(callCtx, gozmq.NewMsgFromFrames(
		[]byte("hello"), []byte("world"),
	)), "client send")

	got, err := server.RecvMsg(callCtx)
	check(err, "server recv")
	fmt.Printf("server received %d frames:\n", got.NumFrames())
	for i, f := range got.Frames() {
		fmt.Printf("  frame %d: %q\n", i, f)
	}

	check(server.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("ack"))), "server send")
	reply, err := client.RecvMsg(callCtx)
	check(err, "client recv")
	fmt.Printf("client received reply: %q\n", reply.Bytes())
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
