// Command security_plain demonstrates PLAIN username/password authentication.
//
//	go run ./examples/security_plain
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/exergy-dev/gozmq"
)

func main() {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	server, err := zctx.NewSocket(gozmq.Pair, gozmq.WithPlainAuth(func(user, pass string) bool {
		return user == "alice" && pass == "correct-horse"
	}))
	check(err, "new PLAIN server")
	defer server.Close()
	check(server.Bind("tcp://127.0.0.1:0"), "bind")

	client, err := zctx.NewSocket(gozmq.Pair, gozmq.WithPlainClient("alice", "correct-horse"))
	check(err, "new PLAIN client")
	defer client.Close()
	check(client.Connect(server.LastEndpoint()), "connect")

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check(client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("authenticated hello"))), "send")
	msg, err := server.RecvMsg(callCtx)
	check(err, "recv")
	fmt.Printf("server accepted PLAIN client and received %q\n", msg.Bytes())
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
