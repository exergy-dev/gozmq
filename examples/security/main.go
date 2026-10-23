// Command security demonstrates an authenticated PAIR exchange with PLAIN
// (username and password) or CURVE (CurveZMQ keys, encrypted).
//
//	go run ./examples/security -mech plain
//	go run ./examples/security -mech curve
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/security/curve"
)

func main() {
	mech := flag.String("mech", "curve", "security mechanism: plain or curve")
	flag.Parse()

	var serverOpt, clientOpt gozmq.Option
	switch *mech {
	case "plain":
		serverOpt = gozmq.WithPlainAuth(func(user, pass string) bool {
			return user == "alice" && pass == "correct-horse"
		})
		clientOpt = gozmq.WithPlainClient("alice", "correct-horse")
	case "curve":
		serverPub, serverSec, err := curve.GenerateKeyPair()
		check(err, "generate server keypair")
		clientPub, clientSec, err := curve.GenerateKeyPair()
		check(err, "generate client keypair")
		fmt.Printf("server public key: %s\n", curve.Z85EncodeKey(serverPub))
		fmt.Printf("client public key: %s\n", curve.Z85EncodeKey(clientPub))
		// Without a ZAP handler the server accepts any client that proves
		// possession of its key; see gozmq.WithZAPHandler for authorization.
		serverOpt = gozmq.WithCurveServer(serverSec[:])
		clientOpt = gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:])
	default:
		log.Fatalf("unknown -mech %q", *mech)
	}

	zctx := gozmq.NewContext()
	defer zctx.Close()
	server, err := zctx.NewSocket(gozmq.Pair, serverOpt)
	check(err, "new server")
	defer server.Close()
	check(server.Bind("tcp://127.0.0.1:0"), "bind")

	client, err := zctx.NewSocket(gozmq.Pair, clientOpt)
	check(err, "new client")
	defer client.Close()
	check(client.Connect(server.LastEndpoint()), "connect")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check(client.SendBytes(ctx, []byte("authenticated hello")), "send")
	got, err := server.RecvBytes(ctx)
	check(err, "recv")
	fmt.Printf("server received over %s: %q\n", *mech, got)
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
