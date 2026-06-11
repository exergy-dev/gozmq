// Command security_curve demonstrates CurveZMQ key generation and encrypted exchange.
//
//	go run ./examples/security_curve
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rkm/gozmq"
	"github.com/rkm/gozmq/security/curve"
)

func main() {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	serverPub, serverSec, err := curve.GenerateKeyPair()
	check(err, "generate server keypair")
	clientPub, clientSec, err := curve.GenerateKeyPair()
	check(err, "generate client keypair")

	fmt.Printf("server public key: %s\n", curve.Z85EncodeKey(serverPub))
	fmt.Printf("client public key: %s\n", curve.Z85EncodeKey(clientPub))

	server, err := zctx.NewSocket(gozmq.Pair, gozmq.WithCurveServer(serverSec[:]))
	check(err, "new CURVE server")
	defer server.Close()
	check(server.Bind("tcp://127.0.0.1:0"), "bind")

	client, err := zctx.NewSocket(gozmq.Pair, gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]))
	check(err, "new CURVE client")
	defer client.Close()
	check(client.Connect(server.LastEndpoint()), "connect")

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check(client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("encrypted hello"))), "send")
	msg, err := server.RecvMsg(callCtx)
	check(err, "recv")
	defer msg.Release()
	fmt.Printf("server received over CURVE: %q\n", msg.Bytes())
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
