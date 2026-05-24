package gozmq_test

import (
	"context"
	"fmt"
	"time"

	"github.com/rkm/gozmq"
)

func ExampleNewContext_pairRoundTrip() {
	ctx := gozmq.NewContext()
	defer ctx.Close()

	server, _ := ctx.NewSocket(gozmq.Pair)
	defer server.Close()
	_ = server.Bind("inproc://example-pair")

	client, _ := ctx.NewSocket(gozmq.Pair)
	defer client.Close()
	_ = client.Connect("inproc://example-pair")

	callCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_ = client.SendBytes(callCtx, []byte("ping"))
	msg, _ := server.RecvMsg(callCtx)
	fmt.Printf("%s\n", msg.Bytes())
	// Output: ping
}

func ExampleSocket_pubSub() {
	ctx := gozmq.NewContext()
	defer ctx.Close()

	pub, _ := ctx.NewSocket(gozmq.Pub)
	defer pub.Close()
	_ = pub.Bind("inproc://example-pubsub")

	sub, _ := ctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte("weather")))
	defer sub.Close()
	_ = sub.Connect("inproc://example-pubsub")

	callCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Allow subscription to propagate, then publish.
	time.Sleep(50 * time.Millisecond)
	_ = pub.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte("weather"), []byte("sunny")))

	msg, _ := sub.RecvMsg(callCtx)
	fmt.Printf("%s %s\n", msg.Frames()[0], msg.Frames()[1])
	// Output: weather sunny
}

func ExampleNewContext_reqRep() {
	ctx := gozmq.NewContext()
	defer ctx.Close()

	rep, _ := ctx.NewSocket(gozmq.Rep)
	defer rep.Close()
	_ = rep.Bind("inproc://example-reqrep")

	req, _ := ctx.NewSocket(gozmq.Req)
	defer req.Close()
	_ = req.Connect("inproc://example-reqrep")

	callCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_ = req.SendBytes(callCtx, []byte("hello"))

	request, _ := rep.RecvMsg(callCtx)
	_ = rep.SendBytes(callCtx, append(request.Bytes(), []byte(" world")...))

	reply, _ := req.RecvMsg(callCtx)
	fmt.Printf("%s\n", reply.Bytes())
	// Output: hello world
}
