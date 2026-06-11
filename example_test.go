package gozmq_test

import (
	"bytes"
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
	msg.Release() // always Release received messages
	// Output: ping
}

func ExampleSocket_pubSub() {
	ctx := gozmq.NewContext()
	defer ctx.Close()

	// An XPUB publisher surfaces each peer's SUBSCRIBE as a receivable
	// message, letting the publisher wait for the subscription to arrive
	// instead of sleeping (PUB/SUB's classic slow-joiner pitfall).
	pub, _ := ctx.NewSocket(gozmq.XPub)
	defer pub.Close()
	_ = pub.Bind("inproc://example-pubsub")

	sub, _ := ctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte("weather")))
	defer sub.Close()
	_ = sub.Connect("inproc://example-pubsub")

	callCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Block until the subscription reaches the publisher, then publish.
	subscribed, _ := pub.RecvMsg(callCtx)
	subscribed.Release()
	_ = pub.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte("weather"), []byte("sunny")))

	msg, _ := sub.RecvMsg(callCtx)
	fmt.Printf("%s %s\n", msg.Frames()[0], msg.Frames()[1])
	msg.Release()
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
	_ = rep.SendBytes(callCtx, append(bytes.Clone(request.Bytes()), []byte(" world")...))
	request.Release()

	reply, _ := req.RecvMsg(callCtx)
	fmt.Printf("%s\n", reply.Bytes())
	reply.Release()
	// Output: hello world
}

func ExampleSocket_routerDealer() {
	ctx := gozmq.NewContext()
	defer ctx.Close()

	router, _ := ctx.NewSocket(gozmq.Router)
	defer router.Close()
	_ = router.Bind("inproc://example-routerdealer")

	dealer, _ := ctx.NewSocket(gozmq.Dealer, gozmq.WithIdentity([]byte("worker-1")))
	defer dealer.Close()
	_ = dealer.Connect("inproc://example-routerdealer")

	callCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Dealer → Router: the ROUTER receives [identity, payload].
	_ = dealer.SendBytes(callCtx, []byte("job-42"))
	request, _ := router.RecvMsg(callCtx)
	identity := bytes.Clone(request.Frames()[0]) // copy before Release
	fmt.Printf("%s sent %s\n", request.Frames()[0], request.Frames()[1])
	request.Release()

	// Router → Dealer: route the reply with the identity as leading frame.
	_ = router.SendMsg(callCtx, gozmq.NewMsgFromFrames(identity, []byte("done")))
	reply, _ := dealer.RecvMsg(callCtx)
	fmt.Printf("%s\n", reply.Bytes())
	reply.Release()
	// Output:
	// worker-1 sent job-42
	// done
}

func ExamplePoller() {
	ctx := gozmq.NewContext()
	defer ctx.Close()

	pullA, _ := ctx.NewSocket(gozmq.Pull)
	defer pullA.Close()
	_ = pullA.Bind("inproc://example-poller-a")

	pullB, _ := ctx.NewSocket(gozmq.Pull)
	defer pullB.Close()
	_ = pullB.Bind("inproc://example-poller-b")

	push, _ := ctx.NewSocket(gozmq.Push)
	defer push.Close()
	_ = push.Connect("inproc://example-poller-b")

	callCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = push.SendBytes(callCtx, []byte("ready"))

	poller := gozmq.NewPoller()
	poller.Add(pullA, gozmq.PollIn)
	poller.Add(pullB, gozmq.PollIn)

	// Poll blocks until at least one registered socket is readable —
	// here only pullB, since nothing was sent to pullA.
	events, _ := poller.Poll(callCtx)
	for _, ev := range events {
		msg, _ := ev.Socket.RecvMsg(callCtx)
		fmt.Printf("%s\n", msg.Bytes())
		msg.Release()
	}
	// Output: ready
}
