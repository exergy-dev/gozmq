package gozmq_test

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/exergy-dev/gozmq"
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

// ExampleProxy is the XSUB/XPUB forwarder from the README: publishers
// connect to one stable address, subscribers to another, and either side
// can come and go without the other reconfiguring.
//
// Subscriptions travel upstream through the proxy without any special
// handling — XPUB surfaces each SUBSCRIBE as a message and Proxy forwards
// it to XSUB, which re-emits it to the real publisher.
func ExampleProxy() {
	zctx := gozmq.NewContext()
	defer zctx.Close()

	// The upstream publisher.
	pub, _ := zctx.NewSocket(gozmq.Pub)
	defer pub.Close()
	_ = pub.Bind("inproc://example-proxy-up")

	// The proxy's two faces.
	xsub, _ := zctx.NewSocket(gozmq.XSub)
	defer xsub.Close()
	_ = xsub.Connect("inproc://example-proxy-up")

	xpub, _ := zctx.NewSocket(gozmq.XPub)
	defer xpub.Close()
	_ = xpub.Bind("inproc://example-proxy-down")

	proxyCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { _ = gozmq.Proxy(proxyCtx, xsub, xpub, nil) }()

	// A subscriber on the far side.
	sub, _ := zctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte("topic.")))
	defer sub.Close()
	_ = sub.Connect("inproc://example-proxy-down")

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// PUB drops messages published before a subscription has propagated,
	// and here it has two hops to travel, so republish until one lands.
	for {
		_ = pub.SendBytes(callCtx, []byte("topic.a|hello"))

		attempt, cancelAttempt := context.WithTimeout(callCtx, 100*time.Millisecond)
		got, err := sub.RecvBytes(attempt)
		cancelAttempt()
		if err == nil {
			fmt.Printf("%s\n", got)
			break
		}
		if callCtx.Err() != nil {
			fmt.Println("subscription never propagated")
			break
		}
	}
	// Output: topic.a|hello
}
