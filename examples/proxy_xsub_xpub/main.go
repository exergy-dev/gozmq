// Command proxy_xsub_xpub demonstrates gozmq.Proxy as a PUB/SUB forwarder —
// the XSUB/XPUB "device" every ZeroMQ deployment eventually needs.
//
//	go run ./examples/proxy_xsub_xpub
//	go run ./examples/proxy_xsub_xpub -topic weather -capture
//
// Topology:
//
//	PUB  ──▶  XSUB ─[ Proxy ]─ XPUB  ──▶  SUB
//	                    │
//	                    └──▶  PAIR (optional capture tap)
//
// Publishers connect to one stable address and subscribers to another, so
// either side can come and go without the other reconfiguring. The
// subscription itself travels right to left through the proxy with no code:
// XPUB surfaces each SUBSCRIBE as a message and the proxy forwards it to
// XSUB, which re-emits it to the real publisher.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/exergy-dev/gozmq"
)

func main() {
	topic := flag.String("topic", "alerts", "topic prefix the subscriber asks for")
	count := flag.Int("count", 5, "messages to publish")
	withCapture := flag.Bool("capture", false, "attach a capture socket that taps forwarded traffic")
	flag.Parse()

	zctx := gozmq.NewContext()
	defer zctx.Close()

	// ---- the proxy's two faces -------------------------------------
	xsub, err := zctx.NewSocket(gozmq.XSub)
	check(err, "new XSUB")
	defer xsub.Close()

	xpub, err := zctx.NewSocket(gozmq.XPub)
	check(err, "new XPUB")
	defer xpub.Close()
	check(xpub.Bind("tcp://127.0.0.1:0"), "bind XPUB")

	// ---- the real publisher ----------------------------------------
	pub, err := zctx.NewSocket(gozmq.Pub)
	check(err, "new PUB")
	defer pub.Close()
	check(pub.Bind("tcp://127.0.0.1:0"), "bind PUB")
	check(xsub.Connect(pub.LastEndpoint()), "XSUB connect upstream")

	// ---- optional capture tap ---------------------------------------
	var capture, tap *gozmq.Socket
	if *withCapture {
		capture, err = zctx.NewSocket(gozmq.Pair)
		check(err, "new capture PAIR")
		defer capture.Close()
		check(capture.Bind("inproc://capture"), "bind capture")

		tap, err = zctx.NewSocket(gozmq.Pair)
		check(err, "new tap PAIR")
		defer tap.Close()
		check(tap.Connect("inproc://capture"), "connect tap")
	}

	// ---- run the proxy ----------------------------------------------
	// Ctrl-C cancels the context, which is the normal way to stop a proxy.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	proxyDone := make(chan error, 1)
	go func() { proxyDone <- gozmq.Proxy(ctx, xsub, xpub, capture) }()

	// ---- a subscriber on the far side --------------------------------
	sub, err := zctx.NewSocket(gozmq.Sub, gozmq.WithSubscribe([]byte(*topic)))
	check(err, "new SUB")
	defer sub.Close()
	check(sub.Connect(xpub.LastEndpoint()), "SUB connect to proxy")

	fmt.Printf("publisher at %s, proxy serving subscribers at %s\n", pub.LastEndpoint(), xpub.LastEndpoint())
	fmt.Printf("subscribed to %q\n\n", *topic)

	// PUB drops anything published before a subscription has arrived, and
	// here it has two hops to travel — so publish until the first message
	// lands, then send the rest.
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	first := awaitFirst(sendCtx, pub, sub, *topic)
	fmt.Printf("received: %s\n", first)

	for i := 1; i < *count; i++ {
		body := fmt.Sprintf("%s: event %d", *topic, i)
		check(pub.SendBytes(sendCtx, []byte(body)), "publish")

		got, err := sub.RecvBytes(sendCtx)
		check(err, "receive")
		fmt.Printf("received: %s\n", got)
	}

	if tap != nil {
		reportCapture(sendCtx, tap)
	}

	stop()
	if err := <-proxyDone; err != nil {
		log.Fatalf("proxy: %v", err)
	}
	fmt.Println("\nproxy stopped cleanly")
}

// reportCapture drains whatever the capture socket has seen so far.
//
// The tap observes everything the proxy forwards — which includes the
// SUBSCRIBE travelling right to left, not just the published data. A
// leading 0x01 byte marks a subscribe, 0x00 a cancel; the rest of the frame
// is the topic prefix. Real deployments usually publish captures to a PUB
// socket and let a separate tool decode them.
func reportCapture(ctx context.Context, tap *gozmq.Socket) {
	fmt.Println("\ncaptured by the tap:")
	for {
		attempt, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		got, err := tap.RecvBytes(attempt)
		cancel()
		if err != nil {
			return
		}
		switch {
		case len(got) > 0 && got[0] == 0x01:
			fmt.Printf("  subscribe %q\n", got[1:])
		case len(got) > 0 && got[0] == 0x00:
			fmt.Printf("  cancel    %q\n", got[1:])
		default:
			fmt.Printf("  message   %s\n", got)
		}
	}
}

// awaitFirst republishes until the subscription has propagated through both
// proxy hops and a message actually arrives.
func awaitFirst(ctx context.Context, pub, sub *gozmq.Socket, topic string) []byte {
	body := []byte(topic + ": event 0")
	for {
		if ctx.Err() != nil {
			log.Fatal("subscription never reached the publisher")
		}
		check(pub.SendBytes(ctx, body), "publish")

		attempt, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		got, err := sub.RecvBytes(attempt)
		cancel()
		if err == nil {
			return got
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			check(err, "receive")
		}
	}
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
