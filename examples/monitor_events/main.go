// Command monitor_events demonstrates socket lifecycle monitoring.
//
//	go run ./examples/monitor_events
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

	serverEvents := make(chan gozmq.Event, 64)
	clientEvents := make(chan gozmq.Event, 64)

	server, err := zctx.NewSocket(gozmq.Pair, gozmq.WithMonitor(serverEvents, gozmq.EventAll))
	check(err, "new server")
	defer server.Close()
	check(server.Bind("tcp://127.0.0.1:0"), "bind")

	client, err := zctx.NewSocket(gozmq.Pair, gozmq.WithMonitor(clientEvents, gozmq.EventAll))
	check(err, "new client")
	defer client.Close()
	check(client.Connect(server.LastEndpoint()), "connect")

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check(client.SendBytes(callCtx, []byte("hello")), "send")
	// RecvBytes returns a single-frame payload directly.
	_, err = server.RecvBytes(callCtx)
	check(err, "recv")

	time.Sleep(150 * time.Millisecond)
	printEvents("server", serverEvents)
	printEvents("client", clientEvents)
}

func printEvents(label string, ch <-chan gozmq.Event) {
	for {
		select {
		case ev := <-ch:
			reason := ""
			if ev.Reason != "" {
				reason = " reason=" + ev.Reason
			}
			fmt.Printf("%s event: %-24s endpoint=%s%s\n", label, eventName(ev.Type), ev.Endpoint, reason)
		default:
			return
		}
	}
}

func eventName(t gozmq.EventType) string {
	switch t {
	case gozmq.EventConnected:
		return "Connected"
	case gozmq.EventConnectDelayed:
		return "ConnectDelayed"
	case gozmq.EventConnectRetried:
		return "ConnectRetried"
	case gozmq.EventConnectFailed:
		return "ConnectFailed"
	case gozmq.EventListening:
		return "Listening"
	case gozmq.EventBindFailed:
		return "BindFailed"
	case gozmq.EventAccepted:
		return "Accepted"
	case gozmq.EventAcceptFailed:
		return "AcceptFailed"
	case gozmq.EventClosed:
		return "Closed"
	case gozmq.EventDisconnected:
		return "Disconnected"
	case gozmq.EventHandshakeSucceeded:
		return "HandshakeSucceeded"
	case gozmq.EventHandshakeFailedProtocol:
		return "HandshakeFailedProtocol"
	case gozmq.EventHandshakeFailedAuth:
		return "HandshakeFailedAuth"
	default:
		return fmt.Sprintf("EventType(%d)", t)
	}
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
