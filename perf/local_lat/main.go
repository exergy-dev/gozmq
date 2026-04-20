// local_lat is the gozmq mirror of libzmq's perf/local_lat.c — it binds a
// REP socket on -endpoint and echoes every message back for -count
// iterations. Pair with remote_lat on the other end to measure REQ/REP
// round-trip latency.
//
// Usage:
//
//	local_lat -endpoint tcp://*:5555 -size 32 -count 100000
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/rkm/gozmq"
)

func main() {
	endpoint := flag.String("endpoint", "tcp://*:5555", "bind endpoint (e.g. tcp://*:5555, ipc:///tmp/bench, inproc://lat)")
	size := flag.Int("size", 32, "message size in bytes (must match remote_lat)")
	count := flag.Int("count", 100000, "roundtrip count to echo before exiting (must match remote_lat)")
	flag.Parse()

	if *size < 0 {
		fmt.Fprintln(os.Stderr, "size must be >= 0")
		os.Exit(1)
	}
	if *count <= 0 {
		fmt.Fprintln(os.Stderr, "count must be > 0")
		os.Exit(1)
	}

	ctxZmq := gozmq.NewContext()
	defer ctxZmq.Close()

	sock, err := ctxZmq.NewSocket(gozmq.Rep)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new socket: %v\n", err)
		os.Exit(1)
	}
	defer sock.Close()

	if err := sock.Bind(*endpoint); err != nil {
		fmt.Fprintf(os.Stderr, "bind %s: %v\n", *endpoint, err)
		os.Exit(1)
	}

	fmt.Printf("message size: %d [B]\n", *size)
	fmt.Printf("roundtrip count: %d\n", *count)

	// Each iteration: Recv → Send. A fresh context per iteration would be
	// wasteful; we use a single long-lived context with a generous timeout
	// covering the whole run.
	callCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	for i := 0; i < *count; i++ {
		m, err := sock.RecvMsg(callCtx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "recv[%d]: %v\n", i, err)
			os.Exit(1)
		}
		if *size > 0 && m.NumFrames() == 1 && len(m.Bytes()) != *size {
			fmt.Fprintf(os.Stderr, "message of incorrect size %d (expected %d)\n", len(m.Bytes()), *size)
			os.Exit(1)
		}
		// Echo the exact body back. Copy so we don't race the session's
		// pooled recv buffer with the send path.
		body := append([]byte(nil), m.Bytes()...)
		m.Release()
		if err := sock.SendMsg(callCtx, gozmq.NewMsgFromBytes(body)); err != nil {
			fmt.Fprintf(os.Stderr, "send[%d]: %v\n", i, err)
			os.Exit(1)
		}
	}
}
