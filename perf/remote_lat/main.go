// remote_lat is the gozmq mirror of libzmq's perf/remote_lat.c — it
// connects a REQ socket to -endpoint, fires -count synchronous round trips
// of -size bytes, and reports the average latency in microseconds plus the
// derived round-trips-per-second.
//
// Usage:
//
//	remote_lat -endpoint tcp://127.0.0.1:5555 -size 32 -count 100000
//
// Pair with local_lat on the peer end. The -size and -count flags must
// match between the two processes.
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
	endpoint := flag.String("endpoint", "tcp://127.0.0.1:5555", "connect endpoint")
	size := flag.Int("size", 32, "message size in bytes (must match local_lat)")
	count := flag.Int("count", 100000, "roundtrip count (must match local_lat)")
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

	sock, err := ctxZmq.NewSocket(gozmq.Req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new socket: %v\n", err)
		os.Exit(1)
	}
	defer sock.Close()

	if err := sock.Connect(*endpoint); err != nil {
		fmt.Fprintf(os.Stderr, "connect %s: %v\n", *endpoint, err)
		os.Exit(1)
	}

	fmt.Printf("message size: %d [B]\n", *size)
	fmt.Printf("roundtrip count: %d\n", *count)

	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	start := time.Now()
	for i := 0; i < *count; i++ {
		if err := sock.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			fmt.Fprintf(os.Stderr, "send[%d]: %v\n", i, err)
			os.Exit(1)
		}
		m, err := sock.RecvMsg(callCtx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "recv[%d]: %v\n", i, err)
			os.Exit(1)
		}
		if *size > 0 && m.NumFrames() == 1 && len(m.Bytes()) != *size {
			fmt.Fprintf(os.Stderr, "message of incorrect size %d (expected %d)\n", len(m.Bytes()), *size)
			os.Exit(1)
		}
		m.Release()
	}
	elapsed := time.Since(start)

	// Avoid divide-by-zero if the whole run is below the timer's resolution.
	avgUsec := float64(elapsed.Nanoseconds()) / float64(*count) / 1000.0
	var msgPerSec float64
	if elapsed > 0 {
		msgPerSec = float64(*count) / elapsed.Seconds()
	}

	fmt.Printf("average latency: %.3f [us]\n", avgUsec)
	fmt.Printf("roundtrip rate: %.0f [msg/s]\n", msgPerSec)
}
