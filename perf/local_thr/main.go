// local_thr is the gozmq mirror of libzmq's perf/local_thr.c — it binds a
// PULL socket on -endpoint, receives -count messages of -size bytes from a
// connected PUSH peer, and reports the observed mean throughput
// (messages/sec and megabits/sec).
//
// Usage:
//
//	local_thr -endpoint tcp://*:5555 -size 32 -count 1000000
//
// Pair with remote_thr on the other end; -size and -count must match.
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
	endpoint := flag.String("endpoint", "tcp://*:5555", "bind endpoint")
	size := flag.Int("size", 32, "message size in bytes (must match remote_thr)")
	count := flag.Int("count", 1_000_000, "message count (must match remote_thr)")
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

	sock, err := ctxZmq.NewSocket(gozmq.Pull)
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
	fmt.Printf("message count: %d\n", *count)

	callCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Receive the first message, THEN start the clock — matches libzmq's
	// perf/local_thr.c convention where the first recv unblocks after the
	// sender has connected and ramped up. This yields steady-state
	// numbers, not connection-establishment numbers.
	first, err := sock.RecvMsg(callCtx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "recv[0]: %v\n", err)
		os.Exit(1)
	}
	if *size > 0 && first.NumFrames() == 1 && len(first.Bytes()) != *size {
		fmt.Fprintf(os.Stderr, "message of incorrect size %d (expected %d)\n", len(first.Bytes()), *size)
		os.Exit(1)
	}

	start := time.Now()
	for i := 1; i < *count; i++ {
		m, err := sock.RecvMsg(callCtx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "recv[%d]: %v\n", i, err)
			os.Exit(1)
		}
		if *size > 0 && m.NumFrames() == 1 && len(m.Bytes()) != *size {
			fmt.Fprintf(os.Stderr, "message of incorrect size %d (expected %d)\n", len(m.Bytes()), *size)
			os.Exit(1)
		}
	}
	elapsed := time.Since(start)

	if elapsed == 0 {
		elapsed = time.Nanosecond
	}
	msgPerSec := float64(*count) / elapsed.Seconds()
	// Mbps: bytes × 8 / 1e6 / seconds
	mbps := float64(*count) * float64(*size) * 8.0 / 1_000_000.0 / elapsed.Seconds()

	fmt.Printf("mean throughput: %.0f [msg/s]\n", msgPerSec)
	fmt.Printf("mean throughput: %.3f [Mb/s]\n", mbps)
}
