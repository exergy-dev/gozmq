// remote_thr is the gozmq mirror of libzmq's perf/remote_thr.c — it
// connects a PUSH socket to -endpoint and sends -count messages of -size
// bytes as fast as HWM allows. The local_thr peer measures and reports
// throughput; remote_thr simply exits after the last send completes.
//
// Usage:
//
//	remote_thr -endpoint tcp://127.0.0.1:5555 -size 32 -count 1000000
//
// -size and -count must match the local_thr peer.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/exergy-dev/gozmq"
)

func main() {
	endpoint := flag.String("endpoint", "tcp://127.0.0.1:5555", "connect endpoint")
	size := flag.Int("size", 32, "message size in bytes (must match local_thr)")
	count := flag.Int("count", 1_000_000, "message count (must match local_thr)")
	cpuProfile := flag.String("cpuprofile", "", "write CPU profile to file")
	memProfile := flag.String("memprofile", "", "write heap profile to file")
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

	// Linger=-1 (infinite) so Close blocks until every queued message has
	// been handed to the peer. libzmq's perf/remote_thr relies on the
	// same infinite-linger default (zmq_setsockopt ZMQ_LINGER = -1 is
	// libzmq's historical default); we match it explicitly since our
	// default is 1s (design §6.4).
	const benchHWM = 1_000_000

	sock, err := ctxZmq.NewSocket(gozmq.Push,
		gozmq.WithLinger(-1),
		gozmq.WithSendHWM(benchHWM),
		gozmq.WithRecvHWM(benchHWM),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "new socket: %v\n", err)
		os.Exit(1)
	}

	if err := sock.Connect(*endpoint); err != nil {
		fmt.Fprintf(os.Stderr, "connect %s: %v\n", *endpoint, err)
		os.Exit(1)
	}

	fmt.Printf("message size: %d [B]\n", *size)
	fmt.Printf("message count: %d\n", *count)

	// Pre-allocate the payload ONCE and re-use it across every send.
	// NewMsgFromBytes wraps without copying so this is truly a
	// single-allocation hot loop, matching libzmq's zmq_msg_init_size +
	// zmq_msg_send convention.
	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	var cpuFile *os.File
	if *cpuProfile != "" {
		cpuFile, err = os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create cpuprofile %s: %v\n", *cpuProfile, err)
			os.Exit(1)
		}
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			fmt.Fprintf(os.Stderr, "start cpu profile: %v\n", err)
			os.Exit(1)
		}
	}

	sendStart := time.Now()
	for i := 0; i < *count; i++ {
		if err := sock.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			fmt.Fprintf(os.Stderr, "send[%d]: %v\n", i, err)
			os.Exit(1)
		}
	}
	fmt.Fprintf(os.Stderr, "remote_thr: all %d sends returned in %v\n", *count, time.Since(sendStart))
	if cpuFile != nil {
		pprof.StopCPUProfile()
		_ = cpuFile.Close()
	}
	if *memProfile != "" {
		memFile, err := os.Create(*memProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create memprofile %s: %v\n", *memProfile, err)
			os.Exit(1)
		}
		if err := pprof.WriteHeapProfile(memFile); err != nil {
			_ = memFile.Close()
			fmt.Fprintf(os.Stderr, "write heap profile: %v\n", err)
			os.Exit(1)
		}
		_ = memFile.Close()
	}

	// Graceful close: wait up to LINGER (default 1s per options.go) for
	// the outbound queue to drain, otherwise the final burst of messages
	// can be dropped on the floor before local_thr's kernel buffer has
	// absorbed them.
	if err := sock.Close(); err != nil {
		// Ignore ErrSocketClosed on double-close paths; anything else is
		// a real problem the operator should see.
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
	}
	// Nudge the runtime so the linger drain goroutine finishes cleanly
	// before the process exits — otherwise on very fast links the last
	// few messages race against os.Exit in library-teardown order.
	time.Sleep(10 * time.Millisecond)
}
