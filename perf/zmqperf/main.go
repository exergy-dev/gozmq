// Command zmqperf is the gozmq counterpart of libzmq's perf tools, one
// binary with four subcommands that pair up across two processes:
//
//	zmqperf local_thr  -endpoint tcp://*:5555         -size 32 -count 1000000   # binds PULL, reports throughput
//	zmqperf remote_thr -endpoint tcp://127.0.0.1:5555 -size 32 -count 1000000   # connects PUSH, sends
//	zmqperf local_lat  -endpoint tcp://*:5555         -size 32 -count 100000    # binds REP, echoes
//	zmqperf remote_lat -endpoint tcp://127.0.0.1:5555 -size 32 -count 100000    # connects REQ, reports latency
//
// -size and -count must match on both ends. The output format follows the
// libzmq tools, so the same scripts can read either. The throughput
// commands take -cpuprofile and -memprofile; local_thr takes -pool to
// receive into the buffer pool (gozmq.WithRecvBufferPool) and release each
// message.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"time"

	"github.com/exergy-dev/gozmq"
)

// benchHWM keeps the sender from blocking on HWM mid-run; libzmq's tools
// use their default of 1000, which is why the comparison scripts note it.
const benchHWM = 1_000_000

// defaultGOGC is the collector setting this tool runs with (-gogc
// overrides it). A receiver of fresh 64 KiB frames at 70k msg/s allocates
// ~4.6 GB/s against a live heap of a few MB, and Go's default pacer (GOGC
// 100, 4 MB minimum heap goal) then runs a cycle every ~60 messages; 400 is
// the ordinary tuning for that allocation rate and is what an application
// at it would set (or a GOMEMLIMIT). libzmq's tools have no collector to
// tune. perf/REPORT.md shows the Go-default figures alongside.
const defaultGOGC = 400

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet("zmqperf "+cmd, flag.ExitOnError)
	bind := cmd == "local_thr" || cmd == "local_lat"
	defEndpoint := "tcp://127.0.0.1:5555"
	if bind {
		defEndpoint = "tcp://*:5555"
	}
	endpoint := fs.String("endpoint", defEndpoint, "endpoint to bind (local_*) or connect (remote_*)")
	size := fs.Int("size", 32, "message size in bytes (must match the peer)")
	count := fs.Int("count", 0, "message or round-trip count (must match the peer)")
	cpuProfile := fs.String("cpuprofile", "", "write a CPU profile here (thr commands)")
	memProfile := fs.String("memprofile", "", "write a heap profile here (thr commands)")
	pool := fs.Bool("pool", false, "local_thr: receive into the buffer pool and release each message")
	gogc := fs.Int("gogc", defaultGOGC, "GC percent for this process; negative keeps the runtime's setting (GOGC env or 100)")
	_ = fs.Parse(os.Args[2:])
	if *gogc >= 0 {
		debug.SetGCPercent(*gogc)
	}
	if *count == 0 {
		*count = map[bool]int{true: 1_000_000, false: 100_000}[cmd == "local_thr" || cmd == "remote_thr"]
	}
	if *size < 0 || *count <= 0 {
		log.Fatal("size must be >= 0 and count > 0")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	zctx := gozmq.NewContext()
	defer zctx.Close()
	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = byte(i)
	}

	switch cmd {
	case "local_thr":
		sock := socket(zctx, gozmq.Pull, *endpoint, bind, gozmq.WithSendHWM(benchHWM), gozmq.WithRecvHWM(benchHWM), gozmq.WithRecvBufferPool(*pool))
		fmt.Printf("message size: %d [B]\nmessage count: %d\n", *size, *count)
		// The clock starts after the first message, as libzmq's local_thr
		// does, so the figure is steady state rather than connection setup.
		recv(ctx, sock, *size, 0).Release()
		stop := profile(*cpuProfile, *memProfile)
		start := time.Now()
		for i := 1; i < *count; i++ {
			recv(ctx, sock, *size, i).Release()
		}
		elapsed := max(time.Since(start), time.Nanosecond)
		stop()
		// The first message is outside the timed window, so count-1 were timed.
		timed := float64(*count - 1)
		fmt.Printf("mean throughput: %.0f [msg/s]\n", timed/elapsed.Seconds())
		fmt.Printf("mean throughput: %.3f [Mb/s]\n", timed*float64(*size)*8/1e6/elapsed.Seconds())
	case "remote_thr":
		// Infinite linger, as libzmq's remote_thr relies on, so Close waits
		// until every queued message has reached the peer.
		sock := socket(zctx, gozmq.Push, *endpoint, bind, gozmq.WithLinger(-1), gozmq.WithSendHWM(benchHWM), gozmq.WithRecvHWM(benchHWM))
		fmt.Printf("message size: %d [B]\nmessage count: %d\n", *size, *count)
		stop := profile(*cpuProfile, *memProfile)
		for i := 0; i < *count; i++ {
			send(ctx, sock, payload, i)
		}
		stop()
		check(sock.Close(), "close")
	case "local_lat":
		sock := socket(zctx, gozmq.Rep, *endpoint, bind)
		fmt.Printf("message size: %d [B]\nroundtrip count: %d\n", *size, *count)
		for i := 0; i < *count; i++ {
			m := recv(ctx, sock, *size, i)
			send(ctx, sock, m.Bytes(), i)
		}
	case "remote_lat":
		sock := socket(zctx, gozmq.Req, *endpoint, bind)
		fmt.Printf("message size: %d [B]\nroundtrip count: %d\n", *size, *count)
		start := time.Now()
		for i := 0; i < *count; i++ {
			send(ctx, sock, payload, i)
			recv(ctx, sock, *size, i)
		}
		elapsed := max(time.Since(start), time.Nanosecond)
		fmt.Printf("average latency: %.3f [us]\n", float64(elapsed.Nanoseconds())/float64(*count)/1000)
		fmt.Printf("roundtrip rate: %.0f [msg/s]\n", float64(*count)/elapsed.Seconds())
	default:
		usage()
	}
}

func usage() {
	log.Fatal("usage: zmqperf local_thr|remote_thr|local_lat|remote_lat [-endpoint E] [-size N] [-count N]")
}

func socket(zctx *gozmq.Context, typ gozmq.SocketType, endpoint string, bind bool, opts ...gozmq.Option) *gozmq.Socket {
	s, err := zctx.NewSocket(typ, opts...)
	check(err, "new socket")
	if bind {
		check(s.Bind(endpoint), "bind "+endpoint)
	} else {
		check(s.Connect(endpoint), "connect "+endpoint)
	}
	return s
}

func send(ctx context.Context, s *gozmq.Socket, body []byte, i int) {
	check(s.SendMsg(ctx, gozmq.NewMsgFromBytes(body)), fmt.Sprintf("send[%d]", i))
}

func recv(ctx context.Context, s *gozmq.Socket, size, i int) *gozmq.Msg {
	m, err := s.RecvMsg(ctx)
	check(err, fmt.Sprintf("recv[%d]", i))
	if size > 0 && m.NumFrames() == 1 && len(m.Bytes()) != size {
		log.Fatalf("message of incorrect size %d (expected %d)", len(m.Bytes()), size)
	}
	return m
}

// profile starts the requested profiles and returns the func that writes
// and closes them.
func profile(cpuPath, memPath string) func() {
	var cpu *os.File
	if cpuPath != "" {
		var err error
		cpu, err = os.Create(cpuPath)
		check(err, "create cpuprofile")
		check(pprof.StartCPUProfile(cpu), "start cpu profile")
	}
	return func() {
		if cpu != nil {
			pprof.StopCPUProfile()
			_ = cpu.Close()
		}
		if memPath != "" {
			f, err := os.Create(memPath)
			check(err, "create memprofile")
			check(pprof.WriteHeapProfile(f), "write heap profile")
			_ = f.Close()
		}
	}
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
