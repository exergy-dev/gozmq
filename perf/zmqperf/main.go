// Command zmqperf is the gozmq counterpart of libzmq's perf tools, one
// binary with four subcommands that pair up across two processes:
//
//	zmqperf local_thr  -endpoint tcp://*:5555         -size 32 -count 1000000   # binds the receiver, reports throughput
//	zmqperf remote_thr -endpoint tcp://127.0.0.1:5555 -size 32 -count 1000000   # connects the sender, sends
//	zmqperf local_lat  -endpoint tcp://*:5555         -size 32 -count 100000    # binds the server, echoes
//	zmqperf remote_lat -endpoint tcp://127.0.0.1:5555 -size 32 -count 100000    # connects the client, reports latency
//
// -size and -count must match on both ends. The output format follows the
// libzmq tools, so the same scripts can read either. Two more subcommands run
// both peers inside one process, the peer on a goroutine, like libzmq's
// perf/inproc_thr and inproc_lat (inproc:// cannot span processes):
//
//	zmqperf inproc_thr -size 32 -count 1000000   # sender goroutine -> receiver, reports throughput
//	zmqperf inproc_lat -size 32 -count 100000    # client -> server goroutine, reports latency
//
// -pattern picks the socket pair (default push_pull for the throughput
// commands, req_rep for the latency ones); see patterns.go for the list and
// what each side does. perf/libzmq_thr/zmqperf.c is the same tool written
// against libzmq, with the same commands, flags and output, and
// perf/libzmq_thr/compare.sh runs the two against each other.
//
// The throughput commands take -cpuprofile and -memprofile. Every received
// message is released, as libzmq's local_thr does with zmq_msg_close, so
// -pool (the default) receives into the buffer pool; -pool=false is the
// plain-memory opt-out. The option is always passed explicitly, so the tool
// behaves the same whatever the library's default is. -gogc N sets the GC
// percent for the process; the default keeps the runtime's setting, and 400
// is a usual value for 64 KiB frames at several GB/s.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/exergy-dev/gozmq"
)

// defaultHWM keeps a sender from blocking (or a PUB from dropping) on HWM
// mid-run. zmqperf.c uses the same default, so both sides of a comparison
// queue the same amount.
const defaultHWM = 1_000_000

// config is everything the pattern roles need from the command line.
type config struct {
	pattern  pattern
	size     int
	count    int
	hwm      int
	prefix   []byte        // SUB/XSUB subscription
	subs     int           // XPUB sender: subscriptions to wait for
	pool     bool          // receivers: WithRecvBufferPool
	curve    bool          // CURVE: the binding side is the server
	wait     time.Duration // PUB: pause before sending (subscription propagation)
	idle     time.Duration // receivers: give up after this long without a message
	payload  []byte
	cpuProf  string
	memProf  string
	endpoint string
}

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet("zmqperf "+cmd, flag.ExitOnError)
	local := cmd == "local_thr" || cmd == "local_lat"
	thr := strings.HasSuffix(cmd, "_thr")
	defEndpoint := "tcp://127.0.0.1:5555"
	if local {
		defEndpoint = "tcp://*:5555"
	}
	defPattern := map[bool]string{true: "push_pull", false: "req_rep"}[thr]
	endpoint := fs.String("endpoint", defEndpoint, "endpoint (local_* bind and remote_* connect unless -mode says otherwise)")
	mode := fs.String("mode", "", "bind or connect; default: local_* bind, remote_* connect")
	patternName := fs.String("pattern", defPattern, "socket pattern: "+patternNames(thr))
	size := fs.Int("size", 32, "message size in bytes (must match the peer)")
	count := fs.Int("count", 0, "message or round-trip count (must match the peer)")
	hwm := fs.Int("hwm", defaultHWM, "send and receive HWM on every socket (0 = unlimited)")
	prefix := fs.String("prefix", "", "pub_sub, xpub_xsub: subscription prefix (the payload is all 'a' bytes, so any run of 'a' matches)")
	subs := fs.Int("subs", 1, "pub_sub, xpub_xsub: subscribers; inproc_thr runs that many (the sender binds when > 1), an xpub_xsub remote_thr waits for that many subscriptions")
	curve := fs.Bool("curve", false, "CURVE security (fixed test keys); the binding side is the server")
	wait := fs.Duration("wait", 500*time.Millisecond, "pub_sub sender: pause after bind/connect before sending")
	idle := fs.Duration("idle", 5*time.Second, "receivers: give up after this long without a message and report what arrived")
	cpuProfile := fs.String("cpuprofile", "", "write a CPU profile here (thr commands)")
	memProfile := fs.String("memprofile", "", "write a heap profile here (thr commands)")
	pool := fs.Bool("pool", true, "receivers: receive into the buffer pool (WithRecvBufferPool) and release each message; -pool=false receives into plain memory")
	gogc := fs.Int("gogc", -1, "GC percent for this process (e.g. 400 for large frames); negative, the default, keeps the runtime's setting (GOGC env or 100)")
	_ = fs.Parse(os.Args[2:])
	if *gogc >= 0 {
		debug.SetGCPercent(*gogc)
	}
	if *count == 0 {
		*count = map[bool]int{true: 1_000_000, false: 100_000}[thr]
	}
	if *size < 0 || *count <= 0 || *subs < 1 {
		log.Fatal("size must be >= 0, count > 0 and subs >= 1")
	}
	p, ok := lookupPattern(*patternName, thr)
	if !ok {
		log.Fatalf("unknown %s pattern %q (have %s)", map[bool]string{true: "throughput", false: "latency"}[thr], *patternName, patternNames(thr))
	}
	bind := local
	switch *mode {
	case "":
	case "bind":
		bind = true
	case "connect":
		bind = false
	default:
		log.Fatalf("-mode must be bind or connect, not %q", *mode)
	}
	cfg := &config{
		pattern: p, size: *size, count: *count, hwm: *hwm, prefix: []byte(*prefix),
		subs: *subs, pool: *pool, curve: *curve, wait: *wait, idle: *idle,
		cpuProf: *cpuProfile, memProf: *memProfile, endpoint: *endpoint,
	}
	cfg.payload = make([]byte, *size)
	for i := range cfg.payload {
		cfg.payload[i] = 'a'
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	zctx := gozmq.NewContext()
	defer zctx.Close()
	registerTLS(zctx, *endpoint)
	header := func(what string) {
		fmt.Printf("message size: %d [B]\n%s count: %d\n", *size, what, *count)
	}

	switch cmd {
	case "local_thr", "remote_thr":
		header("message")
		if local {
			reportThr(cfg, p.recv(ctx, zctx, cfg, bind, nil))
		} else {
			stop := profile(cfg.cpuProf, cfg.memProf)
			p.send(ctx, zctx, cfg, bind)
			stop()
		}
	case "local_lat":
		header("roundtrip")
		p.server(ctx, zctx, cfg, bind, nil)
	case "remote_lat":
		header("roundtrip")
		reportLat(*count, p.client(ctx, zctx, cfg, bind))
	case "inproc_thr":
		if p.recv == nil || p.send == nil || p.tcpOnly {
			log.Fatalf("pattern %s does not run over inproc", p.name)
		}
		cfg.endpoint = "inproc://zmqperf-thr"
		cfg.wait = min(cfg.wait, 100*time.Millisecond)
		header("message")
		// One subscriber: the receiver binds, as in the two-process
		// commands. Fan-out: the sender binds and every receiver connects.
		fanout := *subs > 1
		if fanout && !p.fanout {
			log.Fatalf("-subs > 1 needs pub_sub or xpub_xsub, not %s", p.name)
		}
		results := make([]thrResult, *subs)
		var wg sync.WaitGroup
		ready := make(chan struct{})
		if fanout {
			wg.Go(func() { p.send(ctx, zctx, cfg, true) })
			time.Sleep(10 * time.Millisecond) // the sender's bind lands first
			for i := range results {
				wg.Go(func() { results[i] = p.recv(ctx, zctx, cfg, false, nil) })
			}
		} else {
			// The receiver binds before the sender connects, so a PUB
			// never publishes into an unbound endpoint.
			wg.Go(func() { results[0] = p.recv(ctx, zctx, cfg, true, ready) })
			<-ready
			wg.Go(func() { p.send(ctx, zctx, cfg, false) })
		}
		wg.Wait()
		reportThr(cfg, minRate(results))
	case "inproc_lat":
		if p.server == nil || p.tcpOnly {
			log.Fatalf("pattern %s does not run over inproc", p.name)
		}
		cfg.endpoint = "inproc://zmqperf-lat"
		ready := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() { p.server(ctx, zctx, cfg, true, ready) })
		<-ready
		header("roundtrip")
		elapsed := p.client(ctx, zctx, cfg, false)
		wg.Wait()
		reportLat(*count, elapsed)
	default:
		usage()
	}
}

func usage() {
	log.Fatal("usage: zmqperf local_thr|remote_thr|local_lat|remote_lat|inproc_thr|inproc_lat [-pattern P] [-endpoint E] [-size N] [-count N] [-h for the rest]")
}

// thrResult is one receiver's outcome: how many messages arrived and the
// time for all but the first.
type thrResult struct {
	received int
	elapsed  time.Duration
}

// minRate returns the slowest receiver's result, the figure a fan-out row
// reports, or the first incomplete one.
func minRate(rs []thrResult) thrResult {
	worst := rs[0]
	for _, r := range rs[1:] {
		if r.received < worst.received || (r.received == worst.received && r.elapsed > worst.elapsed) {
			worst = r
		}
	}
	return worst
}

// reportThr prints the throughput of the count-1 messages after the first.
// An incomplete run (a PUB dropped, or the idle timeout fired) prints what
// arrived and exits non-zero, so a driver never mistakes it for a rate.
func reportThr(cfg *config, r thrResult) {
	if r.received < cfg.count {
		fmt.Printf("received: %d of %d\n", r.received, cfg.count)
		os.Exit(1)
	}
	timed := float64(cfg.count - 1)
	el := max(r.elapsed, time.Nanosecond).Seconds()
	fmt.Printf("received: %d\n", r.received)
	fmt.Printf("mean throughput: %.0f [msg/s]\n", timed/el)
	fmt.Printf("mean throughput: %.3f [Mb/s]\n", timed*float64(cfg.size)*8/1e6/el)
}

// reportLat prints the full round-trip latency and rate.
func reportLat(count int, elapsed time.Duration) {
	fmt.Printf("average latency: %.3f [us]\n", float64(elapsed.Nanoseconds())/float64(count)/1000)
	fmt.Printf("roundtrip rate: %.0f [msg/s]\n", float64(count)/elapsed.Seconds())
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
