package main

import (
	"context"
	"log"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/gozmq"
)

// A pattern is one socket pairing and the four roles a run can take on it.
// Throughput patterns fill recv (local_thr) and send (remote_thr); latency
// patterns fill server (local_lat) and client (remote_lat). The bind
// argument says whether the role binds or connects; ready, when not nil, is
// closed once the role's socket is bound or connected and any setup that
// must precede the peer is done (inproc orders its goroutines with it).
//
// Throughput patterns:
//
//	push_pull      PUSH -> PULL
//	pub_sub        PUB -> SUB (subscribed to -prefix; fan-out with several SUBs)
//	xpub_xsub      XPUB -> XSUB (the XSUB sends the subscription message; the
//	               XPUB waits for -subs of them before sending)
//	pair           PAIR -> PAIR
//	dealer_router  DEALER -> ROUTER (the ROUTER receives [identity, body])
//	router_dealer  ROUTER -> DEALER (the DEALER says hello first so the ROUTER
//	               learns its identity, then the ROUTER streams [identity, body])
//	stream_in      raw TCP writer -> STREAM (tcp only; the writer is always Go)
//	stream_out     STREAM -> raw TCP reader (tcp only; the reader is always Go)
//
// Latency patterns (the client sends, the server echoes the whole message):
//
//	req_rep, req_router, dealer_rep, dealer_router, pair
type pattern struct {
	name    string
	recv    func(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool, ready chan<- struct{}) thrResult
	send    func(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool)
	server  func(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool, ready chan<- struct{})
	client  func(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool) time.Duration
	fanout  bool // several receivers per sender make sense (pub_sub, xpub_xsub)
	tcpOnly bool // raw TCP on one side: no inproc, ipc or security
}

var thrPatterns = map[string]pattern{
	"push_pull":     {recv: recvOn(gozmq.Pull, nil), send: sendOn(gozmq.Push, nil)},
	"pub_sub":       {recv: recvOn(gozmq.Sub, nil), send: sendOn(gozmq.Pub, waitForSubscribers), fanout: true},
	"xpub_xsub":     {recv: recvOn(gozmq.XSub, sendSubscription), send: sendOn(gozmq.XPub, recvSubscriptions), fanout: true},
	"pair":          {recv: recvOn(gozmq.Pair, nil), send: sendOn(gozmq.Pair, nil)},
	"dealer_router": {recv: recvOn(gozmq.Router, nil), send: sendOn(gozmq.Dealer, nil)},
	"router_dealer": {recv: recvOn(gozmq.Dealer, sayHello), send: sendRouter},
	"stream_in":     {recv: recvStream, send: sendRawTCP, tcpOnly: true},
	"stream_out":    {recv: recvRawTCP, send: sendStream, tcpOnly: true},
}

var latPatterns = map[string]pattern{
	"req_rep":       {server: echoOn(gozmq.Rep), client: pingOn(gozmq.Req, false)},
	"req_router":    {server: echoOn(gozmq.Router), client: pingOn(gozmq.Req, false)},
	"dealer_rep":    {server: echoOn(gozmq.Rep), client: pingOn(gozmq.Dealer, true)},
	"dealer_router": {server: echoOn(gozmq.Router), client: pingOn(gozmq.Dealer, false)},
	"pair":          {server: echoOn(gozmq.Pair), client: pingOn(gozmq.Pair, false)},
}

func lookupPattern(name string, thr bool) (pattern, bool) {
	m := latPatterns
	if thr {
		m = thrPatterns
	}
	p, ok := m[name]
	p.name = name
	return p, ok
}

func patternNames(thr bool) string {
	m := latPatterns
	if thr {
		m = thrPatterns
	}
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// open creates a socket of typ with the run's HWM, infinite linger (Close
// waits until every queued message reached the peer, as libzmq's tools rely
// on), the receive pool setting and the CURVE role, then binds or connects.
func open(zctx *gozmq.Context, typ gozmq.SocketType, cfg *config, bind bool, extra ...gozmq.Option) *gozmq.Socket {
	opts := []gozmq.Option{gozmq.WithSendHWM(cfg.hwm), gozmq.WithRecvHWM(cfg.hwm), gozmq.WithLinger(-1)}
	if typ != gozmq.Stream {
		opts = append(opts, gozmq.WithRecvBufferPool(cfg.pool))
	}
	if cfg.curve {
		opts = append(opts, curveOption(bind))
	}
	s, err := zctx.NewSocket(typ, append(opts, extra...)...)
	check(err, "new socket")
	if bind {
		check(s.Bind(cfg.endpoint), "bind "+cfg.endpoint)
	} else {
		check(s.Connect(cfg.endpoint), "connect "+cfg.endpoint)
	}
	return s
}

func signal(ready chan<- struct{}) {
	if ready != nil {
		close(ready)
	}
}

// recvOn is the receiver of a ZMQ-to-ZMQ throughput pattern: open typ, run
// the optional setup (after ready, so the peer can connect), then receive.
func recvOn(typ gozmq.SocketType, setup func(context.Context, *gozmq.Socket, *config)) func(context.Context, *gozmq.Context, *config, bool, chan<- struct{}) thrResult {
	return func(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool, ready chan<- struct{}) thrResult {
		var extra []gozmq.Option
		if typ == gozmq.Sub {
			extra = append(extra, gozmq.WithSubscribe(cfg.prefix))
		}
		// A receiver has nothing to flush at exit; zero linger keeps a
		// SUB's re-queued subscription from holding Close open after the
		// publisher is gone.
		s := open(zctx, typ, cfg, bind, append(extra, gozmq.WithLinger(0))...)
		defer s.Close()
		signal(ready)
		if setup != nil {
			setup(ctx, s, cfg)
		}
		return receive(ctx, s, cfg)
	}
}

// receive takes count messages, releasing each, and times all but the
// first: the clock starts after the first message, as libzmq's local_thr
// does, so the figure is steady state rather than connection setup. A
// watchdog cancels the receive after cfg.idle without progress, so a run
// that lost messages (a PUB at HWM, a slow joiner) ends with a short count
// instead of hanging. The profiles cover the timed part.
func receive(ctx context.Context, s *gozmq.Socket, cfg *config) thrResult {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var progress atomic.Int64
	go watchdog(ctx, cancel, &progress, cfg.idle)

	if !recvChecked(ctx, s, cfg.size) {
		return thrResult{}
	}
	progress.Store(1)
	stop := profile(cfg.cpuProf, cfg.memProf)
	start := time.Now()
	n := 1
	for n < cfg.count && recvChecked(ctx, s, cfg.size) {
		n++
		progress.Store(int64(n))
	}
	elapsed := time.Since(start)
	stop()
	return thrResult{received: n, elapsed: elapsed}
}

// watchdog cancels when progress has not moved for a whole idle period.
func watchdog(ctx context.Context, cancel context.CancelFunc, progress *atomic.Int64, idle time.Duration) {
	t := time.NewTicker(idle)
	defer t.Stop()
	last := int64(-1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if p := progress.Load(); p == last {
				cancel()
				return
			} else {
				last = p
			}
		}
	}
}

// recvChecked receives one message, checks that its last frame (the body;
// a ROUTER prepends the identity) has the expected size, and releases it.
// It reports false when the context ended.
func recvChecked(ctx context.Context, s *gozmq.Socket, size int) bool {
	m, err := s.RecvMsg(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		log.Fatalf("recv: %v", err)
	}
	fr := m.Frames()
	if len(fr) == 0 || len(fr[len(fr)-1]) != size {
		log.Fatalf("message of incorrect size (%d frames, last %d bytes, expected %d)", len(fr), len(fr[len(fr)-1]), size)
	}
	m.Release()
	return true
}

// sendOn is the sender of a ZMQ-to-ZMQ throughput pattern: open typ, run
// the optional setup, send the payload count times, close (which waits for
// delivery under infinite linger).
func sendOn(typ gozmq.SocketType, setup func(context.Context, *gozmq.Socket, *config)) func(context.Context, *gozmq.Context, *config, bool) {
	return func(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool) {
		var extra []gozmq.Option
		if typ == gozmq.XPub {
			extra = append(extra, gozmq.WithXPubVerbose(true)) // one subscription message per subscriber
		}
		s := open(zctx, typ, cfg, bind, extra...)
		if setup != nil {
			setup(ctx, s, cfg)
		}
		for i := range cfg.count {
			if err := s.SendMsg(ctx, gozmq.NewMsgFromBytes(cfg.payload)); err != nil {
				log.Fatalf("send[%d]: %v", i, err)
			}
		}
		check(s.Close(), "close")
	}
}

// waitForSubscribers gives the SUBs' subscriptions time to reach the PUB,
// which has no way to observe them (the classic slow joiner).
func waitForSubscribers(_ context.Context, _ *gozmq.Socket, cfg *config) {
	time.Sleep(cfg.wait)
}

// sendSubscription is the XSUB's setup: subscribe by message.
func sendSubscription(ctx context.Context, s *gozmq.Socket, cfg *config) {
	sub := append([]byte{1}, cfg.prefix...)
	check(s.SendMsg(ctx, gozmq.NewMsgFromBytes(sub)), "xsub subscribe")
}

// recvSubscriptions is the XPUB's setup: wait for every subscriber's
// subscription message, which proves the path is open (no slow joiner).
func recvSubscriptions(ctx context.Context, s *gozmq.Socket, cfg *config) {
	for range cfg.subs {
		m, err := s.RecvMsg(ctx)
		check(err, "xpub recv subscription")
		m.Release()
	}
}

// sayHello is the DEALER receiver's setup in router_dealer: the first
// message tells the ROUTER this peer's identity.
func sayHello(ctx context.Context, s *gozmq.Socket, _ *config) {
	check(s.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte("hello"))), "dealer hello")
}

// sendRouter is router_dealer's sender: learn the DEALER's identity from
// its hello, then route the payload to it count times.
func sendRouter(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool) {
	s := open(zctx, gozmq.Router, cfg, bind, gozmq.WithRouterMandatory(true))
	m, err := s.RecvMsg(ctx)
	check(err, "router recv hello")
	id := append([]byte(nil), m.Frames()[0]...)
	m.Release()
	for i := range cfg.count {
		if err := s.SendMsg(ctx, gozmq.NewMsgFromFrames(id, cfg.payload)); err != nil {
			log.Fatalf("send[%d]: %v", i, err)
		}
	}
	check(s.Close(), "close")
}

// echoOn is a latency server: receive count messages and send each back
// whole (a ROUTER's identity frame routes the echo).
func echoOn(typ gozmq.SocketType) func(context.Context, *gozmq.Context, *config, bool, chan<- struct{}) {
	return func(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool, ready chan<- struct{}) {
		s := open(zctx, typ, cfg, bind)
		signal(ready)
		for i := range cfg.count {
			m, err := s.RecvMsg(ctx)
			if err != nil {
				log.Fatalf("recv[%d]: %v", i, err)
			}
			if err := s.SendMsg(ctx, m); err != nil {
				log.Fatalf("send[%d]: %v", i, err)
			}
			m.Release()
		}
		check(s.Close(), "close")
	}
}

// pingOn is a latency client: send the payload and wait for the echo count
// times, returning the total time. envelope adds the empty delimiter frame
// a DEALER needs in front of a REP.
func pingOn(typ gozmq.SocketType, envelope bool) func(context.Context, *gozmq.Context, *config, bool) time.Duration {
	return func(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool) time.Duration {
		s := open(zctx, typ, cfg, bind)
		defer s.Close()
		newMsg := func() *gozmq.Msg { return gozmq.NewMsgFromBytes(cfg.payload) }
		if envelope {
			newMsg = func() *gozmq.Msg { return gozmq.NewMsgFromFrames(nil, cfg.payload) }
		}
		start := time.Now()
		for i := range cfg.count {
			if err := s.SendMsg(ctx, newMsg()); err != nil {
				log.Fatalf("send[%d]: %v", i, err)
			}
			if !recvChecked(ctx, s, cfg.size) {
				log.Fatalf("recv[%d]: %v", i, ctx.Err())
			}
		}
		return max(time.Since(start), time.Nanosecond)
	}
}
