package gozmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Proxy shuttles messages between two sockets in both directions until ctx
// is cancelled or a socket fails, the equivalent of libzmq's zmq_proxy.
//
// Everything Recv'd on frontend is Sent on backend and vice versa, frames
// intact. Because the whole multipart message is moved as one *Msg, routing
// envelopes survive the hop unchanged — which is what makes the classic
// device topologies work:
//
//	XSUB → XPUB   a pub/sub forwarder. Subscriptions travel upstream on
//	              their own because XSUB surfaces SUBSCRIBE and CANCEL as
//	              ordinary messages and XPUB re-emits them; no special
//	              casing here.
//	ROUTER → DEALER   a request/reply broker. The ROUTER-prepended identity
//	              frame rides along, so replies find their way back.
//	PULL → PUSH   a pipeline forwarder.
//
// Half-duplex patterns are handled by only running the direction that can
// work: a PUB or PUSH is never received from, a SUB or PULL never sent to.
// A pairing with no workable direction at all (PUB with PUSH, say) is a
// configuration error and returns immediately.
//
// If capture is non-nil, every forwarded message is also sent to it — the
// usual pattern is a PUB socket for tapping traffic. Capture runs on its
// own goroutine behind a bounded queue, so a tap that keeps up sees every
// message, and one that stalls or fills its high-water mark drops rather
// than applying backpressure to the data path. Capture errors never fail
// the proxy, and a capture socket that blocks never delays its return:
// taps still queued or in flight when Proxy stops may be dropped.
//
// Proxy blocks until it stops. It returns nil for the expected shutdown —
// ctx cancellation, or either socket being closed by another goroutine —
// and the underlying error otherwise. Both directions are torn down before
// it returns, but the sockets themselves are left open; closing them is the
// caller's job.
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	go func() { _ = gozmq.Proxy(ctx, frontend, backend, nil) }()
//
// The two directions run on their own goroutines, so a socket handed to
// Proxy must not be used concurrently by the caller.
func Proxy(ctx context.Context, frontend, backend, capture *Socket) error {
	if frontend == nil || backend == nil {
		return errors.New("gozmq: Proxy requires non-nil frontend and backend sockets")
	}
	if frontend == backend {
		return errors.New("gozmq: Proxy requires two distinct sockets")
	}

	// Cancelling on the first error stops the sibling direction too;
	// without it a proxy whose backend died would sit forever in a
	// frontend Recv.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Half-duplex patterns have one dead direction by construction: a PUSH
	// can never be received from, a PULL never sent to. Running such a
	// direction would fail instantly with ErrFSMViolation and take the
	// working direction down with it, so skip it — the same net effect as
	// libzmq's zmq_proxy, where those sockets simply never poll ready.
	dirs := make([]struct{ from, to *Socket }, 0, 2)
	for _, d := range [2]struct{ from, to *Socket }{
		{frontend, backend},
		{backend, frontend},
	} {
		if canRecv(d.from.Type()) && canSend(d.to.Type()) {
			dirs = append(dirs, d)
		}
	}
	if len(dirs) == 0 {
		return fmt.Errorf("gozmq: Proxy cannot forward in either direction between %s and %s",
			frontend.Type(), backend.Type())
	}

	tap := startCaptureTap(capture)
	defer tap.stop()

	var wg sync.WaitGroup
	errs := make([]error, len(dirs))
	for i, dir := range dirs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cancel()
			errs[i] = proxyOneWay(runCtx, dir.from, dir.to, tap)
		}()
	}
	wg.Wait()

	return errors.Join(errs...)
}

// captureTapDepth is how many messages may be queued for the capture
// socket before new ones are dropped. Deep enough that a tap keeping up on
// average is never lossy, shallow enough that a stalled one cannot pin
// much memory.
const captureTapDepth = 256

// captureTap decouples the capture socket from the forwarding path. The
// data path only ever does a non-blocking channel send, so a capture socket
// that blocks, stalls, or sits at its high-water mark costs the proxy
// nothing beyond dropped taps.
//
// The alternative — calling SendMsg with an already-cancelled context —
// looks equivalent but is not: a select with a ready ctx.Done() arm picks
// randomly among ready cases, so it would drop messages even when the
// capture socket had room.
type captureTap struct {
	ch     chan *Msg
	done   chan struct{}
	cancel context.CancelFunc // aborts a capture SendMsg blocked at shutdown
}

func startCaptureTap(capture *Socket) *captureTap {
	if capture == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &captureTap{
		ch:     make(chan *Msg, captureTapDepth),
		done:   make(chan struct{}),
		cancel: cancel,
	}
	go func() {
		defer close(t.done)
		// Blocking sends are fine here: this goroutine is off the data
		// path, and backpressure just fills t.ch, which then drops. ctx
		// only bounds them at shutdown, where a capture socket with no
		// peer (PUSH, DEALER) would otherwise block stop forever.
		for m := range t.ch {
			if err := capture.SendMsg(ctx, m); err != nil {
				// The capture socket is gone or the proxy is
				// stopping; keep draining so senders never block,
				// but stop trying to send.
				for range t.ch {
				}
				return
			}
		}
	}()
	return t
}

// offer queues m for capture; the same *Msg is also forwarded, which is
// safe because sockets only read the messages they send. It never blocks:
// a full queue drops. A nil tap (no capture socket) is a no-op.
func (t *captureTap) offer(m *Msg) {
	if t == nil {
		return
	}
	select {
	case t.ch <- m:
	default:
	}
}

func (t *captureTap) stop() {
	if t == nil {
		return
	}
	// Both directions have exited, so nothing offers any more: cancel
	// unblocks an in-flight capture send, and closing ch lets the
	// goroutine drain whatever is still queued and exit.
	t.cancel()
	close(t.ch)
	<-t.done
}

// proxyOneWay forwards from → to until the context ends or a socket fails.
// Shutdown causes (context cancellation, either socket closed) return nil;
// anything else is reported.
func proxyOneWay(ctx context.Context, from, to *Socket, tap *captureTap) error {
	for {
		m, err := from.RecvMsg(ctx)
		if err != nil {
			if isProxyShutdown(ctx, err) {
				return nil
			}
			return fmt.Errorf("gozmq: proxy recv from %s: %w", from.Type(), err)
		}

		tap.offer(m)

		if err := to.SendMsg(ctx, m); err != nil {
			if isProxyShutdown(ctx, err) {
				return nil
			}
			return fmt.Errorf("gozmq: proxy send to %s: %w", to.Type(), err)
		}
	}
}

// canRecv and canSend report the directions a socket pattern supports.
// PUB and PUSH never receive; SUB and PULL never send. Every other
// built-in pattern is bidirectional as far as the proxy is concerned
// (REQ/REP alternation is the caller's problem, not the proxy's — those
// patterns are not what you put behind a device anyway).
func canRecv(t SocketType) bool { return t != Pub && t != Push }

func canSend(t SocketType) bool { return t != Sub && t != Pull }

// isProxyShutdown reports whether err is an ordinary stop signal rather
// than a fault worth surfacing: the caller cancelled ctx, or a socket was
// closed out from under the proxy (the documented way to stop it without a
// context).
func isProxyShutdown(ctx context.Context, err error) bool {
	return ctx.Err() != nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrSocketClosed) ||
		errors.Is(err, ErrContextClosed)
}
