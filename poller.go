package gozmq

import (
	"context"
	"reflect"
	"sync"

	"github.com/rkm/gozmq/internal/socket"
)

// PollEvents is a bitmask of interest sets passed to Poller.Add / Modify
// and surfaced inside [PollEvent] when Poll reports readiness. The bit
// values match the libzmq ZMQ_POLLIN / ZMQ_POLLOUT convention so they
// are easy to mentally translate between gozmq and zmq_poll-style code.
type PollEvents uint8

const (
	// PollIn signals interest in (or readiness of) inbound messages —
	// a RecvMsg on the socket is expected not to block on queue
	// emptiness.
	PollIn PollEvents = 1 << 0
	// PollOut signals interest in (or readiness of) outbound capacity —
	// a SendMsg has at least one attached peer to dispatch to. HWM
	// pressure is not surfaced via PollOut; this matches libzmq's
	// "POLLOUT means a peer exists" semantics.
	PollOut PollEvents = 1 << 1
)

// PollEvent reports that a Poller-registered Socket has at least one of
// its registered interest events ready. Events is the intersection of
// the registration mask and the engine's current readiness — it never
// contains bits the caller did not register.
type PollEvent struct {
	// Socket is the *gozmq.Socket the caller passed to Poller.Add.
	Socket *Socket
	// Events is the set of ready directions among the registered mask.
	Events PollEvents
}

// Poller multiplexes readiness across many sockets, equivalent in spirit
// to libzmq's zmq_poll: register N sockets with interest sets, then call
// Poll to block until at least one has at least one of its registered
// events ready.
//
// Concurrency: Add, Remove, Modify, and Poll are individually safe for
// concurrent use by multiple goroutines (serialised by the Poller's
// mutex). The conventional usage is still single-threaded — one
// goroutine owns the Poller and uses Poll as its main event loop.
//
// Readiness model: this is non-consuming. A Poll call reports that a
// Recv (resp. Send) is unlikely to block on emptiness (resp. peer
// absence); the caller is still responsible for actually invoking
// Socket.RecvMsg / Socket.SendMsg on the returned sockets. Subsequent
// Poll calls return fresh readiness — there is no latched edge-trigger
// state between calls.
//
// Engine support: every built-in socket type exposes non-consuming readiness
// signals to the Poller.
type Poller struct {
	mu      sync.Mutex
	entries map[*Socket]PollEvents
}

// NewPoller returns an empty Poller. Sockets are registered via Add.
func NewPoller() *Poller {
	return &Poller{entries: make(map[*Socket]PollEvents)}
}

// Add registers sock with the given interest mask. If sock is already
// registered, Add replaces the existing mask (equivalent to Modify).
// events == 0 leaves the socket present in the registration table but
// with no interest set — equivalent to Remove + Add(sock, 0). Most
// callers prefer Remove for clarity.
func (p *Poller) Add(sock *Socket, events PollEvents) {
	if sock == nil {
		return
	}
	p.mu.Lock()
	if p.entries == nil {
		p.entries = make(map[*Socket]PollEvents)
	}
	p.entries[sock] = events
	p.mu.Unlock()
}

// Remove deregisters sock. Subsequent Poll calls will not consider it
// even if it has data ready. Idempotent: removing an unregistered
// socket is a no-op.
func (p *Poller) Remove(sock *Socket) {
	if sock == nil {
		return
	}
	p.mu.Lock()
	delete(p.entries, sock)
	p.mu.Unlock()
}

// Modify changes the interest mask for an already-registered socket.
// If sock is not currently registered, Modify is equivalent to Add.
func (p *Poller) Modify(sock *Socket, events PollEvents) {
	p.Add(sock, events)
}

// Poll blocks until at least one registered socket has at least one of
// its registered events ready, or ctx is cancelled. On return the
// slice contains exactly the ready sockets with their ready event
// bits (a strict subset of each registration mask). The slice is
// caller-owned; the Poller does not retain it.
//
// Empty-registration behaviour: if no sockets are registered (or all
// registered sockets have events == 0), Poll blocks until ctx fires
// and returns ctx.Err(). This matches libzmq's behaviour for an
// empty poll set; callers should usually treat it as a configuration
// error.
//
// Closed sockets: a socket closed after Add but before Poll is
// effectively unregistered for the duration of that Poll call; it
// cannot become ready again. Callers may Remove the closed socket
// afterwards.
//
// Spurious wakeups: under heavy churn (e.g. a peer attaches and
// immediately detaches between the wake and the readiness scan)
// Poll may loop internally and wait again. It never returns an
// empty event slice with a nil error.
//
// Cost: each Poll call builds a fresh reflect.SelectCase slice over the
// registration set and parks via reflect.Select. The reflection step is
// necessary because the registered set is dynamic and ready channels
// come from heterogeneous engine types, but the per-call allocation and
// reflect overhead is not free. Rough budget: comfortable for hundreds
// of registered sockets at hundreds of Poll/sec; if you need higher
// rates, consolidate ready channels upstream (e.g. one dedicated
// goroutine per socket fanning into a single application channel) and
// avoid Poller in the hot path.
func (p *Poller) Poll(ctx context.Context) ([]PollEvent, error) {
	for {
		// Snapshot the registration set so we can iterate without holding
		// the lock through reflect.Select.
		p.mu.Lock()
		regs := make([]pollerReg, 0, len(p.entries))
		for s, mask := range p.entries {
			if mask == 0 {
				continue
			}
			if s.closed.Load() {
				continue
			}
			regs = append(regs, pollerReg{sock: s, mask: mask})
		}
		p.mu.Unlock()

		// Immediate non-blocking pass: anything ready right now wins
		// without going through reflect.Select.
		if ready := scanReady(regs); len(ready) > 0 {
			return ready, nil
		}

		// Block on any of the registered ready channels (or ctx).
		if err := waitForWake(ctx, regs); err != nil {
			return nil, err
		}
		// A wake fired; re-scan and either return non-empty or loop
		// (level signals + churn race).
	}
}

// pollerReg pairs a registered Socket with its interest mask.
type pollerReg struct {
	sock *Socket
	mask PollEvents
}

// pollable mirrors the internal/socket.Pollable optional interface so
// the public package can type-assert without exposing the internal
// type. Engines implement Pollable in the internal/socket package;
// this private alias keeps the surface clean.
type pollable = socket.Pollable

// scanReady performs a non-blocking pass across the registration set,
// returning the subset whose engine reports ready right now. The
// returned events are the intersection of each entry's mask with the
// engine's current readiness, so callers never see bits they did not
// register.
func scanReady(regs []pollerReg) []PollEvent {
	var out []PollEvent
	for _, r := range regs {
		eng, ok := r.sock.engine.(pollable)
		if !ok {
			continue
		}
		var ev PollEvents
		if r.mask&PollIn != 0 && eng.CanRecvNow() {
			ev |= PollIn
		}
		if r.mask&PollOut != 0 && eng.CanSendNow() {
			ev |= PollOut
		}
		if ev != 0 {
			out = append(out, PollEvent{Socket: r.sock, Events: ev})
		}
	}
	return out
}

// waitForWake parks until at least one registered ready channel fires
// or ctx cancels. Returns nil on a successful wake and ctx.Err() on
// cancellation.
//
// We use reflect.Select because the channel set is dynamic in size
// and the per-direction channels come from different engine types.
// The overhead is negligible compared to the goroutine hops in the
// underlying session/pipe layer.
func waitForWake(ctx context.Context, regs []pollerReg) error {
	cases := make([]reflect.SelectCase, 0, 2*len(regs)+1)
	for _, r := range regs {
		eng, ok := r.sock.engine.(pollable)
		if !ok {
			continue
		}
		if r.mask&PollIn != 0 {
			if ch := eng.ReadyRecv(); ch != nil {
				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ch),
				})
			}
		}
		if r.mask&PollOut != 0 {
			if ch := eng.ReadyWrite(); ch != nil {
				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ch),
				})
			}
		}
	}
	// Always include ctx.Done so Poll honours cancellation even when no
	// engine has a usable ready channel (empty interest, all non-
	// pollable engines, etc.).
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	chosen, _, _ := reflect.Select(cases)
	if chosen == len(cases)-1 {
		return ctx.Err()
	}
	return nil
}
