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
	// a SendMsg has at least one attached peer to dispatch to. UNLIKE
	// libzmq's ZMQ_POLLOUT (which only reports ready when a message can
	// be queued without blocking), gozmq's PollOut does NOT consider HWM
	// pressure: SendMsg after a PollOut wakeup may still block if every
	// peer pipe is full. Event loops ported from zmq_poll should pair
	// PollOut with a Send deadline or context.
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
	gen     uint64 // bumped on every registration change; invalidates the cache

	// cacheMu hands the reusable snapshot to one Poll call at a time.
	// The common single-goroutine event loop always wins the TryLock and
	// reuses the slices across calls; concurrent Poll calls fall back to
	// building a fresh snapshot.
	cacheMu sync.Mutex
	cache   pollSnapshot
}

// pollSnapshot is a reusable snapshot of the registration set: the regs
// scanned for readiness and the reflect.SelectCase slice Poll parks on.
// cases holds one entry per registered interest channel plus a trailing
// slot for ctx.Done, filled in per Poll call. Engines guarantee a stable
// ready channel for their lifetime (a single level-triggered gate), so
// the cases only need rebuilding when the registration set changes or a
// registered socket closes.
type pollSnapshot struct {
	gen   uint64
	valid bool
	regs  []pollerReg
	cases []reflect.SelectCase
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
	p.gen++
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
	p.gen++
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
// Cost: Poll parks via reflect.Select over the registered ready
// channels. The reflection step is necessary because the registered set
// is dynamic and ready channels come from heterogeneous engine types.
// The registration snapshot and SelectCase slice are cached across Poll
// calls (rebuilt only when the registration set changes or a registered
// socket closes), so steady-state Poll allocates only the result slice.
// Rough budget: comfortable for hundreds of registered sockets at
// thousands of Poll/sec; if you need higher rates, consolidate ready
// channels upstream (e.g. one dedicated goroutine per socket fanning
// into a single application channel) and avoid Poller in the hot path.
func (p *Poller) Poll(ctx context.Context) ([]PollEvent, error) {
	snap := &pollSnapshot{}
	if p.cacheMu.TryLock() {
		snap = &p.cache
		defer p.cacheMu.Unlock()
	}
	for {
		p.refresh(snap)

		// Immediate non-blocking pass: anything ready right now wins
		// without going through reflect.Select.
		if ready := scanReady(snap.regs); len(ready) > 0 {
			return ready, nil
		}

		// Block on any of the registered ready channels (or ctx).
		if err := waitForWake(ctx, snap); err != nil {
			return nil, err
		}
		// A wake fired; re-scan and either return non-empty or loop
		// (level signals + churn race).
	}
}

// refresh rebuilds snap when the registration set changed (generation
// bump), a registered socket closed since the snapshot was taken, or
// snap is fresh. The rebuild excludes zero-mask and closed sockets —
// the same snapshot semantics Poll has always had. Cheap when current:
// one mutex hop plus an atomic closed-flag load per cached entry.
func (p *Poller) refresh(snap *pollSnapshot) {
	p.mu.Lock()
	stale := !snap.valid || snap.gen != p.gen
	if !stale {
		for _, r := range snap.regs {
			if r.sock.closed.Load() {
				stale = true
				break
			}
		}
	}
	if !stale {
		p.mu.Unlock()
		return
	}
	snap.gen = p.gen
	snap.valid = true
	snap.regs = snap.regs[:0]
	for s, mask := range p.entries {
		if mask == 0 || s.closed.Load() {
			continue
		}
		snap.regs = append(snap.regs, pollerReg{sock: s, mask: mask})
	}
	p.mu.Unlock()

	// Rebuild the SelectCase set outside p.mu — it calls into engines.
	// Ready channels are stable for an engine's lifetime, so these cases
	// stay correct until the next registration change or socket close.
	snap.cases = snap.cases[:0]
	for _, r := range snap.regs {
		eng, ok := r.sock.engine.(pollable)
		if !ok {
			continue
		}
		if r.mask&PollIn != 0 {
			if ch := eng.ReadyRecv(); ch != nil {
				snap.cases = append(snap.cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ch),
				})
			}
		}
		if r.mask&PollOut != 0 {
			if ch := eng.ReadyWrite(); ch != nil {
				snap.cases = append(snap.cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ch),
				})
			}
		}
	}
	// Trailing slot for ctx.Done, filled in per Poll call by waitForWake
	// so Poll honours cancellation even with an empty interest set.
	snap.cases = append(snap.cases, reflect.SelectCase{Dir: reflect.SelectRecv})
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
// The cases were prepared by refresh; only the trailing ctx slot is
// per-call.
func waitForWake(ctx context.Context, snap *pollSnapshot) error {
	cases := snap.cases
	cases[len(cases)-1].Chan = reflect.ValueOf(ctx.Done())
	chosen, _, _ := reflect.Select(cases)
	if chosen == len(cases)-1 {
		return ctx.Err()
	}
	return nil
}
