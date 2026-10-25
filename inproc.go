package gozmq

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/session"
)

// inproc:// is not a transport. A Connect and its Bind share one Context, so
// the two engines are joined by a single pipe pair: no ZMTP, no session, no
// goroutines. Like libzmq, inproc runs no security mechanism, so sockets
// configured with one cannot use it.

const inprocScheme = "inproc://"

// inprocRegistry is a Context's inproc namespace, keyed by the full endpoint
// string. Its mutex may be held while taking a Socket's mu, never the
// reverse.
type inprocRegistry struct {
	mu      sync.Mutex
	bound   map[string]*Socket
	names   map[*Socket][]string     // endpoints each socket has bound
	pending map[string][]*inprocLink // Connects waiting for their Bind
}

// unbind releases endpoint if s holds it.
func (r *inprocRegistry) unbind(s *Socket, endpoint string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bound[endpoint] != s {
		return false
	}
	delete(r.bound, endpoint)
	if names := slices.DeleteFunc(r.names[s], func(n string) bool { return n == endpoint }); len(names) > 0 {
		r.names[s] = names
	} else {
		delete(r.names, s)
	}
	return true
}

// unbindAll releases every endpoint s holds; CloseCtx calls it.
func (r *inprocRegistry) unbindAll(s *Socket) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ep := range r.names[s] {
		delete(r.bound, ep)
	}
	delete(r.names, s)
}

// cancel drops a waiting Connect. A no-op once a Bind has claimed it.
func (r *inprocRegistry) cancel(l *inprocLink) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w := slices.DeleteFunc(r.pending[l.endpoint], func(p *inprocLink) bool { return p == l }); len(w) > 0 {
		r.pending[l.endpoint] = w
	} else {
		delete(r.pending, l.endpoint)
	}
}

// isInproc reports whether endpoint uses the inproc:// scheme.
func isInproc(endpoint string) bool { return strings.HasPrefix(endpoint, inprocScheme) }

// checkInproc validates an inproc Bind or Connect by s.
func (s *Socket) checkInproc(endpoint string) error {
	switch {
	case endpoint == inprocScheme:
		return fmt.Errorf("%w: inproc endpoint requires a non-empty name", ErrEndpointInvalid)
	case s.socketType == Stream:
		return fmt.Errorf("%w: %q: STREAM sockets carry raw bytes and do not support inproc", ErrEndpointInvalid, endpoint)
	case s.cfg.mech != nil:
		return fmt.Errorf("%w: %q: inproc runs no security handshake, so a socket with PLAIN, CURVE or WithSecurityMech cannot use it",
			ErrEndpointInvalid, endpoint)
	}
	return nil
}

func (s *Socket) bindInproc(endpoint string) error {
	if err := s.checkInproc(endpoint); err != nil {
		s.emit(EventBindFailed, endpoint, err.Error())
		return err
	}
	// Register under the registry lock with the closed check inside it:
	// either CloseCtx's unbindAll sees this name, or we see closed.
	r := &s.ctx.inproc
	r.mu.Lock()
	if s.closed.Load() {
		r.mu.Unlock()
		return ErrSocketClosed
	}
	if _, taken := r.bound[endpoint]; taken {
		r.mu.Unlock()
		err := fmt.Errorf("%w: inproc endpoint %q already bound", ErrEndpointInvalid, endpoint)
		s.emit(EventBindFailed, endpoint, err.Error())
		return err
	}
	r.bound[endpoint] = s
	r.names[s] = append(r.names[s], endpoint)
	waiting := r.pending[endpoint]
	delete(r.pending, endpoint)
	r.mu.Unlock()

	s.mu.Lock()
	s.lastEndpoint = endpoint
	s.mu.Unlock()
	s.emit(EventListening, endpoint, "")

	for _, cl := range waiting {
		if err := wireInproc(cl, s, true); err != nil {
			cl.s.removeLink(&cl.link)
			if !errors.Is(err, ErrSocketClosed) {
				cl.s.emit(EventConnectFailed, endpoint, err.Error())
			}
		}
	}
	return nil
}

// connectInproc wires to the bound socket now, or waits (as a link with no
// pipe yet) until a Bind, Disconnect or Close.
func (s *Socket) connectInproc(endpoint string) error {
	if err := s.checkInproc(endpoint); err != nil {
		s.emit(EventConnectFailed, endpoint, err.Error())
		return err
	}
	cl := newInprocLink(s, endpoint, true)
	r := &s.ctx.inproc
	r.mu.Lock()
	bind := r.bound[endpoint]
	if bind == nil {
		ok := s.addLink(&cl.link)
		if ok {
			r.pending[endpoint] = append(r.pending[endpoint], cl)
		}
		r.mu.Unlock()
		if !ok {
			return ErrSocketClosed
		}
		return nil
	}
	r.mu.Unlock()
	if err := wireInproc(cl, bind, false); err != nil {
		s.emit(EventConnectFailed, endpoint, err.Error())
		return err
	}
	return nil
}

// inprocHWM is libzmq's inproc queue size for one direction: the sender's
// send HWM plus the receiver's recv HWM, unlimited if either is.
func inprocHWM(send, recv int) int {
	if send == 0 || recv == 0 {
		return 0
	}
	// Both are at most MaxHWM after validation, so the sum cannot overflow.
	return send + recv
}

// wireInproc joins cl's socket (the connector) to bind with one pipe pair.
// waiting means cl is already in its socket's links as a pending Connect,
// so it is resolved in place, and only if Disconnect has not dropped it.
func wireInproc(cl *inprocLink, bind *Socket, waiting bool) error {
	conn := cl.s
	if ct, bt := socketTypeName(conn.socketType), socketTypeName(bind.socketType); !session.SocketTypesCompatible(ct, bt) {
		return fmt.Errorf("gozmq: inproc %s socket cannot connect to %s: %w", ct, bt, ErrFSMViolation)
	}
	ce, be := pipe.NewChannelPair(
		inprocHWM(conn.cfg.sendHWM, bind.cfg.recvHWM),
		inprocHWM(bind.cfg.sendHWM, conn.cfg.recvHWM),
		bind.cfg.identity,
		conn.cfg.identity,
	)
	if err := conn.engine.AttachPipe(ce); err != nil {
		_ = ce.Close()
		return err
	}
	if err := bind.engine.AttachPipe(be); err != nil {
		_ = ce.Close()
		_ = conn.engine.DetachPipe(ce)
		return err
	}
	ep := cl.endpoint
	conn.emit(EventConnected, ep, "")
	bind.emit(EventAccepted, ep, "")

	// Publish the connector side first. Until then nobody else can reach
	// the pipe, so a refusal is torn down here. The binder's end and remote
	// are set before that publish: cl.remote (and so bl) becomes reachable
	// only through conn.mu, which orders this write before any reader, so
	// a connector teardown that runs bl.detach never sees a nil end.
	bl := newInprocLink(bind, ep, false)
	bl.end, bl.remote = be, cl
	if !conn.publishInproc(cl, ce, bl, waiting) {
		_ = ce.Close()
		_ = conn.engine.DetachPipe(ce)
		_ = bind.engine.DetachPipe(be)
		conn.emit(EventDisconnected, ep, "")
		bind.emit(EventDisconnected, ep, "")
		return ErrSocketClosed
	}
	if !bind.publishInproc(bl, be, cl, false) {
		// The binder is closing, or the connector already closed the pipe
		// (a Disconnect or CloseCtx raced us). Close the connector's side,
		// which detaches both ends; each detach is once-guarded, so a
		// concurrent connector teardown and this path together run each
		// exactly once and be never stays attached to the binder.
		cl.close()
	}
	return nil
}

// publishInproc records l, with its attached end, in s's links. It refuses
// when s is closing, the pipe is already closed, or (waiting) l has been
// dropped by Disconnect. Checking under s.mu orders it against removeLink:
// a close that ran first is seen here, a later one finds l.
func (s *Socket) publishInproc(l *inprocLink, end *pipe.End, remote *inprocLink, waiting bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() || end.IsClosed() {
		return false
	}
	if _, ok := s.links[&l.link]; waiting && !ok {
		return false
	}
	s.links[&l.link] = struct{}{}
	if l.end == nil {
		// The connector side is set here, under its socket's mu, which is
		// what orders it against a Disconnect/CloseCtx that takes the link;
		// the binder side was set by wireInproc before it became reachable.
		l.end, l.remote = end, remote
	}
	return true
}

// inprocLink is one socket's side of an inproc connection. A Connect that
// is still waiting for its Bind is a link with no pipe yet. end and remote
// are written once, before the link can be reached by anyone who reads them:
// the connector's under its socket's mu while the link is in s.links, the
// binder's before the connector's publish makes it reachable as a remote.
type inprocLink struct {
	link
	s        *Socket
	endpoint string
	end      *pipe.End
	remote   *inprocLink // the other socket's side
	once     sync.Once
}

// newInprocLink returns s's side of an inproc connection to endpoint; the
// Connect side is the one Disconnect closes.
func newInprocLink(s *Socket, endpoint string, connect bool) *inprocLink {
	l := &inprocLink{s: s, endpoint: endpoint}
	if connect {
		l.ep = endpoint
	}
	l.peer = l
	return l
}

// Drain fires once the peer socket has received everything this side sent:
// the peer is usually alive and would never close its end.
func (l *inprocLink) Drain() <-chan struct{} {
	if l.end == nil {
		return nil
	}
	return l.end.SendDrained()
}

// Stop closes the link.
func (l *inprocLink) Stop() error { l.close(); return nil }

// close tears the link down from this side: it closes the pipe, detaches
// this side, then the peer's, synchronously and without holding either
// socket's mu. Messages already queued stay receivable on both sides.
func (l *inprocLink) close() {
	if l.end == nil {
		l.s.ctx.inproc.cancel(l)
		l.s.removeLink(&l.link)
		return
	}
	_ = l.end.Close()
	l.detach()
	l.remote.detach()
}

// detach runs once per side. The link leaves s.links last, so a CloseCtx
// that races it still waits (in once) for the detach to finish before
// emitting EventClosed.
func (l *inprocLink) detach() {
	l.once.Do(func() {
		_ = l.s.engine.DetachPipe(l.end)
		l.s.emit(EventDisconnected, l.endpoint, "")
		l.s.removeLink(&l.link)
	})
}
