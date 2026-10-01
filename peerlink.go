package gozmq

// link is one peer of a Socket — a session (accepted, a reconnector's, or a
// STREAM Connect's) or an inproc connection, attached or still waiting for
// its Bind — behind the one teardown contract CloseCtx and Disconnect use.
//
// Being in Socket.links is ownership: a link that ends on its own removes
// itself, and whoever takes it out under s.mu (CloseCtx, Disconnect) must
// stop it.
type link struct {
	// ep is the Connect endpoint Disconnect matches; empty for a peer that
	// connected to us.
	ep string
	// peer is the *session.Session or the *inprocLink itself. An interface
	// rather than func fields: method values would cost a session two
	// allocations per connection.
	peer interface {
		// Drain starts flushing what this side has queued, keeping the
		// connection up, and returns a channel closed once it is flushed
		// (session: writer done; inproc: the peer has received
		// everything). nil means nothing to wait for.
		Drain() <-chan struct{}
		// Stop is the hard teardown, run after the drain (session: close
		// the conn and wait; inproc: close and detach both sides).
		Stop() error
	}
}

// addLink registers l for teardown. It returns false if the socket is
// already closing, in which case the caller must tear the peer down itself.
func (s *Socket) addLink(l *link) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return false
	}
	s.links[l] = struct{}{}
	return true
}

// removeLink drops l once its peer has gone, so long-lived sockets do not
// accumulate links under churn. A no-op if l was already taken.
func (s *Socket) removeLink(l *link) {
	s.mu.Lock()
	delete(s.links, l)
	s.mu.Unlock()
}
