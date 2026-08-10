package gozmq

import (
	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/session"
	"github.com/exergy-dev/gozmq/internal/stream"
)

// startStreamAdapter is the STREAM-specific counterpart to startSession:
// wraps conn in a raw-bytes Adapter, attaches its engine-facing pipe to
// the streamEngine, and spawns a watcher that DetachPipes + Closes the
// adapter when the peer disconnects. No ZMTP handshake, no security
// mechanism — STREAM peers are by definition non-ZMQ.
func (s *Socket) startStreamAdapter(conn iface.Conn) {
	remote := conn.RemoteEndpoint()
	ad := stream.NewAdapter(s.runCtx, conn, stream.Config{
		SendHWM: s.cfg.sendHWM,
		RecvHWM: s.cfg.recvHWM,
	})

	if !s.addLink(streamLink{ad}) {
		_ = ad.Close()
		return
	}

	p := ad.EnginePipe()
	if err := s.engine.AttachPipe(p); err != nil {
		_ = ad.Close()
		return
	}

	s.watchPipeClose(p, remote, func() { _ = ad.Close() })
}

// connectStream is the STREAM-specific Connect path. It dials once via
// the transport, constructs a stream.Adapter, attaches it, and wires up
// teardown.
func (s *Socket) connectStream(tr iface.Transport, endpoint string) error {
	conn, err := tr.Dial(s.runCtx, endpoint)
	if err != nil {
		s.emit(EventConnectFailed, endpoint, err.Error())
		return err
	}
	s.emit(EventConnected, endpoint, "")
	s.startStreamAdapter(conn)
	return nil
}

// --------------------------------------------------------------------------
// Direct-pipe (inproc session bypass)
// --------------------------------------------------------------------------

// hasSecurityMech reports whether the socket is configured with PLAIN or
// CURVE security. Direct-pipe bypass is only safe when the transport is
// in-process AND no security handshake is required.
func (s *Socket) hasSecurityMech() bool {
	return s.cfg.plainClient || s.cfg.plainAuth != nil ||
		(len(s.cfg.curveSecretKey) == 32)
}

// connectDirectPipe is the inproc fast path that replaces the full
// Reconnector+Session stack with a single pipe.NewPair wired directly
// into both engines. The dialer creates the pair, attaches one end to
// its own engine, and offers the other end to the bind-side listener via
// the DirectPipeAcceptor interface.
func (s *Socket) connectDirectPipe(acceptor iface.DirectPipeAcceptor, endpoint string) error {
	dialerPipe, binderPipe := pipe.NewPair(
		s.cfg.sendHWM, s.cfg.recvHWM,
		pipe.PeerInfo{},
		pipe.PeerInfo{Identity: s.cfg.identity},
	)
	offer := &iface.DirectPipeOffer{
		BindPipe:         binderPipe,
		DialerSocketType: socketTypeName(s.socketType),
		Endpoint:         endpoint,
	}

	// Push the offer to the bind side. If the listener is closed, clean up.
	if err := acceptor.OfferDirectPipe(offer); err != nil {
		_ = dialerPipe.Close()
		_ = binderPipe.Close()
		return err
	}

	s.emit(EventConnected, endpoint, "")
	return s.trackAttachWatchPipe(dialerPipe, endpoint)
}

// trackAttachWatchPipe records p for the CloseCtx linger drain, attaches it to
// the engine, and starts the close watcher. On attach failure it closes p and
// returns the error. Shared by the dialer and acceptor direct-pipe paths.
func (s *Socket) trackAttachWatchPipe(p iface.Pipe, endpoint string) error {
	if !s.addLink(pipeLink{p}) {
		_ = p.Close()
		return ErrSocketClosed
	}
	if err := s.engine.AttachPipe(p); err != nil {
		_ = p.Close()
		return err
	}
	s.watchPipeClose(p, endpoint, func() { _ = p.Close() })
	return nil
}

// acceptDirectPipeLoop runs alongside acceptLoop for inproc listeners
// that support direct-pipe bypass. It accepts DirectPipeOffers from
// connecting sockets and wires them directly into the engine without
// going through the session layer.
func (s *Socket) acceptDirectPipeLoop(acceptor iface.DirectPipeAcceptor) {
	defer s.acceptWG.Done()
	localType := socketTypeName(s.socketType)
	for {
		offer, err := acceptor.AcceptDirectPipe(s.runCtx)
		if err != nil {
			return
		}

		// A security-configured binder must not accept the unauthenticated
		// bypass. Close the offer so the dialer observes a prompt
		// disconnect instead of a silently dead pipe (it cannot see our
		// security config and would otherwise hang).
		if s.hasSecurityMech() {
			_ = offer.BindPipe.Close()
			continue
		}

		// Socket-type compatibility check.
		if !session.SocketTypesCompatible(localType, offer.DialerSocketType) {
			_ = offer.BindPipe.Close()
			continue
		}

		_ = s.trackAttachWatchPipe(offer.BindPipe, offer.Endpoint)
	}
}
