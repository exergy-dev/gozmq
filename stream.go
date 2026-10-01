package gozmq

import (
	"github.com/exergy-dev/gozmq/internal/stream"
	"github.com/exergy-dev/gozmq/transport"
)

// startStreamAdapter is the STREAM-specific counterpart to startSession:
// wraps conn in a raw-bytes Adapter, attaches its engine-facing pipe to
// the streamEngine, and spawns a watcher that DetachPipes + Closes the
// adapter when the peer disconnects. No ZMTP handshake, no security
// mechanism — STREAM peers are by definition non-ZMQ.
func (s *Socket) startStreamAdapter(conn transport.Conn) {
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
func (s *Socket) connectStream(tr transport.Transport, endpoint string) error {
	conn, err := tr.Dial(s.runCtx, endpoint)
	if err != nil {
		s.emit(EventConnectFailed, endpoint, err.Error())
		return err
	}
	s.emit(EventConnected, endpoint, "")
	s.startStreamAdapter(conn)
	return nil
}
