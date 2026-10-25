package session

import (
	"bytes"
	"context"
	"errors"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// rawReadSize caps one conn.Read in raw mode. 32 KiB takes a typical HTTP
// request or response head in one syscall. Each read becomes one message
// as it arrived: raw mode never coalesces reads (the STREAM contract).
const rawReadSize = 32 * 1024

// runRawReader is the reader in raw mode (Config.Raw): no greeting, no
// handshake, no heartbeat. It hands the engine end to OnReady at once,
// then publishes each conn.Read as a single-frame message.
func (s *Session) runRawReader() {
	defer s.exit()
	defer s.shutdown()
	if s.cfg.OnReady != nil {
		if err := s.cfg.OnReady(s.enginePipe); err != nil {
			s.cfg.Logger.Debug("gozmq/session: pipe refused", "err", err)
			return
		}
	}
	s.attached.Store(true)
	buf := make([]byte, rawReadSize)
	for {
		n, err := s.cfg.Conn.Read(buf)
		if n > 0 {
			// Copy just the n bytes read: a queued small chunk must not
			// pin the whole read buffer (1-byte reads times the HWM would
			// hold tens of MiB per peer).
			if s.publishInbound(msg.FromBytes(bytes.Clone(buf[:n]))) != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// runRawWriter is the writer in raw mode: each pipe message is one body
// frame written verbatim. Drain (writerCtx cancelled) writes what is still
// queued and exits leaving the conn open, as in ZMTP mode.
func (s *Session) runRawWriter() {
	defer s.exit()
	defer close(s.writerDone)
	for {
		m, err := s.sessionPipe.RecvOr(s.writerCtx, nil)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.shutdown()
				return
			}
			for {
				m, ok := s.sessionPipe.TryRecv()
				if !ok || !s.writeRaw(m) {
					return
				}
			}
		}
		if !s.writeRaw(m) {
			return
		}
	}
}

// writeRaw writes m's body and reports whether the writer should go on.
// An empty body is the STREAM half-close: every earlier write has already
// returned, so closing the conn now loses nothing.
func (s *Session) writeRaw(m *msg.Msg) bool {
	body, _ := m.SingleFrame()
	var err error
	if len(body) > 0 {
		_, err = s.cfg.Conn.Write(body)
	}
	m.Release()
	if len(body) > 0 && err == nil {
		return true
	}
	if err != nil && !zmqerr.IsShutdown(err) {
		s.cfg.Logger.Debug("gozmq/session: raw write", "err", err)
	}
	s.shutdown()
	return false
}
