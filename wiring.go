package gozmq

import (
	"fmt"

	"github.com/exergy-dev/gozmq/internal/session"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/security"
	"github.com/exergy-dev/gozmq/security/curve"
	"github.com/exergy-dev/gozmq/transport"
)

// sessionConfig builds the session configuration for conn, with the
// mechanism and codec configured for this Socket; startSession adds the
// callbacks. An error means the security mechanism could not be
// constructed (e.g. ephemeral CURVE keygen failure); the connection must
// be torn down rather than proceeding with a weaker mechanism.
func (s *Socket) sessionConfig(conn transport.Conn, role security.Role) (session.Config, error) {
	if s.socketType == Stream {
		// STREAM peers speak no ZMTP: no mechanism, codec or heartbeat.
		return session.Config{
			Conn:    conn,
			Raw:     true,
			SendHWM: s.cfg.sendHWM,
			RecvHWM: s.cfg.recvHWM,
			Logger:  s.cfg.logger,
		}, nil
	}
	mech, err := s.newMechanism(role)
	if err != nil {
		return session.Config{}, err
	}
	// Key ZAP off the mechanism's handshake role, not the transport role: a
	// PLAIN or CURVE server that Connects still authorizes its peer.
	if mech.AsServer() && s.ctx.zapHandler != nil {
		if cfg, ok := mech.(zapConfigurable); ok {
			cfg.ConfigureZAP(s.ctx.zapHandler, s.cfg.zapDomain, conn.RemoteEndpoint())
		}
	}
	// The codec caps wire frames. A mechanism that expands frames (CURVE:
	// 33 bytes per MESSAGE) declares the overhead so that a plaintext frame
	// of exactly MaxMessageSize, which SendMsg accepts, still fits; the
	// session bounds the decoded message separately.
	maxWire := s.cfg.maxMessageSize
	if fo, ok := mech.(frameOverheader); ok && maxWire > 0 {
		maxWire += int64(fo.FrameOverhead())
	}
	codec := zmtp.NewFrameCodec(maxWire)
	return session.Config{
		Conn:              conn,
		Mech:              mech,
		Codec:             codec,
		HeartbeatInterval: s.cfg.heartbeatInterval,
		HeartbeatTimeout:  s.cfg.heartbeatTimeout,
		HandshakeTimeout:  s.cfg.handshakeTimeout,
		SendHWM:           s.cfg.sendHWM,
		RecvHWM:           s.cfg.recvHWM,
		MaxMessageSize:    s.cfg.maxMessageSize,
		RecvBufferPool:    s.cfg.recvBufferPool,
		// Only a publisher consumes its peers' subscriptions.
		SubscriptionCommands: s.socketType == Pub || s.socketType == XPub,
		Logger:               s.cfg.logger,
	}, nil
}

// frameOverheader is implemented by mechanisms whose EncodeFrame grows the
// frame by a fixed number of bytes (CURVE). See sessionConfig.
type frameOverheader interface {
	FrameOverhead() int
}

// zapConfigurable is implemented by mechanisms that take a ZAP handler; the
// built-in ones, and custom ones embedding security.MechBase, get it from
// MechBase.
type zapConfigurable interface {
	ConfigureZAP(handler security.ZAPHandler, domain, address string)
}

// newMechanism picks the security mechanism for a new session based on
// the socket's configured options, which validateSocketConfig has already
// limited to at most one mechanism; with none configured it is NULL.
// A configured mechanism that cannot be constructed is an error — never
// a silent downgrade to a weaker mechanism.
//
// PLAIN and CURVE take their handshake role from the options alone, like
// libzmq's ZMQ_PLAIN_SERVER / ZMQ_CURVE_SERVER: a WithPlainAuth socket is
// the PLAIN server whether it binds or connects. Only NULL and custom
// mechanisms see the transport role.
func (s *Socket) newMechanism(role security.Role) (security.Mech, error) {
	meta := map[string]string{
		"Socket-Type": socketTypeName(s.socketType),
	}
	if len(s.cfg.identity) > 0 {
		meta["Identity"] = string(s.cfg.identity)
	}
	if s.cfg.newSecurityMech != nil {
		m, err := s.cfg.newSecurityMech(role, meta)
		if err != nil {
			return nil, fmt.Errorf("gozmq: WithSecurityMech constructor: %w", err)
		}
		if m == nil {
			return nil, fmt.Errorf("gozmq: WithSecurityMech constructor returned a nil mechanism")
		}
		return m, nil
	}
	switch {
	case len(s.cfg.curveSecretKey) == 32 && s.cfg.curveServer:
		m, err := curve.NewCurveServer(curve.Key(s.cfg.curveSecretKey), meta)
		if err != nil {
			return nil, fmt.Errorf("gozmq: CURVE server mechanism: %w", err)
		}
		return m, nil
	case len(s.cfg.curveSecretKey) == 32 && !s.cfg.curveServer && len(s.cfg.curveServerKey) == 32 && len(s.cfg.curvePublicKey) == 32:
		m, err := curve.NewCurveClient(curve.Key(s.cfg.curveServerKey), curve.Key(s.cfg.curvePublicKey), curve.Key(s.cfg.curveSecretKey), meta)
		if err != nil {
			return nil, fmt.Errorf("gozmq: CURVE client mechanism: %w", err)
		}
		return m, nil
	case s.cfg.plainAuth != nil:
		return security.NewPlainServer(s.cfg.plainAuth, meta), nil
	case s.cfg.plainClient:
		return security.NewPlainClient(s.cfg.plainClientUser, s.cfg.plainClientPass, meta), nil
	}
	if s.cfg.securityConfigured() {
		// validateSocketConfig rejects every incomplete mechanism option, so
		// this is unreachable; it guards against a future option that is
		// configured but not constructible falling back to NULL.
		return nil, fmt.Errorf("gozmq: security mechanism configured but not constructible; refusing to downgrade to NULL")
	}
	return security.NewNull(role, meta), nil
}
