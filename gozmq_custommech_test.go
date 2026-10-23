package gozmq_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/security"
)

// This file is the executable form of the claim made in the docs for
// security.Mech and WithSecurityMech: a ZMTP security mechanism is
// implementable from outside the module. xorMech below embeds
// security.MechBase and imports nothing internal.
//
// xorMech is a TEACHING EXAMPLE AND TEST DOUBLE, NOT A SECURITY MECHANISM.
// A fixed-key XOR provides no confidentiality, no integrity, and no
// authentication whatsoever. Real deployments use NULL on a trusted link,
// or CURVE. It is used here only because it is short enough to read and it
// exercises every part of the Mech contract: a multi-step handshake, READY
// metadata exchange, and per-frame Encode/Decode rewriting.
//
// If the Mech interface ever regresses to naming an internal/ type, this
// file stops compiling.

const xorMechName = "XORTEST"

var xorKey = []byte{0x5a, 0xa5, 0x3c, 0xc3}

type xorMech struct {
	security.MechBase
	sentReady bool
	gotReady  bool
	// frames counts Encode+Decode calls so the test can prove the
	// mechanism really sat on the traffic path, not just the handshake.
	frames *atomic.Int64
}

func (m *xorMech) Name() string { return xorMechName }

// NextHandshake is a single READY exchange in each direction, mirroring
// NULL. security.EncodeReady / DecodeReady supply the ZMTP command +
// metadata wire format, so the mechanism only has to sequence the steps.
func (m *xorMech) NextHandshake(in []byte) (out []byte, done bool, err error) {
	if !m.sentReady {
		if in != nil {
			return nil, false, errors.New("xormech: unexpected input on seed step")
		}
		m.sentReady = true
		return security.EncodeReady(m.Metadata()), false, nil
	}
	if !m.gotReady {
		if in == nil {
			return nil, false, errors.New("xormech: peer closed before READY")
		}
		props, err := security.DecodeReady(in)
		if err != nil {
			return nil, false, err
		}
		m.MergePeerMetadata(props)
		m.gotReady = true
		return nil, true, nil
	}
	return nil, true, nil
}

func xorInPlace(b []byte) []byte {
	out := make([]byte, len(b))
	for i := range b {
		out[i] = b[i] ^ xorKey[i%len(xorKey)]
	}
	return out
}

func (m *xorMech) EncodeFrame(flags uint8, plain []byte) (uint8, []byte, error) {
	m.frames.Add(1)
	return flags, xorInPlace(plain), nil
}

func (m *xorMech) DecodeFrame(flags uint8, wire []byte) (uint8, []byte, error) {
	m.frames.Add(1)
	return flags, xorInPlace(wire), nil
}

var _ security.Mech = (*xorMech)(nil)

func newXORMech(frames *atomic.Int64) func(security.Role, map[string]string) (security.Mech, error) {
	return func(role security.Role, meta map[string]string) (security.Mech, error) {
		// Passing meta straight through is what keeps Socket-Type and
		// Identity in the READY the session layer sends.
		return &xorMech{MechBase: security.NewMechBase(role, meta), frames: frames}, nil
	}
}

func TestWithSecurityMechOutOfTree(t *testing.T) {
	t.Parallel()
	var frames atomic.Int64
	mech := gozmq.WithSecurityMech(newXORMech(&frames))

	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	rep, err := zctx.NewSocket(gozmq.Rep, mech)
	require.NoError(t, err)
	defer func() { _ = rep.Close() }()
	require.NoError(t, rep.Bind("tcp://127.0.0.1:0"))

	req, err := zctx.NewSocket(gozmq.Req, mech)
	require.NoError(t, err)
	defer func() { _ = req.Close() }()
	require.NoError(t, req.Connect(rep.LastEndpoint()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, req.SendBytes(ctx, []byte("ping")))
	got, err := rep.RecvBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, "ping", string(got))

	require.NoError(t, rep.SendBytes(ctx, []byte("pong")))
	reply, err := req.RecvBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, "pong", string(reply))

	require.Greater(t, frames.Load(), int64(0), "custom mechanism never saw a frame")
}

// forwardingMech implements security.Mech purely by delegation, without
// embedding MechBase, so the session can rely on nothing but the interface —
// PeerMetadata included.
type forwardingMech struct{ inner security.Mech }

func (f forwardingMech) Name() string                    { return f.inner.Name() }
func (f forwardingMech) AsServer() bool                  { return f.inner.AsServer() }
func (f forwardingMech) Metadata() map[string]string     { return f.inner.Metadata() }
func (f forwardingMech) PeerMetadata() map[string]string { return f.inner.PeerMetadata() }
func (f forwardingMech) NextHandshake(in []byte) ([]byte, bool, error) {
	return f.inner.NextHandshake(in)
}
func (f forwardingMech) EncodeFrame(flags uint8, b []byte) (uint8, []byte, error) {
	return f.inner.EncodeFrame(flags, b)
}
func (f forwardingMech) DecodeFrame(flags uint8, b []byte) (uint8, []byte, error) {
	return f.inner.DecodeFrame(flags, b)
}

func TestWithSecurityMechWithoutMechBase(t *testing.T) {
	t.Parallel()
	mech := gozmq.WithSecurityMech(func(role security.Role, meta map[string]string) (security.Mech, error) {
		return forwardingMech{security.NewNull(role, meta)}, nil
	})
	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	rep, err := zctx.NewSocket(gozmq.Rep, mech)
	require.NoError(t, err)
	defer func() { _ = rep.Close() }()
	require.NoError(t, rep.Bind("tcp://127.0.0.1:0"))
	req, err := zctx.NewSocket(gozmq.Req, mech)
	require.NoError(t, err)
	defer func() { _ = req.Close() }()
	require.NoError(t, req.Connect(rep.LastEndpoint()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, req.SendBytes(ctx, []byte("ping")))
	got, err := rep.RecvBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, "ping", string(got))
}

// A constructor that fails must fail the session outright — gozmq must never
// silently fall back to NULL, which would be a downgrade.
func TestWithSecurityMechConstructorErrorDoesNotDowngrade(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("mechanism unavailable")

	zctx := gozmq.NewContext()
	defer func() { _ = zctx.Close() }()

	events := make(chan gozmq.Event, 64)
	rep, err := zctx.NewSocket(gozmq.Rep,
		gozmq.WithSecurityMech(func(security.Role, map[string]string) (security.Mech, error) {
			return nil, sentinel
		}),
		gozmq.WithMonitor(events, gozmq.EventAll),
	)
	require.NoError(t, err)
	defer func() { _ = rep.Close() }()
	require.NoError(t, rep.Bind("tcp://127.0.0.1:0"))

	// A plain NULL client must not get a working session out of this.
	var frames atomic.Int64
	client, err := zctx.NewSocket(gozmq.Req, gozmq.WithSecurityMech(newXORMech(&frames)))
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	require.NoError(t, client.Connect(rep.LastEndpoint()))

	// The server never completes a handshake, so no request is ever read.
	testutil.WaitEvent(t, events, 5*time.Second, func(e gozmq.Event) bool { return e.Type == gozmq.EventHandshakeFailedProtocol })
	ctx := testutil.Ctx(t, 100*time.Millisecond)
	_ = client.SendBytes(ctx, []byte("ping"))
	_, err = rep.RecvBytes(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
