package gozmq

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewSocketRejectsInvalidOptionCombinations(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	defer ctx.Close()

	cases := []struct {
		name string
		typ  SocketType
		opt  Option
	}{
		{name: "subscribe-on-pair", typ: Pair, opt: WithSubscribe([]byte("topic"))},
		{name: "router-mandatory-on-dealer", typ: Dealer, opt: WithRouterMandatory(true)},
		{name: "xpub-verbose-on-sub", typ: Sub, opt: WithXPubVerbose(true)},
		{name: "identity-leading-zero", typ: Dealer, opt: WithIdentity([]byte{0, 1})},
		{name: "curve-server-short-key", typ: Router, opt: WithCurveServer([]byte("short"))},
		{name: "max-message-size-negative", typ: Pair, opt: WithMaxMessageSize(-1)},
		{name: "send-hwm-negative", typ: Pair, opt: WithSendHWM(-1)},
		{name: "send-hwm-maxint", typ: Pair, opt: WithSendHWM(math.MaxInt)},
		{name: "recv-hwm-over-max", typ: Pair, opt: WithRecvHWM(MaxHWM + 1)},

		// Incomplete mechanism options must fail construction rather than
		// silently run NULL.
		{name: "plain-auth-nil", typ: Rep, opt: WithPlainAuth(nil)},
		{name: "plain-client-long-user", typ: Req, opt: WithPlainClient(string(make([]byte, 256)), "pw")},
		{name: "plain-client-long-pass", typ: Req, opt: WithPlainClient("user", string(make([]byte, 256)))},
		{name: "curve-server-nil-key", typ: Router, opt: WithCurveServer(nil)},
		{name: "curve-client-nil-keys", typ: Dealer, opt: WithCurveClient(nil, nil, nil)},
		{name: "curve-client-short-key", typ: Dealer, opt: WithCurveClient(make([]byte, 32), make([]byte, 32), make([]byte, 31))},

		// STREAM bypasses ZMTP entirely, so every ZMTP-layer option must
		// fail construction rather than be silently ignored.
		{name: "plain-auth-on-stream", typ: Stream, opt: WithPlainAuth(func(u, p string) bool { return true })},
		{name: "plain-client-on-stream", typ: Stream, opt: WithPlainClient("user", "pass")},
		{name: "curve-server-on-stream", typ: Stream, opt: WithCurveServer(make([]byte, 32))},
		{name: "curve-client-on-stream", typ: Stream, opt: WithCurveClient(make([]byte, 32), make([]byte, 32), make([]byte, 32))},
		{name: "identity-on-stream", typ: Stream, opt: WithIdentity([]byte("id"))},
		{name: "heartbeat-on-stream", typ: Stream, opt: WithHeartbeat(time.Second, 3*time.Second)},
		{name: "handshake-timeout-on-stream", typ: Stream, opt: WithHandshakeTimeout(time.Second)},
		{name: "zap-domain-on-stream", typ: Stream, opt: WithZAPDomain("global")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ctx.NewSocket(tc.typ, tc.opt)
			require.Error(t, err, "NewSocket(%v, %T) unexpectedly succeeded", tc.typ, tc.opt)
		})
	}
}

func TestNewSocketAcceptsLargeMaxMessageSize(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	defer ctx.Close()

	s, err := ctx.NewSocket(Pair, WithMaxMessageSize(1<<40))
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestContextCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	require.NoError(t, ctx.Close())
	require.NoError(t, ctx.Close())
	_, err := ctx.NewSocket(Pair)
	require.ErrorIs(t, err, ErrContextClosed)
}

func TestNewSocketAcceptsMaxHWM(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	defer ctx.Close()
	s, err := ctx.NewSocket(Pair, WithSendHWM(MaxHWM), WithRecvHWM(MaxHWM))
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

// TestSendMsgNilRejectedOnEveryType: a nil message is refused up front with a
// matchable error. REQ and REP used to dereference it, REQ after the empty
// request had already been sent.
func TestSendMsgNilRejectedOnEveryType(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	defer ctx.Close()
	for _, typ := range []SocketType{Pair, Push, Pull, Dealer, Router, Req, Rep, Pub, Sub, XPub, XSub, Stream} {
		s, err := ctx.NewSocket(typ)
		require.NoError(t, err, typ)
		err = s.SendMsg(context.Background(), nil)
		require.ErrorIs(t, err, ErrFSMViolation, "%v: nil message", typ)
		require.NoError(t, s.Close())
	}
}

// TestSendMsgMaxMessageSizeCountsWholeMessage: the send-side check sums the
// frames, matching the receive side, so a gozmq sender never emits a
// multipart message a gozmq receiver tears the session down for.
func TestSendMsgMaxMessageSizeCountsWholeMessage(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	defer ctx.Close()
	s, err := ctx.NewSocket(Pair, WithMaxMessageSize(1000))
	require.NoError(t, err)
	defer s.Close()
	err = s.SendMsg(context.Background(), NewMsgFromFrames(make([]byte, 600), make([]byte, 600)))
	require.ErrorIs(t, err, ErrMalformedFrame)
}
