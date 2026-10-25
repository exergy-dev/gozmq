package gozmq

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/security"
)

// TestNewSocketOptionValidation: NewSocket rejects option combinations that
// could not work (an incomplete mechanism must not silently run NULL, a
// socket cannot hold two mechanisms, STREAM has no ZMTP layer) and accepts
// the documented extremes.
func TestNewSocketOptionValidation(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	defer ctx.Close()
	key := make([]byte, 32)
	auth := WithPlainAuth(func(string, string) bool { return true })
	custom := WithSecurityMech(func(security.Role, map[string]string) (security.Mech, error) { return nil, nil })

	for _, tc := range []struct {
		name    string
		typ     SocketType
		opts    []Option
		wantErr string // "" means NewSocket must succeed
	}{
		{"subscribe-on-pair", Pair, []Option{WithSubscribe([]byte("topic"))}, "err"},
		{"router-mandatory-on-dealer", Dealer, []Option{WithRouterMandatory(true)}, "err"},
		{"xpub-verbose-on-sub", Sub, []Option{WithXPubVerbose(true)}, "err"},
		{"identity-leading-zero", Dealer, []Option{WithIdentity([]byte{0, 1})}, "err"},
		{"max-message-size-negative", Pair, []Option{WithMaxMessageSize(-1)}, "err"},
		{"send-hwm-negative", Pair, []Option{WithSendHWM(-1)}, "err"},
		{"send-hwm-maxint", Pair, []Option{WithSendHWM(math.MaxInt)}, "err"},
		{"recv-hwm-over-max", Pair, []Option{WithRecvHWM(MaxHWM + 1)}, "err"},

		{"plain-auth-nil", Rep, []Option{WithPlainAuth(nil)}, "err"},
		{"plain-client-long-user", Req, []Option{WithPlainClient(string(make([]byte, 256)), "pw")}, "err"},
		{"plain-client-long-pass", Req, []Option{WithPlainClient("user", string(make([]byte, 256)))}, "err"},
		{"curve-server-nil-key", Router, []Option{WithCurveServer(nil)}, "err"},
		{"curve-server-short-key", Router, []Option{WithCurveServer([]byte("short"))}, "err"},
		{"curve-client-nil-keys", Dealer, []Option{WithCurveClient(nil, nil, nil)}, "err"},
		{"curve-client-short-key", Dealer, []Option{WithCurveClient(key, key, make([]byte, 31))}, "err"},
		{"security-mech-nil", Rep, []Option{WithSecurityMech(nil)}, "err"},

		{"plain server+client", Pair, []Option{auth, WithPlainClient("u", "p")}, "WithPlainAuth and WithPlainClient are mutually exclusive"},
		{"plain server+curve", Pair, []Option{auth, WithCurveServer(key)}, "mutually exclusive"},
		{"plain client+curve", Pair, []Option{WithPlainClient("u", "p"), WithCurveClient(key, key, key)}, "mutually exclusive"},
		{"custom+plain server", Rep, []Option{custom, auth}, "WithSecurityMech and WithPlain"},
		{"custom+plain client", Req, []Option{custom, WithPlainClient("u", "p")}, "WithSecurityMech and WithPlain"},
		{"custom+curve server", Rep, []Option{custom, WithCurveServer(key)}, "WithSecurityMech and WithCurveServer are mutually exclusive"},

		{"plain-auth-on-stream", Stream, []Option{auth}, "err"},
		{"plain-client-on-stream", Stream, []Option{WithPlainClient("user", "pass")}, "err"},
		{"curve-server-on-stream", Stream, []Option{WithCurveServer(key)}, "err"},
		{"curve-client-on-stream", Stream, []Option{WithCurveClient(key, key, key)}, "err"},
		{"custom-on-stream", Stream, []Option{custom}, "not applicable to STREAM sockets"},
		{"identity-on-stream", Stream, []Option{WithIdentity([]byte("id"))}, "err"},
		{"heartbeat-on-stream", Stream, []Option{WithHeartbeat(time.Second, 3*time.Second)}, "err"},
		{"handshake-timeout-on-stream", Stream, []Option{WithHandshakeTimeout(time.Second)}, "err"},
		{"zap-domain-on-stream", Stream, []Option{WithZAPDomain("global")}, "err"},

		{"max-message-size-huge", Pair, []Option{WithMaxMessageSize(1 << 40)}, ""},
		{"hwm-at-max", Pair, []Option{WithSendHWM(MaxHWM), WithRecvHWM(MaxHWM)}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := ctx.NewSocket(tc.typ, tc.opts...)
			switch tc.wantErr {
			case "":
				require.NoError(t, err)
				require.NoError(t, s.Close())
			case "err":
				require.Error(t, err)
			default:
				require.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

func TestContextCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	require.NoError(t, ctx.Close())
	require.NoError(t, ctx.Close())
	_, err := ctx.NewSocket(Pair)
	require.ErrorIs(t, err, ErrContextClosed)
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
