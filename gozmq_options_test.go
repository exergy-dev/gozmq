package gozmq

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSocketRejectsInvalidOptionCombinations(t *testing.T) {
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ctx.NewSocket(tc.typ, tc.opt)
			require.Error(t, err, "NewSocket(%v, %T) unexpectedly succeeded", tc.typ, tc.opt)
		})
	}
}

func TestNewSocketAcceptsLargeMaxMessageSize(t *testing.T) {
	ctx := NewContext()
	defer ctx.Close()

	s, err := ctx.NewSocket(Pair, WithMaxMessageSize(1<<40))
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestContextCloseIsIdempotent(t *testing.T) {
	ctx := NewContext()
	require.NoError(t, ctx.Close())
	require.NoError(t, ctx.Close())
	_, err := ctx.NewSocket(Pair)
	require.ErrorIs(t, err, ErrContextClosed)
}
