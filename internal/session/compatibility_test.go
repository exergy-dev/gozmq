package session

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/iface"
)

func TestValidateSocketTypeCompatibility(t *testing.T) {
	t.Parallel()

	accepted := []struct {
		local string
		peer  string
	}{
		{local: "REQ", peer: "REP"},
		{local: "REQ", peer: "ROUTER"},
		{local: "REP", peer: "DEALER"},
		{local: "DEALER", peer: "ROUTER"},
		{local: "ROUTER", peer: "REQ"},
		{local: "PUB", peer: "XSUB"},
		{local: "SUB", peer: "XPUB"},
		{local: "XPUB", peer: "XSUB"},
		{local: "XSUB", peer: "PUB"},
		{local: "PUSH", peer: "PULL"},
		{local: "PULL", peer: "PUSH"},
		{local: "PAIR", peer: "PAIR"},
	}
	for _, tc := range accepted {
		tc := tc
		t.Run(tc.local+"_to_"+tc.peer, func(t *testing.T) {
			t.Parallel()
			err := validateSocketTypeCompatibility(
				map[string]string{"Socket-Type": tc.local},
				map[string]string{"Socket-Type": tc.peer},
			)
			require.NoError(t, err, "validateSocketTypeCompatibility(%s, %s)", tc.local, tc.peer)
		})
	}

	rejected := []struct {
		name  string
		local map[string]string
		peer  map[string]string
		want  string
	}{
		{
			name:  "req_req",
			local: map[string]string{"Socket-Type": "REQ"},
			peer:  map[string]string{"Socket-Type": "REQ"},
			want:  "incompatible",
		},
		{
			name:  "pub_pull",
			local: map[string]string{"Socket-Type": "PUB"},
			peer:  map[string]string{"Socket-Type": "PULL"},
			want:  "incompatible",
		},
		{
			name:  "unknown_local_socket_type",
			local: map[string]string{"Socket-Type": "MYSTERY"},
			peer:  map[string]string{"Socket-Type": "PAIR"},
			want:  "incompatible",
		},
		{
			name:  "stream_rejected",
			local: map[string]string{"Socket-Type": "STREAM"},
			peer:  map[string]string{"Socket-Type": "PAIR"},
			want:  "STREAM sockets do not negotiate ZMTP sessions",
		},
	}
	for _, tc := range rejected {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateSocketTypeCompatibility(tc.local, tc.peer)
			require.Error(t, err, "validateSocketTypeCompatibility returned nil, want error")
			require.True(t, strings.Contains(err.Error(), tc.want),
				"validateSocketTypeCompatibility error = %v, want substring %q", err, tc.want)
		})
	}
}

func TestSession_HandshakeRejectsIncompatibleSocketTypes(t *testing.T) {
	connA, connB := newFakeConnPair()

	sA := New(Config{
		Conn: connA,
		Mech: newFakeMech("NULL", false, map[string]string{
			"Socket-Type": "REQ",
			"Identity":    "client-A",
		}),
		Codec:   fakeCodec{},
		SendHWM: 1,
		RecvHWM: 1,
	})
	sB := New(Config{
		Conn: connB,
		Mech: newFakeMech("NULL", true, map[string]string{
			"Socket-Type": "REQ",
			"Identity":    "server-B",
		}),
		Codec:   fakeCodec{},
		SendHWM: 1,
		RecvHWM: 1,
	})

	sA.Start()
	sB.Start()
	defer func() {
		_ = sA.Stop()
		_ = sB.Stop()
	}()

	waitHandshakeFailure(t, sA, "A")
	waitHandshakeFailure(t, sB, "B")

	mechA := sA.(*sessionImpl).cfg.Mech.(*fakeMech)
	mechB := sB.(*sessionImpl).cfg.Mech.(*fakeMech)
	require.Equal(t, 1, mechA.setPeerCalls, "A SetPeerMetadata calls")
	require.Equal(t, 1, mechB.setPeerCalls, "B SetPeerMetadata calls")
	require.Equal(t, "REQ", mechA.PeerMetadata()["Socket-Type"], "A peer socket type")
	require.Equal(t, "REQ", mechB.PeerMetadata()["Socket-Type"], "B peer socket type")
	require.Nil(t, sA.Pipe(), "A Pipe() = non-nil after handshake rejection, want nil")
	require.Nil(t, sB.Pipe(), "B Pipe() = non-nil after handshake rejection, want nil")
}

func waitHandshakeFailure(t *testing.T, s iface.Session, label string) {
	t.Helper()
	waitClosed(t, s.Handshaked(), 2*time.Second, label+": handshake did not complete within 2s")
	err := s.HandshakeErr()
	require.Error(t, err, "%s: HandshakeErr = nil, want incompatibility failure", label)
	require.True(t, strings.Contains(err.Error(), "incompatible Socket-Type pairing"),
		"%s: HandshakeErr = %v, want incompatibility message", label, err)
}
