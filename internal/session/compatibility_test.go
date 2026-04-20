package session

import (
	"strings"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/iface"
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
			if err != nil {
				t.Fatalf("validateSocketTypeCompatibility(%s, %s) = %v, want nil", tc.local, tc.peer, err)
			}
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
			if err == nil {
				t.Fatal("validateSocketTypeCompatibility returned nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateSocketTypeCompatibility error = %v, want substring %q", err, tc.want)
			}
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
		Role:    iface.RoleClient,
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
		Role:    iface.RoleServer,
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
	if mechA.setPeerCalls != 1 {
		t.Fatalf("A SetPeerMetadata calls = %d, want 1", mechA.setPeerCalls)
	}
	if mechB.setPeerCalls != 1 {
		t.Fatalf("B SetPeerMetadata calls = %d, want 1", mechB.setPeerCalls)
	}
	if got := mechA.PeerMetadata()["Socket-Type"]; got != "REQ" {
		t.Fatalf("A peer socket type = %q, want REQ", got)
	}
	if got := mechB.PeerMetadata()["Socket-Type"]; got != "REQ" {
		t.Fatalf("B peer socket type = %q, want REQ", got)
	}
	if sA.Pipe() != nil {
		t.Fatal("A Pipe() = non-nil after handshake rejection, want nil")
	}
	if sB.Pipe() != nil {
		t.Fatal("B Pipe() = non-nil after handshake rejection, want nil")
	}
}

func waitHandshakeFailure(t *testing.T, s iface.Session, label string) {
	t.Helper()
	select {
	case <-s.Handshaked():
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: handshake did not complete within 2s", label)
	}
	err := s.HandshakeErr()
	if err == nil {
		t.Fatalf("%s: HandshakeErr = nil, want incompatibility failure", label)
	}
	if !strings.Contains(err.Error(), "incompatible Socket-Type pairing") {
		t.Fatalf("%s: HandshakeErr = %v, want incompatibility message", label, err)
	}
}
