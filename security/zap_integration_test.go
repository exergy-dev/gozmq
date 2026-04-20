package security_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	gozmq "github.com/rkm/gozmq"
	"github.com/rkm/gozmq/internal/zmqerr"
	"github.com/rkm/gozmq/security"
	"github.com/rkm/gozmq/security/curve"
)

type zapPolicyFunc func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error)

func (f zapPolicyFunc) Authenticate(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
	return f(domain, address, mech, credentials)
}

func TestZapHandler_IntegratesWithPlainHandshake(t *testing.T) {
	t.Run("accept", func(t *testing.T) {
		var plainAuthCalls atomic.Int32
		handler := security.NewZapHandler(zapPolicyFunc(func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if domain != "global" {
				t.Fatalf("domain = %q, want global", domain)
			}
			if mech != "PLAIN" {
				t.Fatalf("mechanism = %q, want PLAIN", mech)
			}
			if len(credentials) != 2 || string(credentials[0]) != "alice" || string(credentials[1]) != "hunter2" {
				t.Fatalf("credentials = %q, want alice/hunter2", credentials)
			}
			if address == "" {
				t.Fatal("address unexpectedly empty")
			}
			return "alice", map[string]string{"Role": "admin"}, nil
		}))

		ctxZmq := gozmq.NewContext(gozmq.WithZapHandler(handler))
		defer ctxZmq.Close()

		serverEvents := make(chan gozmq.Event, 16)
		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithPlainAuth(func(string, string) bool {
				plainAuthCalls.Add(1)
				return false
			}),
			gozmq.WithZapDomain("global"),
			gozmq.WithMonitor(serverEvents, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		defer server.Close()
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithPlainClient("alice", "hunter2"))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		defer client.Close()
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		ev := waitHandshakeEvent(t, serverEvents)
		if ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %v reason=%s, want success", ev.Type, ev.Reason)
		}
		if plainAuthCalls.Load() != 0 {
			t.Fatalf("plain auth fallback called %d times", plainAuthCalls.Load())
		}

		callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("secret"))); err != nil {
			t.Fatalf("send: %v", err)
		}
		msg, err := server.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if string(msg.Bytes()) != "secret" {
			t.Fatalf("recv = %q, want secret", msg.Bytes())
		}
	})

	t.Run("reject", func(t *testing.T) {
		handler := security.NewZapHandler(zapPolicyFunc(func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, zmqerr.ErrAuthFailed
		}))

		ctxZmq := gozmq.NewContext(gozmq.WithZapHandler(handler))
		defer ctxZmq.Close()

		serverEvents := make(chan gozmq.Event, 16)
		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithPlainAuth(func(string, string) bool { return true }),
			gozmq.WithZapDomain("global"),
			gozmq.WithMonitor(serverEvents, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		defer server.Close()
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithPlainClient("alice", "wrong"))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		defer client.Close()
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		ev := waitHandshakeEvent(t, serverEvents)
		if ev.Type != gozmq.EventHandshakeFailedAuth {
			t.Fatalf("handshake event = %v reason=%s, want auth failure", ev.Type, ev.Reason)
		}
	})
}

func TestZapHandler_IntegratesWithCurveHandshake(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("client keypair: %v", err)
	}

	t.Run("accept", func(t *testing.T) {
		handler := security.NewZapHandler(zapPolicyFunc(func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if domain != "global" {
				t.Fatalf("domain = %q, want global", domain)
			}
			if mech != "CURVE" {
				t.Fatalf("mechanism = %q, want CURVE", mech)
			}
			if len(credentials) != 1 || string(credentials[0]) != string(clientPub[:]) {
				t.Fatalf("credentials = %x, want client public key", credentials)
			}
			if address == "" {
				t.Fatal("address unexpectedly empty")
			}
			return "client", nil, nil
		}))

		ctxZmq := gozmq.NewContext(gozmq.WithZapHandler(handler))
		defer ctxZmq.Close()

		serverEvents := make(chan gozmq.Event, 16)
		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithCurveServer(serverSec[:]),
			gozmq.WithZapDomain("global"),
			gozmq.WithMonitor(serverEvents, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		defer server.Close()
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		defer client.Close()
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		ev := waitHandshakeEvent(t, serverEvents)
		if ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %v reason=%s, want success", ev.Type, ev.Reason)
		}

		callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("curve-secret"))); err != nil {
			t.Fatalf("send: %v", err)
		}
		msg, err := server.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if string(msg.Bytes()) != "curve-secret" {
			t.Fatalf("recv = %q, want curve-secret", msg.Bytes())
		}
	})

	t.Run("reject", func(t *testing.T) {
		handler := security.NewZapHandler(zapPolicyFunc(func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, zmqerr.ErrAuthFailed
		}))

		ctxZmq := gozmq.NewContext(gozmq.WithZapHandler(handler))
		defer ctxZmq.Close()

		serverEvents := make(chan gozmq.Event, 16)
		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithCurveServer(serverSec[:]),
			gozmq.WithZapDomain("global"),
			gozmq.WithMonitor(serverEvents, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		defer server.Close()
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		defer client.Close()
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		ev := waitHandshakeEvent(t, serverEvents)
		if ev.Type != gozmq.EventHandshakeFailedAuth {
			t.Fatalf("handshake event = %v reason=%s, want auth failure", ev.Type, ev.Reason)
		}
	})
}

func waitHandshakeEvent(t *testing.T, ch <-chan gozmq.Event) gozmq.Event {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case ev := <-ch:
			if ev.Type == gozmq.EventHandshakeSucceeded || ev.Type == gozmq.EventHandshakeFailedAuth || ev.Type == gozmq.EventHandshakeFailedProtocol {
				return ev
			}
		case <-timer.C:
			t.Fatal("timed out waiting for handshake event")
		}
	}
}
