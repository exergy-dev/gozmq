//go:build interop

package interop

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/security"
	"github.com/exergy-dev/gozmq/security/curve"
)

func TestZap_Null_GozmqServer(t *testing.T) {
	t.Run("accept", func(t *testing.T) {
		events := make(chan gozmq.Event, 16)
		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(security.ZAPHandler(func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if domain != "global" || mech != "NULL" {
				t.Fatalf("unexpected ZAP request domain=%q mech=%q", domain, mech)
			}
			if len(credentials) != 0 {
				t.Fatalf("unexpected NULL credentials: %v", credentials)
			}
			return "anonymous", nil, nil
		})))
		defer ctxZmq.Close()

		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		t.Cleanup(func() { _ = server.Close() })
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		libCtx := newLibCtx(t)
		client := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
	})

	t.Run("reject", func(t *testing.T) {
		events := make(chan gozmq.Event, 16)
		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(security.ZAPHandler(func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, gozmq.ErrAuthFailed
		})))
		defer ctxZmq.Close()

		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		t.Cleanup(func() { _ = server.Close() })
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		libCtx := newLibCtx(t)
		client := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeFailedAuth {
			t.Fatalf("handshake event = %+v, want auth failure", ev)
		}
	})
}

func TestZap_Null_LibzmqServer(t *testing.T) {
	t.Run("accept", func(t *testing.T) {
		libCtx := newLibCtx(t)
		zapDone := startLibzmqZAPHandler(t, libCtx, func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if domain != "global" || mech != "NULL" {
				t.Fatalf("unexpected ZAP request domain=%q mech=%q", domain, mech)
			}
			if len(credentials) != 0 {
				t.Fatalf("unexpected NULL credentials: %v", credentials)
			}
			return "anonymous", nil, nil
		})

		server := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = server.Close() })
		if err := server.SetOption(ZMQ_ZAP_DOMAIN, "global"); err != nil {
			t.Fatalf("libzmq set ZAP_DOMAIN: %v", err)
		}
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}
		ep, err := server.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}

		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		client, err := gCtx.NewSocket(gozmq.Pair, gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Connect(ep); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
		assertLibzmqZapDone(t, zapDone)
	})

	t.Run("reject", func(t *testing.T) {
		libCtx := newLibCtx(t)
		zapDone := startLibzmqZAPHandler(t, libCtx, func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, gozmq.ErrAuthFailed
		})

		server := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = server.Close() })
		if err := server.SetOption(ZMQ_ZAP_DOMAIN, "global"); err != nil {
			t.Fatalf("libzmq set ZAP_DOMAIN: %v", err)
		}
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}
		ep, err := server.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}

		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		client, err := gCtx.NewSocket(gozmq.Pair, gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Connect(ep); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type == gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want failure", ev)
		}
		assertLibzmqZapDone(t, zapDone)
	})
}

func TestZap_Plain_GozmqServer(t *testing.T) {
	t.Run("accept", func(t *testing.T) {
		events := make(chan gozmq.Event, 16)
		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(security.ZAPHandler(func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if domain != "global" || mech != "PLAIN" {
				t.Fatalf("unexpected ZAP request domain=%q mech=%q", domain, mech)
			}
			if len(credentials) != 2 || string(credentials[0]) != "alice" || string(credentials[1]) != "hunter2" {
				t.Fatalf("unexpected credentials: %q", credentials)
			}
			return "alice", nil, nil
		})))
		defer ctxZmq.Close()

		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithPlainAuth(func(string, string) bool { return false }),
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		t.Cleanup(func() { _ = server.Close() })
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		libCtx := newLibCtx(t)
		client := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = client.Close() })
		if err := client.SetOption(ZMQ_PLAIN_USERNAME, "alice"); err != nil {
			t.Fatalf("libzmq set PLAIN_USERNAME: %v", err)
		}
		if err := client.SetOption(ZMQ_PLAIN_PASSWORD, "hunter2"); err != nil {
			t.Fatalf("libzmq set PLAIN_PASSWORD: %v", err)
		}
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
	})

	t.Run("reject", func(t *testing.T) {
		events := make(chan gozmq.Event, 16)
		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(security.ZAPHandler(func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, gozmq.ErrAuthFailed
		})))
		defer ctxZmq.Close()

		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithPlainAuth(func(string, string) bool { return true }),
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		t.Cleanup(func() { _ = server.Close() })
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		libCtx := newLibCtx(t)
		client := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = client.Close() })
		if err := client.SetOption(ZMQ_PLAIN_USERNAME, "alice"); err != nil {
			t.Fatalf("libzmq set PLAIN_USERNAME: %v", err)
		}
		if err := client.SetOption(ZMQ_PLAIN_PASSWORD, "hunter2"); err != nil {
			t.Fatalf("libzmq set PLAIN_PASSWORD: %v", err)
		}
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeFailedAuth {
			t.Fatalf("handshake event = %+v, want auth failure", ev)
		}
	})
}

func TestZap_Plain_LibzmqServer(t *testing.T) {
	t.Run("accept", func(t *testing.T) {
		libCtx := newLibCtx(t)
		zapDone := startLibzmqZAPHandler(t, libCtx, func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if domain != "global" || mech != "PLAIN" {
				t.Fatalf("unexpected ZAP request domain=%q mech=%q", domain, mech)
			}
			if len(credentials) != 2 || string(credentials[0]) != "alice" || string(credentials[1]) != "hunter2" {
				t.Fatalf("unexpected credentials: %q", credentials)
			}
			return "alice", nil, nil
		})

		server := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = server.Close() })
		if err := server.SetOption(ZMQ_PLAIN_SERVER, 1); err != nil {
			t.Fatalf("libzmq set PLAIN_SERVER: %v", err)
		}
		if err := server.SetOption(ZMQ_ZAP_DOMAIN, "global"); err != nil {
			t.Fatalf("libzmq set ZAP_DOMAIN: %v", err)
		}
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}
		ep, err := server.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}

		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		client, err := gCtx.NewSocket(gozmq.Pair,
			gozmq.WithPlainClient("alice", "hunter2"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Connect(ep); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
		assertLibzmqZapDone(t, zapDone)
	})

	t.Run("reject", func(t *testing.T) {
		libCtx := newLibCtx(t)
		zapDone := startLibzmqZAPHandler(t, libCtx, func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, gozmq.ErrAuthFailed
		})

		server := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = server.Close() })
		if err := server.SetOption(ZMQ_PLAIN_SERVER, 1); err != nil {
			t.Fatalf("libzmq set PLAIN_SERVER: %v", err)
		}
		if err := server.SetOption(ZMQ_ZAP_DOMAIN, "global"); err != nil {
			t.Fatalf("libzmq set ZAP_DOMAIN: %v", err)
		}
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}
		ep, err := server.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}

		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		client, err := gCtx.NewSocket(gozmq.Pair,
			gozmq.WithPlainClient("alice", "hunter2"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Connect(ep); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type == gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want failure", ev)
		}
		assertLibzmqZapDone(t, zapDone)
	})
}

func TestZap_Curve_GozmqServer(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("client keypair: %v", err)
	}

	t.Run("accept", func(t *testing.T) {
		events := make(chan gozmq.Event, 16)
		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(security.ZAPHandler(func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if domain != "global" || mech != "CURVE" {
				t.Fatalf("unexpected ZAP request domain=%q mech=%q", domain, mech)
			}
			if len(credentials) != 1 || string(credentials[0]) != string(clientPub[:]) {
				t.Fatalf("unexpected credentials: %x", credentials)
			}
			return "client", nil, nil
		})))
		defer ctxZmq.Close()

		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithCurveServer(serverSec[:]),
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		t.Cleanup(func() { _ = server.Close() })
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		libCtx := newLibCtx(t)
		client := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = client.Close() })
		if err := client.SetOption(ZMQ_CURVE_SERVERKEY, serverPub[:]); err != nil {
			t.Fatalf("libzmq set CURVE_SERVERKEY: %v", err)
		}
		if err := client.SetOption(ZMQ_CURVE_PUBLICKEY, clientPub[:]); err != nil {
			t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
		}
		if err := client.SetOption(ZMQ_CURVE_SECRETKEY, clientSec[:]); err != nil {
			t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
		}
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
	})

	t.Run("reject", func(t *testing.T) {
		events := make(chan gozmq.Event, 16)
		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(security.ZAPHandler(func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, gozmq.ErrAuthFailed
		})))
		defer ctxZmq.Close()

		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithCurveServer(serverSec[:]),
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		t.Cleanup(func() { _ = server.Close() })
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}

		libCtx := newLibCtx(t)
		client := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = client.Close() })
		if err := client.SetOption(ZMQ_CURVE_SERVERKEY, serverPub[:]); err != nil {
			t.Fatalf("libzmq set CURVE_SERVERKEY: %v", err)
		}
		if err := client.SetOption(ZMQ_CURVE_PUBLICKEY, clientPub[:]); err != nil {
			t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
		}
		if err := client.SetOption(ZMQ_CURVE_SECRETKEY, clientSec[:]); err != nil {
			t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
		}
		if err := client.Connect(server.LastEndpoint()); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeFailedAuth {
			t.Fatalf("handshake event = %+v, want auth failure", ev)
		}
	})
}

func TestZap_Curve_LibzmqServer(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("client keypair: %v", err)
	}

	t.Run("accept", func(t *testing.T) {
		libCtx := newLibCtx(t)
		zapDone := startLibzmqZAPHandler(t, libCtx, func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if domain != "global" || mech != "CURVE" {
				t.Fatalf("unexpected ZAP request domain=%q mech=%q", domain, mech)
			}
			if len(credentials) != 1 || string(credentials[0]) != string(clientPub[:]) {
				t.Fatalf("unexpected credentials: %x", credentials)
			}
			return "client", nil, nil
		})

		server := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = server.Close() })
		if err := server.SetOption(ZMQ_CURVE_SERVER, 1); err != nil {
			t.Fatalf("libzmq set CURVE_SERVER: %v", err)
		}
		if err := server.SetOption(ZMQ_CURVE_SECRETKEY, serverSec[:]); err != nil {
			t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
		}
		if err := server.SetOption(ZMQ_CURVE_PUBLICKEY, serverPub[:]); err != nil {
			t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
		}
		if err := server.SetOption(ZMQ_ZAP_DOMAIN, "global"); err != nil {
			t.Fatalf("libzmq set ZAP_DOMAIN: %v", err)
		}
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}
		ep, err := server.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}

		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		client, err := gCtx.NewSocket(gozmq.Pair,
			gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Connect(ep); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
		assertLibzmqZapDone(t, zapDone)
	})

	t.Run("reject", func(t *testing.T) {
		libCtx := newLibCtx(t)
		zapDone := startLibzmqZAPHandler(t, libCtx, func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, gozmq.ErrAuthFailed
		})

		server := libCtx.NewSocket(ZMQ_PAIR)
		t.Cleanup(func() { _ = server.Close() })
		if err := server.SetOption(ZMQ_CURVE_SERVER, 1); err != nil {
			t.Fatalf("libzmq set CURVE_SERVER: %v", err)
		}
		if err := server.SetOption(ZMQ_CURVE_SECRETKEY, serverSec[:]); err != nil {
			t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
		}
		if err := server.SetOption(ZMQ_CURVE_PUBLICKEY, serverPub[:]); err != nil {
			t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
		}
		if err := server.SetOption(ZMQ_ZAP_DOMAIN, "global"); err != nil {
			t.Fatalf("libzmq set ZAP_DOMAIN: %v", err)
		}
		if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("bind: %v", err)
		}
		ep, err := server.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}

		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		client, err := gCtx.NewSocket(gozmq.Pair,
			gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Connect(ep); err != nil {
			t.Fatalf("connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type == gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want failure", ev)
		}
		assertLibzmqZapDone(t, zapDone)
	})
}

func assertLibzmqZapDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("libzmq ZAP: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("libzmq ZAP handler did not complete")
	}
}
