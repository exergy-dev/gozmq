//go:build interop

package interop

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/security/curve"
)

// heartbeatIdle keeps the session idle past heartbeatTimeout so that both
// peers exchange several PING/PONG rounds: a PING/PONG the far side cannot
// parse either tears the session down or surfaces as user data within it.
const heartbeatIdle = 12 * heartbeatInterval

// hbSecurity configures one security mechanism on both roles: the gozmq
// options and the libzmq setup for the opposite end.
type hbSecurity struct {
	gozmqServer []gozmq.Option
	libClient   func(t *testing.T, ctx *Ctx, s *Sock)
	gozmqClient []gozmq.Option
	libServer   func(t *testing.T, ctx *Ctx, s *Sock)
}

func nullHeartbeatSecurity(*testing.T) hbSecurity {
	noop := func(*testing.T, *Ctx, *Sock) {}
	return hbSecurity{libClient: noop, libServer: noop}
}

func plainHeartbeatSecurity(*testing.T) hbSecurity {
	return hbSecurity{
		gozmqServer: []gozmq.Option{gozmq.WithPlainAuth(func(user, pass string) bool {
			return user == "alice" && pass == "hunter2"
		})},
		libClient: func(t *testing.T, _ *Ctx, s *Sock) {
			t.Helper()
			if err := s.SetOption(ZMQ_PLAIN_USERNAME, "alice"); err != nil {
				t.Fatalf("libzmq set PLAIN_USERNAME: %v", err)
			}
			if err := s.SetOption(ZMQ_PLAIN_PASSWORD, "hunter2"); err != nil {
				t.Fatalf("libzmq set PLAIN_PASSWORD: %v", err)
			}
		},
		gozmqClient: []gozmq.Option{gozmq.WithPlainClient("alice", "hunter2")},
		libServer: func(t *testing.T, ctx *Ctx, s *Sock) {
			t.Helper()
			// A libzmq PLAIN server drops the connection without a ZAP
			// handler to vouch for the credentials.
			startLibzmqZAPHandler(t, ctx, func(_, _, mech string, creds [][]byte) (string, map[string]string, error) {
				if mech != "PLAIN" || len(creds) != 2 || string(creds[0]) != "alice" || string(creds[1]) != "hunter2" {
					return "", nil, gozmq.ErrAuthFailed
				}
				return "alice", nil, nil
			})
			if err := s.SetOption(ZMQ_PLAIN_SERVER, 1); err != nil {
				t.Fatalf("libzmq set PLAIN_SERVER: %v", err)
			}
		},
	}
}

func curveHeartbeatSecurity(t *testing.T) hbSecurity {
	t.Helper()
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("client keypair: %v", err)
	}
	return hbSecurity{
		gozmqServer: []gozmq.Option{gozmq.WithCurveServer(serverSec[:])},
		libClient: func(t *testing.T, _ *Ctx, s *Sock) {
			t.Helper()
			if err := s.SetOption(ZMQ_CURVE_SERVERKEY, serverPub[:]); err != nil {
				t.Fatalf("libzmq set CURVE_SERVERKEY: %v", err)
			}
			if err := s.SetOption(ZMQ_CURVE_PUBLICKEY, clientPub[:]); err != nil {
				t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
			}
			if err := s.SetOption(ZMQ_CURVE_SECRETKEY, clientSec[:]); err != nil {
				t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
			}
		},
		gozmqClient: []gozmq.Option{gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:])},
		libServer: func(t *testing.T, _ *Ctx, s *Sock) {
			t.Helper()
			if err := s.SetOption(ZMQ_CURVE_SERVER, 1); err != nil {
				t.Fatalf("libzmq set CURVE_SERVER: %v", err)
			}
			if err := s.SetOption(ZMQ_CURVE_SECRETKEY, serverSec[:]); err != nil {
				t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
			}
			if err := s.SetOption(ZMQ_CURVE_PUBLICKEY, serverPub[:]); err != nil {
				t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
			}
		},
	}
}

// CURVE carries PING/PONG inside encrypted MESSAGE boxes (the inner flags
// byte marks them as commands), so these are the cases that exercise the
// mechanism's command encoding against libzmq; NULL and PLAIN are controls.

func TestHeartbeat_Curve_GozmqServer_LibzmqClient(t *testing.T) {
	runHeartbeatSecurityInterop(t, curveHeartbeatSecurity(t), true)
}

func TestHeartbeat_Curve_GozmqClient_LibzmqServer(t *testing.T) {
	runHeartbeatSecurityInterop(t, curveHeartbeatSecurity(t), false)
}

func TestHeartbeat_Plain_GozmqServer_LibzmqClient(t *testing.T) {
	runHeartbeatSecurityInterop(t, plainHeartbeatSecurity(t), true)
}

func TestHeartbeat_Plain_GozmqClient_LibzmqServer(t *testing.T) {
	runHeartbeatSecurityInterop(t, plainHeartbeatSecurity(t), false)
}

func TestHeartbeat_Null_GozmqServer_LibzmqClient_Bidirectional(t *testing.T) {
	runHeartbeatSecurityInterop(t, nullHeartbeatSecurity(t), true)
}

func TestHeartbeat_Null_GozmqClient_LibzmqServer_Bidirectional(t *testing.T) {
	runHeartbeatSecurityInterop(t, nullHeartbeatSecurity(t), false)
}

// runHeartbeatSecurityInterop connects a gozmq PAIR and a libzmq PAIR with
// heartbeats enabled on both, idles past the heartbeat timeout, then
// checks that the original session survived (a single handshake, no
// disconnect), that data flows both ways, and that no heartbeat command
// leaked into either side's user-message stream.
func runHeartbeatSecurityInterop(t *testing.T, sec hbSecurity, gozmqBinds bool) {
	t.Helper()
	requireLibzmqHeartbeat(t)

	gOpts := sec.gozmqClient
	libSetup := sec.libServer
	if gozmqBinds {
		gOpts = sec.gozmqServer
		libSetup = sec.libClient
	}

	events := make(chan gozmq.Event, 64)
	gCtx := newGozmqCtx(t)
	gSock, err := gCtx.NewSocket(gozmq.Pair, append([]gozmq.Option{
		gozmq.WithHeartbeat(heartbeatInterval, heartbeatTimeout),
		gozmq.WithMonitor(events, gozmq.EventAll),
	}, gOpts...)...)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = gSock.Close() })

	libCtx := newLibCtx(t)
	lSock := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = lSock.Close() })
	configureLibHeartbeat(t, lSock)
	if err := lSock.SetOption(ZMQ_RCVTIMEO, 5000); err != nil {
		t.Fatalf("libzmq set RCVTIMEO: %v", err)
	}
	// Security options must precede Bind/Connect on libzmq.
	libSetup(t, libCtx, lSock)

	if gozmqBinds {
		if err := gSock.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("gozmq bind: %v", err)
		}
		if err := lSock.Connect(gSock.LastEndpoint()); err != nil {
			t.Fatalf("libzmq connect: %v", err)
		}
	} else {
		if err := lSock.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("libzmq bind: %v", err)
		}
		ep, err := lSock.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}
		if err := gSock.Connect(ep); err != nil {
			t.Fatalf("gozmq connect: %v", err)
		}
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake event = %v, want %v", ev, gozmq.EventHandshakeSucceeded)
	}

	time.Sleep(heartbeatIdle)
	assertSessionIntact(t, events)

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()

	// A heartbeat PING mis-encoded as data would be queued ahead of these
	// messages, so the first receive on each side would return it.
	toLib := [][]byte{[]byte("gozmq-after-idle"), []byte("part-2")}
	if err := gSock.SendMsg(callCtx, gozmq.NewMsgFromFrames(toLib...)); err != nil {
		t.Fatalf("gozmq send after idle: %v", err)
	}
	got, err := lSock.Recv()
	if err != nil {
		t.Fatalf("libzmq recv after idle: %v", err)
	}
	assertFramesEqual(t, got, toLib)

	toGozmq := [][]byte{[]byte("libzmq-after-idle"), []byte("part-2")}
	if err := lSock.Send(toGozmq); err != nil {
		t.Fatalf("libzmq send after idle: %v", err)
	}
	msg, err := gSock.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv after idle: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), toGozmq)

	// Let a few more heartbeat rounds run, then require both inbound
	// queues to be empty: nothing but the two messages above was user data.
	time.Sleep(4 * heartbeatInterval)
	if err := lSock.SetOption(ZMQ_RCVTIMEO, 100); err != nil {
		t.Fatalf("libzmq set RCVTIMEO: %v", err)
	}
	if stray, err := lSock.Recv(); err == nil {
		t.Fatalf("libzmq received unexpected user message %q", stray)
	} else if !isLibzmqTimeout(err) {
		t.Fatalf("libzmq recv: %v", err)
	}
	strayCtx, strayCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer strayCancel()
	if stray, err := gSock.RecvMsg(strayCtx); err == nil {
		t.Fatalf("gozmq received unexpected user message %q", stray.Frames())
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertSessionIntact(t, events)
}

// assertSessionIntact drains the buffered monitor events and fails on any
// sign that the session established at the start of the test was replaced:
// a disconnect, or a further handshake after a reconnect.
func assertSessionIntact(t *testing.T, events <-chan gozmq.Event) {
	t.Helper()
	for {
		select {
		case ev := <-events:
			switch ev.Type {
			case gozmq.EventDisconnected, gozmq.EventClosed,
				gozmq.EventHandshakeSucceeded,
				gozmq.EventHandshakeFailedProtocol,
				gozmq.EventHandshakeFailedAuth:
				t.Fatalf("session did not survive heartbeats: event %v (reason %q)", ev.Type, ev.Reason)
			}
		default:
			return
		}
	}
}
