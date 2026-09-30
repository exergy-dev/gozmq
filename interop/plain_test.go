//go:build interop

package interop

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

func TestPlain_LibzmqClient_GozmqServer(t *testing.T) {
	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithPlainAuth(func(user, pass string) bool {
			return user == "alice" && pass == "hunter2"
		}),
		gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq bind: %v", err)
	}
	ep := server.LastEndpoint()
	if ep == "" {
		t.Fatal("gozmq LastEndpoint empty after bind")
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
	if err := client.Connect(ep); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil {
		t.Fatal("no handshake event within timeout")
	}
	if ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake failed: type=%v reason=%s", ev.Type, ev.Reason)
	}

	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()

	want := [][]byte{[]byte("plain-from-libzmq")}
	if err := client.Send(want); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), want)

	reply := [][]byte{[]byte("plain-from-gozmq"), []byte("reply")}
	if err := server.SendMsg(callCtx, gozmq.NewMsgFromFrames(reply...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := client.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, reply)
}

func TestPlain_LibzmqClient_GozmqServer_BadPassword(t *testing.T) {
	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithPlainAuth(func(user, pass string) bool {
			return user == "alice" && pass == "hunter2"
		}),
		gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq bind: %v", err)
	}
	ep := server.LastEndpoint()
	if ep == "" {
		t.Fatal("gozmq LastEndpoint empty after bind")
	}

	libCtx := newLibCtx(t)
	client := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.SetOption(ZMQ_PLAIN_USERNAME, "alice"); err != nil {
		t.Fatalf("libzmq set PLAIN_USERNAME: %v", err)
	}
	if err := client.SetOption(ZMQ_PLAIN_PASSWORD, "wrong-password"); err != nil {
		t.Fatalf("libzmq set PLAIN_PASSWORD: %v", err)
	}
	if err := client.Connect(ep); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil {
		t.Fatal("no handshake event within timeout")
	}
	if ev.Type != gozmq.EventHandshakeFailedAuth {
		t.Fatalf("handshake event = %v reason=%s, want auth failure", ev.Type, ev.Reason)
	}
}

func TestPlain_GozmqClient_LibzmqServer(t *testing.T) {
	libCtx := newLibCtx(t)
	zapDone := startLibzmqZAPHandler(t, libCtx, func(_domain, _address, mech string, credentials [][]byte) (string, map[string]string, error) {
		if mech != "PLAIN" || len(credentials) != 2 {
			return "", nil, gozmq.ErrAuthFailed
		}
		if string(credentials[0]) != "alice" || string(credentials[1]) != "hunter2" {
			return "", nil, gozmq.ErrAuthFailed
		}
		return "alice", nil, nil
	})

	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.SetOption(ZMQ_PLAIN_SERVER, 1); err != nil {
		t.Fatalf("libzmq set PLAIN_SERVER: %v", err)
	}
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
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
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil {
		t.Fatal("no handshake event within timeout")
	}
	if ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake failed: type=%v reason=%s", ev.Type, ev.Reason)
	}

	select {
	case err := <-zapDone:
		if err != nil {
			t.Fatalf("libzmq ZAP: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("libzmq ZAP handler did not complete")
	}

	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()

	want := [][]byte{[]byte("plain-from-gozmq"), []byte("frame-2")}
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames(want...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, want)

	reply := [][]byte{[]byte("plain-from-libzmq")}
	if err := server.Send(reply); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := client.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), reply)
}

func TestPlain_GozmqClient_LibzmqServer_BadPassword(t *testing.T) {
	libCtx := newLibCtx(t)
	zapDone := startLibzmqZAPHandler(t, libCtx, func(_domain, _address, mech string, credentials [][]byte) (string, map[string]string, error) {
		if mech != "PLAIN" || len(credentials) != 2 {
			return "", nil, gozmq.ErrAuthFailed
		}
		if string(credentials[0]) != "alice" || string(credentials[1]) != "hunter2" {
			return "", nil, gozmq.ErrAuthFailed
		}
		return "alice", nil, nil
	})

	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.SetOption(ZMQ_PLAIN_SERVER, 1); err != nil {
		t.Fatalf("libzmq set PLAIN_SERVER: %v", err)
	}
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithPlainClient("alice", "wrong-password"),
		gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil {
		t.Fatal("no handshake event within timeout")
	}
	if ev.Type != gozmq.EventHandshakeFailedAuth {
		t.Fatalf("handshake event = %v reason=%s, want auth failure", ev.Type, ev.Reason)
	}

	select {
	case err := <-zapDone:
		if err != nil {
			t.Fatalf("libzmq ZAP: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("libzmq ZAP handler did not complete")
	}

	callCtx, cancelCall := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelCall()
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("should-fail"))); err == nil {
		t.Fatal("SendMsg unexpectedly succeeded after auth failure")
	}
}

func TestPlain_LibzmqClient_GozmqServer_EmptyUsername(t *testing.T) {
	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithPlainAuth(func(user, pass string) bool {
			return user == "" && pass == "hunter2"
		}),
		gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq bind: %v", err)
	}

	libCtx := newLibCtx(t)
	client := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.SetOption(ZMQ_PLAIN_USERNAME, ""); err != nil {
		t.Fatalf("libzmq set PLAIN_USERNAME: %v", err)
	}
	if err := client.SetOption(ZMQ_PLAIN_PASSWORD, "hunter2"); err != nil {
		t.Fatalf("libzmq set PLAIN_PASSWORD: %v", err)
	}
	if err := client.Connect(server.LastEndpoint()); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake event = %+v, want success", ev)
	}

	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	if err := client.Send([][]byte{[]byte("empty-user")}); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), [][]byte{[]byte("empty-user")})
}

func TestPlain_GozmqClient_LibzmqServer_EmptyUsername(t *testing.T) {
	libCtx := newLibCtx(t)
	zapDone := startLibzmqZAPHandler(t, libCtx, func(_domain, _address, mech string, credentials [][]byte) (string, map[string]string, error) {
		if mech != "PLAIN" || len(credentials) != 2 {
			return "", nil, gozmq.ErrAuthFailed
		}
		if string(credentials[0]) != "" || string(credentials[1]) != "hunter2" {
			return "", nil, gozmq.ErrAuthFailed
		}
		return "anonymous", nil, nil
	})

	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.SetOption(ZMQ_PLAIN_SERVER, 1); err != nil {
		t.Fatalf("libzmq set PLAIN_SERVER: %v", err)
	}
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithPlainClient("", "hunter2"),
		gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake event = %+v, want success", ev)
	}
	select {
	case err := <-zapDone:
		if err != nil {
			t.Fatalf("libzmq ZAP: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("libzmq ZAP handler did not complete")
	}

	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("empty-user"))); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, [][]byte{[]byte("empty-user")})
}

func TestPlain_LibzmqClient_GozmqServer_TooLongUsername(t *testing.T) {
	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithPlainAuth(func(string, string) bool { return true }),
		gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq bind: %v", err)
	}

	libCtx := newLibCtx(t)
	client := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	longUser := strings.Repeat("u", 256)
	if err := client.SetOption(ZMQ_PLAIN_USERNAME, longUser); err != nil {
		return
	}
	if err := client.SetOption(ZMQ_PLAIN_PASSWORD, "hunter2"); err != nil {
		t.Fatalf("libzmq set PLAIN_PASSWORD: %v", err)
	}
	if err := client.Connect(server.LastEndpoint()); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil {
		t.Fatal("no handshake event within timeout")
	}
	if ev.Type == gozmq.EventHandshakeSucceeded {
		t.Fatalf("oversized username unexpectedly succeeded: %+v", ev)
	}
}

func TestPlain_ReqRep_Transparent(t *testing.T) {
	t.Run("gozmq-req-libzmq-rep", func(t *testing.T) {
		libCtx := newLibCtx(t)
		zapDone := startLibzmqZAPHandler(t, libCtx, func(_domain, _address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if mech != "PLAIN" || len(credentials) != 2 {
				return "", nil, gozmq.ErrAuthFailed
			}
			if string(credentials[0]) != "alice" || string(credentials[1]) != "hunter2" {
				return "", nil, gozmq.ErrAuthFailed
			}
			return "alice", nil, nil
		})
		rep := libCtx.NewSocket(ZMQ_REP)
		t.Cleanup(func() { _ = rep.Close() })
		if err := rep.SetOption(ZMQ_PLAIN_SERVER, 1); err != nil {
			t.Fatalf("libzmq set PLAIN_SERVER: %v", err)
		}
		if err := rep.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("libzmq rep bind: %v", err)
		}
		ep, err := rep.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}

		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		req, err := gCtx.NewSocket(gozmq.Req,
			gozmq.WithPlainClient("alice", "hunter2"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("gozmq req new: %v", err)
		}
		t.Cleanup(func() { _ = req.Close() })
		if err := req.Connect(ep); err != nil {
			t.Fatalf("gozmq req connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
		select {
		case err := <-zapDone:
			if err != nil {
				t.Fatalf("libzmq ZAP: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("libzmq ZAP handler did not complete")
		}

		callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCall()
		if err := req.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("req-plain"))); err != nil {
			t.Fatalf("gozmq req send: %v", err)
		}
		got, err := rep.Recv()
		if err != nil {
			t.Fatalf("libzmq rep recv: %v", err)
		}
		assertFramesEqual(t, got, [][]byte{[]byte("req-plain")})
		if err := rep.Send([][]byte{[]byte("rep-plain")}); err != nil {
			t.Fatalf("libzmq rep send: %v", err)
		}
		back, err := req.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("gozmq req recv: %v", err)
		}
		assertFramesEqual(t, back.Frames(), [][]byte{[]byte("rep-plain")})
	})

	t.Run("libzmq-req-gozmq-rep", func(t *testing.T) {
		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		rep, err := gCtx.NewSocket(gozmq.Rep,
			gozmq.WithPlainAuth(func(user, pass string) bool {
				return user == "alice" && pass == "hunter2"
			}),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("gozmq rep new: %v", err)
		}
		t.Cleanup(func() { _ = rep.Close() })
		if err := rep.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("gozmq rep bind: %v", err)
		}

		libCtx := newLibCtx(t)
		req := libCtx.NewSocket(ZMQ_REQ)
		t.Cleanup(func() { _ = req.Close() })
		if err := req.SetOption(ZMQ_PLAIN_USERNAME, "alice"); err != nil {
			t.Fatalf("libzmq set PLAIN_USERNAME: %v", err)
		}
		if err := req.SetOption(ZMQ_PLAIN_PASSWORD, "hunter2"); err != nil {
			t.Fatalf("libzmq set PLAIN_PASSWORD: %v", err)
		}
		if err := req.Connect(rep.LastEndpoint()); err != nil {
			t.Fatalf("libzmq req connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}

		callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCall()
		if err := req.Send([][]byte{[]byte("req-plain")}); err != nil {
			t.Fatalf("libzmq req send: %v", err)
		}
		got, err := rep.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("gozmq rep recv: %v", err)
		}
		assertFramesEqual(t, got.Frames(), [][]byte{[]byte("req-plain")})
		if err := rep.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("rep-plain"))); err != nil {
			t.Fatalf("gozmq rep send: %v", err)
		}
		back, err := req.Recv()
		if err != nil {
			t.Fatalf("libzmq req recv: %v", err)
		}
		assertFramesEqual(t, back, [][]byte{[]byte("rep-plain")})
	})
}

func TestPlain_DealerRouter_Transparent(t *testing.T) {
	t.Run("gozmq-dealer-libzmq-router", func(t *testing.T) {
		libCtx := newLibCtx(t)
		zapDone := startLibzmqZAPHandler(t, libCtx, func(_domain, _address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if mech != "PLAIN" || len(credentials) != 2 {
				return "", nil, gozmq.ErrAuthFailed
			}
			if string(credentials[0]) != "alice" || string(credentials[1]) != "hunter2" {
				return "", nil, gozmq.ErrAuthFailed
			}
			return "alice", nil, nil
		})
		router := libCtx.NewSocket(ZMQ_ROUTER)
		t.Cleanup(func() { _ = router.Close() })
		if err := router.SetOption(ZMQ_PLAIN_SERVER, 1); err != nil {
			t.Fatalf("libzmq set PLAIN_SERVER: %v", err)
		}
		if err := router.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("libzmq router bind: %v", err)
		}
		ep, err := router.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}

		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		dealer, err := gCtx.NewSocket(gozmq.Dealer,
			gozmq.WithIdentity([]byte("worker-plain-1")),
			gozmq.WithPlainClient("alice", "hunter2"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("gozmq dealer new: %v", err)
		}
		t.Cleanup(func() { _ = dealer.Close() })
		if err := dealer.Connect(ep); err != nil {
			t.Fatalf("gozmq dealer connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
		select {
		case err := <-zapDone:
			if err != nil {
				t.Fatalf("libzmq ZAP: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("libzmq ZAP handler did not complete")
		}

		callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCall()
		if err := dealer.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("dealer-plain"))); err != nil {
			t.Fatalf("gozmq dealer send: %v", err)
		}
		got, err := router.Recv()
		if err != nil {
			t.Fatalf("libzmq router recv: %v", err)
		}
		if len(got) != 2 || string(got[0]) != "worker-plain-1" || string(got[1]) != "dealer-plain" {
			t.Fatalf("router got %v, want [worker-plain-1 dealer-plain]", got)
		}
		if err := router.Send([][]byte{[]byte("worker-plain-1"), []byte("router-plain")}); err != nil {
			t.Fatalf("libzmq router send: %v", err)
		}
		back, err := dealer.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("gozmq dealer recv: %v", err)
		}
		assertFramesEqual(t, back.Frames(), [][]byte{[]byte("router-plain")})
	})

	t.Run("libzmq-dealer-gozmq-router", func(t *testing.T) {
		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		router, err := gCtx.NewSocket(gozmq.Router,
			gozmq.WithPlainAuth(func(user, pass string) bool {
				return user == "alice" && pass == "hunter2"
			}),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("gozmq router new: %v", err)
		}
		t.Cleanup(func() { _ = router.Close() })
		if err := router.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("gozmq router bind: %v", err)
		}

		libCtx := newLibCtx(t)
		dealer := libCtx.NewSocket(ZMQ_DEALER)
		t.Cleanup(func() { _ = dealer.Close() })
		if err := dealer.SetOption(ZMQ_ROUTING_ID, []byte("worker-plain-2")); err != nil {
			t.Fatalf("libzmq dealer set ROUTING_ID: %v", err)
		}
		if err := dealer.SetOption(ZMQ_PLAIN_USERNAME, "alice"); err != nil {
			t.Fatalf("libzmq set PLAIN_USERNAME: %v", err)
		}
		if err := dealer.SetOption(ZMQ_PLAIN_PASSWORD, "hunter2"); err != nil {
			t.Fatalf("libzmq set PLAIN_PASSWORD: %v", err)
		}
		if err := dealer.Connect(router.LastEndpoint()); err != nil {
			t.Fatalf("libzmq dealer connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}

		callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCall()
		if err := dealer.Send([][]byte{[]byte("dealer-plain")}); err != nil {
			t.Fatalf("libzmq dealer send: %v", err)
		}
		got, err := router.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("gozmq router recv: %v", err)
		}
		if got.NumFrames() != 2 || string(got.Frames()[0]) != "worker-plain-2" || string(got.Frames()[1]) != "dealer-plain" {
			t.Fatalf("router got %v, want [worker-plain-2 dealer-plain]", got.Frames())
		}
		if err := router.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte("worker-plain-2"), []byte("router-plain"))); err != nil {
			t.Fatalf("gozmq router send: %v", err)
		}
		back, err := dealer.Recv()
		if err != nil {
			t.Fatalf("libzmq dealer recv: %v", err)
		}
		assertFramesEqual(t, back, [][]byte{[]byte("router-plain")})
	})
}

func TestPlain_PubSub_Transparent(t *testing.T) {
	t.Run("gozmq-pub-libzmq-sub", func(t *testing.T) {
		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		pub, err := gCtx.NewSocket(gozmq.Pub,
			gozmq.WithPlainAuth(func(user, pass string) bool {
				return user == "alice" && pass == "hunter2"
			}),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("gozmq pub new: %v", err)
		}
		t.Cleanup(func() { _ = pub.Close() })
		if err := pub.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("gozmq pub bind: %v", err)
		}

		libCtx := newLibCtx(t)
		sub := libCtx.NewSocket(ZMQ_SUB)
		t.Cleanup(func() { _ = sub.Close() })
		if err := sub.SetOption(ZMQ_PLAIN_USERNAME, "alice"); err != nil {
			t.Fatalf("libzmq set PLAIN_USERNAME: %v", err)
		}
		if err := sub.SetOption(ZMQ_PLAIN_PASSWORD, "hunter2"); err != nil {
			t.Fatalf("libzmq set PLAIN_PASSWORD: %v", err)
		}
		if err := sub.Subscribe([]byte("alpha")); err != nil {
			t.Fatalf("libzmq sub subscribe: %v", err)
		}
		if err := sub.Connect(pub.LastEndpoint()); err != nil {
			t.Fatalf("libzmq sub connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
		time.Sleep(200 * time.Millisecond)

		callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCall()
		for _, payload := range []string{"alphaOne", "betaTwo", "alphaThree"} {
			if err := pub.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte(payload))); err != nil {
				t.Fatalf("gozmq pub send %s: %v", payload, err)
			}
		}
		for _, want := range []string{"alphaOne", "alphaThree"} {
			got, err := sub.Recv()
			if err != nil {
				t.Fatalf("libzmq sub recv %s: %v", want, err)
			}
			if len(got) != 1 || string(got[0]) != want {
				t.Fatalf("libzmq sub got %v, want %q", got, want)
			}
		}
	})

	t.Run("libzmq-pub-gozmq-sub", func(t *testing.T) {
		libCtx := newLibCtx(t)
		zapDone := startLibzmqZAPHandler(t, libCtx, func(_domain, _address, mech string, credentials [][]byte) (string, map[string]string, error) {
			if mech != "PLAIN" || len(credentials) != 2 {
				return "", nil, gozmq.ErrAuthFailed
			}
			if string(credentials[0]) != "alice" || string(credentials[1]) != "hunter2" {
				return "", nil, gozmq.ErrAuthFailed
			}
			return "alice", nil, nil
		})
		pub := libCtx.NewSocket(ZMQ_PUB)
		t.Cleanup(func() { _ = pub.Close() })
		if err := pub.SetOption(ZMQ_PLAIN_SERVER, 1); err != nil {
			t.Fatalf("libzmq set PLAIN_SERVER: %v", err)
		}
		if err := pub.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("libzmq pub bind: %v", err)
		}
		ep, err := pub.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint: %v", err)
		}

		events := make(chan gozmq.Event, 16)
		gCtx := newGozmqCtx(t)
		sub, err := gCtx.NewSocket(gozmq.Sub,
			gozmq.WithPlainClient("alice", "hunter2"),
			gozmq.WithMonitor(events, gozmq.EventAll))
		if err != nil {
			t.Fatalf("gozmq sub new: %v", err)
		}
		t.Cleanup(func() { _ = sub.Close() })
		if err := sub.Subscribe([]byte("alpha")); err != nil {
			t.Fatalf("gozmq sub subscribe: %v", err)
		}
		if err := sub.Connect(ep); err != nil {
			t.Fatalf("gozmq sub connect: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ev := waitHandshakeEvent(waitCtx, events)
		if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
			t.Fatalf("handshake event = %+v, want success", ev)
		}
		select {
		case err := <-zapDone:
			if err != nil {
				t.Fatalf("libzmq ZAP: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("libzmq ZAP handler did not complete")
		}
		time.Sleep(200 * time.Millisecond)

		callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCall()
		for _, payload := range []string{"alphaOne", "betaTwo", "alphaThree"} {
			if err := pub.Send([][]byte{[]byte(payload)}); err != nil {
				t.Fatalf("libzmq pub send %s: %v", payload, err)
			}
		}
		for _, want := range []string{"alphaOne", "alphaThree"} {
			got, err := sub.RecvMsg(callCtx)
			if err != nil {
				t.Fatalf("gozmq sub recv %s: %v", want, err)
			}
			if string(got.Bytes()) != want {
				t.Fatalf("gozmq sub got %q, want %q", got.Bytes(), want)
			}
		}
	})
}
