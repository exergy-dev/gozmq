//go:build interop

package interop

import (
	"context"
	"testing"
	"time"

	"github.com/rkm/gozmq"
	"github.com/rkm/gozmq/security/curve"
)

// handshakeSettle is the window we sleep after Connect before the first Send
// to let the 5-step CURVE handshake drive to completion. libzmq's reactor on
// loopback is fast but not instantaneous; a short settle keeps the tests
// deterministic across CI load without pretending the slow-joiner pattern
// has been solved.
const handshakeSettle = 200 * time.Millisecond

// waitHandshakeEvent blocks until an EventHandshakeSucceeded arrives on ch
// (success path) or one of the handshake-failure events does (failure
// path). Returns the first such event. If ctx fires first, returns nil.
func waitHandshakeEvent(ctx context.Context, ch <-chan gozmq.Event) *gozmq.Event {
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			switch ev.Type {
			case gozmq.EventHandshakeSucceeded,
				gozmq.EventHandshakeFailedAuth,
				gozmq.EventHandshakeFailedProtocol:
				ev := ev
				return &ev
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// TestCurve_GozmqClient_LibzmqServer stands up a libzmq PAIR in CURVE
// server mode and a gozmq PAIR client with the matching long-term keys.
// Validates that gozmq's CURVE client HELLO/INITIATE is parsed by libzmq,
// that gozmq sees EventHandshakeSucceeded, and that MESSAGE frames
// round-trip in both directions across the boundary.
func TestCurve_GozmqClient_LibzmqServer(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("client keypair: %v", err)
	}

	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	// Order matters on libzmq: CURVE options must be set before Bind.
	if err := server.SetOption(ZMQ_CURVE_SERVER, 1); err != nil {
		t.Fatalf("libzmq set CURVE_SERVER: %v", err)
	}
	if err := server.SetOption(ZMQ_CURVE_SECRETKEY, serverSec[:]); err != nil {
		t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
	}
	if err := server.SetOption(ZMQ_CURVE_PUBLICKEY, serverPub[:]); err != nil {
		t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
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
		gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]),
		gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	// Wait up to 3s for the handshake-result event. A successful 5-step
	// CURVE handshake across loopback is typically well under 50ms; the
	// generous deadline is there only to soak up CI scheduler jitter.
	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil {
		t.Fatal("no handshake event within timeout")
	}
	if ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake failed: type=%v reason=%s", ev.Type, ev.Reason)
	}

	time.Sleep(handshakeSettle)

	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()

	want := [][]byte{[]byte("curve-client-msg"), []byte(""), []byte("frame-three")}
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames(want...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, want)

	reply := [][]byte{[]byte("curve-server-reply")}
	if err := server.Send(reply); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := client.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), reply)
}

// TestCurve_LibzmqClient_GozmqServer is the mirror of the gozmq→libzmq
// round-trip: gozmq binds as CURVE server, libzmq connects as CURVE
// client. Single-frame each direction.
func TestCurve_LibzmqClient_GozmqServer(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("client keypair: %v", err)
	}

	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithCurveServer(serverSec[:]),
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
	if err := client.SetOption(ZMQ_CURVE_SERVERKEY, serverPub[:]); err != nil {
		t.Fatalf("libzmq set CURVE_SERVERKEY: %v", err)
	}
	if err := client.SetOption(ZMQ_CURVE_PUBLICKEY, clientPub[:]); err != nil {
		t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
	}
	if err := client.SetOption(ZMQ_CURVE_SECRETKEY, clientSec[:]); err != nil {
		t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
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

	time.Sleep(handshakeSettle)

	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()

	want := [][]byte{[]byte("from-libzmq-single")}
	if err := client.Send(want); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), want)

	reply := [][]byte{[]byte("from-gozmq")}
	if err := server.SendMsg(callCtx, gozmq.NewMsgFromFrames(reply...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := client.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, reply)
}

// TestCurve_LibzmqClient_GozmqServer_Multipart exercises multipart from
// libzmq to gozmq over CURVE. RFC 26 carries multipart state in the
// encrypted MESSAGE inner flags byte; this test proves gozmq restores that
// correctly when reading a libzmq peer.
func TestCurve_LibzmqClient_GozmqServer_Multipart(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("client keypair: %v", err)
	}

	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithCurveServer(serverSec[:]),
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
	if err := client.SetOption(ZMQ_CURVE_SERVERKEY, serverPub[:]); err != nil {
		t.Fatalf("libzmq set CURVE_SERVERKEY: %v", err)
	}
	if err := client.SetOption(ZMQ_CURVE_PUBLICKEY, clientPub[:]); err != nil {
		t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
	}
	if err := client.SetOption(ZMQ_CURVE_SECRETKEY, clientSec[:]); err != nil {
		t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
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

	time.Sleep(handshakeSettle)

	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()

	want := [][]byte{[]byte("from-libzmq-a"), []byte(""), []byte("from-libzmq-c")}
	if err := client.Send(want); err != nil {
		t.Fatalf("libzmq send multipart: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv multipart: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), want)
}

// TestCurve_WrongServerKey_Rejected has the gozmq client pinning a server
// public key that does NOT correspond to the libzmq server's private key.
// The WELCOME box is encrypted to a key the client cannot open, so the
// client's parseWelcome returns ErrAuthFailed; libzmq on its side cannot
// open INITIATE (wrong transient) and tears the TCP connection down, at
// which point gozmq's Reconnector loops. The test confirms that a bounded
// SendMsg times out rather than ever succeeding.
//
// Unlike the round-trip tests above, this exercise does not reach the
// MESSAGE envelope — the handshake itself is expected to fail — so it is
// unaffected by the nonce-reset bug and runs to completion.
func TestCurve_WrongServerKey_Rejected(t *testing.T) {
	serverPubActual, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	// A second, unrelated keypair — we hand its PUBLIC half to the client
	// as the "expected" server key. The secret never leaves the test.
	wrongPub, _, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("wrong keypair: %v", err)
	}
	if wrongPub == serverPubActual {
		t.Fatal("generated wrong-key collision — re-run")
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("client keypair: %v", err)
	}

	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.SetOption(ZMQ_CURVE_SERVER, 1); err != nil {
		t.Fatalf("libzmq set CURVE_SERVER: %v", err)
	}
	if err := server.SetOption(ZMQ_CURVE_SECRETKEY, serverSec[:]); err != nil {
		t.Fatalf("libzmq set CURVE_SECRETKEY: %v", err)
	}
	if err := server.SetOption(ZMQ_CURVE_PUBLICKEY, serverPubActual[:]); err != nil {
		t.Fatalf("libzmq set CURVE_PUBLICKEY: %v", err)
	}
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Pair,
		// Deliberately wrong: wrongPub is not the server's public key.
		gozmq.WithCurveClient(wrongPub[:], clientPub[:], clientSec[:]))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	// Bounded window — the handshake must NOT complete. 500ms is well past
	// the loopback handshake RTT (single-digit ms) yet tight enough that a
	// genuine regression surfaces quickly.
	callCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("should-never-arrive")))
	if err == nil {
		t.Fatal("SendMsg succeeded with wrong server key; expected timeout/auth failure")
	}
	// We accept any error here — context deadline or an explicit auth/EOF.
	// The guarantee we care about is "did not succeed within 500ms".
}

// TestCurve_MalformedHandshake interposes a TCP splicer between a libzmq
// CURVE server and a gozmq CURVE client, flips one byte inside the 144-
// byte cookie box of the server's WELCOME frame on the wire, and asserts
// the client's handshake fails rather than silently succeeding. The byte
// flip invalidates Poly1305 authentication on the cookie box; parseWelcome
// must surface ErrAuthFailed, which the session layer translates into an
// EventHandshakeFailed* monitor event.
//
// WELCOME body layout per RFC 26 §2.2: 1-byte name length || "WELCOME"
// (7 bytes) || 16-byte nonce || 144-byte cookie-and-S' box. Offset 100
// into the body lands inside the box (which starts at offset 24).
func TestCurve_MalformedHandshake(t *testing.T) {
	serverPub, serverSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	clientPub, clientSec, err := curve.GenerateKeyPair()
	if err != nil {
		t.Fatalf("client keypair: %v", err)
	}

	libCtx := newLibCtx(t)
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
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	upstream, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	proxy := newFlagSniffingProxy(t, upstream)
	const welcomeBodyLen = 1 + 7 + 16 + 144 // 168
	const flipOffset = 100                  // inside the cookie box
	proxy.SetMutator(func(clientToServer bool, frameIdx int, body []byte) []byte {
		// WELCOME is the first server→client frame.
		if clientToServer || frameIdx != 0 {
			return nil
		}
		if len(body) != welcomeBodyLen {
			t.Errorf("first S→C body is %d bytes, expected %d (RFC 26 §2.2 shape mismatch — did the handshake change?)", len(body), welcomeBodyLen)
			return nil
		}
		out := append([]byte(nil), body...)
		out[flipOffset] ^= 0x01
		return out
	})

	events := make(chan gozmq.Event, 16)
	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Pair,
		gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]),
		gozmq.WithMonitor(events, gozmq.EventAll))
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(proxy.Endpoint()); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil {
		t.Fatal("no handshake event within 3s — expected EventHandshakeFailed*")
	}
	if ev.Type == gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake succeeded despite corrupted WELCOME cookie box: %+v", ev)
	}
	// Either FailedAuth (decrypt failure classified as auth) or
	// FailedProtocol (framing-level) satisfies the contract: the client
	// must not accept a WELCOME whose cookie box fails Poly1305.
	t.Logf("handshake failed as expected: type=%v reason=%s", ev.Type, ev.Reason)
}
