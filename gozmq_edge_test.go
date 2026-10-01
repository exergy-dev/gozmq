package gozmq_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

// TestClosedSocket_AllOpsReturnErrSocketClosed asserts the uniform
// post-Close contract: every public operation on a closed socket fails
// with ErrSocketClosed.
func TestClosedSocket_AllOpsReturnErrSocketClosed(t *testing.T) {
	_, s := newTestSocket(t, gozmq.Sub)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	bg := context.Background()
	ops := []struct {
		name string
		call func() error
	}{
		{"SendMsg", func() error { return s.SendMsg(bg, gozmq.NewMsgFromBytes([]byte("x"))) }},
		{"SendBytes", func() error { return s.SendBytes(bg, []byte("x")) }},
		{"RecvMsg", func() error { _, err := s.RecvMsg(bg); return err }},
		{"RecvBytes", func() error { _, err := s.RecvBytes(bg); return err }},
		{"Bind", func() error { return s.Bind("inproc://closed-ops") }},
		{"Connect", func() error { return s.Connect("inproc://closed-ops") }},
		{"Disconnect", func() error { return s.Disconnect("inproc://closed-ops") }},
		{"Unbind", func() error { return s.Unbind("inproc://closed-ops") }},
		{"Subscribe", func() error { return s.Subscribe([]byte("topic")) }},
		{"Monitor", func() error { return s.Monitor(make(chan gozmq.Event, 1), gozmq.EventAll) }},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			if err := op.call(); !errors.Is(err, gozmq.ErrSocketClosed) {
				t.Fatalf("%s on closed socket = %v, want ErrSocketClosed", op.name, err)
			}
		})
	}
}

// TestZeroLengthMessages round-trips an empty single frame and an
// [empty][empty] multipart over PAIR on both transports — zero-length
// bodies are legal ZMTP frames and must survive framing intact.
func TestZeroLengthMessages(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{"tcp", "tcp://127.0.0.1:0"},
		{"inproc", "inproc://zero-length"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, server, client := newSocketPair(t, gozmq.Pair, tc.endpoint)
			callCtx := testCtx(t)

			// Empty single frame.
			if err := client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{})); err != nil {
				t.Fatalf("send empty: %v", err)
			}
			got, err := server.RecvMsg(callCtx)
			if err != nil {
				t.Fatalf("recv empty: %v", err)
			}
			if got.NumFrames() != 1 || len(got.Bytes()) != 0 {
				t.Fatalf("empty frame: frames=%d len=%d, want 1 frame of 0 bytes", got.NumFrames(), len(got.Bytes()))
			}
			got.Release()

			// [empty][empty] multipart.
			if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte{}, []byte{})); err != nil {
				t.Fatalf("send empty multipart: %v", err)
			}
			got, err = server.RecvMsg(callCtx)
			if err != nil {
				t.Fatalf("recv empty multipart: %v", err)
			}
			if got.NumFrames() != 2 || len(got.Frames()[0]) != 0 || len(got.Frames()[1]) != 0 {
				t.Fatalf("empty multipart: got %d frames %v", got.NumFrames(), got.Frames())
			}
			got.Release()
		})
	}
}

// TestConcurrentClose_UnblocksParkedSendAndRecv parks one goroutine in
// SendMsg (blocked at HWM against a peer that never drains) and another
// in RecvMsg (idle peer), then Closes the socket: both must unblock with
// ErrSocketClosed and nothing may deadlock or leak (goleak in TestMain).
func TestConcurrentClose_UnblocksParkedSendAndRecv(t *testing.T) {
	_, receiver, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"inproc://concurrent-close",
		[]gozmq.Option{gozmq.WithRecvHWM(1)},
		[]gozmq.Option{gozmq.WithSendHWM(1), gozmq.WithLinger(0)},
	)

	// Settle: reverse round-trip proves both ends are attached.
	callCtx := testCtx(t)
	if err := receiver.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("sync"))); err != nil {
		t.Fatalf("settle send: %v", err)
	}
	if m, err := sender.RecvMsg(callCtx); err != nil {
		t.Fatalf("settle recv: %v", err)
	} else {
		m.Release()
	}

	// Sender goroutine: spin SendMsg until it blocks at HWM (receiver
	// never drains) and is finally released by Close.
	sendErr := make(chan error, 1)
	firstSent := make(chan struct{})
	var once sync.Once
	go func() {
		for {
			if err := sender.SendMsg(context.Background(), gozmq.NewMsgFromBytes([]byte("x"))); err != nil {
				sendErr <- err
				return
			}
			once.Do(func() { close(firstSent) })
		}
	}()

	// Recv goroutine: parked forever — the peer never sends again.
	recvErr := make(chan error, 1)
	go func() {
		_, err := sender.RecvMsg(context.Background())
		recvErr <- err
	}()

	// Wait until at least one send succeeded so the loop is live, then
	// close underneath both goroutines.
	select {
	case <-firstSent:
	case <-callCtx.Done():
		t.Fatal("first send never completed")
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for name, ch := range map[string]chan error{"send": sendErr, "recv": recvErr} {
		select {
		case err := <-ch:
			if !errors.Is(err, gozmq.ErrSocketClosed) {
				t.Fatalf("parked %s unblocked with %v, want ErrSocketClosed", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("parked %s did not unblock within 5s of Close", name)
		}
	}
}

// TestRecvMsg_ContextCancellation cancels a parked RecvMsg directly and
// asserts the ctx error propagates out of the engine.
func TestRecvMsg_ContextCancellation(t *testing.T) {
	_, _, client := newSocketPair(t, gozmq.Pair, "inproc://recv-cancel")

	recvCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.RecvMsg(recvCtx)
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RecvMsg after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RecvMsg did not return within 5s of ctx cancel")
	}
}

// TestMaxMessageSize_RecvSideTeardown verifies the receive-side
// enforcement of WithMaxMessageSize: a peer whose accumulated multipart
// message exceeds the receiver's cap has its session torn down
// (ErrMalformedFrame inside the reader), surfacing on the receiver's
// monitor as EventDisconnected — and the message never reaches RecvMsg.
func TestMaxMessageSize_RecvSideTeardown(t *testing.T) {
	const limit = 64
	srvEvents := make(chan gozmq.Event, 64)
	_, receiver, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"tcp://127.0.0.1:0",
		[]gozmq.Option{gozmq.WithMaxMessageSize(limit), gozmq.WithMonitor(srvEvents, gozmq.EventAll)},
		nil, // sender keeps the default (large) cap so its synchronous check passes
	)

	callCtx := testCtx(t)
	// Two frames of limit-10 bytes: each frame is under the receiver's
	// per-frame cap, but the multipart accumulation (2*(limit-10) > limit)
	// trips the message-level check.
	frame := make([]byte, limit-10)
	if err := sender.SendMsg(callCtx, gozmq.NewMsgFromFrames(frame, frame)); err != nil {
		t.Fatalf("send over-limit multipart: %v", err)
	}

	collected := waitForEvents(t, srvEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventDisconnected)
	})
	if !hasType(collected, gozmq.EventDisconnected) {
		t.Fatalf("receiver did not observe EventDisconnected; events=%v", eventTypes(collected))
	}

	// The over-limit message must never surface.
	recvCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if m, err := receiver.RecvMsg(recvCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RecvMsg = (%v, %v), want DeadlineExceeded (message must be dropped)", m, err)
	}
}

// TestPoller_Modify registers PollIn|PollOut, narrows the mask to PollIn
// via Modify, and asserts the poller stops reporting writability while
// still waking for inbound data.
func TestPoller_Modify(t *testing.T) {
	_, server, client := newSocketPair(t, gozmq.Pair, "inproc://poller-modify")

	p := gozmq.NewPoller()
	p.Add(client, gozmq.PollIn|gozmq.PollOut)

	// Peer attached, nothing inbound: exactly PollOut.
	events := waitPoll(t, p, 5*time.Second)
	if len(events) != 1 || events[0].Socket != client || events[0].Events != gozmq.PollOut {
		t.Fatalf("initial poll events = %+v, want client PollOut only", events)
	}

	// Narrow to PollIn: writability must no longer wake Poll.
	p.Modify(client, gozmq.PollIn)
	shortCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if ev, err := p.Poll(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("post-Modify Poll = (%+v, %v), want DeadlineExceeded", ev, err)
	}

	// Inbound data still wakes the narrowed registration — with only the
	// PollIn bit reported even though the socket remains writable.
	callCtx := testCtx(t)
	if err := server.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("wake"))); err != nil {
		t.Fatalf("send: %v", err)
	}
	events = waitPoll(t, p, 5*time.Second)
	if len(events) != 1 || events[0].Socket != client || events[0].Events != gozmq.PollIn {
		t.Fatalf("post-send poll events = %+v, want client PollIn only", events)
	}
	if m, err := client.RecvMsg(callCtx); err != nil {
		t.Fatalf("drain: %v", err)
	} else {
		m.Release()
	}
}

// TestSocketUnbind stops the listener matched by its resolved endpoint:
// established connections keep working, new clients can no longer be
// accepted, and an unknown endpoint wraps ErrEndpointUnknown.
func TestSocketUnbind(t *testing.T) {
	ctxZmq, puller, pusher := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)
	ep := puller.LastEndpoint()

	// Establish the pre-existing connection with a round-trip.
	callCtx := testCtx(t)
	if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("before"))); err != nil {
		t.Fatalf("send before unbind: %v", err)
	}
	if m, err := puller.RecvMsg(callCtx); err != nil {
		t.Fatalf("recv before unbind: %v", err)
	} else {
		m.Release()
	}

	// Unknown endpoint → ErrEndpointUnknown.
	if err := puller.Unbind("tcp://127.0.0.1:1"); !errors.Is(err, gozmq.ErrEndpointUnknown) {
		t.Fatalf("Unbind(unknown) = %v, want ErrEndpointUnknown", err)
	}

	// Unbind the resolved endpoint (LastEndpoint form).
	if err := puller.Unbind(ep); err != nil {
		t.Fatalf("Unbind(%q): %v", ep, err)
	}

	// The established connection survives.
	if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("after"))); err != nil {
		t.Fatalf("send after unbind: %v", err)
	}
	if m, err := puller.RecvMsg(callCtx); err != nil {
		t.Fatalf("recv after unbind: %v", err)
	} else {
		if string(m.Bytes()) != "after" {
			t.Fatalf("recv after unbind = %q, want %q", m.Bytes(), "after")
		}
		m.Release()
	}

	// A new client can no longer be accepted: its dial is refused and the
	// reconnector reports connect trouble instead of a handshake success.
	lateEvents := make(chan gozmq.Event, 64)
	late, err := ctxZmq.NewSocket(gozmq.Push,
		gozmq.WithMonitor(lateEvents, gozmq.EventAll),
		gozmq.WithReconnectInterval(5*time.Millisecond),
		gozmq.WithReconnectIntervalMax(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new late pusher: %v", err)
	}
	defer late.Close()
	if err := late.Connect(ep); err != nil {
		t.Fatalf("late connect: %v", err)
	}
	collected := waitForEvents(t, lateEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventConnectDelayed) || hasType(evts, gozmq.EventConnectRetried)
	})
	if !hasType(collected, gozmq.EventConnectDelayed) && !hasType(collected, gozmq.EventConnectRetried) {
		t.Fatalf("late client saw no connect-failure events; events=%v", eventTypes(collected))
	}
	if hasType(collected, gozmq.EventHandshakeSucceeded) {
		t.Fatalf("late client handshook against an unbound listener; events=%v", eventTypes(collected))
	}
}

// TestRecvBytes covers the misuse-proof single-frame receive: a
// round-trip copy on the happy path, and an error on multipart.
func TestRecvBytes(t *testing.T) {
	_, server, client := newSocketPair(t, gozmq.Pair, "inproc://recv-bytes")
	callCtx := testCtx(t)

	if err := client.SendBytes(callCtx, []byte("payload")); err != nil {
		t.Fatalf("SendBytes: %v", err)
	}
	got, err := server.RecvBytes(callCtx)
	if err != nil {
		t.Fatalf("RecvBytes: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("RecvBytes = %q, want payload", got)
	}

	// Multipart → error (use RecvMsg for those).
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte("a"), []byte("b"))); err != nil {
		t.Fatalf("send multipart: %v", err)
	}
	if got, err := server.RecvBytes(callCtx); err == nil {
		t.Fatalf("RecvBytes(multipart) = %q, want error", got)
	} else if !strings.Contains(err.Error(), "multipart") {
		t.Fatalf("RecvBytes(multipart) err = %v, want mention of multipart", err)
	}
}

// TestPlainAuth_WrongPassword: a PLAIN client presenting bad credentials
// must surface EventHandshakeFailedAuth on its monitor channel
// (exercising classifyHandshakeEvent's ErrAuthFailed branch).
func TestPlainAuth_WrongPassword(t *testing.T) {
	cliEvents := make(chan gozmq.Event, 64)
	newSocketPairTypes(t, gozmq.Pair, gozmq.Pair, "tcp://127.0.0.1:0",
		[]gozmq.Option{gozmq.WithPlainAuth(func(u, p string) bool {
			return u == "alice" && p == "hunter2"
		})},
		[]gozmq.Option{
			gozmq.WithPlainClient("alice", "wrong-password"),
			gozmq.WithMonitor(cliEvents, gozmq.EventAll),
			gozmq.WithReconnectInterval(10 * time.Millisecond),
			gozmq.WithReconnectIntervalMax(50 * time.Millisecond),
		},
	)

	collected := waitForEvents(t, cliEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventHandshakeFailedAuth)
	})
	if !hasType(collected, gozmq.EventHandshakeFailedAuth) {
		t.Fatalf("no EventHandshakeFailedAuth; events=%v", eventTypes(collected))
	}
	if hasType(collected, gozmq.EventHandshakeSucceeded) {
		t.Fatalf("handshake succeeded with wrong password; events=%v", eventTypes(collected))
	}
}

// TestConnect_UnknownTransportEmitsConnectFailed: Connect with an
// unregistered scheme returns ErrTransportUnknown synchronously AND
// emits EventConnectFailed (terminal — no reconnect loop to retry it).
func TestConnect_UnknownTransportEmitsConnectFailed(t *testing.T) {
	evts := make(chan gozmq.Event, 8)
	_, s := newTestSocket(t, gozmq.Pair, gozmq.WithMonitor(evts, gozmq.EventAll))

	if err := s.Connect("bogus+scheme://x"); !errors.Is(err, gozmq.ErrTransportUnknown) {
		t.Fatalf("Connect(bogus scheme) = %v, want ErrTransportUnknown", err)
	}
	// emit() runs synchronously before Connect returns, onto a buffered
	// channel — the event is already there.
	select {
	case ev := <-evts:
		if ev.Type != gozmq.EventConnectFailed {
			t.Fatalf("event type = %v, want EventConnectFailed", ev.Type)
		}
		if ev.Reason == "" {
			t.Fatal("EventConnectFailed.Reason empty, want error text")
		}
	default:
		t.Fatal("no event emitted for failed Connect")
	}
}

// TestSocketType_String covers the Stringer including the unknown-value
// fallback.
func TestSocketType_String(t *testing.T) {
	if got := gozmq.Pair.String(); got != "PAIR" {
		t.Fatalf("Pair.String() = %q, want PAIR", got)
	}
	if got := gozmq.SocketType(99).String(); got != "SocketType(99)" {
		t.Fatalf("SocketType(99).String() = %q, want SocketType(99)", got)
	}
}

// TestNewMsg covers the NewMsg constructor end to end: an empty message,
// incremental frame appends, a round trip, and the always-safe Release
// contract for caller-constructed messages.
func TestNewMsg(t *testing.T) {
	m := gozmq.NewMsg()
	if m.NumFrames() != 0 {
		t.Fatalf("NewMsg NumFrames = %d, want 0", m.NumFrames())
	}
	m.AppendFrame([]byte("alpha"))
	m.AppendFrame([]byte("beta"))
	if m.NumFrames() != 2 {
		t.Fatalf("NumFrames = %d, want 2", m.NumFrames())
	}

	_, server, client := newSocketPair(t, gozmq.Pair, "inproc://newmsg-roundtrip")
	callCtx := testCtx(t)
	if err := client.SendMsg(callCtx, m); err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	defer got.Release()
	if got.NumFrames() != 2 || string(got.Frames()[0]) != "alpha" || string(got.Frames()[1]) != "beta" {
		t.Fatalf("got frames %q", got.Frames())
	}

	// Release on a caller-constructed message is documented as always safe.
	empty := gozmq.NewMsg()
	empty.Release()
}

// TestWithLogger proves the configured logger actually receives the
// socket's internal diagnostics: a refused dial must produce the
// reconnector's debug log line on the custom logger.
func TestWithLogger(t *testing.T) {
	var (
		mu  sync.Mutex
		buf bytes.Buffer
	)
	syncW := writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	})
	logger := slog.New(slog.NewTextHandler(syncW, &slog.HandlerOptions{Level: slog.LevelDebug}))

	zctx, sock := newTestSocket(t, gozmq.Pair,
		gozmq.WithLogger(logger),
		gozmq.WithReconnectInterval(10*time.Millisecond))
	defer zctx.Close()
	defer sock.Close()

	// Port 1 on loopback is essentially guaranteed to refuse.
	if err := sock.Connect("tcp://127.0.0.1:1"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		logged := strings.Contains(buf.String(), "dial failed")
		mu.Unlock()
		if logged {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("custom logger never received the dial-failure diagnostic; got: %q", buf.String())
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
