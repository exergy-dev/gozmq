package gozmq_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// TestClosedSocket_AllOpsReturnErrSocketClosed asserts the uniform
// post-Close contract: every public operation on a closed socket fails
// with ErrSocketClosed.
func TestClosedSocket_AllOpsReturnErrSocketClosed(t *testing.T) {
	t.Parallel()
	_, s := newTestSocket(t, gozmq.Sub)
	require.NoError(t, s.Close(), "close")

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
			require.ErrorIs(t, op.call(), gozmq.ErrSocketClosed, "%s on closed socket", op.name)
		})
	}
}

// TestZeroLengthMessages round-trips an empty single frame and an
// [empty][empty] multipart over PAIR on both transports — zero-length
// bodies are legal ZMTP frames and must survive framing intact.
func TestZeroLengthMessages(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{"tcp", "tcp://127.0.0.1:0"},
		{"inproc", "inproc://zero-length"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, server, client := newSocketPair(t, gozmq.Pair, tc.endpoint)
			callCtx := testutil.Ctx(t)

			// Empty single frame.
			require.NoError(t, client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte{})), "send empty")
			got, err := server.RecvMsg(callCtx)
			require.NoError(t, err, "recv empty")
			require.Len(t, got.Frames(), 1, "empty frame: want 1 frame of 0 bytes")
			require.Empty(t, got.Bytes(), "empty frame: want 1 frame of 0 bytes")
			got.Release()

			// [empty][empty] multipart.
			require.NoError(t, client.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte{}, []byte{})), "send empty multipart")
			got, err = server.RecvMsg(callCtx)
			require.NoError(t, err, "recv empty multipart")
			require.Len(t, got.Frames(), 2, "empty multipart")
			require.Empty(t, got.Frames()[0], "empty multipart")
			require.Empty(t, got.Frames()[1], "empty multipart")
			got.Release()
		})
	}
}

// TestConcurrentClose_UnblocksParkedSendAndRecv parks one goroutine in
// SendMsg (blocked at HWM against a peer that never drains) and another
// in RecvMsg (idle peer), then Closes the socket: both must unblock with
// ErrSocketClosed and nothing may deadlock or leak (goleak in TestMain).
func TestConcurrentClose_UnblocksParkedSendAndRecv(t *testing.T) {
	t.Parallel()
	_, receiver, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"inproc://concurrent-close",
		[]gozmq.Option{gozmq.WithRecvHWM(1)},
		[]gozmq.Option{gozmq.WithSendHWM(1), gozmq.WithLinger(0)},
	)

	// Settle: reverse round-trip proves both ends are attached.
	callCtx := testutil.Ctx(t)
	require.NoError(t, receiver.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("sync"))), "settle send")
	m, err := sender.RecvMsg(callCtx)
	require.NoError(t, err, "settle recv")
	m.Release()

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
	require.NoError(t, sender.Close(), "close")

	for name, ch := range map[string]chan error{"send": sendErr, "recv": recvErr} {
		select {
		case err := <-ch:
			require.ErrorIs(t, err, gozmq.ErrSocketClosed, "parked %s unblocked", name)
		case <-time.After(5 * time.Second):
			t.Fatalf("parked %s did not unblock within 5s of Close", name)
		}
	}
}

// TestRecvMsg_ContextCancellation cancels a parked RecvMsg directly and
// asserts the ctx error propagates out of the engine.
func TestRecvMsg_ContextCancellation(t *testing.T) {
	t.Parallel()
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
		require.ErrorIs(t, err, context.Canceled, "RecvMsg after cancel")
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
	t.Parallel()
	const limit = 64
	srvEvents := make(chan gozmq.Event, 64)
	_, receiver, sender := newSocketPairTypes(t, gozmq.Pair, gozmq.Pair,
		"tcp://127.0.0.1:0",
		[]gozmq.Option{gozmq.WithMaxMessageSize(limit), gozmq.WithMonitor(srvEvents, gozmq.EventAll)},
		nil, // sender keeps the default (large) cap so its synchronous check passes
	)

	callCtx := testutil.Ctx(t)
	// Two frames of limit-10 bytes: each frame is under the receiver's
	// per-frame cap, but the multipart accumulation (2*(limit-10) > limit)
	// trips the message-level check.
	frame := make([]byte, limit-10)
	require.NoError(t, sender.SendMsg(callCtx, gozmq.NewMsgFromFrames(frame, frame)), "send over-limit multipart")

	collected := waitForEvents(t, srvEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventDisconnected)
	})
	require.True(t, hasType(collected, gozmq.EventDisconnected),
		"receiver did not observe EventDisconnected; events=%v", eventTypes(collected))

	// The over-limit message must never surface.
	recvCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	m, err := receiver.RecvMsg(recvCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "RecvMsg = (%v, %v): message must be dropped", m, err)
}

// TestPoller_AddNarrowsMask registers PollIn|PollOut, narrows the mask to
// PollIn with a second Add, and asserts the poller stops reporting writability while
// still waking for inbound data.
func TestPoller_AddNarrowsMask(t *testing.T) {
	t.Parallel()
	_, server, client := newSocketPair(t, gozmq.Pair, "inproc://poller-modify")

	p := gozmq.NewPoller()
	p.Add(client, gozmq.PollIn|gozmq.PollOut)

	// Peer attached, nothing inbound: exactly PollOut.
	events := waitPoll(t, p, 5*time.Second)
	require.Len(t, events, 1, "initial poll events = %+v, want client PollOut only", events)
	require.Same(t, client, events[0].Socket, "initial poll")
	require.Equal(t, gozmq.PollOut, events[0].Events, "initial poll: want PollOut only")

	// Narrow to PollIn: writability must no longer wake Poll.
	p.Add(client, gozmq.PollIn)
	shortCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ev, err := p.Poll(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "post-narrowing Poll = (%+v, %v)", ev, err)

	// Inbound data still wakes the narrowed registration — with only the
	// PollIn bit reported even though the socket remains writable.
	callCtx := testutil.Ctx(t)
	require.NoError(t, server.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("wake"))), "send")
	events = waitPoll(t, p, 5*time.Second)
	require.Len(t, events, 1, "post-send poll events = %+v, want client PollIn only", events)
	require.Same(t, client, events[0].Socket, "post-send poll")
	require.Equal(t, gozmq.PollIn, events[0].Events, "post-send poll: want PollIn only")
	m, err := client.RecvMsg(callCtx)
	require.NoError(t, err, "drain")
	m.Release()
}

// TestSocketUnbind stops the listener matched by its resolved endpoint:
// established connections keep working, new clients can no longer be
// accepted, and an unknown endpoint wraps ErrEndpointUnknown.
func TestSocketUnbind(t *testing.T) {
	t.Parallel()
	ctxZmq, puller, pusher := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)
	ep := puller.LastEndpoint()

	// Establish the pre-existing connection with a round-trip.
	callCtx := testutil.Ctx(t)
	require.NoError(t, pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("before"))), "send before unbind")
	m, err := puller.RecvMsg(callCtx)
	require.NoError(t, err, "recv before unbind")
	m.Release()

	// Unknown endpoint → ErrEndpointUnknown.
	require.ErrorIs(t, puller.Unbind("tcp://127.0.0.1:1"), gozmq.ErrEndpointUnknown, "Unbind(unknown)")

	// Unbind the resolved endpoint (LastEndpoint form).
	require.NoError(t, puller.Unbind(ep), "Unbind(%q)", ep)

	// The established connection survives.
	require.NoError(t, pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("after"))), "send after unbind")
	m, err = puller.RecvMsg(callCtx)
	require.NoError(t, err, "recv after unbind")
	require.Equal(t, "after", string(m.Bytes()), "recv after unbind")
	m.Release()

	// A new client can no longer be accepted: its dial is refused and the
	// reconnector reports connect trouble instead of a handshake success.
	lateEvents := make(chan gozmq.Event, 64)
	late, err := ctxZmq.NewSocket(gozmq.Push,
		gozmq.WithMonitor(lateEvents, gozmq.EventAll),
		gozmq.WithReconnectInterval(5*time.Millisecond),
		gozmq.WithReconnectIntervalMax(20*time.Millisecond),
	)
	require.NoError(t, err, "new late pusher")
	defer late.Close()
	require.NoError(t, late.Connect(ep), "late connect")
	collected := waitForEvents(t, lateEvents, 3*time.Second, func(evts []gozmq.Event) bool {
		return hasType(evts, gozmq.EventConnectDelayed) || hasType(evts, gozmq.EventConnectRetried)
	})
	require.True(t, hasType(collected, gozmq.EventConnectDelayed) || hasType(collected, gozmq.EventConnectRetried),
		"late client saw no connect-failure events; events=%v", eventTypes(collected))
	require.False(t, hasType(collected, gozmq.EventHandshakeSucceeded),
		"late client handshook against an unbound listener; events=%v", eventTypes(collected))
}

// TestRecvBytes covers the misuse-proof single-frame receive: a
// round-trip copy on the happy path, and an error on multipart.
func TestRecvBytes(t *testing.T) {
	t.Parallel()
	_, server, client := newSocketPair(t, gozmq.Pair, "inproc://recv-bytes")
	callCtx := testutil.Ctx(t)

	require.NoError(t, client.SendBytes(callCtx, []byte("payload")), "SendBytes")
	got, err := server.RecvBytes(callCtx)
	require.NoError(t, err, "RecvBytes")
	require.Equal(t, "payload", string(got), "RecvBytes")

	// Multipart → error (use RecvMsg for those).
	require.NoError(t, client.SendMsg(callCtx, gozmq.NewMsgFromFrames([]byte("a"), []byte("b"))), "send multipart")
	got, err = server.RecvBytes(callCtx)
	require.ErrorContains(t, err, "multipart", "RecvBytes(multipart) = %q", got)
}

// TestPlainAuth_WrongPassword: a PLAIN client presenting bad credentials
// must surface EventHandshakeFailedAuth on its monitor channel
// (exercising classifyHandshakeEvent's ErrAuthFailed branch).
func TestPlainAuth_WrongPassword(t *testing.T) {
	t.Parallel()
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
	require.True(t, hasType(collected, gozmq.EventHandshakeFailedAuth),
		"no EventHandshakeFailedAuth; events=%v", eventTypes(collected))
	require.False(t, hasType(collected, gozmq.EventHandshakeSucceeded),
		"handshake succeeded with wrong password; events=%v", eventTypes(collected))
}

// TestConnect_UnknownTransportEmitsConnectFailed: Connect with an
// unregistered scheme returns ErrTransportUnknown synchronously AND
// emits EventConnectFailed (terminal — no reconnect loop to retry it).
func TestConnect_UnknownTransportEmitsConnectFailed(t *testing.T) {
	t.Parallel()
	evts := make(chan gozmq.Event, 8)
	_, s := newTestSocket(t, gozmq.Pair, gozmq.WithMonitor(evts, gozmq.EventAll))

	require.ErrorIs(t, s.Connect("bogus+scheme://x"), gozmq.ErrTransportUnknown, "Connect(bogus scheme)")
	// emit() runs synchronously before Connect returns, onto a buffered
	// channel — the event is already there.
	select {
	case ev := <-evts:
		require.Equal(t, gozmq.EventConnectFailed, ev.Type, "event type")
		require.NotEmpty(t, ev.Reason, "EventConnectFailed.Reason empty, want error text")
	default:
		t.Fatal("no event emitted for failed Connect")
	}
}

// TestSocketType_String covers the Stringer including the unknown-value
// fallback.
func TestSocketType_String(t *testing.T) {
	t.Parallel()
	require.Equal(t, "PAIR", gozmq.Pair.String())
	require.Equal(t, "SocketType(99)", gozmq.SocketType(99).String())
}

// TestNewMsg covers the NewMsg constructor end to end: an empty message,
// incremental frame appends, a round trip, and the always-safe Release
// contract for caller-constructed messages.
func TestNewMsg(t *testing.T) {
	t.Parallel()
	m := gozmq.NewMsg()
	require.Equal(t, 0, m.NumFrames(), "NewMsg NumFrames")
	m.AppendFrame([]byte("alpha"))
	m.AppendFrame([]byte("beta"))
	require.Equal(t, 2, m.NumFrames(), "NumFrames")

	_, server, client := newSocketPair(t, gozmq.Pair, "inproc://newmsg-roundtrip")
	callCtx := testutil.Ctx(t)
	require.NoError(t, client.SendMsg(callCtx, m), "send")
	got, err := server.RecvMsg(callCtx)
	require.NoError(t, err, "recv")
	defer got.Release()
	require.Equal(t, [][]byte{[]byte("alpha"), []byte("beta")}, got.Frames())

	// Release on a caller-constructed message is documented as always safe.
	empty := gozmq.NewMsg()
	empty.Release()
}

// TestWithLogger proves the configured logger actually receives the
// socket's internal diagnostics: a refused dial must produce the
// reconnector's debug log line on the custom logger.
func TestWithLogger(t *testing.T) {
	t.Parallel()
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
	require.NoError(t, sock.Connect("tcp://127.0.0.1:1"), "connect")

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
