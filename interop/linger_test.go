//go:build interop

package interop

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

const (
	lingerPayloadSize = 128 << 10
	lingerBurstLimit  = 256
)

func TestLinger_GozmqSender_LibzmqReceiver(t *testing.T) {
	for _, tc := range []struct {
		name      string
		linger    time.Duration
		expectAll bool
	}{
		{name: "linger-drains", linger: 2 * time.Second, expectAll: true},
		{name: "linger-zero-drops", linger: 0, expectAll: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			libCtx := newLibCtx(t)
			receiver := libCtx.NewSocket(ZMQ_PAIR)
			t.Cleanup(func() { _ = receiver.Close() })
			if err := receiver.SetOption(ZMQ_RCVHWM, 1); err != nil {
				t.Fatalf("libzmq RCVHWM: %v", err)
			}
			if err := receiver.SetOption(ZMQ_RCVTIMEO, 100); err != nil {
				t.Fatalf("libzmq RCVTIMEO: %v", err)
			}
			if err := receiver.Bind("tcp://127.0.0.1:0"); err != nil {
				t.Fatalf("libzmq bind: %v", err)
			}
			ep, err := receiver.LastEndpoint()
			if err != nil {
				t.Fatalf("LastEndpoint: %v", err)
			}

			gCtx := newGozmqCtx(t)
			events := make(chan gozmq.Event, 16)
			sender, err := gCtx.NewSocket(
				gozmq.Pair,
				gozmq.WithLinger(tc.linger),
				gozmq.WithSendHWM(1),
				gozmq.WithMonitor(events, gozmq.EventAll),
			)
			if err != nil {
				t.Fatalf("gozmq new: %v", err)
			}
			t.Cleanup(func() { _ = sender.Close() })
			if err := sender.Connect(ep); err != nil {
				t.Fatalf("gozmq connect: %v", err)
			}

			waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if ev := waitForEventType(waitCtx, events, gozmq.EventConnected); ev == nil {
				t.Fatal("missing EventConnected before linger test")
			}

			sent := fillGozmqSenderUntilBlocked(t, sender)
			closeDone := make(chan error, 1)
			go func() { closeDone <- sender.Close() }()
			time.Sleep(50 * time.Millisecond)
			got := drainLibReceiver(t, receiver)
			if err := <-closeDone; err != nil {
				t.Fatalf("gozmq close: %v", err)
			}

			assertLingerResult(t, sent, got, tc.expectAll)
		})
	}
}

func TestLinger_LibzmqSender_GozmqReceiver(t *testing.T) {
	for _, tc := range []struct {
		name      string
		lingerMS  int
		expectAll bool
	}{
		{name: "linger-drains", lingerMS: 2000, expectAll: true},
		{name: "linger-zero-drops", lingerMS: 0, expectAll: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gCtx := newGozmqCtx(t)
			events := make(chan gozmq.Event, 16)
			receiver, err := gCtx.NewSocket(
				gozmq.Pair,
				gozmq.WithRecvHWM(1),
				gozmq.WithMonitor(events, gozmq.EventAll),
			)
			if err != nil {
				t.Fatalf("gozmq new: %v", err)
			}
			t.Cleanup(func() { _ = receiver.Close() })
			if err := receiver.Bind("tcp://127.0.0.1:0"); err != nil {
				t.Fatalf("gozmq bind: %v", err)
			}
			ep := receiver.LastEndpoint()
			if ep == "" {
				t.Fatal("gozmq LastEndpoint empty after bind")
			}

			libCtx := newLibCtx(t)
			sender := libCtx.NewSocket(ZMQ_PAIR)
			t.Cleanup(func() { _ = sender.Close() })
			if err := sender.SetOption(ZMQ_LINGER, tc.lingerMS); err != nil {
				t.Fatalf("libzmq LINGER: %v", err)
			}
			if err := sender.SetOption(ZMQ_SNDHWM, 1); err != nil {
				t.Fatalf("libzmq SNDHWM: %v", err)
			}
			if err := sender.SetOption(ZMQ_SNDTIMEO, 10); err != nil {
				t.Fatalf("libzmq SNDTIMEO: %v", err)
			}
			if err := sender.Connect(ep); err != nil {
				t.Fatalf("libzmq connect: %v", err)
			}

			waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ev := waitHandshakeEvent(waitCtx, events)
			if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
				t.Fatalf("handshake event = %v, want %v", ev, gozmq.EventHandshakeSucceeded)
			}

			sent := fillLibSenderUntilBlocked(t, sender)
			closeDone := make(chan error, 1)
			go func() { closeDone <- sender.Close() }()
			time.Sleep(50 * time.Millisecond)
			got := drainGozmqReceiver(t, receiver)
			if err := <-closeDone; err != nil {
				t.Fatalf("libzmq close: %v", err)
			}

			assertLingerResult(t, sent, got, tc.expectAll)
		})
	}
}

func fillGozmqSenderUntilBlocked(t *testing.T, sender *gozmq.Socket) int {
	t.Helper()
	payload := bytes.Repeat([]byte("g"), lingerPayloadSize)
	sent := 0
	for ; sent < lingerBurstLimit; sent++ {
		sendCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		err := sender.SendMsg(sendCtx, gozmq.NewMsgFromBytes(payload))
		cancel()
		if err != nil {
			break
		}
	}
	if sent == 0 {
		t.Fatal("gozmq sender did not queue any messages before blocking")
	}
	if sent == lingerBurstLimit {
		t.Fatalf("gozmq sender never saturated; increase lingerBurstLimit or payload size (sent=%d)", sent)
	}
	return sent
}

func fillLibSenderUntilBlocked(t *testing.T, sender *Sock) int {
	t.Helper()
	payload := bytes.Repeat([]byte("l"), lingerPayloadSize)
	sent := 0
	for ; sent < lingerBurstLimit; sent++ {
		if err := sender.Send([][]byte{payload}); err != nil {
			if isLibzmqTimeout(err) {
				break
			}
			t.Fatalf("libzmq send[%d]: %v", sent, err)
		}
	}
	if sent == 0 {
		t.Fatal("libzmq sender did not queue any messages before blocking")
	}
	if sent == lingerBurstLimit {
		t.Fatalf("libzmq sender never saturated; increase lingerBurstLimit or payload size (sent=%d)", sent)
	}
	return sent
}

func drainLibReceiver(t *testing.T, receiver *Sock) int {
	t.Helper()
	got := 0
	for {
		frames, err := receiver.Recv()
		if err != nil {
			if isLibzmqTimeout(err) {
				return got
			}
			t.Fatalf("libzmq recv: %v", err)
		}
		if len(frames) != 1 || len(frames[0]) != lingerPayloadSize {
			t.Fatalf("libzmq recv got %d frames len=%d", len(frames), len(frames[0]))
		}
		got++
	}
}

func drainGozmqReceiver(t *testing.T, receiver *gozmq.Socket) int {
	t.Helper()
	got := 0
	for {
		recvCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		msg, err := receiver.RecvMsg(recvCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
				return got
			}
			t.Fatalf("gozmq recv: %v", err)
		}
		if msg.NumFrames() != 1 || len(msg.Bytes()) != lingerPayloadSize {
			t.Fatalf("gozmq recv got %d frames len=%d", msg.NumFrames(), len(msg.Bytes()))
		}
		got++
	}
}

func assertLingerResult(t *testing.T, sent, got int, expectAll bool) {
	t.Helper()
	if expectAll {
		if got != sent {
			t.Fatalf("linger drain received %d/%d messages, want all", got, sent)
		}
		return
	}
	if got >= sent {
		t.Fatalf("linger=0 received %d/%d messages, want a drop", got, sent)
	}
}
