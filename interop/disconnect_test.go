//go:build interop

package interop

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rkm/gozmq"
)

func TestDisconnect_LibzmqClient_GozmqServer(t *testing.T) {
	gCtx := newGozmqCtx(t)
	events := make(chan gozmq.Event, 16)
	server, err := gCtx.NewSocket(
		gozmq.Pair,
		gozmq.WithMonitor(events, gozmq.EventAll),
		gozmq.WithHeartbeat(200*time.Millisecond, time.Second),
	)
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
	if err := client.SetOption(ZMQ_LINGER, 0); err != nil {
		t.Fatalf("libzmq LINGER: %v", err)
	}
	if err := client.Connect(ep); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev := waitHandshakeEvent(waitCtx, events)
	if ev == nil || ev.Type != gozmq.EventHandshakeSucceeded {
		t.Fatalf("handshake event = %v, want %v", ev, gozmq.EventHandshakeSucceeded)
	}

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	if err := client.Send([][]byte{[]byte("before-disconnect")}); err != nil {
		t.Fatalf("libzmq send before disconnect: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv before disconnect: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), [][]byte{[]byte("before-disconnect")})

	if err := client.Disconnect(ep); err != nil {
		t.Fatalf("libzmq disconnect: %v", err)
	}
	if err := server.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("disconnect-probe"))); err != nil {
		t.Fatalf("gozmq send after libzmq disconnect: %v", err)
	}
	if ev := waitForEventType(waitCtx, events, gozmq.EventDisconnected); ev == nil {
		t.Fatal("missing EventDisconnected after libzmq disconnect")
	}
}

func TestDisconnect_GozmqClient_LibzmqServer(t *testing.T) {
	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	ep, err := server.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	mon, monEP := newLibMonitor(t, libCtx, server)
	t.Cleanup(func() { _ = mon.Close() })

	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(
		gozmq.Pair,
		gozmq.WithReconnectInterval(10*time.Millisecond),
		gozmq.WithReconnectIntervalMax(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	if ev := waitForLibMonitorEvent(t, mon, ZMQ_EVENT_ACCEPTED, 5*time.Second); ev == nil {
		t.Fatalf("missing ZMQ_EVENT_ACCEPTED on libzmq monitor %q", monEP)
	}

	if err := client.Disconnect(ep); err != nil {
		t.Fatalf("gozmq disconnect: %v", err)
	}

	if ev := waitForLibMonitorEvent(t, mon, ZMQ_EVENT_DISCONNECTED, 5*time.Second); ev == nil {
		t.Fatalf("missing ZMQ_EVENT_DISCONNECTED on libzmq monitor %q", monEP)
	}
	if ev := waitForLibMonitorEvent(t, mon, ZMQ_EVENT_ACCEPTED, 250*time.Millisecond); ev != nil {
		t.Fatalf("unexpected libzmq re-accept after Disconnect: %+v", *ev)
	}
}

type libMonitorEvent struct {
	Event    uint16
	Value    uint32
	Endpoint string
}

func newLibMonitor(t *testing.T, ctx *Ctx, s *Sock) (*Sock, string) {
	t.Helper()
	ep := fmt.Sprintf("inproc://libmon-%s-%d", t.Name(), time.Now().UnixNano())
	if err := s.Monitor(ep, 0); err != nil {
		t.Fatalf("Monitor(%q): %v", ep, err)
	}
	mon := ctx.NewSocket(ZMQ_PAIR)
	if err := mon.SetOption(ZMQ_RCVTIMEO, 100); err != nil {
		t.Fatalf("monitor RCVTIMEO: %v", err)
	}
	if err := mon.Connect(ep); err != nil {
		t.Fatalf("monitor connect %q: %v", ep, err)
	}
	return mon, ep
}

func waitForLibMonitorEvent(t *testing.T, mon *Sock, want int, timeout time.Duration) *libMonitorEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		frames, err := mon.Recv()
		if err != nil {
			if isLibzmqTimeout(err) {
				continue
			}
			t.Fatalf("monitor recv: %v", err)
		}
		ev := parseLibMonitorEvent(t, frames)
		if int(ev.Event) == want {
			return ev
		}
	}
	return nil
}

func parseLibMonitorEvent(t *testing.T, frames [][]byte) *libMonitorEvent {
	t.Helper()
	if len(frames) != 2 {
		t.Fatalf("monitor frames = %d, want 2", len(frames))
	}
	if len(frames[0]) < 6 {
		t.Fatalf("monitor header len = %d, want >= 6", len(frames[0]))
	}
	return &libMonitorEvent{
		Event:    binary.LittleEndian.Uint16(frames[0][:2]),
		Value:    binary.LittleEndian.Uint32(frames[0][2:6]),
		Endpoint: string(frames[1]),
	}
}

func isLibzmqTimeout(err error) bool {
	return strings.Contains(err.Error(), "Resource temporarily unavailable") || strings.Contains(err.Error(), "errno 11")
}
