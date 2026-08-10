//go:build interop

package interop

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

// TestPushPull_GozmqPush_LibzmqPull: gozmq PUSH connects to a libzmq PULL
// bind; verifies a single multipart message crosses the boundary intact.
func TestPushPull_GozmqPush_LibzmqPull(t *testing.T) {
	libCtx := newLibCtx(t)
	puller := libCtx.NewSocket(ZMQ_PULL)
	t.Cleanup(func() { _ = puller.Close() })
	if err := puller.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}
	ep, err := puller.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	pusher, err := gCtx.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = pusher.Close() })
	if err := pusher.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := [][]byte{[]byte("job-42"), []byte("payload-bytes")}
	if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromFrames(want...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := puller.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, want)
}

// TestPushPull_LibzmqPush_GozmqPull: libzmq PUSH connects to a gozmq PULL
// bind.
func TestPushPull_LibzmqPush_GozmqPull(t *testing.T) {
	gCtx := newGozmqCtx(t)
	puller, err := gCtx.NewSocket(gozmq.Pull)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = puller.Close() })
	if err := puller.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq bind: %v", err)
	}
	ep := puller.LastEndpoint()
	if ep == "" {
		t.Fatal("gozmq LastEndpoint empty")
	}

	libCtx := newLibCtx(t)
	pusher := libCtx.NewSocket(ZMQ_PUSH)
	t.Cleanup(func() { _ = pusher.Close() })
	if err := pusher.Connect(ep); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := [][]byte{[]byte("job-7"), []byte("more")}
	if err := pusher.Send(want); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := puller.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), want)
}

// TestPushPull_LoadBalance_GozmqPush_ThreeLibzmqPulls asserts that a single
// gozmq PUSH load-balances fairly across three libzmq PULL peers. With 9
// messages and 3 PULLs we expect exactly 3 messages on each in round-robin
// order — matching libzmq's PUSH fair-queueing contract (design §9).
func TestPushPull_LoadBalance_GozmqPush_ThreeLibzmqPulls(t *testing.T) {
	const numPullers = 3
	const numMsgs = numPullers * 3 // 9

	libCtx := newLibCtx(t)
	pullers := make([]*Sock, numPullers)
	endpoints := make([]string, numPullers)
	for i := 0; i < numPullers; i++ {
		s := libCtx.NewSocket(ZMQ_PULL)
		t.Cleanup(func() { _ = s.Close() })
		if err := s.Bind("tcp://127.0.0.1:0"); err != nil {
			t.Fatalf("libzmq bind [%d]: %v", i, err)
		}
		ep, err := s.LastEndpoint()
		if err != nil {
			t.Fatalf("LastEndpoint [%d]: %v", i, err)
		}
		pullers[i] = s
		endpoints[i] = ep
	}

	gCtx := newGozmqCtx(t)
	pusher, err := gCtx.NewSocket(gozmq.Push)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = pusher.Close() })
	for i, ep := range endpoints {
		if err := pusher.Connect(ep); err != nil {
			t.Fatalf("gozmq connect [%d]: %v", i, err)
		}
	}

	// Give PUSH a beat so every connected PULL completes its handshake and
	// enters the round-robin set. Without this, early sends may all land
	// on the first-ready peer.
	time.Sleep(300 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < numMsgs; i++ {
		payload := []byte(fmt.Sprintf("msg-%02d", i))
		if err := pusher.SendMsg(callCtx, gozmq.NewMsgFromBytes(payload)); err != nil {
			t.Fatalf("gozmq send %d: %v", i, err)
		}
	}

	// Collect: each puller should receive exactly numMsgs/numPullers
	// messages. We drain in parallel to avoid ordering assumptions about
	// which puller gets which message first.
	type result struct {
		idx  int
		data [][]byte
	}
	results := make(chan result, numPullers)
	for i, puller := range pullers {
		go func(idx int, s *Sock) {
			msgs := make([][]byte, 0, numMsgs/numPullers)
			for j := 0; j < numMsgs/numPullers; j++ {
				frames, err := s.Recv()
				if err != nil {
					results <- result{idx: idx}
					return
				}
				msgs = append(msgs, frames[0])
			}
			results <- result{idx: idx, data: msgs}
		}(i, puller)
	}

	counts := make([]int, numPullers)
	var all []string
	deadline := time.After(5 * time.Second)
	for i := 0; i < numPullers; i++ {
		select {
		case r := <-results:
			counts[r.idx] = len(r.data)
			for _, d := range r.data {
				all = append(all, string(d))
			}
		case <-deadline:
			t.Fatalf("timed out waiting for puller results; counts so far = %v", counts)
		}
	}
	for i, c := range counts {
		if c != numMsgs/numPullers {
			t.Errorf("puller %d received %d, want %d (all counts: %v)", i, c, numMsgs/numPullers, counts)
		}
	}
	sort.Strings(all)
	if len(all) != numMsgs {
		t.Fatalf("total received %d, want %d", len(all), numMsgs)
	}
	for i := 0; i < numMsgs; i++ {
		want := fmt.Sprintf("msg-%02d", i)
		if all[i] != want {
			t.Errorf("received[%d]=%q, want %q (full set: %v)", i, all[i], want, all)
		}
	}
}
