//go:build interop

package interop

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

// TestReqRep_GozmqReq_LibzmqRep: gozmq REQ connects to a libzmq REP bind and
// drives three round-trips of strict-alternating req/rep frames. Validates
// the REQ empty-delimiter envelope matches what libzmq REP expects on the
// wire.
func TestReqRep_GozmqReq_LibzmqRep(t *testing.T) {
	libCtx := newLibCtx(t)
	rep := libCtx.NewSocket(ZMQ_REP)
	t.Cleanup(func() { _ = rep.Close() })
	if err := rep.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("libzmq rep bind: %v", err)
	}
	ep, err := rep.LastEndpoint()
	if err != nil {
		t.Fatalf("LastEndpoint: %v", err)
	}

	gCtx := newGozmqCtx(t)
	req, err := gCtx.NewSocket(gozmq.Req)
	if err != nil {
		t.Fatalf("gozmq req new: %v", err)
	}
	t.Cleanup(func() { _ = req.Close() })
	if err := req.Connect(ep); err != nil {
		t.Fatalf("gozmq req connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		reqBody := []byte(fmt.Sprintf("req-%d", i))
		repBody := []byte(fmt.Sprintf("rep-%d", i))

		if err := req.SendMsg(callCtx, gozmq.NewMsgFromBytes(reqBody)); err != nil {
			t.Fatalf("gozmq req send[%d]: %v", i, err)
		}
		got, err := rep.Recv()
		if err != nil {
			t.Fatalf("libzmq rep recv[%d]: %v", i, err)
		}
		// libzmq REP strips the routing prefix (identity + empty delimiter)
		// before surfacing to the application; the app sees only the body
		// frame(s).
		assertFramesEqual(t, got, [][]byte{reqBody})

		if err := rep.Send([][]byte{repBody}); err != nil {
			t.Fatalf("libzmq rep send[%d]: %v", i, err)
		}
		back, err := req.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("gozmq req recv[%d]: %v", i, err)
		}
		assertFramesEqual(t, back.Frames(), [][]byte{repBody})
	}
}

// TestReqRep_LibzmqReq_GozmqRep: libzmq REQ connects to a gozmq REP bind.
// Three round-trips, same framing expectations.
func TestReqRep_LibzmqReq_GozmqRep(t *testing.T) {
	gCtx := newGozmqCtx(t)
	rep, err := gCtx.NewSocket(gozmq.Rep)
	if err != nil {
		t.Fatalf("gozmq rep new: %v", err)
	}
	t.Cleanup(func() { _ = rep.Close() })
	if err := rep.Bind("tcp://127.0.0.1:0"); err != nil {
		t.Fatalf("gozmq rep bind: %v", err)
	}
	ep := rep.LastEndpoint()
	if ep == "" {
		t.Fatal("gozmq LastEndpoint empty after bind")
	}

	libCtx := newLibCtx(t)
	req := libCtx.NewSocket(ZMQ_REQ)
	t.Cleanup(func() { _ = req.Close() })
	if err := req.Connect(ep); err != nil {
		t.Fatalf("libzmq req connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		reqBody := []byte(fmt.Sprintf("req-%d", i))
		repBody := []byte(fmt.Sprintf("rep-%d", i))

		if err := req.Send([][]byte{reqBody}); err != nil {
			t.Fatalf("libzmq req send[%d]: %v", i, err)
		}
		got, err := rep.RecvMsg(callCtx)
		if err != nil {
			t.Fatalf("gozmq rep recv[%d]: %v", i, err)
		}
		assertFramesEqual(t, got.Frames(), [][]byte{reqBody})

		if err := rep.SendMsg(callCtx, gozmq.NewMsgFromBytes(repBody)); err != nil {
			t.Fatalf("gozmq rep send[%d]: %v", i, err)
		}
		back, err := req.Recv()
		if err != nil {
			t.Fatalf("libzmq req recv[%d]: %v", i, err)
		}
		assertFramesEqual(t, back, [][]byte{repBody})
	}
}
