//go:build interop

package interop

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

func TestPayloadEdges_GozmqClient_LibzmqServer(t *testing.T) {
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

	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(ep); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	for _, tc := range payloadEdgeCases() {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipInShort && testing.Short() {
				t.Skip("skipping large payload in short mode")
			}
			callCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames(tc.frames...)); err != nil {
				t.Fatalf("gozmq send: %v", err)
			}
			got, err := server.Recv()
			if err != nil {
				t.Fatalf("libzmq recv: %v", err)
			}
			assertFramesEqual(t, got, tc.frames)
		})
	}
}

func TestPayloadEdges_LibzmqClient_GozmqServer(t *testing.T) {
	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair)
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
	if err := client.Connect(ep); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	for _, tc := range payloadEdgeCases() {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipInShort && testing.Short() {
				t.Skip("skipping large payload in short mode")
			}
			callCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			if err := client.Send(tc.frames); err != nil {
				t.Fatalf("libzmq send: %v", err)
			}
			msg, err := server.RecvMsg(callCtx)
			if err != nil {
				t.Fatalf("gozmq recv: %v", err)
			}
			assertFramesEqual(t, msg.Frames(), tc.frames)
		})
	}
}

type payloadEdgeCase struct {
	name        string
	frames      [][]byte
	skipInShort bool
}

func payloadEdgeCases() []payloadEdgeCase {
	cases := []payloadEdgeCase{
		{name: "single-empty-frame", frames: [][]byte{{}}},
		{name: "multipart-empty-middle", frames: [][]byte{[]byte("head"), {}, []byte("tail")}},
		{name: "frame-255", frames: [][]byte{payloadFilled(255, 'a')}},
		{name: "frame-256", frames: [][]byte{payloadFilled(256, 'b')}},
		{name: "frame-257", frames: [][]byte{payloadFilled(257, 'c')}},
		{name: "frame-1mib", frames: [][]byte{payloadFilled(1<<20, 'd')}},
		{name: "frame-16mib", frames: [][]byte{payloadFilled(16<<20, 'e')}, skipInShort: true},
		{name: "multipart-32-frames", frames: payloadMultipart(32)},
	}
	return cases
}

func payloadFilled(n int, b byte) []byte {
	return bytes.Repeat([]byte{b}, n)
}

func payloadMultipart(n int) [][]byte {
	frames := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		frames = append(frames, []byte(fmt.Sprintf("frame-%02d-%s", i, string(bytes.Repeat([]byte{'x' + byte(i%3)}, i%5+1)))))
	}
	return frames
}
