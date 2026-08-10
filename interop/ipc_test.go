//go:build interop

package interop

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
)

func TestIPCFilesystem_GozmqClient_LibzmqServer(t *testing.T) {
	endpoint := "ipc://" + filepath.Join(t.TempDir(), "gozmq-libzmq.sock")

	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind(endpoint); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}

	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(endpoint); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := [][]byte{[]byte("ipc-fs-gozmq"), []byte("to-libzmq")}
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames(want...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, want)
}

func TestIPCFilesystem_LibzmqClient_GozmqServer(t *testing.T) {
	endpoint := "ipc://" + filepath.Join(t.TempDir(), "libzmq-gozmq.sock")

	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind(endpoint); err != nil {
		t.Fatalf("gozmq bind: %v", err)
	}

	libCtx := newLibCtx(t)
	client := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(endpoint); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := [][]byte{[]byte("ipc-fs-libzmq"), []byte("to-gozmq")}
	if err := client.Send(want); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), want)
}

func TestIPCAbstract_GozmqClient_LibzmqServer(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("abstract namespace is Linux-only")
	}
	endpoint := ipcAbstractEndpoint(t)

	libCtx := newLibCtx(t)
	server := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind(endpoint); err != nil {
		t.Fatalf("libzmq bind: %v", err)
	}

	gCtx := newGozmqCtx(t)
	client, err := gCtx.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(endpoint); err != nil {
		t.Fatalf("gozmq connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := [][]byte{[]byte("ipc-abs-gozmq"), []byte("to-libzmq")}
	if err := client.SendMsg(callCtx, gozmq.NewMsgFromFrames(want...)); err != nil {
		t.Fatalf("gozmq send: %v", err)
	}
	got, err := server.Recv()
	if err != nil {
		t.Fatalf("libzmq recv: %v", err)
	}
	assertFramesEqual(t, got, want)
}

func TestIPCAbstract_LibzmqClient_GozmqServer(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("abstract namespace is Linux-only")
	}
	endpoint := ipcAbstractEndpoint(t)

	gCtx := newGozmqCtx(t)
	server, err := gCtx.NewSocket(gozmq.Pair)
	if err != nil {
		t.Fatalf("gozmq new: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if err := server.Bind(endpoint); err != nil {
		t.Fatalf("gozmq bind: %v", err)
	}

	libCtx := newLibCtx(t)
	client := libCtx.NewSocket(ZMQ_PAIR)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Connect(endpoint); err != nil {
		t.Fatalf("libzmq connect: %v", err)
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := [][]byte{[]byte("ipc-abs-libzmq"), []byte("to-gozmq")}
	if err := client.Send(want); err != nil {
		t.Fatalf("libzmq send: %v", err)
	}
	msg, err := server.RecvMsg(callCtx)
	if err != nil {
		t.Fatalf("gozmq recv: %v", err)
	}
	assertFramesEqual(t, msg.Frames(), want)
}

func ipcAbstractEndpoint(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("ipc://@gozmq-interop-%s-%d", t.Name(), time.Now().UnixNano())
}
