package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/rkm/gozmq/internal/zmqerr"
)

// tmpSock returns a unique socket path under t.TempDir. Paths must be
// short enough to fit in sun_path (108 bytes on Linux); t.TempDir typically
// lives under /tmp which is comfortably under the limit.
func tmpSock(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

// TestIPC_Filesystem_RoundTrip exercises the happy path: bind, dial,
// bidirectional write/read over a filesystem-namespace Unix socket.
func TestIPC_Filesystem_RoundTrip(t *testing.T) {
	tr := NewIPCTransport()
	path := tmpSock(t, "rt.sock")
	endpoint := "ipc://" + path

	l, err := tr.Listen(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	if l.Endpoint() != endpoint {
		t.Fatalf("Endpoint = %q, want %q", l.Endpoint(), endpoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	acceptCh := make(chan *ipcConn, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			t.Errorf("Accept: %v", err)
			close(acceptCh)
			return
		}
		acceptCh <- c.(*ipcConn)
	}()

	dial, err := tr.Dial(ctx, endpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer dial.Close()

	srv := <-acceptCh
	if srv == nil {
		t.Fatal("accept failed")
	}
	defer srv.Close()

	// Endpoint reporting.
	if !ipcHasPrefix(dial.(*ipcConn).RemoteEndpoint(), "ipc://") {
		t.Errorf("client Remote = %q", dial.(*ipcConn).RemoteEndpoint())
	}

	want := []byte("ping-ipc")
	if _, err := dial.Write(want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(srv, got); err != nil {
		t.Fatalf("ReadFull server: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q want %q", got, want)
	}

	want2 := []byte("pong-ipc")
	if _, err := srv.Write(want2); err != nil {
		t.Fatalf("Write server: %v", err)
	}
	got2 := make([]byte, len(want2))
	if _, err := io.ReadFull(dial, got2); err != nil {
		t.Fatalf("ReadFull client: %v", err)
	}
	if !bytes.Equal(got2, want2) {
		t.Fatalf("got %q want %q", got2, want2)
	}
}

// TestIPC_SocketFileCleanup asserts that closing the listener removes the
// socket file from the filesystem. Otherwise repeated Bind/Unbind cycles
// accumulate stale inodes.
func TestIPC_SocketFileCleanup(t *testing.T) {
	tr := NewIPCTransport()
	path := tmpSock(t, "cleanup.sock")
	endpoint := "ipc://" + path

	l, err := tr.Listen(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	// After Listen the file must exist.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("socket file missing after Listen: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After Close it must be gone.
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket file still exists after Close: err=%v", err)
	}
}

// TestIPC_FilePerm asserts that WithIPCFilePerm chmods the socket file to
// the requested mode. The kernel may mask a few bits (umask, sticky-ish
// bits on some platforms); we check the low 9 permission bits exactly.
func TestIPC_FilePerm(t *testing.T) {
	// Darwin/BSD may refuse chmod on sockets in some configurations; skip
	// on non-Linux to keep the test deterministic.
	if runtime.GOOS != "linux" {
		t.Skip("ipc file-perm test is Linux-only")
	}
	tr := NewIPCTransportWith(WithIPCFilePerm(0o600))
	path := tmpSock(t, "perm.sock")
	endpoint := "ipc://" + path

	l, err := tr.Listen(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Fatalf("perm = %#o, want %#o", got, 0o600)
	}
}

// TestIPC_AbstractNamespace exercises the "@name" abstract-namespace
// syntax. Linux-only; skipped elsewhere.
func TestIPC_AbstractNamespace(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("abstract namespace is Linux-only")
	}
	tr := NewIPCTransport()
	// Include t.Name to avoid collisions when -count>1 re-runs the test.
	endpoint := "ipc://@gozmq-abstract-" + t.Name()

	l, err := tr.Listen(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	if l.Endpoint() != endpoint {
		t.Fatalf("Endpoint = %q, want %q", l.Endpoint(), endpoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	acceptCh := make(chan *ipcConn, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			t.Errorf("Accept: %v", err)
			close(acceptCh)
			return
		}
		acceptCh <- c.(*ipcConn)
	}()

	dial, err := tr.Dial(ctx, endpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer dial.Close()

	srv := <-acceptCh
	if srv == nil {
		t.Fatal("accept failed")
	}
	defer srv.Close()

	want := []byte("abs")
	if _, err := dial.Write(want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(srv, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q want %q", got, want)
	}
}

// TestIPC_AbstractNamespace_NoFile asserts that the abstract-namespace
// form does NOT create a filesystem entry. If "@x" accidentally routes
// into the filesystem, os.Stat("@x") succeeds and we catch the bug.
func TestIPC_AbstractNamespace_NoFile(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("abstract namespace is Linux-only")
	}
	tr := NewIPCTransport()
	endpoint := "ipc://@gozmq-no-file-" + t.Name()
	l, err := tr.Listen(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()
	// The literal path "@name" (relative to cwd) should not exist.
	if _, err := os.Stat("@gozmq-no-file-" + t.Name()); err == nil {
		t.Fatal("abstract socket unexpectedly created a filesystem entry")
	}
}

// TestIPC_AbstractNamespace_NonLinux asserts that on non-Linux platforms
// the abstract form is rejected cleanly.
func TestIPC_AbstractNamespace_NonLinux(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux supports abstract namespace; this test guards other OSes")
	}
	tr := NewIPCTransport()
	_, err := tr.Listen(context.Background(), "ipc://@foo")
	if !errors.Is(err, zmqerr.ErrEndpointInvalid) {
		t.Fatalf("Listen(abstract) on %s err = %v, want wraps ErrEndpointInvalid", runtime.GOOS, err)
	}
}

// TestIPC_ListenerClose_WakesAccept asserts Close wakes a parked Accept.
func TestIPC_ListenerClose_WakesAccept(t *testing.T) {
	tr := NewIPCTransport()
	path := tmpSock(t, "close-wake.sock")
	endpoint := "ipc://" + path
	l, err := tr.Listen(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := l.Accept(context.Background())
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, zmqerr.ErrEndpointInvalid) {
			t.Fatalf("Accept err = %v, want wraps ErrEndpointInvalid", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not wake on Close")
	}
}

// TestIPC_AcceptHonorsCtx asserts ctx cancellation wakes Accept.
func TestIPC_AcceptHonorsCtx(t *testing.T) {
	tr := NewIPCTransport()
	path := tmpSock(t, "ctx.sock")
	endpoint := "ipc://" + path
	l, err := tr.Listen(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := l.Accept(ctx)
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Accept err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not wake on ctx cancel")
	}
}

// TestIPC_BadEndpoint covers parse failures: wrong scheme, empty path,
// empty abstract name.
func TestIPC_BadEndpoint(t *testing.T) {
	tr := NewIPCTransport()
	cases := []string{
		"tcp://x:1",  // wrong scheme
		"ipc://",     // empty path
		"ipc://@",    // empty abstract name
		"no-scheme",  // missing ://
	}
	for _, ep := range cases {
		if _, err := tr.Listen(context.Background(), ep); !errors.Is(err, zmqerr.ErrEndpointInvalid) {
			t.Errorf("Listen(%q) err = %v, want wraps ErrEndpointInvalid", ep, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := tr.Dial(ctx, ep)
		cancel()
		if !errors.Is(err, zmqerr.ErrEndpointInvalid) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Dial(%q) err = %v, want wraps ErrEndpointInvalid", ep, err)
		}
	}
}

// TestIPC_ManyConcurrentDials sanity-checks the listener under a modest
// burst of concurrent dials.
func TestIPC_ManyConcurrentDials(t *testing.T) {
	tr := NewIPCTransport()
	path := tmpSock(t, "many.sock")
	endpoint := "ipc://" + path
	l, err := tr.Listen(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	const N = 8
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			c, err := l.Accept(ctx)
			cancel()
			if err != nil {
				t.Errorf("Accept[%d]: %v", i, err)
				return
			}
			c.Close()
		}
	}()

	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := tr.Dial(ctx, endpoint)
			if err != nil {
				t.Errorf("Dial: %v", err)
				return
			}
			c.Close()
		}()
	}
	wg.Wait()
}

// ipcHasPrefix is a tiny test helper; avoids pulling in "strings" just for
// one HasPrefix call elsewhere but kept simple.
func ipcHasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}
