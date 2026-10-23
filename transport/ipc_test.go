package transport

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
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
	t.Parallel()
	tr := NewIPCTransport()
	path := tmpSock(t, "rt.sock")
	endpoint := "ipc://" + path

	l, err := tr.Listen(context.Background(), endpoint)
	require.NoError(t, err, "Listen")
	defer l.Close()

	require.Equal(t, endpoint, l.Endpoint())

	ctx := testutil.Ctx(t, 2*time.Second)

	acceptCh := make(chan *conn, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			assert.NoError(t, err, "Accept")
			close(acceptCh)
			return
		}
		acceptCh <- c.(*conn)
	}()

	dial, err := tr.Dial(ctx, endpoint)
	require.NoError(t, err, "Dial")
	defer dial.Close()

	srv := <-acceptCh
	require.NotNil(t, srv, "accept failed")
	defer srv.Close()

	// Endpoint reporting.
	assert.True(t, ipcHasPrefix(dial.(*conn).RemoteEndpoint(), "ipc://"), "client Remote = %q", dial.(*conn).RemoteEndpoint())

	want := []byte("ping-ipc")
	_, err = dial.Write(want)
	require.NoError(t, err, "Write")
	got := make([]byte, len(want))
	_, err = io.ReadFull(srv, got)
	require.NoError(t, err, "ReadFull server")
	require.Equal(t, want, got)

	want2 := []byte("pong-ipc")
	_, err = srv.Write(want2)
	require.NoError(t, err, "Write server")
	got2 := make([]byte, len(want2))
	_, err = io.ReadFull(dial, got2)
	require.NoError(t, err, "ReadFull client")
	require.Equal(t, want2, got2)
}

// TestIPC_SocketFileCleanup asserts that closing the listener removes the
// socket file from the filesystem. Otherwise repeated Bind/Unbind cycles
// accumulate stale inodes.
func TestIPC_SocketFileCleanup(t *testing.T) {
	t.Parallel()
	tr := NewIPCTransport()
	path := tmpSock(t, "cleanup.sock")
	endpoint := "ipc://" + path

	l, err := tr.Listen(context.Background(), endpoint)
	require.NoError(t, err, "Listen")
	// After Listen the file must exist.
	_, err = os.Stat(path)
	require.NoError(t, err, "socket file missing after Listen")
	require.NoError(t, l.Close(), "Close")
	// After Close it must be gone.
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist, "socket file still exists after Close")
}

// TestIPC_FilePerm asserts that WithIPCFilePerm chmods the socket file to
// the requested mode. The kernel may mask a few bits (umask, sticky-ish
// bits on some platforms); we check the low 9 permission bits exactly.
func TestIPC_FilePerm(t *testing.T) {
	t.Parallel()
	// Darwin/BSD may refuse chmod on sockets in some configurations; skip
	// on non-Linux to keep the test deterministic.
	if runtime.GOOS != "linux" {
		t.Skip("ipc file-perm test is Linux-only")
	}
	tr := &IPCTransport{FilePerm: 0o600}
	path := tmpSock(t, "perm.sock")
	endpoint := "ipc://" + path

	l, err := tr.Listen(context.Background(), endpoint)
	require.NoError(t, err, "Listen")
	defer l.Close()

	fi, err := os.Stat(path)
	require.NoError(t, err, "Stat")
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
}

// TestIPC_AbstractNamespace exercises the "@name" abstract-namespace
// syntax. Linux-only; skipped elsewhere.
func TestIPC_AbstractNamespace(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("abstract namespace is Linux-only")
	}
	tr := NewIPCTransport()
	// Include t.Name to avoid collisions when -count>1 re-runs the test.
	endpoint := "ipc://@gozmq-abstract-" + t.Name()

	l, err := tr.Listen(context.Background(), endpoint)
	require.NoError(t, err, "Listen")
	defer l.Close()

	require.Equal(t, endpoint, l.Endpoint())

	ctx := testutil.Ctx(t, 2*time.Second)

	acceptCh := make(chan *conn, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			assert.NoError(t, err, "Accept")
			close(acceptCh)
			return
		}
		acceptCh <- c.(*conn)
	}()

	dial, err := tr.Dial(ctx, endpoint)
	require.NoError(t, err, "Dial")
	defer dial.Close()

	srv := <-acceptCh
	require.NotNil(t, srv, "accept failed")
	defer srv.Close()

	want := []byte("abs")
	_, err = dial.Write(want)
	require.NoError(t, err, "Write")
	got := make([]byte, len(want))
	_, err = io.ReadFull(srv, got)
	require.NoError(t, err, "ReadFull")
	require.Equal(t, want, got)
}

// TestIPC_AbstractNamespace_NoFile asserts that the abstract-namespace
// form does NOT create a filesystem entry. If "@x" accidentally routes
// into the filesystem, os.Stat("@x") succeeds and we catch the bug.
func TestIPC_AbstractNamespace_NoFile(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("abstract namespace is Linux-only")
	}
	tr := NewIPCTransport()
	endpoint := "ipc://@gozmq-no-file-" + t.Name()
	l, err := tr.Listen(context.Background(), endpoint)
	require.NoError(t, err, "Listen")
	defer l.Close()
	// The literal path "@name" (relative to cwd) should not exist.
	_, err = os.Stat("@gozmq-no-file-" + t.Name())
	require.Error(t, err, "abstract socket unexpectedly created a filesystem entry")
}

// TestIPC_AbstractNamespace_NonLinux asserts that on non-Linux platforms
// the abstract form is rejected cleanly.
func TestIPC_AbstractNamespace_NonLinux(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "linux" {
		t.Skip("Linux supports abstract namespace; this test guards other OSes")
	}
	tr := NewIPCTransport()
	_, err := tr.Listen(context.Background(), "ipc://@foo")
	require.ErrorIs(t, err, zmqerr.ErrEndpointInvalid, "Listen(abstract) on %s", runtime.GOOS)
}

// TestIPC_ListenerClose_WakesAccept asserts Close wakes a parked Accept.
func TestIPC_ListenerClose_WakesAccept(t *testing.T) {
	t.Parallel()
	tr := NewIPCTransport()
	path := tmpSock(t, "close-wake.sock")
	endpoint := "ipc://" + path
	l, err := tr.Listen(context.Background(), endpoint)
	require.NoError(t, err, "Listen")

	err = testutil.RequireUnblocks(t, func() error {
		_, err := l.Accept(context.Background())
		return err
	}, func() { require.NoError(t, l.Close()) })
	require.ErrorIs(t, err, zmqerr.ErrEndpointInvalid)
}

// TestIPC_AcceptHonorsCtx asserts ctx cancellation wakes Accept.
func TestIPC_AcceptHonorsCtx(t *testing.T) {
	t.Parallel()
	tr := NewIPCTransport()
	path := tmpSock(t, "ctx.sock")
	endpoint := "ipc://" + path
	l, err := tr.Listen(context.Background(), endpoint)
	require.NoError(t, err, "Listen")
	defer l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	err = testutil.RequireUnblocks(t, func() error {
		_, err := l.Accept(ctx)
		return err
	}, cancel)
	require.ErrorIs(t, err, context.Canceled)
}

// TestIPC_BadEndpoint covers parse failures: wrong scheme, empty path,
// empty abstract name.
func TestIPC_BadEndpoint(t *testing.T) {
	t.Parallel()
	tr := NewIPCTransport()
	cases := []string{
		"tcp://x:1", // wrong scheme
		"ipc://",    // empty path
		"ipc://@",   // empty abstract name
		"no-scheme", // missing ://
	}
	for _, ep := range cases {
		_, err := tr.Listen(context.Background(), ep)
		assert.ErrorIs(t, err, zmqerr.ErrEndpointInvalid, "Listen(%q)", ep)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err = tr.Dial(ctx, ep)
		cancel()
		if !errors.Is(err, zmqerr.ErrEndpointInvalid) && !errors.Is(err, context.DeadlineExceeded) {
			assert.Failf(t, "unexpected Dial error", "Dial(%q) err = %v, want wraps ErrEndpointInvalid", ep, err)
		}
	}
}

// TestIPC_ManyConcurrentDials sanity-checks the listener under a modest
// burst of concurrent dials.
func TestIPC_ManyConcurrentDials(t *testing.T) {
	t.Parallel()
	tr := NewIPCTransport()
	path := tmpSock(t, "many.sock")
	endpoint := "ipc://" + path
	l, err := tr.Listen(context.Background(), endpoint)
	require.NoError(t, err, "Listen")
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
				assert.NoError(t, err, "Accept[%d]", i)
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
				assert.NoError(t, err, "Dial")
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
