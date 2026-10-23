package transport

import (
	"context"
	"net"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/testutil"
)

// TestIPC_StaleSocketFileIsReclaimed: a socket file left behind by a crashed
// process (nothing listening) must not make Bind fail with EADDRINUSE.
func TestIPC_StaleSocketFileIsReclaimed(t *testing.T) {
	t.Parallel()
	path := tmpSock(t, "stale.sock")
	crashed, err := net.Listen("unix", path)
	require.NoError(t, err)
	crashed.(*net.UnixListener).SetUnlinkOnClose(false)
	require.NoError(t, crashed.Close()) // file stays, like after a crash
	_, err = os.Stat(path)
	require.NoError(t, err, "stale socket file should still exist")

	tr := NewIPCTransport()
	l, err := tr.Listen(context.Background(), "ipc://"+path)
	require.NoError(t, err, "Listen over a stale socket file")
	defer l.Close()

	ctx := testutil.Ctx(t, 2*time.Second)
	go func() {
		if c, err := l.Accept(ctx); err == nil {
			_ = c.Close()
		}
	}()
	c, err := tr.Dial(ctx, "ipc://"+path)
	require.NoError(t, err, "Dial the reclaimed endpoint")
	_ = c.Close()
}

// TestIPC_LiveSocketFileIsNotReclaimed: the stale-file recovery must never
// steal an endpoint that someone is still listening on.
func TestIPC_LiveSocketFileIsNotReclaimed(t *testing.T) {
	t.Parallel()
	path := tmpSock(t, "live.sock")
	tr := NewIPCTransport()
	live, err := tr.Listen(context.Background(), "ipc://"+path)
	require.NoError(t, err)
	defer live.Close()

	_, err = tr.Listen(context.Background(), "ipc://"+path)
	require.Error(t, err, "second Listen on a live endpoint must fail")

	ctx := testutil.Ctx(t, 2*time.Second)
	go func() {
		if c, err := live.Accept(ctx); err == nil {
			_ = c.Close()
		}
	}()
	c, err := tr.Dial(ctx, "ipc://"+path)
	require.NoError(t, err, "live listener lost its socket file")
	_ = c.Close()
}

// TestIPC_NonSocketFileIsNotRemoved: a regular file at the path is not ours
// to delete.
func TestIPC_NonSocketFileIsNotRemoved(t *testing.T) {
	t.Parallel()
	path := tmpSock(t, "regular")
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	_, err := NewIPCTransport().Listen(context.Background(), "ipc://"+path)
	require.Error(t, err)
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "keep", string(b))
}

// TestIPC_CloseRemovesOnlyItsOwnFile: Close must not unlink by path on its
// own. net.UnixListener.Close already unlinks the file it created; a second,
// unconditional os.Remove could delete a socket file that a concurrent Bind
// created at the same path in between. Simulated here by disabling the
// inner unlink and rebinding the path before Close.
func TestIPC_CloseRemovesOnlyItsOwnFile(t *testing.T) {
	t.Parallel()
	path := tmpSock(t, "own.sock")
	tr := NewIPCTransport()
	l1, err := tr.Listen(context.Background(), "ipc://"+path)
	require.NoError(t, err)
	inner := reflect.ValueOf(l1).Elem().FieldByName("l")
	require.True(t, inner.IsValid(), "listener has no inner net.Listener field")
	ul := (*net.UnixListener)(inner.Elem().UnsafePointer())
	ul.SetUnlinkOnClose(false)
	require.NoError(t, os.Remove(path))

	l2, err := tr.Listen(context.Background(), "ipc://"+path)
	require.NoError(t, err)
	defer l2.Close()

	require.NoError(t, l1.Close())
	_, err = os.Stat(path)
	require.NoError(t, err, "closing l1 deleted l2's socket file")
}

// TestIPC_AbstractNamespaceIsAbstract confirms that Go's own '@' mapping
// (syscall.SockaddrUnix on Linux) binds in the abstract namespace: the
// kernel lists it with a leading '@' and no file appears.
func TestIPC_AbstractNamespaceIsAbstract(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("abstract namespace is Linux-only")
	}
	name := "@gozmq-abstract-check-" + t.Name()
	l, err := NewIPCTransport().Listen(context.Background(), "ipc://"+name)
	require.NoError(t, err)
	defer l.Close()
	require.Equal(t, "ipc://"+name, l.Endpoint())

	tbl, err := os.ReadFile("/proc/net/unix")
	if err != nil {
		t.Skipf("cannot read /proc/net/unix: %v", err)
	}
	require.True(t, strings.Contains(string(tbl), " "+name+"\n"),
		"%s not listed as an abstract socket", name)
	_, err = os.Stat(name)
	require.ErrorIs(t, err, os.ErrNotExist)
}
