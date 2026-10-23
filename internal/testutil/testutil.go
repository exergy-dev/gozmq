// Package testutil holds the test helpers shared by every package's tests.
//
// It is imported by in-package tests of the root package, security, curve
// and session, so it may depend only on the standard library, nacl/box,
// testify and goleak: never on gozmq packages.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/crypto/nacl/box"
)

// VerifyMain runs the package's tests and fails if goroutines leaked:
//
//	func TestMain(m *testing.M) { testutil.VerifyMain(m) }
func VerifyMain(m *testing.M) { goleak.VerifyTestMain(m) }

// Ctx returns a context cancelled when t ends; d defaults to 5 s.
func Ctx(t testing.TB, d ...time.Duration) context.Context {
	t.Helper()
	timeout := 5 * time.Second
	if len(d) > 0 {
		timeout = d[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

// WaitEvent reads ch until pred accepts an event, which it returns, or
// timeout passes, which fails the test.
func WaitEvent[E any](t testing.TB, ch <-chan E, timeout time.Duration, pred func(E) bool) E {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case ev := <-ch:
			if pred(ev) {
				return ev
			}
		case <-timer.C:
			var zero E
			require.FailNowf(t, "timed out", "no matching event within %v", timeout)
			return zero
		}
	}
}

// CollectUntil reads ch into a slice until pred accepts the slice or timeout
// passes, and returns what was collected either way.
func CollectUntil[E any](ch <-chan E, timeout time.Duration, pred func([]E) bool) []E {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var out []E
	for !pred(out) {
		select {
		case ev := <-ch:
			out = append(out, ev)
		case <-timer.C:
			return out
		}
	}
	return out
}

// RequireUnblocks asserts that a goroutine blocked in op returns after
// unblock runs, and returns op's result.
func RequireUnblocks[R any](t testing.TB, op func() R, unblock func()) R {
	t.Helper()
	done := make(chan R, 1)
	go func() { done <- op() }()
	select {
	case r := <-done:
		require.FailNow(t, "operation returned before it was unblocked")
		return r
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	select {
	case r := <-done:
		return r
	case <-time.After(5 * time.Second):
		var zero R
		require.FailNow(t, "operation did not unblock")
		return zero
	}
}

// CurveKeys returns a fresh Curve25519 key pair.
func CurveKeys(t testing.TB) (pub, sec [32]byte) {
	t.Helper()
	p, s, err := box.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return *p, *s
}

// Greeting is a 64-byte ZMTP 3.x greeting for mech (as a client).
func Greeting(mech string, minor byte) []byte {
	g := make([]byte, 64)
	g[0], g[9], g[10], g[11] = 0xFF, 0x7F, 3, minor
	copy(g[12:], mech)
	return g
}

// Metadata encodes props as a ZMTP property list, keys in sorted order.
func Metadata(props map[string]string) []byte {
	var b []byte
	for _, k := range slices.Sorted(func(yield func(string) bool) {
		for k := range props {
			if !yield(k) {
				return
			}
		}
	}) {
		b = append(b, byte(len(k)))
		b = append(b, k...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(props[k])))
		b = append(b, props[k]...)
	}
	return b
}

// Command is a short ZMTP command frame: name plus body.
func Command(name string, body []byte) []byte {
	cmd := append([]byte{byte(len(name))}, name...)
	cmd = append(cmd, body...)
	return append([]byte{0x04, byte(len(cmd))}, cmd...)
}

// SubscribeFrame is the single-frame SUBSCRIBE (0x01) or CANCEL (0x00)
// message a SUB sends for prefix.
func SubscribeFrame(subscribe bool, prefix []byte) []byte {
	b := make([]byte, 1+len(prefix))
	if subscribe {
		b[0] = 1
	}
	copy(b[1:], prefix)
	return b
}

// RawNullHandshake dials addr, greets as ZMTP 3.minor with NULL, completes
// the NULL handshake with socketType plus meta, and returns the connection
// (closed when t ends) once the server's READY has been read.
func RawNullHandshake(t testing.TB, addr string, minor byte, socketType string, meta map[string]string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.SetDeadline(time.Now().Add(10*time.Second)))
	_, err = c.Write(Greeting("NULL", minor))
	require.NoError(t, err)
	_, err = io.ReadFull(c, make([]byte, 64))
	require.NoError(t, err, "server greeting")
	props := map[string]string{"Socket-Type": socketType}
	for k, v := range meta {
		props[k] = v
	}
	// A NULL server is passive: the client sends READY first.
	_, err = c.Write(Command("READY", Metadata(props)))
	require.NoError(t, err)
	var hdr [2]byte
	_, err = io.ReadFull(c, hdr[:])
	require.NoError(t, err, "server READY header")
	require.Equal(t, byte(0x04), hdr[0], "expected a COMMAND frame")
	_, err = io.ReadFull(c, make([]byte, int(hdr[1])))
	require.NoError(t, err, "server READY body")
	return c
}
