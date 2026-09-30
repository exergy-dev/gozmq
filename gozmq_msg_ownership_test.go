package gozmq_test

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
)

// Message-ownership regressions. Received frames used to be backed by a
// refcounted buffer pool, which corrupted data whenever the frames outlived
// the pool's idea of their lifetime. Frames are now ordinary Go memory owned
// by the caller; these tests pin the patterns that used to break. Run them
// with -race.

// largePayload returns a distinct >4 KiB payload for message i.
func largePayload(i, size int) []byte {
	b := make([]byte, size)
	for j := range b {
		b[j] = byte(i) ^ byte(j)
	}
	return b
}

// TestMsgOwnership_ProxyLargeFrameRoundTrip: a received large-frame *Msg
// re-sent verbatim through a second TCP hop must arrive byte-exact.
func TestMsgOwnership_ProxyLargeFrameRoundTrip(t *testing.T) {
	// Two PUSH(bind)→PULL(connect) hops. PUSH blocks until its peer
	// attaches, so no handshake settle is needed before traffic.
	_, source, proxyIn := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", nil, nil)
	_, proxyOut, sink := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", nil, nil)

	const (
		n    = 60
		size = 64 * 1024
	)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	proxyDone := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			m, err := proxyIn.RecvMsg(ctx)
			if err != nil {
				proxyDone <- err
				return
			}
			if err := proxyOut.SendMsg(ctx, m); err != nil {
				proxyDone <- err
				return
			}
		}
		proxyDone <- nil
	}()

	sendDone := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if err := source.SendMsg(ctx, gozmq.NewMsgFromBytes(largePayload(i, size))); err != nil {
				sendDone <- err
				return
			}
		}
		sendDone <- nil
	}()

	for i := 0; i < n; i++ {
		m, err := sink.RecvMsg(ctx)
		require.NoErrorf(t, err, "sink recv %d", i)
		require.Equalf(t, largePayload(i, size), m.Bytes(), "payload %d corrupted in proxy/writer path", i)
	}
	require.NoError(t, <-sendDone, "sender")
	require.NoError(t, <-proxyDone, "proxy")
}

// TestMsgOwnership_ForwardThenRelease: RecvMsg → SendMsg → Release, the
// pattern the docs used to prescribe. Release used to hand the buffers back
// to the pool while the writev still referenced them (most forwarded 8 KiB
// frames arrived corrupted); it is now a no-op.
func TestMsgOwnership_ForwardThenRelease(t *testing.T) {
	_, mid, src := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)
	_, dst, out := newSocketPairTypes(t, gozmq.Pull, gozmq.Push, "tcp://127.0.0.1:0", nil, nil)

	const (
		n    = 2000
		size = 8192
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		for i := 0; i < n; i++ {
			if src.SendBytes(ctx, bytes.Repeat([]byte{byte(i)}, size)) != nil {
				return
			}
		}
	}()
	go func() {
		for i := 0; i < n; i++ {
			m, err := mid.RecvMsg(ctx)
			if err != nil {
				return
			}
			if out.SendMsg(ctx, m) != nil {
				return
			}
			m.Release()
		}
	}()

	bad := 0
	for i := 0; i < n; i++ {
		b, err := dst.RecvBytes(ctx)
		require.NoErrorf(t, err, "recv %d", i)
		if !bytes.Equal(b, bytes.Repeat([]byte{byte(i)}, size)) {
			bad++
		}
	}
	require.Zerof(t, bad, "%d of %d forwarded messages arrived corrupted", bad, n)
}

// TestMsgOwnership_RetainedFrameSurvivesGC: a frame kept after its *Msg is
// dropped must not change when the GC runs and more large frames arrive. A
// finalizer on the *Msg used to recycle the buffer under the live frame.
func TestMsgOwnership_RetainedFrameSurvivesGC(t *testing.T) {
	_, push, pull := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const size = 64 * 1024
	want := bytes.Repeat([]byte{'A'}, size)
	for i := 0; i < 20; i++ {
		require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(bytes.Clone(want))))
		m, err := pull.RecvMsg(ctx)
		require.NoError(t, err)
		frame := m.Frames()[0] // keep only the bytes; the *Msg is dropped
		runtime.GC()
		runtime.GC()
		for j := 0; j < 4; j++ {
			require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(bytes.Repeat([]byte{'B'}, size))))
			_, err := pull.RecvMsg(ctx)
			require.NoError(t, err)
		}
		require.Truef(t, bytes.Equal(want, frame), "retained frame changed (iteration %d)", i)
	}
}

// TestMsgOwnership_EchoDeferRelease: forwarding a received message to an
// inproc peer and releasing it on return (the old documented idiom) must
// not corrupt what the peer receives, even as more frames arrive.
func TestMsgOwnership_EchoDeferRelease(t *testing.T) {
	_, push, pull := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", nil, nil)
	_, fwd, sink := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "inproc://msg-ownership-echo", nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const size = 64 * 1024
	want := bytes.Repeat([]byte{'A'}, size)
	for i := 0; i < 20; i++ {
		require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(bytes.Clone(want))))
		func() {
			m, err := pull.RecvMsg(ctx)
			require.NoError(t, err)
			defer m.Release()
			require.NoError(t, fwd.SendMsg(ctx, m))
		}()
		require.NoError(t, push.SendMsg(ctx, gozmq.NewMsgFromBytes(bytes.Repeat([]byte{'B'}, size))))
		_, err := pull.RecvMsg(ctx)
		require.NoError(t, err)
		got, err := sink.RecvMsg(ctx)
		require.NoError(t, err)
		require.Truef(t, bytes.Equal(want, got.Frames()[0]), "forwarded frame corrupted (iteration %d)", i)
	}
}

// TestMsgOwnership_ProxyRouterCaptureRace: a ROUTER frontend with a capture
// socket hands the same *Msg to the backend and to the capture tap. ROUTER
// Send used to move pool ownership out of the caller's message, racing the
// tap's concurrent read of it (-race report).
func TestMsgOwnership_ProxyRouterCaptureRace(t *testing.T) {
	_, front, client := newSocketPairTypes(t, gozmq.Router, gozmq.Dealer, "tcp://127.0.0.1:0", nil, nil)
	_, back, worker := newSocketPairTypes(t, gozmq.Dealer, gozmq.Dealer, "tcp://127.0.0.1:0", nil, nil)
	_, capture := newTestSocket(t, gozmq.Pub)
	require.NoError(t, capture.Bind("inproc://msg-ownership-capture"))

	pctx, pcancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gozmq.Proxy(pctx, front, back, capture) }()
	defer func() { pcancel(); <-done }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { // echo worker
		for {
			m, err := worker.RecvMsg(ctx)
			if err != nil {
				return
			}
			if worker.SendMsg(ctx, m) != nil {
				return
			}
		}
	}()
	body := bytes.Repeat([]byte{'z'}, 64*1024)
	for i := 0; i < 50; i++ {
		require.NoError(t, client.SendMsg(ctx, gozmq.NewMsgFromBytes(body)))
		m, err := client.RecvMsg(ctx)
		require.NoError(t, err)
		require.Truef(t, bytes.Equal(body, m.Frames()[0]), "reply %d corrupted", i)
	}
}
