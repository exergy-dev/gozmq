package gozmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/stretchr/testify/require"
)

// TestProxy_PooledLargeFrameRoundTrip is a regression test for a writer
// use-after-free (R1): the session writer recycled a message's pooled
// frame buffers BEFORE the pending writev consumed them. For large
// (>4 KiB) inbound frames under NULL security over TCP, the reader hands
// the engine a *Msg backed by a size-tiered pool buffer. When a proxy
// re-sends that received *Msg, it flows straight into the writer; the
// premature Recycle returned the buffer to the pool while the writev
// still aliased it, allowing a concurrent reader on another session to
// hand the same buffer out and corrupt the bytes mid-flight.
//
// The two-hop proxy below drives exactly that path: source PUSH ->
// proxyIn PULL (receives pooled large frames) -> proxyOut PUSH (re-sends
// the pooled *Msg) -> sink PULL (verifies byte-exact delivery). Runs many
// large distinct payloads so the pool is actively cycled while writes are
// in flight; run with -race to catch the aliased bufpool.Put.
func TestProxy_PooledLargeFrameRoundTrip(t *testing.T) {
	// Two PUSH(bind)→PULL(connect) hops. PUSH blocks until its peer
	// attaches, so no handshake settle is needed before traffic.
	_, source, proxyIn := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", nil, nil)
	_, proxyOut, sink := newSocketPairTypes(t, gozmq.Push, gozmq.Pull, "tcp://127.0.0.1:0", nil, nil)

	const (
		n    = 60
		size = 64 * 1024 // > largeFrameThreshold (4 KiB); a real pool tier.
	)
	want := func(i int) []byte {
		b := make([]byte, size)
		for j := range b {
			b[j] = byte(i) ^ byte(j)
		}
		return b
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Proxy goroutine: receive a pooled *Msg and re-send it verbatim. We
	// must NOT Release after handing to SendMsg — the writer recycles the
	// pooled buffers once the send completes (ownership handoff).
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

	// Sender goroutine: push n distinct large payloads into the proxy.
	sendDone := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if err := source.SendMsg(ctx, gozmq.NewMsgFromBytes(want(i))); err != nil {
				sendDone <- err
				return
			}
		}
		sendDone <- nil
	}()

	// Verify byte-exact, in-order delivery at the sink.
	for i := 0; i < n; i++ {
		m, err := sink.RecvMsg(ctx)
		require.NoErrorf(t, err, "sink recv %d", i)
		require.Equalf(t, want(i), m.Bytes(), "payload %d corrupted in proxy/writer path", i)
		m.Release()
	}

	require.NoError(t, <-sendDone, "sender")
	require.NoError(t, <-proxyDone, "proxy")
}
