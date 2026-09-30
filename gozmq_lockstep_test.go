package gozmq_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
)

// recvAsync runs s.RecvMsg in a goroutine and reports its outcome.
type recvResult struct {
	m   *gozmq.Msg
	err error
}

func recvAsync(ctx context.Context, s *gozmq.Socket) <-chan recvResult {
	ch := make(chan recvResult, 1)
	go func() {
		m, err := s.RecvMsg(ctx)
		ch <- recvResult{m, err}
	}()
	return ch
}

// requireRecv waits for a parked RecvMsg and asserts it delivered want.
func requireRecv(t *testing.T, ch <-chan recvResult, want string) {
	t.Helper()
	r := <-ch // the RecvMsg's own context bounds the wait
	require.False(t, errors.Is(r.err, io.EOF), "RecvMsg surfaced io.EOF on a transient disconnect")
	require.NoError(t, r.err, "RecvMsg did not deliver the next peer's message")
	require.Equal(t, want, string(r.m.Bytes()))
	r.m.Release()
}

// TestPairReconnectingPeerKeepsWaiting: a connecting PAIR parked in RecvMsg
// when its bound peer goes away keeps waiting and receives from the peer
// that binds in its place, rather than failing with io.EOF.
func TestPairReconnectingPeerKeepsWaiting(t *testing.T) {
	ctx := gozmq.NewContext()
	t.Cleanup(func() { _ = ctx.Close() })
	callCtx := testCtx(t)

	server, err := ctx.NewSocket(gozmq.Pair)
	require.NoError(t, err)
	require.NoError(t, server.Bind("tcp://127.0.0.1:0"))
	endpoint := server.LastEndpoint()

	client, err := ctx.NewSocket(gozmq.Pair, gozmq.WithReconnectInterval(10*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Connect(endpoint))

	require.NoError(t, server.SendBytes(callCtx, []byte("first")))
	got, err := client.RecvBytes(callCtx)
	require.NoError(t, err)
	require.Equal(t, "first", string(got))

	parked := recvAsync(callCtx, client)
	require.NoError(t, server.Close())
	select {
	case r := <-parked:
		require.Failf(t, "RecvMsg returned while no peer was attached", "msg=%v err=%v", r.m, r.err)
	case <-time.After(100 * time.Millisecond):
	}

	server2, err := ctx.NewSocket(gozmq.Pair)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server2.Close() })
	require.NoError(t, server2.Bind(endpoint))
	require.NoError(t, server2.SendBytes(callCtx, []byte("second")))
	requireRecv(t, parked, "second")
}

// TestPairBoundPeerAcceptsNextPeer: a bound PAIR parked in RecvMsg when its
// connected peer disconnects receives from the next peer to connect.
func TestPairBoundPeerAcceptsNextPeer(t *testing.T) {
	ctx, server, client := newSocketPair(t, gozmq.Pair, "tcp://127.0.0.1:0")
	callCtx := testCtx(t)

	require.NoError(t, client.SendBytes(callCtx, []byte("first")))
	got, err := server.RecvBytes(callCtx)
	require.NoError(t, err)
	require.Equal(t, "first", string(got))

	parked := recvAsync(callCtx, server)
	require.NoError(t, client.Close())
	select {
	case r := <-parked:
		require.Failf(t, "RecvMsg returned while no peer was attached", "msg=%v err=%v", r.m, r.err)
	case <-time.After(100 * time.Millisecond):
	}

	client2, err := ctx.NewSocket(gozmq.Pair)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client2.Close() })
	require.NoError(t, client2.Connect(server.LastEndpoint()))
	require.NoError(t, client2.SendBytes(callCtx, []byte("second")))
	requireRecv(t, parked, "second")
}

// TestReqConcurrentSendsOneWins: concurrent SendMsg calls on one REQ put
// exactly one request in flight; the rest fail with an error wrapping
// ErrFSMViolation.
func TestReqConcurrentSendsOneWins(t *testing.T) {
	_, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, "tcp://127.0.0.1:0", nil, nil)
	callCtx := testCtx(t)

	const n = 8
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			<-start
			errs <- req.SendBytes(callCtx, []byte("q"))
		}()
	}
	close(start)
	var ok int
	for i := 0; i < n; i++ {
		err := <-errs
		if err == nil {
			ok++
			continue
		}
		require.ErrorIs(t, err, gozmq.ErrFSMViolation)
	}
	require.Equal(t, 1, ok, "exactly one concurrent Send should win")

	got, err := rep.RecvBytes(callCtx)
	require.NoError(t, err)
	require.Equal(t, "q", string(got))
	require.NoError(t, rep.SendBytes(callCtx, []byte("a")))
	reply, err := req.RecvBytes(callCtx)
	require.NoError(t, err)
	require.Equal(t, "a", string(reply))
}

// TestRepReplyToVanishedRequester: REP's reply to a requester that has
// disconnected is dropped silently (libzmq semantics) and the socket can
// serve the next request.
func TestRepReplyToVanishedRequester(t *testing.T) {
	ctx, rep, req := newSocketPairTypes(t, gozmq.Rep, gozmq.Req, "tcp://127.0.0.1:0", nil, nil)
	callCtx := testCtx(t)

	require.NoError(t, req.SendBytes(callCtx, []byte("q1")))
	got, err := rep.RecvBytes(callCtx)
	require.NoError(t, err)
	require.Equal(t, "q1", string(got))
	require.NoError(t, req.Close())
	require.NoError(t, rep.SendBytes(callCtx, []byte("lost")))

	req2, err := ctx.NewSocket(gozmq.Req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = req2.Close() })
	require.NoError(t, req2.Connect(rep.LastEndpoint()))
	require.NoError(t, req2.SendBytes(callCtx, []byte("q2")))
	got, err = rep.RecvBytes(callCtx)
	require.NoError(t, err)
	require.Equal(t, "q2", string(got))
	require.NoError(t, rep.SendBytes(callCtx, []byte("a2")))
	reply, err := req2.RecvBytes(callCtx)
	require.NoError(t, err)
	require.Equal(t, "a2", string(reply))
}
