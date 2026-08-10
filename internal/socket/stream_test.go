package socket_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/stream"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// --------------------------------------------------------------------------
// Unit tests (pre-existing coverage).
// --------------------------------------------------------------------------

// TestStream_InboundTaggedByIdentity asserts that a message arriving on
// pipe-A surfaces through the engine with pipe-A's identity as its
// leading frame, and vice versa for pipe-B.
func TestStream_InboundTaggedByIdentity(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	idA := []byte("peer-alpha")
	idB := []byte("peer-beta")

	engA, peerA := pipe.NewPair(4, 4, pipe.PeerInfo{Identity: idA}, pipe.PeerInfo{})
	engB, peerB := pipe.NewPair(4, 4, pipe.PeerInfo{Identity: idB}, pipe.PeerInfo{})
	defer func() {
		_ = engA.Close()
		_ = peerA.Close()
		_ = engB.Close()
		_ = peerB.Close()
	}()

	mustAttach(t, eng, engA, engB)

	ctx := testCtx(t)

	// Peer A pushes raw bytes; Peer B pushes raw bytes. Each should
	// surface on the engine tagged with the sender's identity.
	require.NoError(t, peerA.Send(ctx, msg.FromBytes([]byte("GET / HTTP/1.1\r\n"))), "peerA send")
	require.NoError(t, peerB.Send(ctx, msg.FromBytes([]byte("HELO example.com\r\n"))), "peerB send")

	// Collect two messages; order is non-deterministic (fair queue).
	got := make(map[string]string)
	for i := 0; i < 2; i++ {
		m, err := eng.Recv(ctx)
		require.NoError(t, err, "recv %d", i)
		require.Equal(t, 2, m.NumFrames(), "recv %d: frames (identity + body)", i)
		id := string(m.Frames()[0])
		body := string(m.Frames()[1])
		require.NotContains(t, got, id, "duplicate identity %q", id)
		got[id] = body
	}
	assert.Equal(t, "GET / HTTP/1.1\r\n", got[string(idA)], "peer A body")
	assert.Equal(t, "HELO example.com\r\n", got[string(idB)], "peer B body")
}

// TestStream_OutboundRoutesByIdentity asserts that Send routes by the
// leading identity frame and that the body frames are concatenated into
// a single raw-bytes frame on the destination pipe.
func TestStream_OutboundRoutesByIdentity(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	idA := []byte("peer-a")
	idB := []byte("peer-b")

	engA, peerA := pipe.NewPair(4, 4, pipe.PeerInfo{Identity: idA}, pipe.PeerInfo{})
	engB, peerB := pipe.NewPair(4, 4, pipe.PeerInfo{Identity: idB}, pipe.PeerInfo{})
	defer func() {
		_ = engA.Close()
		_ = peerA.Close()
		_ = engB.Close()
		_ = peerB.Close()
	}()

	mustAttach(t, eng, engA, engB)

	ctx := testCtx(t)

	// Send a multi-frame body to peer-a; concatenation should land on peerA.
	require.NoError(t, eng.Send(ctx, msg.FromFrames(idA, []byte("HTTP/1.1 "), []byte("200 OK\r\n"))), "send to A")
	// Send a single-frame body to peer-b.
	require.NoError(t, eng.Send(ctx, msg.FromFrames(idB, []byte("250 OK\r\n"))), "send to B")

	mA, err := peerA.Recv(ctx)
	require.NoError(t, err, "peerA recv")
	require.Equal(t, 1, mA.NumFrames(), "peerA frames (concatenated)")
	require.Equal(t, []byte("HTTP/1.1 200 OK\r\n"), mA.Bytes())

	mB, err := peerB.Recv(ctx)
	require.NoError(t, err, "peerB recv")
	require.Equal(t, []byte("250 OK\r\n"), mB.Bytes())
}

// TestStream_UnknownIdentity asserts that Send to an unrecognized
// identity returns ErrRouteUnknown. STREAM is always mandatory — libzmq
// has no quiet-drop mode for unknown peers on this socket type.
func TestStream_UnknownIdentity(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	err := eng.Send(context.Background(), msg.FromFrames([]byte("nope"), []byte("hi")))
	require.ErrorIs(t, err, zmqerr.ErrRouteUnknown)
}

// TestStream_MissingIdentityFrame asserts Send with an empty message
// returns ErrRouteUnknown rather than panicking.
func TestStream_MissingIdentityFrame(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	err := eng.Send(context.Background(), msg.New())
	require.ErrorIs(t, err, zmqerr.ErrRouteUnknown)
}

// TestStream_DetachRemovesRoute asserts that after DetachPipe, sending to
// that pipe's identity returns ErrRouteUnknown (the route is gone).
func TestStream_DetachRemovesRoute(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	id := []byte("detach-me")
	engP, peer := pipe.NewPair(4, 4, pipe.PeerInfo{Identity: id}, pipe.PeerInfo{})
	defer func() {
		_ = engP.Close()
		_ = peer.Close()
	}()

	mustAttach(t, eng, engP)
	require.NoError(t, eng.DetachPipe(engP), "detach")
	err := eng.Send(context.Background(), msg.FromFrames(id, []byte("x")))
	require.ErrorIs(t, err, zmqerr.ErrRouteUnknown, "after detach")
}

// TestStream_AnonymousPeerGetsSynthesizedIdentity asserts that a pipe
// attached with an empty PeerIdentity still surfaces inbound messages
// tagged with *some* non-empty identity (the engine synthesizes one).
// This matches the ROUTER anonymous-peer convention: the app sees a
// stable 5-byte identity it can reuse on Send to address the same peer.
func TestStream_AnonymousPeerGetsSynthesizedIdentity(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	// No Identity on either side.
	engP, peer := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer func() {
		_ = engP.Close()
		_ = peer.Close()
	}()
	mustAttach(t, eng, engP)

	ctx := testCtx(t)

	require.NoError(t, peer.Send(ctx, msg.FromBytes([]byte("raw"))), "peer send")
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, 2, m.NumFrames())
	id := m.Frames()[0]
	require.NotEmpty(t, id, "synthesized identity was empty")
	// Synthesized identities start with a zero byte per libzmq convention.
	assert.Equal(t, byte(0), id[0], "synthesized identity leading byte")

	// Replay the synthesized identity on Send — it should route back.
	require.NoError(t, eng.Send(ctx, msg.FromFrames(id, []byte("reply"))), "send back")
	got, err := peer.Recv(ctx)
	require.NoError(t, err, "peer recv")
	require.Equal(t, []byte("reply"), got.Bytes())
}

// TestStream_CloseUnblocksRecv asserts that Close wakes a pending Recv.
func TestStream_CloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()

	done := make(chan error, 1)
	go func() {
		_, err := eng.Recv(context.Background())
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, eng.Close(), "close")
	select {
	case err := <-done:
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed)
	case <-time.After(time.Second):
		require.Fail(t, "recv did not unblock on close")
	}
}

// --------------------------------------------------------------------------
// Round-trip / scenario tests.
//
// These sit at the pipe boundary rather than over a real TCP socket so the
// engine contract can be exercised in isolation from the transport adapter
// (internal/stream.Adapter) that drives it in production. The fixture below
// simulates that adapter:
//
//   wire (peer side)  <-- pipe.NewPair -->  (engine side) streamEngine
//
// Each pipe is the in-test stand-in for a single TCP conn. Bytes written
// on the peer end surface as a Recv on the engine (tagged with identity);
// bytes the engine Sends to a given identity surface as a Recv on that
// peer end. This is the exact boundary the transport adapter bridges, so
// these tests lock in the engine contract any such adapter must honour.
// --------------------------------------------------------------------------

// streamPeer is the test-side stand-in for one STREAM peer's TCP conn.
type streamPeer struct {
	id       []byte     // identity the engine stamps on messages from this peer
	peer     iface.Pipe // peer-side end of the simulated wire
	engine   iface.Pipe // engine-side end, registered via AttachPipe
	received bytes.Buffer
}

// send pushes raw bytes from the simulated peer toward the engine.
func (p *streamPeer) send(ctx context.Context, b []byte) error {
	return p.peer.Send(ctx, msg.FromBytes(b))
}

// recv reads whatever the engine has written back to this peer, blocking
// until at least one chunk arrives (or ctx cancels). It appends to the
// peer's received buffer and returns the chunk for convenience.
func (p *streamPeer) recv(ctx context.Context) ([]byte, error) {
	m, err := p.peer.Recv(ctx)
	if err != nil {
		return nil, err
	}
	b := m.Bytes()
	p.received.Write(b)
	return b, nil
}

// newStreamPeer wires a new simulated peer onto the engine. The caller
// is responsible for closing both pipe ends.
func newStreamPeer(t *testing.T, eng iface.SocketEngine, id []byte) *streamPeer {
	t.Helper()
	engEnd, peerEnd := pipe.NewPair(8, 8, pipe.PeerInfo{Identity: id}, pipe.PeerInfo{})
	if err := eng.AttachPipe(engEnd); err != nil {
		_ = engEnd.Close()
		_ = peerEnd.Close()
		require.NoError(t, err, "attach")
	}
	return &streamPeer{id: id, peer: peerEnd, engine: engEnd}
}

func (p *streamPeer) close() {
	_ = p.peer.Close()
	_ = p.engine.Close()
}

// TestStream_HTTPEchoRoundTrip models the canonical STREAM use case: an
// HTTP-style client sends a request, a STREAM "server" parses the body
// off the Recv'd Msg (2 frames: identity + body) and echoes that body
// back to the same identity. Asserts both sides see the full byte stream.
func TestStream_HTTPEchoRoundTrip(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	client := newStreamPeer(t, eng, []byte("http-client"))
	defer client.close()

	ctx := testCtx(t)

	// --- client → server ----------------------------------------------------
	const request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	require.NoError(t, client.send(ctx, []byte(request)), "client send")

	// Server-side: Recv yields {identity, body}.
	recvd, err := eng.Recv(ctx)
	require.NoError(t, err, "server recv")
	require.Equal(t, 2, recvd.NumFrames(), "server recv frames (identity + body)")
	gotID := recvd.Frames()[0]
	gotBody := recvd.Frames()[1]
	require.Equal(t, client.id, gotID, "server saw identity")
	require.Equal(t, []byte(request), gotBody, "server saw body")

	// --- server → client (echo) ---------------------------------------------
	require.NoError(t, eng.Send(ctx, msg.FromFrames(gotID, gotBody)), "server echo send")

	echoed, err := client.recv(ctx)
	require.NoError(t, err, "client recv echo")
	require.Equal(t, []byte(request), echoed, "client echo")
}

// TestStream_Multiplex runs 3 concurrent STREAM clients against one STREAM
// server. Each client sends a distinct payload; the server must receive
// all 3 tagged with distinct identities and must reply correctly to each
// based on the identity frame it was given.
func TestStream_Multiplex(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	const nPeers = 3
	peers := make([]*streamPeer, nPeers)
	for i := 0; i < nPeers; i++ {
		peers[i] = newStreamPeer(t, eng, []byte(fmt.Sprintf("client-%d", i)))
		defer peers[i].close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Each peer sends its own payload concurrently.
	var wg sync.WaitGroup
	wg.Add(nPeers)
	for i, p := range peers {
		go func(i int, p *streamPeer) {
			defer wg.Done()
			payload := fmt.Sprintf("payload-from-%d", i)
			if err := p.send(ctx, []byte(payload)); err != nil {
				assert.NoError(t, err, "peer %d send", i)
			}
		}(i, p)
	}

	// Server receives all 3; map identity → payload. The order is non-
	// deterministic because FairQueue round-robins across attached pipes.
	got := make(map[string]string, nPeers)
	for i := 0; i < nPeers; i++ {
		m, err := eng.Recv(ctx)
		require.NoError(t, err, "server recv %d", i)
		require.Equal(t, 2, m.NumFrames(), "server recv %d: frames", i)
		id := string(m.Frames()[0])
		body := string(m.Frames()[1])
		require.NotContains(t, got, id, "duplicate identity %q", id)
		got[id] = body
	}
	wg.Wait()

	// Verify each peer's payload arrived under its own identity...
	for i, p := range peers {
		want := fmt.Sprintf("payload-from-%d", i)
		assert.Equal(t, want, got[string(p.id)], "peer %d payload", i)
	}

	// ...and that server replies target the right peer.
	for i, p := range peers {
		reply := fmt.Sprintf("reply-to-%d", i)
		require.NoError(t, eng.Send(ctx, msg.FromFrames(p.id, []byte(reply))), "server reply %d", i)
	}
	for i, p := range peers {
		b, err := p.recv(ctx)
		require.NoError(t, err, "peer %d recv reply", i)
		want := fmt.Sprintf("reply-to-%d", i)
		assert.Equal(t, want, string(b), "peer %d reply", i)
	}
}

// TestStream_PeerDisconnect models a client closing its side of the
// connection. In a real TCP-wired STREAM socket the transport adapter
// would translate the conn.Read EOF into pipe.Close + engine.DetachPipe;
// we simulate that here by closing the peer-side pipe and then calling
// DetachPipe. Asserts:
//
//  1. Before detach, the engine can receive from the peer.
//  2. After detach, Send to the (now-gone) identity returns
//     ErrRouteUnknown (the route is gone, matching libzmq-mandatory).
//  3. The engine's Recv no longer drains from that peer (the FairQueue
//     dropped it).
func TestStream_PeerDisconnect(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	id := []byte("transient-client")
	p := newStreamPeer(t, eng, id)
	// Manual close sequence: do not defer p.close() because we want to
	// observe partial-teardown behavior.

	ctx := testCtx(t)

	// 1. Peer sends; engine receives tagged with identity.
	require.NoError(t, p.send(ctx, []byte("hello")), "peer send")
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "engine recv")
	require.Equal(t, id, m.Frames()[0], "identity")

	// 2. Client closes (simulates EOF on conn.Read → adapter closes pipe
	//    and calls DetachPipe).
	_ = p.peer.Close()
	require.NoError(t, eng.DetachPipe(p.engine), "detach")
	_ = p.engine.Close()

	// 3. Send to the detached identity is now a routing error.
	err = eng.Send(ctx, msg.FromFrames(id, []byte("lost message")))
	require.ErrorIs(t, err, zmqerr.ErrRouteUnknown, "post-detach send")

	// 4. Engine.Recv must not block indefinitely on the detached peer.
	//    Drain briefly: a short-deadline Recv should time out (no peers
	//    left to deliver anything).
	deadline, dc := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer dc()
	_, err = eng.Recv(deadline)
	require.ErrorIs(t, err, context.DeadlineExceeded, "post-detach recv (no peers)")
}

// TestStream_HalfCloseZeroLengthBody asserts the libzmq half-close
// convention: the server Sends a message whose body is empty to signal
// "close this peer." The transport-adapter contract (documented on
// NewStreamEngine) is that it translates that empty-body frame into
// conn.Close. Here we assert the engine's half of that contract: an
// empty body surfaces on the target pipe as a zero-byte single-frame
// message, without dropping or reordering the data queued ahead of it.
func TestStream_HalfCloseZeroLengthBody(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	id := []byte("half-close-me")
	p := newStreamPeer(t, eng, id)
	defer p.close()

	ctx := testCtx(t)

	// 1. Send real data first. Then send the zero-length-body sentinel.
	require.NoError(t, eng.Send(ctx, msg.FromFrames(id, []byte("final payload\n"))), "data send")
	// libzmq variant 1: identity + no body frames (NumFrames=1 on input).
	require.NoError(t, eng.Send(ctx, msg.FromFrames(id)), "identity-only send")
	// libzmq variant 2: identity + single empty body frame.
	require.NoError(t, eng.Send(ctx, msg.FromFrames(id, nil)), "empty-body send")

	// 2. Client sees data, then the zero-length sentinel(s). The engine
	//    forwards a single-frame Msg per Send; the body is the concatenation
	//    of the input body frames (which is "" for the sentinel cases).
	first, err := p.recv(ctx)
	require.NoError(t, err, "recv data")
	require.Equal(t, []byte("final payload\n"), first)

	// Both sentinel variants must surface as a zero-length body.
	for i := 0; i < 2; i++ {
		b, err := p.recv(ctx)
		require.NoError(t, err, "sentinel %d recv", i)
		require.Empty(t, b, "sentinel %d body", i)
	}

	// 3. Data-loss assertion: the cumulative receive buffer must contain
	//    exactly the real payload, no more, no less (empty-body frames
	//    don't add bytes). This catches any future regression where the
	//    engine accidentally drops queued data ahead of the half-close.
	require.Equal(t, "final payload\n", p.received.String(), "cumulative bytes")
}

// TestStream_PeerCloseDrainsInFlight asserts that when a peer's pipe
// closes, a subsequent Recv on that pipe observes io.EOF — the adapter's
// signal to call DetachPipe. This is a boundary test for the transport
// adapter: it must drain the pipe until io.EOF before detaching, or
// in-flight inbound bytes are lost.
func TestStream_PeerCloseDrainsInFlight(t *testing.T) {
	t.Parallel()
	eng := socket.NewStreamEngine()
	defer eng.Close()

	id := []byte("drain-me")
	p := newStreamPeer(t, eng, id)
	defer p.close()

	ctx := testCtx(t)

	// Push data, then close the peer side immediately (no wait).
	require.NoError(t, p.send(ctx, []byte("inflight-a")), "send a")
	require.NoError(t, p.send(ctx, []byte("inflight-b")), "send b")
	_ = p.peer.Close()

	// Engine must still drain the queued-up bytes (order-preserving).
	seen := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		m, err := eng.Recv(ctx)
		require.NoError(t, err, "drain recv %d", i)
		require.Equal(t, id, m.Frames()[0], "drain %d identity", i)
		seen = append(seen, string(m.Frames()[1]))
	}
	require.Equal(t, []string{"inflight-a", "inflight-b"}, seen, "drain order")

	// Engine-side pipe.Recv should now return io.EOF — the adapter's
	// signal to DetachPipe. Use a short-deadline read to confirm.
	dl, dc := context.WithTimeout(context.Background(), time.Second)
	defer dc()
	_, err := p.engine.Recv(dl)
	require.ErrorIs(t, err, io.EOF, "engine pipe.Recv after peer close")
}

// --------------------------------------------------------------------------
// TCP-wired tests. These stand up a real TCP listener, wire both sides'
// conns into internal/stream.Adapter instances, attach the server side's
// adapter pipe to a streamEngine, and exercise the full raw-bytes path
// an application sees through gozmq.Socket(Stream).
// --------------------------------------------------------------------------

// tcpConn wraps *net.TCPConn to satisfy iface.Conn (net.Conn does not
// expose the ZMQ endpoint-string accessors the adapter requires).
type tcpConn struct {
	net.Conn
}

func (c *tcpConn) LocalEndpoint() string  { return "tcp://" + c.Conn.LocalAddr().String() }
func (c *tcpConn) RemoteEndpoint() string { return "tcp://" + c.Conn.RemoteAddr().String() }

// tcpPair spins up a 127.0.0.1 listener, accepts one connection, and
// returns (serverSide, clientSide) wrapped as iface.Conn. Closing either
// side is the caller's responsibility.
func tcpPair(t *testing.T) (srv, cli iface.Conn, close func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")

	type result struct {
		c   net.Conn
		err error
	}
	accepted := make(chan result, 1)
	go func() {
		c, err := l.Accept()
		accepted <- result{c, err}
	}()

	clientConn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		_ = l.Close()
		require.NoError(t, err, "dial")
	}
	r := <-accepted
	if r.err != nil {
		_ = clientConn.Close()
		_ = l.Close()
		require.NoError(t, r.err, "accept")
	}
	_ = l.Close()

	return &tcpConn{Conn: r.c}, &tcpConn{Conn: clientConn}, func() {
		_ = r.c.Close()
		_ = clientConn.Close()
	}
}

// TestStream_TCP_HTTPEcho stands up a STREAM server over real TCP and
// has it echo an HTTP-style request back to the client via identity-
// based routing. End-to-end check on the adapter + engine contract.
func TestStream_TCP_HTTPEcho(t *testing.T) {
	t.Parallel()
	srvConn, cliConn, teardown := tcpPair(t)
	defer teardown()

	eng := socket.NewStreamEngine()
	defer eng.Close()

	srvAd := stream.NewAdapter(context.Background(), srvConn, stream.Config{})
	defer srvAd.Close()
	mustAttach(t, eng, srvAd.EnginePipe())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const request = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"

	// Client (raw net.Conn) sends the request bytes.
	_, err := cliConn.Write([]byte(request))
	require.NoError(t, err, "client write")

	// Server reads the tagged Msg and echoes the body back.
	var gotReq bytes.Buffer
	var peerID []byte
	for gotReq.Len() < len(request) {
		m, err := eng.Recv(ctx)
		require.NoError(t, err, "server recv")
		require.Equal(t, 2, m.NumFrames())
		peerID = m.Frames()[0]
		gotReq.Write(m.Frames()[1])
	}
	require.Equal(t, request, gotReq.String(), "server got")

	// Echo back.
	require.NoError(t, eng.Send(ctx, msg.FromFrames(peerID, gotReq.Bytes())), "server send")

	buf := make([]byte, len(request))
	_, err = io.ReadFull(cliConn, buf)
	require.NoError(t, err, "client read")
	require.Equal(t, request, string(buf), "client got")
}

// TestStream_TCP_Multiplex runs 3 concurrent TCP clients against one
// STREAM server (one adapter per accepted conn), asserts the server
// fair-queues them and can reply to each via its identity.
func TestStream_TCP_Multiplex(t *testing.T) {
	t.Parallel()
	const nClients = 3

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	defer l.Close()
	addr := l.Addr().String()

	eng := socket.NewStreamEngine()
	defer eng.Close()

	// Track adapters for cleanup.
	var adapters []*stream.Adapter
	defer func() {
		for _, ad := range adapters {
			_ = ad.Close()
		}
	}()

	// Accept side: one adapter per incoming conn.
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for i := 0; i < nClients; i++ {
			c, err := l.Accept()
			if err != nil {
				return
			}
			ad := stream.NewAdapter(context.Background(), &tcpConn{Conn: c}, stream.Config{})
			adapters = append(adapters, ad)
			if err := eng.AttachPipe(ad.EnginePipe()); err != nil {
				return
			}
		}
	}()

	// Spin up nClients TCP dialers, each sending a distinct payload.
	clients := make([]net.Conn, nClients)
	for i := 0; i < nClients; i++ {
		c, err := net.Dial("tcp", addr)
		require.NoError(t, err, "dial %d", i)
		clients[i] = c
		defer c.Close()
	}
	<-acceptDone

	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c net.Conn) {
			defer wg.Done()
			payload := fmt.Sprintf("payload-%d\n", i)
			if _, err := c.Write([]byte(payload)); err != nil {
				assert.NoError(t, err, "client %d write", i)
			}
		}(i, c)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Server collects nClients messages (one conn.Read per payload is
	// typical but not guaranteed; if a message arrives split we merge
	// into the slot indexed by peer identity).
	type slot struct {
		id   []byte
		body bytes.Buffer
	}
	slots := make(map[string]*slot)
	for len(slots) < nClients {
		m, err := eng.Recv(ctx)
		require.NoError(t, err, "server recv")
		id := string(m.Frames()[0])
		sl, ok := slots[id]
		if !ok {
			sl = &slot{id: m.Frames()[0]}
			slots[id] = sl
		}
		sl.body.Write(m.Frames()[1])
	}
	wg.Wait()

	// Map payload->identity so we can reply deterministically.
	payloadToID := make(map[string][]byte, nClients)
	for _, sl := range slots {
		payloadToID[sl.body.String()] = sl.id
	}
	for i := 0; i < nClients; i++ {
		want := fmt.Sprintf("payload-%d\n", i)
		require.Contains(t, payloadToID, want, "missing payload %q", want)
	}

	// Reply to each peer; the client should read exactly the reply we
	// sent for its identity.
	for i := 0; i < nClients; i++ {
		payload := fmt.Sprintf("payload-%d\n", i)
		id := payloadToID[payload]
		reply := fmt.Sprintf("reply-%d\n", i)
		require.NoError(t, eng.Send(ctx, msg.FromFrames(id, []byte(reply))), "server reply %d", i)
	}

	for i, c := range clients {
		want := fmt.Sprintf("reply-%d\n", i)
		buf := make([]byte, len(want))
		_, err := io.ReadFull(c, buf)
		require.NoError(t, err, "client %d read", i)
		assert.Equal(t, want, string(buf), "client %d reply", i)
	}
}

// TestStream_TCP_PeerDisconnect asserts that a client closing its TCP
// conn causes the server-side adapter to close its engine pipe,
// surfacing io.EOF on engine recv from that peer (the app-level signal
// to DetachPipe).
func TestStream_TCP_PeerDisconnect(t *testing.T) {
	t.Parallel()
	srvConn, cliConn, teardown := tcpPair(t)
	defer teardown()

	eng := socket.NewStreamEngine()
	defer eng.Close()

	ad := stream.NewAdapter(context.Background(), srvConn, stream.Config{})
	defer ad.Close()
	engPipe := ad.EnginePipe()
	mustAttach(t, eng, engPipe)

	ctx := testCtx(t)

	// Send one chunk, then close the client.
	_, err := cliConn.Write([]byte("hello\n"))
	require.NoError(t, err, "client write")
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "engine recv")
	require.Equal(t, []byte("hello\n"), m.Frames()[1])
	peerID := m.Frames()[0]

	// Close the client — adapter's reader should see EOF, close the
	// pipe; engPipe.Closed() must fire.
	_ = cliConn.Close()

	select {
	case <-engPipe.Closed():
	case <-time.After(2 * time.Second):
		require.Fail(t, "engine pipe did not close after client disconnect")
	}

	// Post-detach, Send to the peer's identity should fail with
	// ErrRouteUnknown after we actually Detach.
	require.NoError(t, eng.DetachPipe(engPipe), "detach")
	err = eng.Send(context.Background(), msg.FromFrames(peerID, []byte("lost")))
	require.ErrorIs(t, err, zmqerr.ErrRouteUnknown, "post-detach send")
}

// TestStream_TCP_HalfClose asserts that sending an empty-body Msg to a
// peer's identity causes the adapter to close its conn — the far side
// sees EOF on subsequent Read.
func TestStream_TCP_HalfClose(t *testing.T) {
	t.Parallel()
	srvConn, cliConn, teardown := tcpPair(t)
	defer teardown()

	eng := socket.NewStreamEngine()
	defer eng.Close()

	ad := stream.NewAdapter(context.Background(), srvConn, stream.Config{})
	defer ad.Close()
	mustAttach(t, eng, ad.EnginePipe())

	ctx := testCtx(t)

	// Prime: have the client write something so the server learns the
	// peer's identity.
	_, err := cliConn.Write([]byte("hi"))
	require.NoError(t, err, "client write")
	m, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	peerID := m.Frames()[0]

	// Server sends a final payload, then the half-close sentinel
	// (identity-only, which the engine translates to an empty-body
	// frame on the outbound pipe).
	require.NoError(t, eng.Send(ctx, msg.FromFrames(peerID, []byte("goodbye\n"))), "data send")
	require.NoError(t, eng.Send(ctx, msg.FromFrames(peerID)), "half-close send")

	// Client must see "goodbye\n" then EOF.
	buf, err := io.ReadAll(cliConn)
	require.NoError(t, err, "client ReadAll")
	require.Equal(t, []byte("goodbye\n"), buf, "client got")
}
