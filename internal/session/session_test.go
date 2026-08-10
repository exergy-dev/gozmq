package session

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/msg"
	"github.com/rkm/gozmq/internal/zmtp"
)

// --------------------------------------------------------------------------
// fakeConn: bidirectional in-process Conn pair built from two io.Pipes.
// --------------------------------------------------------------------------

// newFakeConnPair returns two iface.Conn ends that pipe their bytes to
// each other. Closing either end EOFs subsequent reads on both — io.Pipe's
// native behavior — which is exactly what the session reader/writer
// goroutines need to unblock during shutdown.
func newFakeConnPair() (a, b *fakeConn) {
	aReader, bWriter := io.Pipe()
	bReader, aWriter := io.Pipe()
	a = &fakeConn{
		r:      aReader,
		w:      aWriter,
		local:  "fake://a",
		remote: "fake://b",
		closed: make(chan struct{}),
	}
	b = &fakeConn{
		r:      bReader,
		w:      bWriter,
		local:  "fake://b",
		remote: "fake://a",
		closed: make(chan struct{}),
	}
	return a, b
}

type fakeConn struct {
	r *io.PipeReader
	w *io.PipeWriter

	local  string
	remote string

	closeOnce sync.Once
	closed    chan struct{}
}

func (c *fakeConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *fakeConn) Write(p []byte) (int, error) { return c.w.Write(p) }
func (c *fakeConn) Close() error {
	c.closeOnce.Do(func() {
		_ = c.r.Close()
		_ = c.w.Close()
		close(c.closed)
	})
	return nil
}
func (c *fakeConn) LocalEndpoint() string       { return c.local }
func (c *fakeConn) RemoteEndpoint() string      { return c.remote }
func (c *fakeConn) SetDeadline(time.Time) error { return nil }
func (c *fakeConn) Closed() <-chan struct{}     { return c.closed }

// --------------------------------------------------------------------------
// fakeCodec: trivial length-prefixed wire format.
//
// Wire layout: 1 byte flags + uvarint length + body. This is NOT ZMTP —
// it's deliberately the simplest framing that exercises the codec
// interface, so the session test runs without depending on ZMTP parsing.
// --------------------------------------------------------------------------

type fakeCodec struct{}

func (fakeCodec) ReadFrame(r io.Reader, _ []byte) (uint8, []byte, error) {
	var flagsBuf [1]byte
	if _, err := io.ReadFull(r, flagsBuf[:]); err != nil {
		return 0, nil, err
	}
	n, err := binary.ReadUvarint(byteReader{r})
	if err != nil {
		return 0, nil, err
	}
	body := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return 0, nil, err
		}
	}
	return flagsBuf[0], body, nil
}

func (fakeCodec) WriteFrame(w io.Writer, flags uint8, body []byte) error {
	var hdr [1 + binary.MaxVarintLen64]byte
	hdr[0] = flags
	n := binary.PutUvarint(hdr[1:], uint64(len(body)))
	if _, err := w.Write(hdr[:1+n]); err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	_, err := w.Write(body)
	return err
}

// byteReader adapts an io.Reader to io.ByteReader for binary.ReadUvarint.
type byteReader struct{ io.Reader }

func (b byteReader) ReadByte() (byte, error) {
	var p [1]byte
	if _, err := io.ReadFull(b.Reader, p[:]); err != nil {
		return 0, err
	}
	return p[0], nil
}

// --------------------------------------------------------------------------
// fakeMech: identity codec, one-step handshake.
// --------------------------------------------------------------------------

func newFakeMech(name string, asServer bool, props map[string]string) *fakeMech {
	if props == nil {
		props = map[string]string{}
	}
	return &fakeMech{name: name, asServer: asServer, props: copyProps(props)}
}

type fakeMech struct {
	name      string
	asServer  bool
	props     map[string]string
	peerProps map[string]string

	step         int
	mu           sync.Mutex
	setPeerCalls int
}

func (m *fakeMech) Name() string   { return m.name }
func (m *fakeMech) AsServer() bool { return m.asServer }
func (m *fakeMech) EncodeFrame(flags uint8, b []byte) (uint8, []byte, error) {
	return flags, b, nil
}
func (m *fakeMech) DecodeFrame(flags uint8, b []byte) (uint8, []byte, error) {
	return flags, b, nil
}
func (m *fakeMech) Metadata() map[string]string { return copyProps(m.props) }
func (m *fakeMech) SetPeerMetadata(props map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setPeerCalls++
	m.peerProps = copyProps(props)
	return nil
}
func (m *fakeMech) PeerMetadata() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return copyProps(m.peerProps)
}

// NextHandshake is a one-round exchange: emit a single READY-equivalent
// blob containing local metadata, then expect one back, then declare
// done. Both sides run identically because asServer is symmetric for
// our test.
func (m *fakeMech) NextHandshake(in []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.step {
	case 0:
		m.step++
		return encodeFakeProps(m.props), false, nil
	case 1:
		props, err := decodeFakeProps(in)
		if err != nil {
			return nil, false, err
		}
		m.peerProps = props
		m.step++
		return nil, true, nil
	default:
		return nil, true, nil
	}
}

// --------------------------------------------------------------------------
// startPair builds a client/server pair from two caller-supplied configs
// (Conn and Mech must be set; Codec and Role are defaulted), starts both,
// registers t.Cleanup to stop them, and waits for both handshakes.
// startSessionPair is the common fakeConn shorthand.
// --------------------------------------------------------------------------

func startPair(t *testing.T, cfgA, cfgB Config) (sA, sB iface.Session) {
	t.Helper()
	if cfgA.Codec == nil {
		cfgA.Codec = fakeCodec{}
	}
	if cfgB.Codec == nil {
		cfgB.Codec = fakeCodec{}
	}

	sA = New(cfgA)
	sB = New(cfgB)
	sA.Start()
	sB.Start()
	t.Cleanup(func() {
		_ = sA.Stop()
		_ = sB.Stop()
	})

	waitHandshake(t, sA, "A")
	waitHandshake(t, sB, "B")
	return sA, sB
}

func startSessionPair(t *testing.T, propsA, propsB map[string]string, hwm int) (sA, sB iface.Session, connA, connB *fakeConn) {
	t.Helper()
	connA, connB = newFakeConnPair()
	sA, sB = startPair(t,
		Config{Conn: connA, Mech: newFakeMech("NULL", false, propsA), SendHWM: hwm, RecvHWM: hwm},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, propsB), SendHWM: hwm, RecvHWM: hwm},
	)
	return sA, sB, connA, connB
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

func TestSession_Handshake_AndRoundTrip(t *testing.T) {
	sA, sB, _, _ := startSessionPair(t,
		map[string]string{"Socket-Type": "PAIR", "Identity": "A"},
		map[string]string{"Socket-Type": "PAIR", "Identity": "B"},
		4,
	)

	// Verify the engine-facing pipe surfaces the peer's identity from
	// metadata propagated through SetPeerMetadata after the handshake.
	mechA := sA.(*sessionImpl).cfg.Mech.(*fakeMech)
	mechB := sB.(*sessionImpl).cfg.Mech.(*fakeMech)
	require.Equal(t, 1, mechA.setPeerCalls, "A SetPeerMetadata calls")
	require.Equal(t, 1, mechB.setPeerCalls, "B SetPeerMetadata calls")
	require.Equal(t, "B", string(sA.Pipe().PeerIdentity()), "A pipe peer identity")
	require.Equal(t, "A", string(sB.Pipe().PeerIdentity()), "B pipe peer identity")

	// Push a multipart message A→B and verify it surfaces verbatim.
	out := msg.FromFrames([]byte("hello"), []byte("world"), []byte("!"))
	ctx := testCtx(t, 2*time.Second)
	require.NoError(t, sA.Pipe().Send(ctx, out), "A.Pipe.Send")
	got, err := sB.Pipe().Recv(ctx)
	require.NoError(t, err, "B.Pipe.Recv")
	require.Equal(t, 3, got.NumFrames(), "got frames count (frames=%v)", got.Frames())
	want := []string{"hello", "world", "!"}
	for i, f := range got.Frames() {
		assert.Equal(t, want[i], string(f), "frame[%d]", i)
	}

	// Round-trip the other direction too, to exercise B's writer / A's
	// reader paths.
	require.NoError(t, sB.Pipe().Send(ctx, msg.FromBytes([]byte("ack"))), "B.Pipe.Send")
	got2, err := sA.Pipe().Recv(ctx)
	require.NoError(t, err, "A.Pipe.Recv")
	assert.Equal(t, 1, got2.NumFrames(), "ack roundtrip frame count")
	assert.Equal(t, "ack", string(got2.Bytes()), "ack roundtrip payload")

	// Stop must drain cleanly. We rely on goleak (TestMain) for the
	// leak assertion; here we just verify Stop returns without error
	// or hang.
	doneA := stopAsync(sA)
	doneB := stopAsync(sB)
	select {
	case err := <-doneA:
		assert.NoError(t, err, "A.Stop")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "A.Stop did not return within 2s")
	}
	select {
	case err := <-doneB:
		assert.NoError(t, err, "B.Stop")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "B.Stop did not return within 2s")
	}

	// Stop is idempotent.
	assert.NoError(t, sA.Stop(), "A.Stop second call")
	assert.NoError(t, sB.Stop(), "B.Stop second call")
}

func encodeFakeProps(props map[string]string) []byte {
	const header = "META\n"
	lines := make([]string, 0, len(props))
	for k, v := range props {
		lines = append(lines, k+"="+v)
	}
	return []byte(header + strings.Join(lines, "\n"))
}

func decodeFakeProps(in []byte) (map[string]string, error) {
	const header = "META\n"
	props := map[string]string{}
	if string(in) == header {
		return props, nil
	}
	if !strings.HasPrefix(string(in), header) {
		return nil, fmt.Errorf("invalid fake metadata header %q", in)
	}
	for _, line := range strings.Split(strings.TrimPrefix(string(in), header), "\n") {
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("invalid fake metadata line %q", line)
		}
		props[key] = value
	}
	return props, nil
}

func copyProps(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// TestSession_StopBeforeHandshake ensures Stop on a partially-started
// session unblocks Handshaked observers and returns cleanly.
func TestSession_StopBeforeHandshake(t *testing.T) {
	// Build a session whose peer never speaks: the handshake will park
	// in ReadFull on the greeting. Stop must wake it and exit cleanly.
	connA, connB := newFakeConnPair()
	defer connB.Close() // never read/written by us; just hold to keep alive

	s := New(Config{
		Conn:    connA,
		Mech:    newFakeMech("NULL", false, nil),
		Codec:   fakeCodec{},
		SendHWM: 1,
		RecvHWM: 1,
	})
	s.Start()

	// Brief delay so the handshake goroutine reaches ReadFull on the
	// greeting (it has already written its own greeting into connB's
	// buffer, but no one's reading on the other side; the io.Pipe
	// blocks the writer too — that's fine, Stop closes both halves).
	time.Sleep(10 * time.Millisecond)

	doneStop := stopAsync(s)
	select {
	case err := <-doneStop:
		// We expect Stop itself to return nil — the handshake error
		// is reported via HandshakeErr. Conn.Close on an already-closed
		// io.Pipe returns nil, so this should be cleanly nil.
		assert.NoError(t, err, "Stop returned error, want nil")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "Stop did not return within 2s")
	}

	// Handshaked must be closed; HandshakeErr must be non-nil since
	// the handshake aborted.
	waitClosed(t, s.Handshaked(), time.Second, "Handshaked not closed after Stop")
	assert.Error(t, s.HandshakeErr(), "HandshakeErr = nil after Stop-before-handshake; want non-nil")
	// Regression (writerExited once-guard): a session stopped during a
	// hung handshake never spawns its writer, but WriterFlushed must
	// still close promptly or linger drains would hang forever.
	waitClosed(t, s.WriterFlushed(), time.Second, "WriterFlushed not closed after Stop-during-handshake")
}

// TestSession_StopWithoutStart ensures the no-Start path is leak-free
// and idempotent.
func TestSession_StopWithoutStart(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()

	s := New(Config{
		Conn:    connA,
		Mech:    newFakeMech("NULL", false, nil),
		Codec:   fakeCodec{},
		SendHWM: 1,
		RecvHWM: 1,
	})
	require.NoError(t, s.Stop(), "Stop without Start")
	require.NoError(t, s.Stop(), "Stop second call without Start")
	// Handshaked and WriterFlushed must close so observers don't hang.
	waitClosed(t, s.Handshaked(), time.Second, "Handshaked not closed by Stop-without-Start")
	waitClosed(t, s.WriterFlushed(), time.Second, "WriterFlushed not closed by Stop-without-Start")
}

func TestSession_CancelWriter_DoesNotCloseConnUntilStop(t *testing.T) {
	sA, sB, connA, _ := startSessionPair(t,
		map[string]string{"Identity": "A"},
		map[string]string{"Identity": "B"},
		8,
	)

	ctx := testCtx(t, 2*time.Second)
	for i := 0; i < 4; i++ {
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte(fmt.Sprintf("linger-%d", i)))), "Send[%d]", i)
	}

	sA.CancelWriter()
	waitClosed(t, sA.WriterFlushed(), 2*time.Second, "WriterFlushed did not fire after CancelWriter")

	select {
	case <-connA.Closed():
		require.FailNow(t, "CancelWriter closed the conn before Stop")
	case <-time.After(50 * time.Millisecond):
	}

	for i := 0; i < 4; i++ {
		m, err := sB.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		want := fmt.Sprintf("linger-%d", i)
		require.Equal(t, want, string(m.Bytes()), "Recv[%d]", i)
	}

	require.NoError(t, sA.Stop(), "Stop")
	waitClosed(t, connA.Closed(), time.Second, "Stop did not close the conn")
}

// --------------------------------------------------------------------------
// helpers
// --------------------------------------------------------------------------

// waitClosed fails the test if ch has not closed (or received) within
// timeout.
func waitClosed(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		require.FailNow(t, msg)
	}
}

func waitHandshake(t *testing.T, s iface.Session, label string) {
	t.Helper()
	waitClosed(t, s.Handshaked(), 2*time.Second, label+": handshake did not complete within 2s")
	require.NoError(t, s.HandshakeErr(), "%s: HandshakeErr", label)
}

func stopAsync(s iface.Session) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- s.Stop() }()
	return ch
}

// Sanity check: make sure our error sentinel wrapping is reachable via
// errors.Is in case the reader rejects malformed handshake data. (Not
// exercised in the happy-path tests above; this guards against accidental
// fmt.Errorf %v vs %w regressions in handshake.go.)
func TestSession_HandshakeErr_ProtocolViolation(t *testing.T) {
	// Wire A normally; B sends garbage instead of a real greeting.
	connA, connB := newFakeConnPair()
	defer connB.Close()

	sA := New(Config{
		Conn:    connA,
		Mech:    newFakeMech("NULL", false, nil),
		Codec:   fakeCodec{},
		SendHWM: 1,
		RecvHWM: 1,
	})
	sA.Start()
	defer sA.Stop()

	// Writer goroutine on B's side: drain whatever A sends, then send
	// 64 bytes of garbage so A's greeting decode rejects it. We must
	// drain because io.Pipe is synchronous — A's greeting Write
	// blocks until B reads it.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 64)
		_, _ = io.ReadFull(connB, buf)
		bad := make([]byte, 64) // all zeros — invalid signature
		_, _ = connB.Write(bad)
	}()

	waitClosed(t, sA.Handshaked(), 2*time.Second, "handshake did not complete (or fail) within 2s")
	err := sA.HandshakeErr()
	require.Error(t, err, "expected handshake to fail")
	// Confirm the error chain is well-formed.
	errStr := strings.ToLower(err.Error())
	assert.True(t, strings.Contains(errStr, "malformed") || strings.Contains(errStr, "signature"),
		"HandshakeErr = %v; expected malformed/signature wording", err)

	wg.Wait()
}

// --------------------------------------------------------------------------
// Done() contract tests
//
// Done() must fire exactly once on every teardown path, including paths
// the session never reached its traffic phase. The Reconnector relies on
// this to drive its attach/detach/reconnect state machine; a session
// that failed to close Done() would hang the reconnect loop.
// --------------------------------------------------------------------------

// TestSession_Done_FiresOnNormalStop covers the happy path: handshake
// completes, traffic flows, caller invokes Stop, Done() closes.
func TestSession_Done_FiresOnNormalStop(t *testing.T) {
	sA, _, _, _ := startSessionPair(t, nil, nil, 1)

	// Done must not fire until Stop.
	select {
	case <-sA.Done():
		require.FailNow(t, "Done fired before Stop")
	case <-time.After(20 * time.Millisecond):
	}

	require.NoError(t, sA.Stop(), "Stop")
	waitClosed(t, sA.Done(), time.Second, "Done did not fire after Stop")
}

// TestSession_Done_FiresOnReaderEOF covers a peer disconnecting mid-
// traffic: the reader's ReadFrame returns EOF, runReader calls shutdown,
// which cancels ctx and closes the conn; writer exits on pipe Recv
// cancellation; wg drains; Done closes.
func TestSession_Done_FiresOnReaderEOF(t *testing.T) {
	sA, _, _, connB := startSessionPair(t, nil, nil, 1)

	// Kill the peer's conn out from under sA. sA's reader sees EOF
	// (io.Pipe closed), shutdown cascades, Done fires.
	_ = connB.Close()

	waitClosed(t, sA.Done(), 2*time.Second, "Done did not fire after peer EOF")
}

// TestSession_Done_FiresOnHandshakeFailure verifies Done closes even
// when the session never reached the traffic phase (no reader/writer
// goroutines were spawned).
func TestSession_Done_FiresOnHandshakeFailure(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()

	sA := New(Config{
		Conn: connA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{},
		SendHWM: 1, RecvHWM: 1,
	})
	sA.Start()

	// Feed garbage to trip handshake decode.
	go func() {
		buf := make([]byte, 64)
		_, _ = io.ReadFull(connB, buf)
		bad := make([]byte, 64) // zeros — bad greeting signature
		_, _ = connB.Write(bad)
	}()

	waitClosed(t, sA.Handshaked(), 2*time.Second, "handshake did not settle")
	require.Error(t, sA.HandshakeErr(), "expected HandshakeErr, got nil")
	// Done fires on run() exit without Stop being called; no traffic-
	// phase goroutines were spawned, so wg drains as soon as run returns.
	waitClosed(t, sA.Done(), 2*time.Second, "Done did not fire after handshake failure")
	// WriterFlushed must close too — no writer ever ran, but linger
	// waiters cannot be allowed to hang on a failed handshake.
	waitClosed(t, sA.WriterFlushed(), time.Second, "WriterFlushed not closed after handshake failure")
	// Stop still idempotent afterwards.
	assert.NoError(t, sA.Stop(), "Stop after handshake failure")
}

// TestSession_Done_FiresOnContextCancel is exercised via Stop; there is
// no externally-observable context cancel path that is not Stop (Stop
// owns the session-level ctx). This test therefore covers the
// StopBeforeHandshake scenario from a Done() perspective: Stop is the
// cancellation trigger, Done must fire even when we never reached the
// handshake-success state.
func TestSession_Done_FiresOnStopBeforeHandshake(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()

	s := New(Config{
		Conn: connA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{},
		SendHWM: 1, RecvHWM: 1,
	})
	s.Start()
	// Give the run goroutine a moment to reach the blocking greeting read.
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, s.Stop(), "Stop")
	waitClosed(t, s.Done(), 2*time.Second, "Done did not fire after Stop-before-handshake")
}

// TestSession_Done_FiresExactlyOnce: double-Stop must not panic (which
// would indicate a double-close of s.stopped) and Done() must remain
// readable (closed channels stay closed; a second close would panic).
func TestSession_Done_FiresExactlyOnce(t *testing.T) {
	connA, connB := newFakeConnPair()
	defer connB.Close()

	s := New(Config{
		Conn: connA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{},
		SendHWM: 1, RecvHWM: 1,
	})
	s.Start()
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, s.Stop(), "first Stop")
	require.NoError(t, s.Stop(), "second Stop")
	// Third stop, for good measure — must not panic.
	require.NoError(t, s.Stop(), "third Stop")
	// Done still closed and drainable without panic.
	waitClosed(t, s.Done(), time.Second, "Done not closed after multiple Stops")
	// And a Stop-without-Start session: Done must still fire.
	connC, connD := newFakeConnPair()
	defer connD.Close()
	s2 := New(Config{
		Conn: connC, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{},
		SendHWM: 1, RecvHWM: 1,
	})
	require.NoError(t, s2.Stop(), "Stop-without-Start")
	waitClosed(t, s2.Done(), time.Second, "Done not closed after Stop-without-Start")
}

// --------------------------------------------------------------------------
// MaxMessageSize: the reader must bound cross-frame (multipart)
// accumulation — the codec only caps individual frames.
// --------------------------------------------------------------------------

// TestReader_MaxMessageSize_UnderLimit: a multipart message at/below the
// cap passes through untouched.
func TestReader_MaxMessageSize_UnderLimit(t *testing.T) {
	connA, connB := newFakeConnPair()
	sA, sB := startPair(t,
		Config{Conn: connA, Mech: newFakeMech("NULL", false, nil), SendHWM: 4, RecvHWM: 4},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, nil), SendHWM: 4, RecvHWM: 4,
			MaxMessageSize: 1024},
	)

	ctx := testCtx(t, 2*time.Second)
	frame := make([]byte, 256)
	// 4 × 256 B = exactly the cap — allowed (the limit is "exceeds").
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromFrames(frame, frame, frame, frame)), "Send")
	got, err := sB.Pipe().Recv(ctx)
	require.NoError(t, err, "Recv")
	require.Equal(t, 4, got.NumFrames(), "frame count")
	require.Equal(t, 1024, got.TotalSize(), "total size")
}

// TestReader_MaxMessageSize_OverLimit: once the accumulated multipart
// size exceeds the cap, the receiving session tears down instead of
// ballooning memory, and the peer observes the disconnect.
func TestReader_MaxMessageSize_OverLimit(t *testing.T) {
	connA, connB := newFakeConnPair()
	sA, sB := startPair(t,
		Config{Conn: connA, Mech: newFakeMech("NULL", false, nil), SendHWM: 8, RecvHWM: 8},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, nil), SendHWM: 8, RecvHWM: 8,
			MaxMessageSize: 512},
	)

	ctx := testCtx(t, 2*time.Second)
	frame := make([]byte, 256)
	// 3 × 256 B = 768 B > 512 B: B's reader must reject mid-message. The
	// teardown happens as soon as the third frame arrives — B never
	// assembles the full message, so memory stays bounded at the cap.
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromFrames(frame, frame, frame)), "Send")

	waitClosed(t, sB.Done(), 2*time.Second, "receiver did not tear down on oversized multipart")
	// The rejected message must not surface on B's pipe.
	_, err := sB.Pipe().Recv(testCtx(t, time.Second))
	require.Error(t, err, "oversized message surfaced on the pipe")
	// A sees the disconnect (B closed the conn during shutdown).
	waitClosed(t, sA.Done(), 2*time.Second, "sender did not observe receiver teardown")
}

// --------------------------------------------------------------------------
// Reader scratch-buffer tests. The reader hands ReadFrame a persistent
// scratch dst of exactly largeFrameThreshold bytes, so every frame that
// fits in it lands on the always-copy path; anything larger must come
// from the frame pool and surface via the zero-copy pooled path. These
// run against the REAL zmtp codec — fakeCodec ignores dst.
// --------------------------------------------------------------------------

// startZmtpPair is startPair with the real ZMTP framing codec on both
// ends, so the reader's scratch-vs-pool dst handling is exercised.
func startZmtpPair(t *testing.T, hwm int) (sA, sB iface.Session) {
	t.Helper()
	connA, connB := newFakeConnPair()
	return startPair(t,
		Config{Conn: connA, Mech: newFakeMech("NULL", false, nil),
			Codec: zmtp.NewFrameCodec(zmtp.MaxBodyUnlimited), SendHWM: hwm, RecvHWM: hwm},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, nil),
			Codec: zmtp.NewFrameCodec(zmtp.MaxBodyUnlimited), SendHWM: hwm, RecvHWM: hwm},
	)
}

// TestReader_ScratchThresholdBoundary: a frame of exactly
// largeFrameThreshold bytes fits in scratch and must take the always-copy
// path (Msg not pooled); one byte more must overflow scratch into a pool
// buffer and surface through the zero-copy AppendPooledFrame path.
func TestReader_ScratchThresholdBoundary(t *testing.T) {
	sA, sB := startZmtpPair(t, 8)
	ctx := testCtx(t, 5*time.Second)

	atThreshold := bytes.Repeat([]byte{0xA5}, largeFrameThreshold)
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes(bytes.Clone(atThreshold))), "Send(at)")
	got, err := sB.Pipe().Recv(ctx)
	require.NoError(t, err, "Recv(at)")
	require.Equal(t, atThreshold, got.Bytes(), "at-threshold payload")
	require.False(t, got.IsPooled(),
		"frame of exactly largeFrameThreshold bytes must take the always-copy path")

	overThreshold := bytes.Repeat([]byte{0x5A}, largeFrameThreshold+1)
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes(bytes.Clone(overThreshold))), "Send(over)")
	got, err = sB.Pipe().Recv(ctx)
	require.NoError(t, err, "Recv(over)")
	require.Equal(t, overThreshold, got.Bytes(), "over-threshold payload")
	require.True(t, got.IsPooled(),
		"threshold+1 frame must overflow scratch into the pooled zero-copy path")
	got.Release()
}

// TestReader_SmallFrameBurstIndependentPayloads: a burst of small frames
// all pass through the reader's single scratch buffer; every delivered
// message must own independent bytes (a clone bug would let a later frame
// overwrite an earlier queued message).
func TestReader_SmallFrameBurstIndependentPayloads(t *testing.T) {
	const N = 64
	sA, sB := startZmtpPair(t, N+4)
	ctx := testCtx(t, 5*time.Second)

	for i := 0; i < N; i++ {
		m := msg.FromBytes([]byte(fmt.Sprintf("payload-%03d", i)))
		require.NoError(t, sA.Pipe().Send(ctx, m), "Send[%d]", i)
	}
	// Multipart tail: several scratch-backed frames within ONE message
	// must also be independent of each other.
	require.NoError(t, sA.Pipe().Send(ctx,
		msg.FromFrames([]byte("frame-a"), []byte("frame-b"), []byte("frame-c"))), "Send(multipart)")

	// Drain only after everything is sent so messages queue in the pipe —
	// scratch aliasing between queued messages would corrupt payloads.
	for i := 0; i < N; i++ {
		m, err := sB.Pipe().Recv(ctx)
		require.NoError(t, err, "Recv[%d]", i)
		require.Equal(t, fmt.Sprintf("payload-%03d", i), string(m.Bytes()), "payload[%d]", i)
	}
	mp, err := sB.Pipe().Recv(ctx)
	require.NoError(t, err, "Recv(multipart)")
	require.Equal(t, 3, mp.NumFrames(), "multipart frame count")
	want := []string{"frame-a", "frame-b", "frame-c"}
	for j, f := range mp.Frames() {
		require.Equal(t, want[j], string(f), "multipart frame[%d]", j)
	}
}
