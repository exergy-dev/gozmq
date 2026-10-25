package session

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/msg"
	"github.com/exergy-dev/gozmq/security"
)

// --------------------------------------------------------------------------
// fakeConn: bidirectional in-process Conn pair built from two io.Pipes.
// --------------------------------------------------------------------------

// newFakeConnPair returns two transport.Conn ends that pipe their bytes to
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

func (fakeCodec) ReadFrame(r io.Reader, _ []byte, _ zmtp.BodyPool) (uint8, []byte, error) {
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

func (fakeCodec) AppendFrameHeader(dst []byte, flags uint8, bodyLen int) ([]byte, error) {
	return binary.AppendUvarint(append(dst, flags), uint64(bodyLen)), nil
}

// WriteFrame is the test peers' helper for writing one fake frame.
func (c fakeCodec) WriteFrame(w io.Writer, flags uint8, body []byte) error {
	hdr, _ := c.AppendFrameHeader(nil, flags, len(body))
	_, err := w.Write(append(hdr, body...))
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

	step          int
	mu            sync.Mutex
	peerMetaCalls int
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
func (m *fakeMech) PeerMetadata() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peerMetaCalls++
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
// tSession is a started Session plus the handshake outcome its callbacks
// report, so tests can wait for the handshake and reach the engine pipe.
// --------------------------------------------------------------------------

type tSession struct {
	*Session
	handshaked chan struct{} // closed by OnReady, or by OnClosed if it never ran
	pipe       *pipe.End
	hsErr      error
}

func startTestSession(cfg Config) *tSession {
	ts := &tSession{handshaked: make(chan struct{})}
	cfg.OnReady = func(p *pipe.End) error {
		ts.pipe = p
		close(ts.handshaked)
		return nil
	}
	cfg.OnClosed = func(err error, attached bool) {
		if !attached {
			ts.hsErr = err
			close(ts.handshaked)
		}
	}
	ts.Session = New(cfg)
	ts.Start()
	return ts
}

func (ts *tSession) Handshaked() <-chan struct{} { return ts.handshaked }
func (ts *tSession) HandshakeErr() error         { return ts.hsErr }
func (ts *tSession) Pipe() *pipe.End             { return ts.pipe }

// --------------------------------------------------------------------------
// startPair builds a client/server pair from two caller-supplied configs
// (Conn and Mech must be set; Codec and Role are defaulted), starts both,
// registers t.Cleanup to stop them, and waits for both handshakes.
// startSessionPair is the common fakeConn shorthand.
// --------------------------------------------------------------------------

func startPair(t *testing.T, cfgA, cfgB Config) (sA, sB *tSession) {
	t.Helper()
	if cfgA.Codec == nil {
		cfgA.Codec = fakeCodec{}
	}
	if cfgB.Codec == nil {
		cfgB.Codec = fakeCodec{}
	}

	sA = startTestSession(cfgA)
	sB = startTestSession(cfgB)
	t.Cleanup(func() {
		_ = sA.Stop()
		_ = sB.Stop()
	})

	waitHandshake(t, sA, "A")
	waitHandshake(t, sB, "B")
	return sA, sB
}

func startSessionPair(t *testing.T, propsA, propsB map[string]string, hwm int) (sA, sB *tSession, connA, connB *fakeConn) {
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
	t.Parallel()
	sA, sB, _, _ := startSessionPair(t,
		map[string]string{"Socket-Type": "PAIR", "Identity": "A"},
		map[string]string{"Socket-Type": "PAIR", "Identity": "B"},
		4,
	)

	// Verify the engine-facing pipe surfaces the peer's identity from
	// metadata the session read through Mech.PeerMetadata after the handshake.
	mechA := sA.cfg.Mech.(*fakeMech)
	mechB := sB.cfg.Mech.(*fakeMech)
	require.Positive(t, mechA.peerMetaCalls, "A PeerMetadata calls")
	require.Positive(t, mechB.peerMetaCalls, "B PeerMetadata calls")
	require.Equal(t, "B", string(sA.Pipe().PeerIdentity()), "A pipe peer identity")
	require.Equal(t, "A", string(sB.Pipe().PeerIdentity()), "B pipe peer identity")

	// Push a multipart message A→B and verify it surfaces verbatim.
	out := msg.FromFrames([]byte("hello"), []byte("world"), []byte("!"))
	ctx := testutil.Ctx(t, 2*time.Second)
	require.NoError(t, sA.Pipe().Send(ctx, out), "A.Pipe.Send")
	got, err := sB.Pipe().RecvOr(ctx, nil)
	require.NoError(t, err, "B.Pipe.Recv")
	require.Equal(t, 3, got.NumFrames(), "got frames count (frames=%v)", got.Frames())
	want := []string{"hello", "world", "!"}
	for i, f := range got.Frames() {
		assert.Equal(t, want[i], string(f), "frame[%d]", i)
	}

	// Round-trip the other direction too, to exercise B's writer / A's
	// reader paths.
	require.NoError(t, sB.Pipe().Send(ctx, msg.FromBytes([]byte("ack"))), "B.Pipe.Send")
	got2, err := sA.Pipe().RecvOr(ctx, nil)
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
	t.Parallel()
	// Build a session whose peer never speaks: the handshake will park
	// in ReadFull on the greeting. Stop must wake it and exit cleanly.
	connA, connB := newFakeConnPair()
	defer connB.Close() // never read/written by us; just hold to keep alive

	s := startTestSession(Config{
		Conn:    connA,
		Mech:    newFakeMech("NULL", false, nil),
		Codec:   fakeCodec{},
		SendHWM: 1,
		RecvHWM: 1,
	})

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
	// A session stopped during a hung handshake never reaches traffic,
	// but Drain must still finish promptly or linger drains would hang.
	waitClosed(t, s.Drain(), time.Second, "Drain not finished after Stop-during-handshake")
}

func TestSession_Drain_DoesNotCloseConnUntilStop(t *testing.T) {
	t.Parallel()
	sA, sB, connA, _ := startSessionPair(t,
		map[string]string{"Identity": "A"},
		map[string]string{"Identity": "B"},
		8,
	)

	ctx := testutil.Ctx(t, 2*time.Second)
	for i := 0; i < 4; i++ {
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte(fmt.Sprintf("linger-%d", i)))), "Send[%d]", i)
	}

	waitClosed(t, sA.Drain(), 2*time.Second, "Drain did not finish")

	select {
	case <-connA.Closed():
		require.FailNow(t, "Drain closed the conn before Stop")
	case <-time.After(50 * time.Millisecond):
	}

	for i := 0; i < 4; i++ {
		m, err := sB.Pipe().RecvOr(ctx, nil)
		require.NoError(t, err, "Recv[%d]", i)
		want := fmt.Sprintf("linger-%d", i)
		require.Equal(t, want, string(m.Bytes()), "Recv[%d]", i)
	}

	require.NoError(t, sA.Stop(), "Stop")
	waitClosed(t, connA.Closed(), time.Second, "Stop did not close the conn")
}

// xorMech is a fakeMech whose frame transform is NOT the identity, so a
// session that skipped EncodeFrame/DecodeFrame would put plaintext on the
// wire. Its name is configurable to prove the fast path is not chosen by
// Name().
type xorMech struct{ *fakeMech }

func xorBytes(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		out[i] = c ^ 0x5a
	}
	return out
}

func (m xorMech) EncodeFrame(flags uint8, b []byte) (uint8, []byte, error) {
	return flags, xorBytes(b), nil
}
func (m xorMech) DecodeFrame(flags uint8, b []byte) (uint8, []byte, error) {
	return flags, xorBytes(b), nil
}

// identityFramingMech opts into the zero-copy framing path explicitly.
type identityFramingMech struct{ *fakeMech }

func (identityFramingMech) IdentityFraming() bool { return true }

// appendMech is a fakeMech that also implements security.FrameAppender, so
// the writer seals its bodies into the batch scratch.
type appendMech struct{ *fakeMech }

func (appendMech) AppendEncodeFrame(dst []byte, flags uint8, plain []byte) ([]byte, uint8, error) {
	return append(dst, plain...), flags, nil
}

// isIdentity reports how New classifies mech's framing.
func isIdentity(mech security.Mech) bool { return New(Config{Mech: mech}).identityMech }

// TestSession_FramePathsIgnoreMechName: a mechanism named "PLAIN" (or
// "NULL") that transforms frame bodies must still have its EncodeFrame/
// DecodeFrame applied; only a mechanism that declares IdentityFraming
// takes the bypass.
func TestSession_FramePathsIgnoreMechName(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"PLAIN", "NULL"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			connA, connB := newFakeConnPair()
			sA, sB := startPair(t,
				Config{Conn: connA, Mech: xorMech{newFakeMech(name, false, nil)}, SendHWM: 4, RecvHWM: 4},
				// B advertises the same name (the greeting rejects a
				// mismatch) but its fake mech frames as the identity
				// transform, so it sees the raw wire bytes A produced.
				Config{Conn: connB, Mech: newFakeMech(name, true, nil), SendHWM: 4, RecvHWM: 4},
			)
			identity := isIdentity(xorMech{newFakeMech(name, false, nil)})
			require.False(t, identity, "name alone must not select identity framing")
			ctx := testutil.Ctx(t, 2*time.Second)
			require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("hello"))))
			got, err := sB.Pipe().RecvOr(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, xorBytes([]byte("hello")), got.Bytes(), "A's EncodeFrame was bypassed")

			// And the reverse direction: A must DecodeFrame what B sends.
			require.NoError(t, sB.Pipe().Send(ctx, msg.FromBytes(xorBytes([]byte("back")))))
			got, err = sA.Pipe().RecvOr(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, "back", string(got.Bytes()), "A's DecodeFrame was bypassed")
		})
	}

	identity := isIdentity(identityFramingMech{newFakeMech("CUSTOM", false, nil)})
	require.True(t, identity, "IdentityFraming() true must select identity framing")
	identity = isIdentity(newFakeMech("NULL", false, nil))
	require.False(t, identity, "a mech without IdentityFraming takes the generic path")
}

// lingerRecordingConn records SetLinger calls. Stop must close the conn
// plainly: SO_LINGER with a timeout makes close(2) block (and RST on
// expiry), while the kernel already delivers queued data after a normal
// close.
type lingerRecordingConn struct {
	*fakeConn
	lingerCalls atomic.Int32
}

func (c *lingerRecordingConn) SetLinger(int) error {
	c.lingerCalls.Add(1)
	return nil
}

func TestSession_StopDoesNotSetLinger(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	lc := &lingerRecordingConn{fakeConn: connA}
	sA, sB := startPair(t,
		Config{Conn: lc, Mech: newFakeMech("NULL", false, nil), SendHWM: 4, RecvHWM: 4},
		Config{Conn: connB, Mech: newFakeMech("NULL", true, nil), SendHWM: 4, RecvHWM: 4},
	)
	require.NoError(t, sA.Stop())
	require.NoError(t, sB.Stop())
	require.Zero(t, lc.lingerCalls.Load(), "Stop/shutdown must not set SO_LINGER")
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

func waitHandshake(t *testing.T, s *tSession, label string) {
	t.Helper()
	waitClosed(t, s.Handshaked(), 2*time.Second, label+": handshake did not complete within 2s")
	require.NoError(t, s.HandshakeErr(), "%s: HandshakeErr", label)
}

func stopAsync(s *tSession) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- s.Stop() }()
	return ch
}

// Sanity check: make sure our error sentinel wrapping is reachable via
// errors.Is in case the reader rejects malformed handshake data. (Not
// exercised in the happy-path tests above; this guards against accidental
// fmt.Errorf %v vs %w regressions in handshake.go.)
func TestSession_HandshakeErr_ProtocolViolation(t *testing.T) {
	t.Parallel()
	// Wire A normally; B sends garbage instead of a real greeting.
	connA, connB := newFakeConnPair()
	defer connB.Close()

	sA := startTestSession(Config{
		Conn:    connA,
		Mech:    newFakeMech("NULL", false, nil),
		Codec:   fakeCodec{},
		SendHWM: 1,
		RecvHWM: 1,
	})
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

// TestSession_Done_Fires runs every teardown path: the session is started,
// Done must stay open until the row's trigger runs, then close within the
// row's timeout, then the row's extra assertions hold.
func TestSession_Done_Fires(t *testing.T) {
	t.Parallel()
	// startLone starts one session on a fresh conn pair with the peer end
	// unanswered, so the session sits in its greeting read; the peer end is
	// returned for rows that need to drive it.
	startLone := func(t *testing.T) (*tSession, *fakeConn) {
		t.Helper()
		connA, connB := newFakeConnPair()
		t.Cleanup(func() { _ = connB.Close() })
		s := startTestSession(Config{
			Conn: connA, Mech: newFakeMech("NULL", false, nil), Codec: fakeCodec{},
			SendHWM: 1, RecvHWM: 1,
		})
		t.Cleanup(func() { _ = s.Stop() })
		return s, connB
	}
	stopOnce := func(t *testing.T, s *tSession, _ *fakeConn) {
		require.NoError(t, s.Stop(), "Stop")
	}

	cases := []struct {
		name string
		// start returns the session under test and the peer's conn.
		start func(t *testing.T) (*tSession, *fakeConn)
		// trigger starts the teardown.
		trigger func(t *testing.T, s *tSession, peer *fakeConn)
		timeout time.Duration
		// after holds once Done has closed.
		after func(t *testing.T, s *tSession)
	}{
		{
			// The happy path: handshake completes, traffic flows, the
			// caller invokes Stop.
			name: "normal stop",
			start: func(t *testing.T) (*tSession, *fakeConn) {
				sA, _, _, connB := startSessionPair(t, nil, nil, 1)
				return sA, connB
			},
			trigger: stopOnce,
			timeout: time.Second,
		},
		{
			// A peer disconnecting mid-traffic: the reader's ReadFrame
			// returns EOF, runReader calls shutdown, which cancels ctx and
			// closes the conn; the writer exits on pipe Recv cancellation;
			// the last one out closes Done.
			name: "reader EOF",
			start: func(t *testing.T) (*tSession, *fakeConn) {
				sA, _, _, connB := startSessionPair(t, nil, nil, 1)
				return sA, connB
			},
			trigger: func(t *testing.T, _ *tSession, peer *fakeConn) { _ = peer.Close() },
			timeout: 2 * time.Second,
		},
		{
			// Done closes even when the session never reached the traffic
			// phase, without Stop being called: both goroutines exit once
			// the handshake fails.
			name:  "handshake failure",
			start: startLone,
			trigger: func(t *testing.T, s *tSession, peer *fakeConn) {
				// Feed garbage to trip handshake decode.
				go func() {
					buf := make([]byte, 64)
					_, _ = io.ReadFull(peer, buf)
					bad := make([]byte, 64) // zeros — bad greeting signature
					_, _ = peer.Write(bad)
				}()
				waitClosed(t, s.Handshaked(), 2*time.Second, "handshake did not settle")
				require.Error(t, s.HandshakeErr(), "expected HandshakeErr, got nil")
			},
			timeout: 2 * time.Second,
			after: func(t *testing.T, s *tSession) {
				// Drain must finish too — linger waiters cannot be allowed
				// to hang on a failed handshake.
				waitClosed(t, s.Drain(), time.Second, "Drain not finished after handshake failure")
				// Stop still idempotent afterwards.
				assert.NoError(t, s.Stop(), "Stop after handshake failure")
			},
		},
		{
			// There is no externally-observable context cancel path that
			// is not Stop (Stop owns the session-level ctx), so Stop is the
			// cancellation trigger: Done must fire even when the session
			// never reached the handshake-success state.
			name:    "stop before handshake",
			start:   startLone,
			trigger: stopOnce,
			timeout: 2 * time.Second,
		},
		{
			// Repeated Stop must not panic (which would indicate a
			// double-close of done) and Done() must remain readable
			// (closed channels stay closed; a second close would panic).
			name:  "exactly once",
			start: startLone,
			trigger: func(t *testing.T, s *tSession, _ *fakeConn) {
				require.NoError(t, s.Stop(), "first Stop")
				require.NoError(t, s.Stop(), "second Stop")
				// Third stop, for good measure — must not panic.
				require.NoError(t, s.Stop(), "third Stop")
			},
			timeout: time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, peer := tc.start(t)

			// Done must not fire until the trigger.
			select {
			case <-s.Done():
				require.FailNow(t, "Done fired before the teardown trigger")
			case <-time.After(20 * time.Millisecond):
			}

			tc.trigger(t, s, peer)
			waitClosed(t, s.Done(), tc.timeout, "Done did not fire after "+tc.name)
			if tc.after != nil {
				tc.after(t, s)
			}
		})
	}
}

// --------------------------------------------------------------------------
// MaxMessageSize: the reader must bound cross-frame (multipart)
// accumulation — the codec only caps individual frames.
// --------------------------------------------------------------------------

// TestReader_MaxMessageSize: a multipart message at/below the cap passes
// through untouched; once the accumulated multipart size exceeds the cap,
// the receiving session tears down instead of ballooning memory, and the
// peer observes the disconnect.
func TestReader_MaxMessageSize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		limit  int64
		frames int // of 256 B each
		over   bool
	}{
		// 4 × 256 B = exactly the cap — allowed (the limit is "exceeds").
		{"under limit", 1024, 4, false},
		// 3 × 256 B = 768 B > 512 B: B's reader must reject mid-message.
		// The teardown happens as soon as the third frame arrives — B never
		// assembles the full message, so memory stays bounded at the cap.
		{"over limit", 512, 3, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			connA, connB := newFakeConnPair()
			sA, sB := startPair(t,
				Config{Conn: connA, Mech: newFakeMech("NULL", false, nil), SendHWM: 8, RecvHWM: 8},
				Config{Conn: connB, Mech: newFakeMech("NULL", true, nil), SendHWM: 8, RecvHWM: 8,
					MaxMessageSize: tc.limit},
			)

			ctx := testutil.Ctx(t, 2*time.Second)
			frames := make([][]byte, tc.frames)
			for i := range frames {
				frames[i] = make([]byte, 256)
			}
			require.NoError(t, sA.Pipe().Send(ctx, msg.FromFrames(frames...)), "Send")

			if !tc.over {
				got, err := sB.Pipe().RecvOr(ctx, nil)
				require.NoError(t, err, "Recv")
				require.Equal(t, tc.frames, got.NumFrames(), "frame count")
				require.Equal(t, 256*tc.frames, got.TotalSize(), "total size")
				return
			}
			waitClosed(t, sB.Done(), 2*time.Second, "receiver did not tear down on oversized multipart")
			// The rejected message must not surface on B's pipe.
			_, err := sB.Pipe().RecvOr(testutil.Ctx(t, time.Second), nil)
			require.Error(t, err, "oversized message surfaced on the pipe")
			// A sees the disconnect (B closed the conn during shutdown).
			waitClosed(t, sA.Done(), 2*time.Second, "sender did not observe receiver teardown")
		})
	}
}

// --------------------------------------------------------------------------
// Reader scratch-buffer tests. The reader hands ReadFrame a persistent
// scratch dst of readScratchSize bytes; frames that fit are read into it
// and must be copied out, larger frames get their own codec buffer. These
// run against the REAL zmtp codec — fakeCodec ignores dst.
// --------------------------------------------------------------------------

// startZmtpPair is startPair with the real ZMTP framing codec on both
// ends, so the reader's scratch dst handling is exercised. The mechs
// declare IdentityFraming so decoded frames alias the read buffer.
func startZmtpPair(t *testing.T, hwm int) (sA, sB *tSession) {
	t.Helper()
	connA, connB := newFakeConnPair()
	return startPair(t,
		Config{Conn: connA, Mech: identityFramingMech{newFakeMech("NULL", false, nil)},
			Codec: zmtp.NewFrameCodec(zmtp.MaxBodyUnlimited), SendHWM: hwm, RecvHWM: hwm},
		Config{Conn: connB, Mech: identityFramingMech{newFakeMech("NULL", true, nil)},
			Codec: zmtp.NewFrameCodec(zmtp.MaxBodyUnlimited), SendHWM: hwm, RecvHWM: hwm},
	)
}

// TestReader_ScratchSizeBoundary: frames of exactly readScratchSize bytes
// (read into scratch, must be copied out) and one byte more (own buffer,
// adopted) are queued back to back; each must arrive intact, so no
// delivered frame may alias scratch.
func TestReader_ScratchSizeBoundary(t *testing.T) {
	t.Parallel()
	sA, sB := startZmtpPair(t, 8)
	ctx := testutil.Ctx(t, 5*time.Second)

	payloads := [][]byte{
		bytes.Repeat([]byte{0xA5}, readScratchSize),
		bytes.Repeat([]byte{0x5A}, readScratchSize+1),
		bytes.Repeat([]byte{0x3C}, readScratchSize),
		bytes.Repeat([]byte{0xC3}, readScratchSize-1),
	}
	for i, p := range payloads {
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes(bytes.Clone(p))), "Send[%d]", i)
	}
	for i, p := range payloads {
		got, err := sB.Pipe().RecvOr(ctx, nil)
		require.NoError(t, err, "Recv[%d]", i)
		require.Equal(t, p, got.Bytes(), "payload[%d]", i)
	}
}

// TestReader_SmallFrameBurstIndependentPayloads: a burst of small frames
// all pass through the reader's single scratch buffer; every delivered
// message must own independent bytes (a clone bug would let a later frame
// overwrite an earlier queued message).
func TestReader_SmallFrameBurstIndependentPayloads(t *testing.T) {
	t.Parallel()
	const N = 64
	sA, sB := startZmtpPair(t, N+4)
	ctx := testutil.Ctx(t, 5*time.Second)

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
		m, err := sB.Pipe().RecvOr(ctx, nil)
		require.NoError(t, err, "Recv[%d]", i)
		require.Equal(t, fmt.Sprintf("payload-%03d", i), string(m.Bytes()), "payload[%d]", i)
		require.Equal(t, len(m.Bytes()), cap(m.Bytes()), "cap[%d]: an append must not reach a neighbour", i)
	}
	mp, err := sB.Pipe().RecvOr(ctx, nil)
	require.NoError(t, err, "Recv(multipart)")
	require.Equal(t, 3, mp.NumFrames(), "multipart frame count")
	want := []string{"frame-a", "frame-b", "frame-c"}
	for j, f := range mp.Frames() {
		require.Equal(t, want[j], string(f), "multipart frame[%d]", j)
		require.Equal(t, len(f), cap(f), "multipart cap[%d]", j)
	}
	// Appending through one frame must not clobber the next.
	_ = append(mp.Frames()[0], "CLOBBER"...)
	require.Equal(t, "frame-b", string(mp.Frames()[1]), "append leaked into the next frame")
}

// requireIndependentFrames checks the frames of a delivered message match
// want, each has cap == len (an append cannot reach scratch or a
// neighbouring frame) and none is nil.
func requireIndependentFrames(t *testing.T, m *msg.Msg, want [][]byte, label string) {
	t.Helper()
	require.Equal(t, len(want), m.NumFrames(), "%s: frame count", label)
	for i, f := range m.Frames() {
		require.Equal(t, want[i], f, "%s: frame[%d]", label, i)
		require.NotNil(t, f, "%s: frame[%d] nil", label, i)
		require.Equal(t, len(f), cap(f), "%s: frame[%d] cap", label, i)
	}
}

// TestReader_ManyFrameMessageFlushesScratch: one message of 20 x 300 B
// frames outgrows the reader's scratch, so its pending frames are copied
// out mid-message and the rest accumulate from the front again (the mixed
// path); the next, small message must still arrive intact.
func TestReader_ManyFrameMessageFlushesScratch(t *testing.T) {
	t.Parallel()
	sA, sB := startZmtpPair(t, 8)
	ctx := testutil.Ctx(t, 5*time.Second)

	big := make([][]byte, 20)
	for i := range big {
		big[i] = bytes.Repeat([]byte{byte(0x30 + i)}, 300)
	}
	small := [][]byte{[]byte("topic-03"), []byte("payload")}
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromFrames(big...)))
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromFrames(small...)))

	got, err := sB.Pipe().RecvOr(ctx, nil)
	require.NoError(t, err)
	requireIndependentFrames(t, got, big, "20x300")
	got, err = sB.Pipe().RecvOr(ctx, nil)
	require.NoError(t, err)
	requireIndependentFrames(t, got, small, "after flush")
}

// TestReader_EmptyDelimiterFrame: [empty, body] (the REQ/REP envelope
// shape) and all-empty messages are delivered with non-nil empty frames.
func TestReader_EmptyDelimiterFrame(t *testing.T) {
	t.Parallel()
	sA, sB := startZmtpPair(t, 8)
	ctx := testutil.Ctx(t, 5*time.Second)

	msgs := [][][]byte{
		{{}, []byte("request body")},
		{{}},
		{{}, {}},
		{[]byte("id"), {}, []byte("body")},
	}
	for _, fr := range msgs {
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromFrames(fr...)))
	}
	for i, fr := range msgs {
		got, err := sB.Pipe().RecvOr(ctx, nil)
		require.NoError(t, err)
		requireIndependentFrames(t, got, fr, fmt.Sprintf("msg[%d]", i))
	}
}

// TestReader_PooledMidSizeFrames: with RecvBufferPool, a message of
// [3000 B, 3000 B] has a second frame that fits scratch but not what is
// left of it, so it comes from the pool; both frames must arrive intact
// and, being <= 4 KiB, not lent.
func TestReader_PooledMidSizeFrames(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	sA, sB := startPair(t,
		Config{Conn: connA, Mech: identityFramingMech{newFakeMech("NULL", false, nil)},
			Codec: zmtp.NewFrameCodec(zmtp.MaxBodyUnlimited), SendHWM: 8, RecvHWM: 8},
		Config{Conn: connB, Mech: identityFramingMech{newFakeMech("NULL", true, nil)},
			Codec: zmtp.NewFrameCodec(zmtp.MaxBodyUnlimited), SendHWM: 8, RecvHWM: 8,
			RecvBufferPool: true},
	)
	ctx := testutil.Ctx(t, 5*time.Second)

	for round := 0; round < 3; round++ {
		fr := [][]byte{bytes.Repeat([]byte{0xA1}, 3000), bytes.Repeat([]byte{0xB2}, 3000)}
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromFrames(fr...)))
		got, err := sB.Pipe().RecvOr(ctx, nil)
		require.NoError(t, err)
		require.False(t, got.Lent(), "round %d: frames <= 4 KiB are never lent", round)
		requireIndependentFrames(t, got, fr, fmt.Sprintf("round %d", round))
	}
	// A frame above scratch is still lent.
	huge := bytes.Repeat([]byte{0xC3}, readScratchSize+1)
	require.NoError(t, sA.Pipe().Send(ctx, msg.FromFrames([]byte("hdr"), huge)))
	got, err := sB.Pipe().RecvOr(ctx, nil)
	require.NoError(t, err)
	require.True(t, got.Lent(), "large frame must be lent")
	require.Equal(t, "hdr", string(got.Frames()[0]))
	require.Equal(t, huge, got.Frames()[1])
	got.Release()
}

// TestReader_CommandBetweenMoreFrames: a PING arriving between the frames
// of a message is read into scratch's tail without disturbing the frames
// already held there; the message arrives intact and the PONG goes out.
func TestReader_CommandBetweenMoreFrames(t *testing.T) {
	t.Parallel()
	connA, connB := newFakeConnPair()
	s := startTestSession(Config{
		Conn:    connA,
		Mech:    identityFramingMech{newFakeMech("NULL", false, nil)},
		Codec:   zmtp.NewFrameCodec(zmtp.MaxBodyUnlimited),
		SendHWM: 8,
		RecvHWM: 8,
	})
	t.Cleanup(func() { _ = s.Stop() })
	peer := newManualPeer(t, connB, nil)
	waitHandshake(t, s, "A")
	frames := peer.startReadLoop()
	ctx := testutil.Ctx(t, 5*time.Second)

	// Frames under 128 B encode identically under fakeCodec (the peer
	// helpers') and ZMTP.
	first, second := bytes.Repeat([]byte{'x'}, 100), bytes.Repeat([]byte{'y'}, 100)
	require.NoError(t, fakeCodec{}.WriteFrame(peer.conn, zmtp.FlagMore, first))
	peer.sendPing(10, []byte("ctx"))
	require.NoError(t, fakeCodec{}.WriteFrame(peer.conn, 0, second))

	got, err := s.Pipe().RecvOr(ctx, nil)
	require.NoError(t, err)
	requireIndependentFrames(t, got, [][]byte{first, second}, "message around PING")
	require.Equal(t, []byte("ctx"), waitCommand(t, frames, "PONG", 2*time.Second))
}

// TestReader_SubscriptionCommands: inbound SUBSCRIBE/CANCEL commands reach
// the engine as 0x01/0x00-prefixed messages only with SubscriptionCommands
// (PUB/XPUB); any other session drops them. The commands are queued before
// data1 is sent, so the writer puts them on the wire before data2.
func TestReader_SubscriptionCommands(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		connA, connB := newFakeConnPair()
		sA, sB := startPair(t,
			Config{Conn: connA, Mech: newFakeMech("NULL", false, nil), SendHWM: 8, RecvHWM: 8},
			Config{Conn: connB, Mech: newFakeMech("NULL", true, nil), SendHWM: 8, RecvHWM: 8,
				SubscriptionCommands: enabled},
		)
		ctx := testutil.Ctx(t, 2*time.Second)
		sA.enqueueCommand("SUBSCRIBE", []byte("topic"))
		sA.enqueueCommand("CANCEL", []byte("topic"))
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("data1"))))
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte("data2"))))

		var got []string
		for {
			m, err := sB.Pipe().RecvOr(ctx, nil)
			require.NoError(t, err, "enabled=%v", enabled)
			if string(m.Bytes()) == "data2" {
				break
			}
			got = append(got, string(m.Bytes()))
		}
		want := []string{"data1"}
		if enabled {
			want = []string{"\x01topic", "\x00topic", "data1"}
		}
		require.ElementsMatch(t, want, got, "enabled=%v", enabled)
		if enabled {
			require.Less(t, slices.Index(got, "\x01topic"), slices.Index(got, "\x00topic"), "command order")
		}
	}
}

// TestPlainBodiesRetainsOneScratch: the unpooled reader keeps one
// intermediate between large frames (up to the retention cap) so a stream
// of large frames allocates only the bodies themselves.
func TestPlainBodiesRetainsOneScratch(t *testing.T) {
	t.Parallel()
	var p plainBodies
	a := p.GetScratch(64 << 10)
	require.Len(t, a, 64<<10)
	p.PutScratch(a)
	b := p.GetScratch(32 << 10)
	require.Same(t, &a[0], &b[0], "retained scratch not reused for a smaller request")
	require.Len(t, b, 32<<10)
	p.PutScratch(b)
	c := p.GetScratch(65 << 10)
	require.NotSame(t, &a[0], &c[0], "undersized scratch handed out")
	p.PutScratch(c) // larger: replaces the retained one
	require.Equal(t, 65<<10, cap(p.scratch))

	huge := p.GetScratch(maxRetainedRecvScratch + 1)
	p.PutScratch(huge)
	require.Equal(t, 65<<10, cap(p.scratch), "oversized intermediate retained")

	// Bodies are plain allocations the caller owns; Put is a no-op.
	body := p.Get(10)
	require.Len(t, body, 10)
	p.Put(body)
	require.Equal(t, 65<<10, cap(p.scratch))
}

// TestReader_SingleMessagePromptDelivery: one message to a receiver parked
// in Recv arrives at once. The reader finds nothing buffered after the
// frame, so it flushes (the deferred wakeup) before its next blocking read.
func TestReader_SingleMessagePromptDelivery(t *testing.T) {
	t.Parallel()
	sA, sB := startZmtpPair(t, 16)
	ctx := testutil.Ctx(t, 5*time.Second)
	for i := range 20 {
		got := make(chan error, 1)
		go func() {
			m, err := sB.Pipe().RecvOr(ctx, nil)
			if err == nil && string(m.Bytes()) != fmt.Sprintf("one-%d", i) {
				err = fmt.Errorf("got %q", m.Bytes())
			}
			got <- err
		}()
		time.Sleep(2 * time.Millisecond) // let Recv park
		require.NoError(t, sA.Pipe().Send(ctx, msg.FromBytes([]byte(fmt.Sprintf("one-%d", i)))))
		select {
		case err := <-got:
			require.NoError(t, err, "round %d", i)
		case <-time.After(2 * time.Second):
			require.Fail(t, "message not delivered promptly", "round %d", i)
		}
	}
}

// TestReader_BurstDeliveredInOrder: 1000 messages through a small HWM (so
// the reader repeatedly hits the full-pipe path and the 256-message flush
// cap) all arrive, in order, to a receiver that sometimes parks.
func TestReader_BurstDeliveredInOrder(t *testing.T) {
	t.Parallel()
	for _, hwm := range []int{8, 0} {
		t.Run(fmt.Sprintf("hwm=%d", hwm), func(t *testing.T) {
			t.Parallel()
			const N = 1000
			sA, sB := startZmtpPair(t, hwm)
			ctx := testutil.Ctx(t, 20*time.Second)
			sendErr := make(chan error, 1)
			go func() {
				for i := range N {
					if err := sA.Pipe().Send(ctx, msg.FromBytes([]byte(fmt.Sprintf("m%04d", i)))); err != nil {
						sendErr <- err
						return
					}
				}
				sendErr <- nil
			}()
			for i := range N {
				m, err := sB.Pipe().RecvOr(ctx, nil)
				require.NoError(t, err, "Recv[%d]", i)
				require.Equal(t, fmt.Sprintf("m%04d", i), string(m.Bytes()), "order[%d]", i)
				if i%97 == 0 {
					time.Sleep(time.Millisecond) // let the pipe fill / the receiver park
				}
			}
			require.NoError(t, <-sendErr)
		})
	}
}

// TestFlushReader_FlushesBeforeUnderlyingRead: a partial frame in the
// bufio buffer can leave the reader blocked in a conn Read with Buffered()
// > 0, so the wrapper flushes before every underlying Read.
func TestFlushReader_FlushesBeforeUnderlyingRead(t *testing.T) {
	t.Parallel()
	a, b := pipe.NewPair(8, 8, nil, nil)
	defer a.Close()
	defer b.Close()
	var now atomic.Int64
	now.Store(42)
	s := &Session{sessionPipe: a, cfg: Config{Now: now.Load}}
	idle := make(chan struct{}, 1)
	b.SetNotify(func() { idle <- struct{}{} })
	_, ok := b.RecvOrIdle()
	require.False(t, ok)

	require.True(t, a.TrySendDeferred(msg.FromBytes([]byte("x"))))
	s.inboundOwed = 1
	var sawFlush bool
	r := flushReader{s: s, r: readerFunc(func([]byte) (int, error) {
		sawFlush = s.inboundOwed == 0
		return 0, io.EOF
	})}
	_, err := r.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.True(t, sawFlush, "flush must precede the underlying Read")
	select {
	case <-idle:
	case <-time.After(time.Second):
		require.Fail(t, "idle notification not run by the flush")
	}
	require.EqualValues(t, 42, s.lastInboundUnix.Load(), "inbound clock stamped at the flush")
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
