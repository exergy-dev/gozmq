//go:build interop

package interop

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

const (
	heartbeatInterval = 200 * time.Millisecond
	heartbeatTimeout  = time.Second
	// heartbeatIdle keeps the session idle well past heartbeatTimeout so the
	// peers exchange several PING/PONG rounds: one the far side cannot parse
	// either tears the session down or surfaces as user data.
	heartbeatIdle = 12 * heartbeatInterval
)

func requireLibzmqHeartbeat(t *testing.T) {
	t.Helper()
	maj, min, pat := Version()
	if maj < 4 || (maj == 4 && min < 2) {
		t.Skipf("libzmq heartbeat requires >= 4.2.0; have %d.%d.%d", maj, min, pat)
	}
}

func libHeartbeat(e *libEnd) {
	e.set(ZMQ_HEARTBEAT_IVL, int(heartbeatInterval/time.Millisecond))
	e.set(ZMQ_HEARTBEAT_TIMEOUT, int(heartbeatTimeout/time.Millisecond))
	e.set(ZMQ_HEARTBEAT_TTL, int(heartbeatTimeout/time.Millisecond))
}

// TestHeartbeat: with heartbeats on both sides, an idle session survives
// past the timeout, carries data afterwards with no heartbeat command
// leaking into either user-message stream, and the gozmq side notices when
// libzmq finally closes. CURVE carries PING/PONG inside encrypted MESSAGE
// boxes, so those rows exercise the mechanism's command encoding; NULL and
// PLAIN are the controls.
func TestHeartbeat(t *testing.T) {
	t.Parallel()
	requireLibzmqHeartbeat(t)
	mechs := []func(t *testing.T) *mech{
		func(*testing.T) *mech { return nullMech() },
		func(*testing.T) *mech { return plainMech(plainAuth, plainUser, plainPass) },
		curveMech,
	}
	for _, build := range mechs {
		for _, dir := range bothDirections {
			m := build(t)
			t.Run(m.name+"/"+dir.String(), func(t *testing.T) {
				t.Parallel()
				hb := []gozmq.Option{gozmq.WithHeartbeat(heartbeatInterval, heartbeatTimeout)}
				l := newLink(t, dir, spec{
					server: gozmq.Pair, client: gozmq.Pair, mech: m,
					serverOpts: hb, clientOpts: hb,
					serverLib: libHeartbeat, clientLib: libHeartbeat,
				})
				l.requireHandshake()

				time.Sleep(heartbeatIdle) // the idle period is the subject
				l.g.requireSessionIntact()

				// A PING mis-encoded as data would be queued ahead of these.
				toLib := [][]byte{[]byte("gozmq-after-idle"), []byte("part-2")}
				l.g.send(toLib...)
				requireFrames(t, toLib, l.l.recv())
				toGozmq := [][]byte{[]byte("libzmq-after-idle"), []byte("part-2")}
				l.l.send(toGozmq...)
				requireFrames(t, toGozmq, l.g.recv())

				// A few more rounds, then both inbound queues must be empty.
				time.Sleep(4 * heartbeatInterval)
				stray, err := l.l.recvWithin(100 * time.Millisecond)
				require.True(t, isTimeout(err), "libzmq received %q, err %v", stray, err)
				stray, err = l.g.recvWithin(100 * time.Millisecond)
				require.True(t, isTimeout(err), "gozmq received %q, err %v", stray, err)
				l.g.requireSessionIntact()

				l.closeLib()
				l.g.waitFor(evDisconnected)
			})
		}
	}
}

// TestDisconnect: an explicit client-side disconnect is seen by the other
// implementation's server, and the client does not come back.
func TestDisconnect(t *testing.T) {
	t.Parallel()
	t.Run("libzmq-client", func(t *testing.T) {
		t.Parallel()
		sp := pairSpec()
		sp.serverOpts = []gozmq.Option{gozmq.WithHeartbeat(heartbeatInterval, heartbeatTimeout)}
		sp.clientLib = func(e *libEnd) { e.set(ZMQ_LINGER, 0) }
		l := newLink(t, gozmqBinds, sp)
		l.requireHandshake()
		l.l.send([]byte("before-disconnect"))
		requireFrames(t, [][]byte{[]byte("before-disconnect")}, l.g.recv())

		l.l.disconnect(l.endpoint)
		l.g.send([]byte("disconnect-probe"))
		l.g.waitFor(evDisconnected)
	})
	t.Run("gozmq-client", func(t *testing.T) {
		t.Parallel()
		sp := pairSpec()
		sp.clientOpts = []gozmq.Option{
			gozmq.WithReconnectInterval(10 * time.Millisecond),
			gozmq.WithReconnectIntervalMax(20 * time.Millisecond),
		}
		l := newLink(t, libzmqBinds, sp)
		l.l.waitFor(evAccepted)
		l.g.disconnect(l.endpoint)
		l.l.waitFor(evDisconnected)
		// Disconnect is final: the fast reconnect schedule must not fire.
		l.l.noEvent(evAccepted, 250*time.Millisecond)
	})
}

const (
	lingerPayloadSize = 128 << 10
	lingerBurstLimit  = 256
)

// TestLinger: a sender closed with a positive linger drains everything it
// had queued to the other implementation; closed with linger zero, it drops.
func TestLinger(t *testing.T) {
	t.Parallel()
	for _, senderBinds := range []direction{libzmqBinds, gozmqBinds} {
		// The sender connects; the receiver binds.
		sender := map[direction]string{libzmqBinds: "gozmq-sender", gozmqBinds: "libzmq-sender"}[senderBinds]
		for _, tc := range []struct {
			name   string
			linger time.Duration
			drains bool
		}{
			{name: "linger-drains", linger: 2 * time.Second, drains: true},
			{name: "linger-zero-drops", linger: 0},
		} {
			t.Run(sender+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				sp := pairSpec()
				sp.serverOpts = []gozmq.Option{gozmq.WithRecvHWM(1)}
				sp.serverLib = func(e *libEnd) { e.set(ZMQ_RCVHWM, 1) }
				sp.clientOpts = []gozmq.Option{gozmq.WithLinger(tc.linger), gozmq.WithSendHWM(1)}
				sp.clientLib = func(e *libEnd) {
					e.set(ZMQ_LINGER, int(tc.linger/time.Millisecond))
					e.set(ZMQ_SNDHWM, 1)
				}
				l := newLink(t, senderBinds, sp)
				l.requireHandshake()

				sent := fillUntilBlocked(t, l.client)
				closeDone := testutil.Async(l.client.close)
				// No observable marks a zero-linger drop, and on loopback the
				// receiver would otherwise drain the queue before the drop
				// lands (making it look like a drain). A positive linger
				// instead keeps Close blocked here until we drain below. Short
				// and only because no event exists to wait on.
				time.Sleep(50 * time.Millisecond)
				got := drain(t, l.server)
				require.NoError(t, <-closeDone, "sender close")
				if tc.drains {
					require.Equal(t, sent, got, "linger drain delivered %d/%d", got, sent)
				} else {
					require.Less(t, got, sent, "linger=0 delivered %d/%d, want a drop", got, sent)
				}
			})
		}
	}
}

// fillUntilBlocked sends 128 KiB messages until a 10 ms send times out and
// returns how many were accepted; the HWM of 1 on both sides makes that a
// handful.
func fillUntilBlocked(t *testing.T, sender end) int {
	t.Helper()
	payload := bytes.Repeat([]byte("x"), lingerPayloadSize)
	sent := 0
	for ; sent < lingerBurstLimit; sent++ {
		if err := sender.sendWithin(10*time.Millisecond, payload); err != nil {
			require.True(t, isTimeout(err), "send[%d]: %v", sent, err)
			break
		}
	}
	require.NotZero(t, sent, "sender did not queue any messages before blocking")
	require.Less(t, sent, lingerBurstLimit, "sender never saturated")
	return sent
}

// drain receives until the receiver has been quiet for 100 ms (or reports
// EOF) and returns the message count, checking each is one 128 KiB frame.
func drain(t *testing.T, receiver end) int {
	t.Helper()
	got := 0
	for {
		frames, err := receiver.recvWithin(100 * time.Millisecond)
		if err != nil {
			require.True(t, isTimeout(err) || errors.Is(err, io.EOF), "recv: %v", err)
			return got
		}
		require.Len(t, frames, 1)
		require.Len(t, frames[0], lingerPayloadSize)
		got++
	}
}

// TestReconnect: a client of either implementation reconnects to a server
// that is closed and rebound on the same port, and delivers again. Not
// parallel: it binds fixed ports.
func TestReconnect(t *testing.T) {
	const backoff = 25 * time.Millisecond
	for _, dir := range bothDirections {
		t.Run(dir.String(), func(t *testing.T) {
			sp := pairSpec()
			sp.endpoint = fixedTCPEndpoint(t)
			sp.retryBind = true
			sp.clientOpts = []gozmq.Option{gozmq.WithReconnectInterval(backoff), gozmq.WithReconnectIntervalMax(backoff)}
			sp.clientLib = func(e *libEnd) { e.set(ZMQ_RECONNECT_IVL, int(backoff/time.Millisecond)) }
			l := newLink(t, dir, sp)
			l.requireHandshake()
			l.client.send([]byte("before-rebind"))
			requireFrames(t, [][]byte{[]byte("before-rebind")}, l.server.recv())

			server2 := l.restartServer()
			l.client.waitFor(evDisconnected)
			l.client.waitFor(evConnected)
			l.client.send([]byte("after-rebind"))
			requireFrames(t, [][]byte{[]byte("after-rebind")}, server2.recv())
		})
	}
}

// fixedTCPEndpoint reserves a loopback port and releases it for the test to
// bind.
func fixedTCPEndpoint(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, ln.Close())
	return "tcp://" + ln.Addr().String()
}

// TestMaxMessageSize: a receiver's maximum message size is enforced against
// the other implementation by dropping the connection.
func TestMaxMessageSize(t *testing.T) {
	t.Parallel()
	const limit = 1024
	for _, dir := range bothDirections {
		t.Run(dir.String(), func(t *testing.T) {
			t.Parallel()
			sp := pairSpec()
			sp.serverOpts = []gozmq.Option{gozmq.WithMaxMessageSize(limit)}
			sp.serverLib = func(e *libEnd) { e.set(ZMQ_MAXMSGSIZE, int64(limit)) }
			l := newLink(t, dir, sp)
			l.requireHandshake() // proves the link is up before the oversize send
			l.client.send(make([]byte, limit+1))
			l.server.waitFor(evDisconnected)
		})
	}
}
