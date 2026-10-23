//go:build interop

package interop

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
)

// pattern is one socket pattern's link shape plus the exchange that proves
// it works across the boundary. The same bodies run under NULL here and
// under PLAIN in TestSecurity.
type pattern struct {
	name string
	spec spec
	body func(t *testing.T, l *link)
}

var (
	pairPattern         = pattern{name: "pair", spec: pairSpec(), body: pairBody}
	pushPullPattern     = pattern{name: "push-pull", spec: spec{server: gozmq.Pull, client: gozmq.Push}, body: pushPullBody}
	dealerRouterPattern = pattern{name: "dealer-router", spec: dealerRouterSpec(), body: dealerRouterBody}
	reqRepPattern       = pattern{name: "req-rep", spec: spec{server: gozmq.Rep, client: gozmq.Req}, body: reqRepBody}
	pubSubPattern       = pattern{name: "pub-sub", spec: spec{server: gozmq.Pub, client: gozmq.Sub}, body: pubSubBody}
	xpubXsubPattern     = pattern{name: "xpub-xsub", spec: spec{server: gozmq.XPub, client: gozmq.XSub}, body: xpubXsubBody}

	patterns = []pattern{pairPattern, pushPullPattern, dealerRouterPattern, reqRepPattern, pubSubPattern, xpubXsubPattern}
)

func TestPatterns(t *testing.T) {
	t.Parallel()
	for _, p := range patterns {
		for _, dir := range bothDirections {
			t.Run(p.name+"/"+dir.String(), func(t *testing.T) {
				t.Parallel()
				l := newLink(t, dir, p.spec)
				l.requireHandshake()
				p.body(t, l)
			})
		}
	}
}

func pairBody(t *testing.T, l *link) { roundTrip(t, l.client, l.server) }

// pushPullBody: a multipart message crosses from PUSH to PULL intact.
func pushPullBody(t *testing.T, l *link) {
	want := [][]byte{[]byte("job-42"), []byte("payload-bytes")}
	l.client.send(want...)
	requireFrames(t, want, l.server.recv())
}

const dealerID = "worker-1"

// dealerRouterSpec gives the DEALER a fixed identity on either
// implementation, set before Connect so it travels in the handshake.
func dealerRouterSpec() spec {
	return spec{
		server:     gozmq.Router,
		client:     gozmq.Dealer,
		clientOpts: []gozmq.Option{gozmq.WithIdentity([]byte(dealerID))},
		clientLib:  func(e *libEnd) { e.set(ZMQ_ROUTING_ID, []byte(dealerID)) },
	}
}

// dealerRouterBody: the ROUTER sees [identity, body]; a reply addressed to
// that identity reaches the DEALER with the envelope stripped.
func dealerRouterBody(t *testing.T, l *link) {
	l.client.send([]byte("dealer-payload"))
	requireFrames(t, [][]byte{[]byte(dealerID), []byte("dealer-payload")}, l.server.recv())
	l.server.send([]byte(dealerID), []byte("router-reply"))
	requireFrames(t, [][]byte{[]byte("router-reply")}, l.client.recv())
}

// reqRepBody: three strictly alternating round trips; each side's
// application sees only the body, never the delimiter envelope.
func reqRepBody(t *testing.T, l *link) {
	for i := range 3 {
		req := []byte(fmt.Sprintf("req-%d", i))
		rep := []byte(fmt.Sprintf("rep-%d", i))
		l.client.send(req)
		requireFrames(t, [][]byte{req}, l.server.recv())
		l.server.send(rep)
		requireFrames(t, [][]byte{rep}, l.client.recv())
	}
}

var (
	topic      = []byte("alpha")
	published  = []string{"alphaOne", "betaTwo", "alphaThree"}
	wantTopics = []string{"alphaOne", "alphaThree"}
)

// pubSubBody: the SUB subscribed to "alpha" receives the two matching
// messages, in order, and not the "beta" one between them.
func pubSubBody(t *testing.T, l *link) {
	l.client.subscribe(topic)
	probe := syncSubscription(t, l.server, l.client)
	for _, p := range published {
		l.server.send([]byte(p))
	}
	requireTopics(t, l.client, probe)
}

// syncSubscription publishes probe messages on the topic until the
// subscriber delivers one, and returns the probe payload. Neither
// implementation tells a publisher when a subscription has been applied, so
// this replaces a slow-joiner sleep. Probes still in flight are skipped by
// requireTopics.
func syncSubscription(t *testing.T, pub, sub end) []byte {
	t.Helper()
	probe := append(append([]byte{}, topic...), 0, 'p', 'r', 'o', 'b', 'e')
	deadline := time.Now().Add(waitTimeout)
	for {
		pub.send(probe)
		if _, err := sub.recvWithin(20 * time.Millisecond); err == nil {
			return probe
		}
		require.False(t, time.Now().After(deadline), "subscription never propagated")
	}
}

// requireTopics reads from sub, skipping probes, until wantTopics arrived in
// order.
func requireTopics(t *testing.T, sub end, probe []byte) {
	t.Helper()
	for _, want := range wantTopics {
		for {
			got := sub.recv()
			if len(got) == 1 && bytes.Equal(got[0], probe) {
				continue
			}
			requireFrames(t, [][]byte{[]byte(want)}, got)
			break
		}
	}
}

// xpubXsubBody: the XSUB sends a raw subscribe frame, the XPUB surfaces it to
// the application, and only matching topics then reach the XSUB.
func xpubXsubBody(t *testing.T, l *link) {
	sub := testutil.SubscribeFrame(true, topic)
	l.client.send(sub)
	requireFrames(t, [][]byte{sub}, l.server.recv())
	for _, p := range published {
		l.server.send([]byte(p))
	}
	for _, want := range wantTopics {
		requireFrames(t, [][]byte{[]byte(want)}, l.client.recv())
	}
}

// TestPushPull_LoadBalance: one gozmq PUSH connected to three libzmq PULLs
// delivers nine messages as exactly three per peer, none lost or duplicated.
func TestPushPull_LoadBalance(t *testing.T) {
	t.Parallel()
	const pullers, perPuller = 3, 3
	lCtx := NewCtx()
	t.Cleanup(func() { _ = lCtx.Close() })
	gCtx := gozmq.NewContext()
	t.Cleanup(func() { _ = gCtx.Close() })

	push := newGozmqEnd(t, gCtx, gozmq.Push)
	pulls := make([]*libEnd, pullers)
	for i := range pulls {
		pulls[i] = newLibEnd(t, lCtx, ZMQ_PULL)
		require.NoError(t, pulls[i].s.Bind("tcp://127.0.0.1:0"))
		ep, err := pulls[i].s.LastEndpoint()
		require.NoError(t, err)
		require.NoError(t, push.s.Connect(ep))
	}
	// Every peer must be in the round-robin set before the first send, or
	// the distribution skews. Each handshake success is necessary; the
	// settle after them covers the small gap until the PUSH socket has
	// registered the third pipe in its load balancer, for which there is no
	// event. Short and only because no observable marks that registration.
	for range pulls {
		require.Equal(t, gozmq.EventHandshakeSucceeded, push.handshake())
	}
	time.Sleep(200 * time.Millisecond)

	var want []string
	for i := range pullers * perPuller {
		msg := fmt.Sprintf("msg-%02d", i)
		want = append(want, msg)
		push.send([]byte(msg))
	}
	var all []string
	for i, pull := range pulls {
		for range perPuller {
			got := pull.recv()
			require.Len(t, got, 1)
			all = append(all, string(got[0]))
		}
		_, err := pull.recvWithin(50 * time.Millisecond)
		require.True(t, isTimeout(err), "puller %d received a fourth message: %v", i, err)
	}
	sort.Strings(all)
	require.Equal(t, want, all)
}

// TestRouterMandatory: a ROUTER in mandatory mode fails a send to an unknown
// identity, on each implementation, while a known peer is connected.
func TestRouterMandatory(t *testing.T) {
	t.Parallel()
	for _, dir := range bothDirections {
		t.Run(dir.String(), func(t *testing.T) {
			t.Parallel()
			sp := dealerRouterSpec()
			sp.serverOpts = []gozmq.Option{gozmq.WithRouterMandatory(true)}
			sp.serverLib = func(e *libEnd) { e.set(ZMQ_ROUTER_MANDATORY, 1) }
			l := newLink(t, dir, sp)
			l.requireHandshake()
			// A delivered hello proves the known peer is routable first.
			l.client.send([]byte("hello"))
			requireFrames(t, [][]byte{[]byte(dealerID), []byte("hello")}, l.server.recv())

			err := l.server.sendWithin(250*time.Millisecond, []byte("unknown-peer"), []byte("no-route"))
			require.Error(t, err, "send to unknown identity")
			if dir == gozmqBinds {
				require.ErrorIs(t, err, gozmq.ErrRouteUnknown)
			}
		})
	}
}

// TestXPubVerbose: a verbose XPUB surfaces a repeated subscription instead of
// deduplicating it.
func TestXPubVerbose(t *testing.T) {
	t.Parallel()
	for _, dir := range bothDirections {
		t.Run(dir.String(), func(t *testing.T) {
			t.Parallel()
			l := newLink(t, dir, spec{
				server:     gozmq.XPub,
				client:     gozmq.XSub,
				serverOpts: []gozmq.Option{gozmq.WithXPubVerbose(true)},
				serverLib:  func(e *libEnd) { e.set(ZMQ_XPUB_VERBOSE, 1) },
			})
			l.requireHandshake()
			sub := testutil.SubscribeFrame(true, topic)
			for range 2 {
				l.client.send(sub)
				requireFrames(t, [][]byte{sub}, l.server.recv())
			}
		})
	}
}

// TestStream: raw-TCP STREAM framing. libzmq announces the new connection as
// [identity, ""], then addresses the gozmq peer by identity; gozmq echoes
// with the identity it was given.
func TestStream(t *testing.T) {
	t.Parallel()
	for _, dir := range bothDirections {
		t.Run(dir.String(), func(t *testing.T) {
			t.Parallel()
			l := newLink(t, dir, spec{server: gozmq.Stream, client: gozmq.Stream})
			connected := l.l.recv()
			require.Len(t, connected, 2, "connect notification")
			require.Empty(t, connected[1], "connect notification body")
			peer := connected[0]

			l.l.send(peer, []byte("ping-from-libzmq"))
			got := l.g.recv()
			require.Len(t, got, 2)
			require.Equal(t, "ping-from-libzmq", string(got[1]))
			l.g.send(got[0], []byte("pong-from-gozmq"))
			requireFrames(t, [][]byte{peer, []byte("pong-from-gozmq")}, l.l.recv())
		})
	}
}

// TestTransports: PAIR round trips over the non-default transports.
func TestTransports(t *testing.T) {
	t.Parallel()
	transports := []struct {
		name     string
		endpoint func(t *testing.T) string
		optional bool // skip when the host cannot bind it
	}{
		{name: "ipc", endpoint: func(t *testing.T) string {
			return "ipc://" + filepath.Join(t.TempDir(), "pair.sock")
		}},
		{name: "ipc-abstract", endpoint: func(t *testing.T) string {
			if runtime.GOOS != "linux" {
				t.Skip("abstract namespace is Linux-only")
			}
			return fmt.Sprintf("ipc://@gozmq-interop-%s-%d", t.Name(), time.Now().UnixNano())
		}},
		// libzmq has no tcp6:// scheme: the adapter rewrites the endpoint
		// to tcp:// and sets ZMQ_IPV6 on the libzmq side.
		{name: "tcp6", endpoint: func(*testing.T) string { return "tcp6://[::1]:0" }, optional: true},
	}
	for _, tr := range transports {
		for _, dir := range bothDirections {
			t.Run(tr.name+"/"+dir.String(), func(t *testing.T) {
				t.Parallel()
				sp := pairSpec()
				sp.endpoint = tr.endpoint(t)
				sp.skipIfBindFails = tr.optional
				l := newLink(t, dir, sp)
				l.requireHandshake()
				roundTrip(t, l.client, l.server)
			})
		}
	}
}

// TestPayloadEdges: frame sizes at the short/long framing boundary, large
// frames and wide multiparts cross intact in both directions.
func TestPayloadEdges(t *testing.T) {
	t.Parallel()
	filled := func(n int, b byte) []byte { return bytes.Repeat([]byte{b}, n) }
	var wide [][]byte
	for i := range 32 {
		wide = append(wide, []byte(fmt.Sprintf("frame-%02d-%s", i, bytes.Repeat([]byte{'x' + byte(i%3)}, i%5+1))))
	}
	cases := []struct {
		name   string
		frames [][]byte
		large  bool // skipped with -short
	}{
		{name: "single-empty-frame", frames: [][]byte{{}}},
		{name: "multipart-empty-middle", frames: [][]byte{[]byte("head"), {}, []byte("tail")}},
		{name: "frame-255", frames: [][]byte{filled(255, 'a')}},
		{name: "frame-256", frames: [][]byte{filled(256, 'b')}},
		{name: "frame-257", frames: [][]byte{filled(257, 'c')}},
		{name: "frame-1mib", frames: [][]byte{filled(1<<20, 'd')}},
		{name: "frame-16mib", frames: [][]byte{filled(16<<20, 'e')}, large: true},
		{name: "multipart-32-frames", frames: wide},
	}
	for _, dir := range bothDirections {
		t.Run(dir.String(), func(t *testing.T) {
			t.Parallel()
			l := newLink(t, dir, pairSpec())
			l.requireHandshake()
			// The cases share one link, so they run in order.
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					if tc.large && testing.Short() {
						t.Skip("large payload skipped in -short mode")
					}
					// Error-returning calls, so a failure is charged to this
					// subtest rather than the link's parent test.
					require.NoError(t, l.client.sendWithin(20*time.Second, tc.frames...))
					got, err := l.server.recvWithin(20 * time.Second)
					require.NoError(t, err)
					requireFrames(t, tc.frames, got)
				})
			}
		})
	}
}
