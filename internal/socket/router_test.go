package socket_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// makeRouterPeers creates len(ids) pipe pairs where engineEnds[i] sees a
// peer whose PeerIdentity is ids[i]. Returned ends must be closed by the
// caller.
func makeRouterPeers(ids [][]byte, hwm int) (engineEnds, peerEnds []*pipe.End) {
	engineEnds = make([]*pipe.End, len(ids))
	peerEnds = make([]*pipe.End, len(ids))
	for i, id := range ids {
		a, b := pipe.NewPair(hwm, hwm, pipe.PeerInfo{Identity: id}, pipe.PeerInfo{})
		engineEnds[i] = a
		peerEnds[i] = b
	}
	return
}

// TestRouter_RoutesByIdentity asserts that ROUTER.Send delivers only to
// the peer whose PeerIdentity matches the leading frame.
func TestRouter_RoutesByIdentity(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(false)
	defer eng.Close()

	ids := [][]byte{[]byte("peerA"), []byte("peerB"), []byte("peerC")}
	engineEnds, peerEnds := makeRouterPeers(ids, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	mustAttach(t, eng, engineEnds...)

	ctx := testutil.Ctx(t)

	// Send a message targeted at peerB.
	out := msg.FromFrames([]byte("peerB"), []byte("hello"))
	require.NoError(t, eng.Send(ctx, out), "send")

	// peerB's peer-end should receive "hello"; the others should not.
	got, err := peerEnds[1].Recv(ctx)
	require.NoError(t, err, "peerB recv")
	require.Equal(t, 1, got.NumFrames())
	require.Equal(t, "hello", string(got.Bytes()))
	// Other peers must not have received anything.
	for _, idx := range []int{0, 2} {
		ctx2, c := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := peerEnds[idx].Recv(ctx2)
		c()
		require.ErrorIs(t, err, context.DeadlineExceeded, "peer %d unexpectedly got message", idx)
	}
}

// TestRouter_PrependsIdentityOnRecv asserts that ROUTER.Recv returns a
// message whose leading frame is the source peer's identity.
func TestRouter_PrependsIdentityOnRecv(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(false)
	defer eng.Close()

	ids := [][]byte{[]byte("alpha"), []byte("bravo")}
	engineEnds, peerEnds := makeRouterPeers(ids, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	mustAttach(t, eng, engineEnds...)

	ctx := testutil.Ctx(t)

	// Peer "bravo" sends a message into the router.
	require.NoError(t, peerEnds[1].Send(ctx, msg.FromBytes([]byte("ping"))), "peer send")
	got, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, 2, got.NumFrames())
	require.Equal(t, []byte("bravo"), got.Frames()[0])
	require.Equal(t, []byte("ping"), got.Frames()[1])
}

// TestRouter_UnroutableSilentWhenNotMandatory asserts that a Send to an
// unknown identity is silently dropped with mandatory=false.
func TestRouter_UnroutableSilentWhenNotMandatory(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(false)
	defer eng.Close()

	ids := [][]byte{[]byte("known")}
	engineEnds, peerEnds := makeRouterPeers(ids, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	mustAttach(t, eng, engineEnds[0])

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := msg.FromFrames([]byte("unknown"), []byte("hi"))
	require.NoError(t, eng.Send(ctx, out), "send (silent drop)")
	// known peer must not have received anything.
	ctx2, c := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer c()
	_, err := peerEnds[0].Recv(ctx2)
	require.ErrorIs(t, err, context.DeadlineExceeded, "known peer unexpectedly received")
}

// TestRouter_UnroutableErrorsWhenMandatory asserts that Send to an
// unknown identity with mandatory=true returns ErrRouteUnknown.
func TestRouter_UnroutableErrorsWhenMandatory(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(true)
	defer eng.Close()

	ids := [][]byte{[]byte("known")}
	engineEnds, peerEnds := makeRouterPeers(ids, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	mustAttach(t, eng, engineEnds[0])

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := msg.FromFrames([]byte("unknown"), []byte("hi"))
	require.ErrorIs(t, eng.Send(ctx, out), zmqerr.ErrRouteUnknown)
}

// TestRouter_SendRejectsEmptyMessage asserts that Send with no frames
// is rejected as ErrRouteUnknown (there is no identity frame).
func TestRouter_SendRejectsEmptyMessage(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(false)
	defer eng.Close()

	err := eng.Send(context.Background(), msg.FromFrames())
	require.ErrorIs(t, err, zmqerr.ErrRouteUnknown)
}

// TestRouter_DetachClearsRoute asserts that detaching a peer removes its
// identity from the routing table.
func TestRouter_DetachClearsRoute(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(true)
	defer eng.Close()

	ids := [][]byte{[]byte("peerX")}
	engineEnds, peerEnds := makeRouterPeers(ids, 4)
	defer closePipes(append(engineEnds, peerEnds...)...)
	mustAttach(t, eng, engineEnds[0])
	require.NoError(t, eng.DetachPipe(engineEnds[0]), "detach")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := msg.FromFrames([]byte("peerX"), []byte("hi"))
	require.ErrorIs(t, eng.Send(ctx, out), zmqerr.ErrRouteUnknown, "send after detach")
}

// TestRouter_AnonymousPeerGetsSynthesizedIdentity asserts that a peer
// attached with an empty PeerIdentity is still routable: Recv returns a
// non-empty leading frame (the synthesized identity) and Send with that
// same frame reaches the anonymous peer.
func TestRouter_AnonymousPeerGetsSynthesizedIdentity(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(true)
	defer eng.Close()

	a, b := pipe.NewPair(4, 4, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)

	// Anonymous peer sends; Router receives with a leading synthesized
	// identity.
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("hello"))), "peer send")
	got, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, 2, got.NumFrames())
	synthID := got.Frames()[0]
	require.Len(t, synthID, 5, "synthesized identity length")
	require.Equal(t, byte(0), synthID[0], "synthesized identity leading byte")

	// Replay the synthesized identity on Send; peer must receive the
	// reply body.
	reply := msg.FromFrames(synthID, []byte("world"))
	require.NoError(t, eng.Send(ctx, reply), "send")
	got2, err := b.Recv(ctx)
	require.NoError(t, err, "peer recv")
	require.Equal(t, 1, got2.NumFrames())
	require.Equal(t, "world", string(got2.Bytes()))
}

// TestRouter_SendToVanishedPeer: a peer whose pipe closed but has not been
// detached yet is an unknown route, not a raw pipe error: a silent drop
// without mandatory, ErrRouteUnknown with it.
func TestRouter_SendToVanishedPeer(t *testing.T) {
	t.Parallel()
	for _, mandatory := range []bool{false, true} {
		t.Run(fmt.Sprintf("mandatory=%v", mandatory), func(t *testing.T) {
			t.Parallel()
			eng := socket.NewRouterEngine(mandatory)
			defer eng.Close()
			engineEnds, peerEnds := makeRouterPeers([][]byte{[]byte("gone")}, 4)
			defer closePipes(engineEnds...)
			mustAttach(t, eng, engineEnds...)
			require.NoError(t, peerEnds[0].Close())

			err := eng.Send(testutil.Ctx(t), msg.FromFrames([]byte("gone"), []byte("x")))
			if mandatory {
				require.ErrorIs(t, err, zmqerr.ErrRouteUnknown)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(1), eng.Metrics().Dropped)
		})
	}
}

// TestRouter_SynthesizedIdentitiesAreDistinct: every anonymous peer gets its
// own routing identity, including when a named peer already holds the
// zero-led identity the generator would produce next, so replies can never
// be misrouted to another client.
func TestRouter_SynthesizedIdentitiesAreDistinct(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(true)
	defer eng.Close()
	ctx := testutil.Ctx(t)

	attach := func(info pipe.PeerInfo) *pipe.End {
		a, b := pipe.NewPair(4, 4, info, pipe.PeerInfo{})
		t.Cleanup(func() { closePipes(a, b) })
		mustAttach(t, eng, a)
		return b
	}
	recvID := func() string {
		got, err := eng.Recv(ctx)
		require.NoError(t, err)
		return string(got.Frames()[0])
	}

	// Learn where the generator is, then squat on its next value.
	first := attach(pipe.PeerInfo{})
	require.NoError(t, first.Send(ctx, msg.FromBytes([]byte("first"))))
	firstID := []byte(recvID())
	require.Len(t, firstID, 5)
	squatter := make([]byte, 5)
	binary.BigEndian.PutUint32(squatter[1:], binary.BigEndian.Uint32(firstID[1:])+1)

	const n = 200
	peers := []*pipe.End{attach(pipe.PeerInfo{Identity: squatter})}
	for range n {
		peers = append(peers, attach(pipe.PeerInfo{}))
	}
	for i, b := range peers {
		require.NoError(t, b.Send(ctx, msg.FromBytes([]byte{byte(i)})), "peer %d send", i)
	}
	seen := map[string]bool{string(firstID): true}
	for range n + 1 {
		id := recvID()
		require.False(t, seen[id], "identity %x assigned twice", id)
		seen[id] = true
	}
	require.True(t, seen[string(squatter)], "named peer keeps its identity")
}

// TestRouter_IdentityStableUnderDetachChurn stresses the Recv-vs-Detach
// race fix. While one goroutine drives sends from 10 peers, another
// detaches and reattaches a subset of peers at random intervals. For
// every message the ROUTER Recvs:
//   - the leading identity frame must be non-empty (the anonymous-
//     peer variant of the old bug stamped empty frames when Detach
//     beat Recv), and
//   - for named peers, the leading frame must equal the body, which
//     the peer set to its own configured identity (the named-peer
//     variant of the old bug could cross-wire messages onto a newly-
//     attached pipe's identity if PeerIdentity collided).
//
// Half the peers are anonymous (empty PeerIdentity, synthesized
// routing id) and half are named. The test drives enough churn to
// exercise the in-flight Detach window; stamping from the identity stored
// on the source pipe eliminates both failure modes.
//
// Run with -race -count=20 to shake loose any remaining ordering bug.
func TestRouter_IdentityStableUnderDetachChurn(t *testing.T) {
	t.Parallel()
	eng := socket.NewRouterEngine(false)
	defer eng.Close()

	const nPeers = 10
	const iterations = 200
	const churnPeers = 3

	// peerSlot owns the current live pipe pair for one peer. mu
	// serializes swap (detach + reattach) against active use (send).
	// id is the body the peer stamps into each message (and equals
	// the routing identity for named peers). anon=true means
	// PeerIdentity was left empty so the ROUTER synthesizes a routing
	// id at attach time; the leading frame for those messages is
	// opaque to the test but must be non-empty.
	type peerSlot struct {
		id   []byte
		anon bool
		mu   sync.Mutex
		eng  *pipe.End // router-side end (currently attached to eng)
		prx  *pipe.End // peer-side end
	}

	slots := make([]*peerSlot, nPeers)
	for i := 0; i < nPeers; i++ {
		id := []byte(fmt.Sprintf("peer-%02d", i))
		anon := i%2 == 0
		var peerID []byte
		if !anon {
			peerID = id
		}
		a, b := pipe.NewPair(8, 8, pipe.PeerInfo{Identity: peerID}, pipe.PeerInfo{})
		if err := eng.AttachPipe(a); err != nil {
			require.NoError(t, err, "initial attach %d", i)
		}
		slots[i] = &peerSlot{id: id, anon: anon, eng: a, prx: b}
	}
	defer func() {
		for _, s := range slots {
			s.mu.Lock()
			if s.eng != nil {
				_ = s.eng.Close()
			}
			if s.prx != nil {
				_ = s.prx.Close()
			}
			s.mu.Unlock()
		}
	}()

	var stop atomic.Bool

	// Churn goroutine: every iteration, pick churnPeers random slots
	// and detach+reattach them.
	var churnWg sync.WaitGroup
	churnWg.Add(1)
	go func() {
		defer churnWg.Done()
		// Deterministic per-test churn order but fresh every run.
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for !stop.Load() {
			// Pick churnPeers distinct indices.
			idx := rng.Perm(nPeers)[:churnPeers]
			for _, i := range idx {
				s := slots[i]
				s.mu.Lock()
				oldEng := s.eng
				oldPrx := s.prx
				// Detach the old pipe from the engine first. A
				// receiver may be taking a message from this pipe at
				// the moment of Detach; the engine must stamp the
				// *old* identity on that message (not empty, not the
				// new peer's id).
				_ = eng.DetachPipe(oldEng)
				// Freshly construct the peer pair. For named peers we
				// reuse the same PeerIdentity — deliberately creating
				// the collision scenario that motivated the race fix.
				// For anonymous peers we leave PeerIdentity empty; the
				// ROUTER synthesizes a fresh routing id each attach.
				var peerID []byte
				if !s.anon {
					peerID = s.id
				}
				a, b := pipe.NewPair(8, 8, pipe.PeerInfo{Identity: peerID}, pipe.PeerInfo{})
				if err := eng.AttachPipe(a); err != nil {
					s.mu.Unlock()
					// engine closed — exit.
					_ = a.Close()
					_ = b.Close()
					_ = oldEng.Close()
					_ = oldPrx.Close()
					return
				}
				s.eng = a
				s.prx = b
				s.mu.Unlock()
				// Close the old ends after the swap. A message a
				// receiver took from oldEng before the detach carries
				// the old identity on the pipe's routing ID.
				_ = oldEng.Close()
				_ = oldPrx.Close()
				// Short pause so we don't just hammer the lock.
				time.Sleep(time.Duration(rng.Intn(150)) * time.Microsecond)
			}
		}
	}()

	// Tracker goroutine: drain eng.Recv and validate every received
	// message's leading frame. Runs until the main goroutine signals
	// done via cancel.
	//
	// Validation:
	//   1. Leading frame is never empty (catches the anonymous variant
	//      of the old race: Detach beat Recv → idOf miss → fallback to
	//      empty PeerIdentity()).
	//   2. Body matches some peer's id (sanity).
	//   3. For named-peer bodies, leading frame == body (catches the
	//      collision variant: new pipe with same PeerIdentity still
	//      can't cause a wrong-id stamp because our fix keys on the
	//      pipe pointer, not the identity bytes).
	//   4. For anonymous-peer bodies, leading frame is 5 bytes and
	//      starts with byte 0 — the libzmq anonymous-router shape.
	namedIDs := make(map[string]bool, nPeers)
	anonIDs := make(map[string]bool, nPeers)
	for _, s := range slots {
		if s.anon {
			anonIDs[string(s.id)] = true
		} else {
			namedIDs[string(s.id)] = true
		}
	}

	recvCtx, recvCancel := context.WithCancel(context.Background())
	defer recvCancel()
	var recvWg sync.WaitGroup
	var recvErr atomic.Value // holds error
	var recvCount atomic.Int64
	recvWg.Add(1)
	go func() {
		defer recvWg.Done()
		for {
			m, err := eng.Recv(recvCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if errors.Is(err, zmqerr.ErrSocketClosed) {
					return
				}
				recvErr.Store(fmt.Errorf("recv: %w", err))
				return
			}
			if m.NumFrames() < 2 {
				recvErr.Store(fmt.Errorf("recv: got %d frames, want >=2", m.NumFrames()))
				return
			}
			lead := m.Frames()[0]
			body := m.Frames()[1]
			if len(lead) == 0 {
				recvErr.Store(fmt.Errorf("recv: empty leading identity frame (body=%q)", body))
				return
			}
			switch {
			case namedIDs[string(body)]:
				// Named peer: leading frame must equal the body
				// (which equals the peer's PeerIdentity).
				if !bytes.Equal(lead, body) {
					recvErr.Store(fmt.Errorf("recv named cross-wired: lead=%q body=%q", lead, body))
					return
				}
			case anonIDs[string(body)]:
				// Anonymous peer: leading frame is synthesized by the
				// ROUTER at attach — 5 bytes starting with 0x00. We
				// cannot pin it to a specific value because a reattach
				// re-synthesizes, but it must be well-formed.
				if len(lead) != 5 || lead[0] != 0 {
					recvErr.Store(fmt.Errorf("recv anon bad-shape: lead=%v body=%q", lead, body))
					return
				}
			default:
				recvErr.Store(fmt.Errorf("recv unknown body: lead=%q body=%q", lead, body))
				return
			}
			recvCount.Add(1)
		}
	}()

	// Main driver: for iterations rounds, pick a random peer, grab its
	// slot lock (so it can't be mid-swapped), and send a message whose
	// body is the peer's identity. The Recv tracker will validate that
	// the ROUTER stamps the leading frame correctly. Send failures
	// under churn (e.g. saturation while the tracker is draining) are
	// tolerated — we care about the invariant that every successfully-
	// routed message has a valid identity, not that every send makes
	// it through.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var sendsDone atomic.Int64
	for i := 0; i < iterations; i++ {
		idx := rng.Intn(nPeers)
		s := slots[idx]
		s.mu.Lock()
		prx := s.prx
		sendCtx, sendCancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := prx.Send(sendCtx, msg.FromBytes(append([]byte(nil), s.id...)))
		sendCancel()
		s.mu.Unlock()
		if err == nil {
			sendsDone.Add(1)
		}
		// Tight schedule: don't sleep — let churn and send interleave.
	}

	// Stop the churn goroutine before checking recvErr so we have a
	// quiescent state.
	stop.Store(true)
	churnWg.Wait()

	// Wait briefly for the Recv goroutine to drain any final messages.
	// The exact count depends on churn (messages still queued on a pipe
	// detached while open are never received), so we don't assert an
	// exact count — just that every message that made it through
	// carried a correct identity.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err, _ := recvErr.Load().(error); err != nil {
			require.NoError(t, err, "tracker")
		}
		time.Sleep(10 * time.Millisecond)
		// Heuristic stop: no more messages for a quiet window.
		prev := recvCount.Load()
		time.Sleep(50 * time.Millisecond)
		if recvCount.Load() == prev {
			break
		}
	}
	recvCancel()
	recvWg.Wait()
	if err, _ := recvErr.Load().(error); err != nil {
		require.NoError(t, err, "tracker (post-stop)")
	}
	assert.NotZero(t, recvCount.Load(), "tracker saw zero messages; expected some of %d (sends=%d)", iterations, sendsDone.Load())
	t.Logf("tracker validated %d messages (successful sends: %d / %d)", recvCount.Load(), sendsDone.Load(), iterations)
}

// TestRouter_RecvAfterDetachKeepsIdentity: messages a peer delivered before
// it left, received after DetachPipe, carry that peer's routing identity,
// not a replacement peer's, both as the sole peer and among several.
func TestRouter_RecvAfterDetachKeepsIdentity(t *testing.T) {
	t.Parallel()
	for _, extraPeer := range []bool{false, true} {
		t.Run(fmt.Sprintf("extraPeer=%v", extraPeer), func(t *testing.T) {
			t.Parallel()
			eng := socket.NewRouterEngine(false)
			defer eng.Close()
			ctx := testutil.Ctx(t)

			if extraPeer {
				x, y := pipe.NewPair(8, 8, pipe.PeerInfo{Identity: []byte("idle")}, pipe.PeerInfo{})
				defer closePipes(x, y)
				require.NoError(t, eng.AttachPipe(x))
			}
			a, b := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{}) // anonymous
			defer closePipes(a, b)
			require.NoError(t, eng.AttachPipe(a))
			require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("a0"))))
			m, err := eng.Recv(ctx)
			require.NoError(t, err)
			idA := m.Frames()[0]

			const n = 4
			for i := 1; i <= n; i++ {
				require.NoError(t, b.Send(ctx, msg.FromBytes(fmt.Appendf(nil, "a%d", i))))
			}
			require.NoError(t, b.Close())
			require.NoError(t, eng.DetachPipe(a))
			require.NoError(t, a.Close())

			c, d := pipe.NewPair(8, 8, pipe.PeerInfo{}, pipe.PeerInfo{})
			defer closePipes(c, d)
			require.NoError(t, eng.AttachPipe(c))
			require.NoError(t, d.Send(ctx, msg.FromBytes([]byte("c"))))

			next := 1
			for range n + 1 {
				m, err := eng.Recv(ctx)
				require.NoError(t, err)
				id, body := m.Frames()[0], string(m.Frames()[1])
				if body == "c" {
					require.NotEqual(t, idA, id, "replacement peer reused the departed identity")
					continue
				}
				require.Equal(t, fmt.Sprintf("a%d", next), body, "per-peer FIFO")
				require.Equal(t, idA, id, "departed peer's message restamped")
				next++
			}
		})
	}
}
