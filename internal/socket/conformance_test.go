package socket_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// TestEngineConformance pins the cross-engine pieces of the Engine
// contract in one table instead of per-engine copies: error sentinels on
// a closed engine, FSM sentinels on the recv-only/send-only directions,
// attach/detach idempotence, attach-after-close, and Close unblocking a
// parked Send/Recv with ErrSocketClosed, both with no pipe attached and
// with the op parked on an attached pipe (Send at HWM, Recv on an empty
// pipe).
func TestEngineConformance(t *testing.T) {
	t.Parallel()
	mkSub := func() socket.Engine { e, _ := socket.NewSubEngine(); return e }
	mkXSub := func() socket.Engine { e, _ := socket.NewXSubEngine(); return e }
	routed := func() *msg.Msg { return msg.FromFrames([]byte("id"), []byte("x")) }
	plain := func() *msg.Msg { return msg.FromBytes([]byte("x")) }

	cases := []struct {
		name    string
		mk      func() socket.Engine
		sendMsg func() *msg.Msg
		// blockSend/blockRecv: the direction parks with no peer/data and
		// must be unblocked by Close with ErrSocketClosed.
		blockSend, blockRecv bool
		// attachedSend/attachedRecv: the direction parks on an attached
		// pipe (Send at HWM, Recv on an empty pipe) and must be unblocked
		// by Close with ErrSocketClosed. False where the engine never
		// parks there: PUB/XPUB drop at HWM, XSUB bounds its forward with
		// a timeout, REP cannot Send before a Recv.
		attachedSend, attachedRecv bool
		// closedSendErr/closedRecvErr: sentinel returned after Close. An
		// ErrFSMViolation here is also asserted on the open engine.
		closedSendErr, closedRecvErr error
		// reattachOK: attaching the same pipe twice is a no-op (every
		// engine except PAIR, which rejects any second attach).
		reattachOK bool
	}{
		{"pair", socket.NewPairEngine, plain, true, true, true, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, false},
		{"push", socket.NewPushEngine, plain, true, false, true, false, zmqerr.ErrSocketClosed, zmqerr.ErrFSMViolation, true},
		{"pull", socket.NewPullEngine, plain, false, true, false, true, zmqerr.ErrFSMViolation, zmqerr.ErrSocketClosed, true},
		{"dealer", socket.NewDealerEngine, plain, true, true, true, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"pub", socket.NewPubEngine, plain, false, false, false, false, zmqerr.ErrSocketClosed, zmqerr.ErrFSMViolation, true},
		{"sub", mkSub, plain, false, true, false, true, zmqerr.ErrFSMViolation, zmqerr.ErrSocketClosed, true},
		{"xpub", func() socket.Engine { return socket.NewXPubEngineV(false) }, plain, false, true, false, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"xsub", mkXSub, plain, false, true, false, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"req", socket.NewReqEngine, plain, true, false, true, false, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"rep", socket.NewRepEngine, plain, false, true, false, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"router", func() socket.Engine { return socket.NewRouterEngine(false) }, routed, false, true, true, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"stream", socket.NewStreamEngine, routed, false, true, true, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := testutil.Ctx(t)
			send := func(eng socket.Engine) func() error {
				return func() error { return eng.Send(context.Background(), tc.sendMsg()) }
			}
			recv := func(eng socket.Engine) func() error {
				return func() error { _, err := eng.Recv(context.Background()); return err }
			}

			// Attach/detach idempotence, and the FSM sentinels on an open
			// engine with a peer attached.
			eng := tc.mk()
			a, b := pipe.NewPair(2, 2, pipe.PeerInfo{}, pipe.PeerInfo{})
			defer closePipes(a, b)
			require.NoError(t, eng.AttachPipe(a), "first attach")
			if tc.reattachOK {
				require.NoError(t, eng.AttachPipe(a), "re-attach must be a no-op")
			} else {
				require.ErrorIs(t, eng.AttachPipe(a), zmqerr.ErrFSMViolation, "pair re-attach")
			}
			if errors.Is(tc.closedSendErr, zmqerr.ErrFSMViolation) {
				require.ErrorIs(t, eng.Send(ctx, tc.sendMsg()), zmqerr.ErrFSMViolation, "send on a recv-only engine")
			}
			if errors.Is(tc.closedRecvErr, zmqerr.ErrFSMViolation) {
				_, err := eng.Recv(ctx)
				require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "recv on a send-only engine")
			}
			require.NoError(t, eng.DetachPipe(a), "detach")
			require.NoError(t, eng.DetachPipe(a), "double detach must be a no-op")
			require.NoError(t, eng.Close(), "close")
			require.NoError(t, eng.Close(), "double close must be a no-op")

			// Closed-engine sentinels (and attach-after-close).
			eng = tc.mk()
			require.NoError(t, eng.Close())
			require.ErrorIs(t, eng.AttachPipe(a), zmqerr.ErrSocketClosed, "attach after close")
			require.ErrorIs(t, eng.Send(ctx, tc.sendMsg()), tc.closedSendErr, "send after close")
			_, err := eng.Recv(ctx)
			require.ErrorIs(t, err, tc.closedRecvErr, "recv after close")

			// Close unblocks Send/Recv parked with no pipe attached.
			eng = tc.mk()
			var ops []func() error
			if tc.blockSend {
				ops = append(ops, send(eng))
			}
			if tc.blockRecv {
				ops = append(ops, recv(eng))
			}
			requireCloseUnblocks(t, eng, ops...)

			// Close unblocks Send/Recv parked on an attached pipe: Send at
			// HWM (the single slot is pre-filled), Recv on an empty pipe.
			if !tc.attachedSend && !tc.attachedRecv {
				return
			}
			eng = tc.mk()
			// The engine's end sees a peer whose identity the routed
			// message names, so ROUTER/STREAM route to this pipe.
			pa, pb := pipe.NewPair(1, 1, pipe.PeerInfo{Identity: []byte("id")}, pipe.PeerInfo{})
			defer closePipes(pa, pb)
			require.NoError(t, eng.AttachPipe(pa), "attach")
			ops = ops[:0]
			if tc.attachedSend {
				require.NoError(t, pa.Send(ctx, tc.sendMsg()), "fill the pipe to HWM")
				ops = append(ops, send(eng))
			}
			if tc.attachedRecv {
				ops = append(ops, recv(eng))
			}
			requireCloseUnblocks(t, eng, ops...)
		})
	}
}

// requireCloseUnblocks parks every op concurrently, asserts none returns on
// its own, closes eng, and asserts each op then returns ErrSocketClosed.
// With no ops it only closes eng.
func requireCloseUnblocks(t *testing.T, eng socket.Engine, ops ...func() error) {
	t.Helper()
	if len(ops) == 0 {
		require.NoError(t, eng.Close(), "close")
		return
	}
	errs := testutil.RequireUnblocks(t, func() []error {
		errs := make([]error, len(ops))
		var wg sync.WaitGroup
		for i, op := range ops {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = op()
			}()
		}
		wg.Wait()
		return errs
	}, func() { require.NoError(t, eng.Close(), "close") })
	for i, err := range errs {
		require.ErrorIs(t, err, zmqerr.ErrSocketClosed, "parked op %d after close", i)
	}
}
