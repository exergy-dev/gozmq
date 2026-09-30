package socket_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/msg"
)

// TestEngineConformance pins the cross-engine pieces of the Engine
// contract in one table instead of per-engine copies: error sentinels on
// a closed engine, attach/detach idempotence, attach-after-close, and
// Close unblocking parked Send/Recv with ErrSocketClosed.
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
		// closedSendErr/closedRecvErr: sentinel returned after Close.
		closedSendErr, closedRecvErr error
		// reattachOK: attaching the same pipe twice is a no-op (every
		// engine except PAIR, which rejects any second attach).
		reattachOK bool
	}{
		{"pair", socket.NewPairEngine, plain, true, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, false},
		{"push", socket.NewPushEngine, plain, true, false, zmqerr.ErrSocketClosed, zmqerr.ErrFSMViolation, true},
		{"pull", socket.NewPullEngine, plain, false, true, zmqerr.ErrFSMViolation, zmqerr.ErrSocketClosed, true},
		{"dealer", socket.NewDealerEngine, plain, true, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"pub", socket.NewPubEngine, plain, false, false, zmqerr.ErrSocketClosed, zmqerr.ErrFSMViolation, true},
		{"sub", mkSub, plain, false, true, zmqerr.ErrFSMViolation, zmqerr.ErrSocketClosed, true},
		{"xpub", func() socket.Engine { return socket.NewXPubEngineV(false) }, plain, false, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"xsub", mkXSub, plain, false, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"req", socket.NewReqEngine, plain, true, false, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"rep", socket.NewRepEngine, plain, false, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"router", func() socket.Engine { return socket.NewRouterEngine(false) }, routed, false, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
		{"stream", socket.NewStreamEngine, routed, false, true, zmqerr.ErrSocketClosed, zmqerr.ErrSocketClosed, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := testCtx(t)

			// Attach/detach idempotence.
			eng := tc.mk()
			a, b := pipe.NewPair(2, 2, pipe.PeerInfo{}, pipe.PeerInfo{})
			defer closePipes(a, b)
			require.NoError(t, eng.AttachPipe(a), "first attach")
			if tc.reattachOK {
				require.NoError(t, eng.AttachPipe(a), "re-attach must be a no-op")
			} else {
				require.ErrorIs(t, eng.AttachPipe(a), zmqerr.ErrFSMViolation, "pair re-attach")
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

			// Close unblocks parked Send/Recv with ErrSocketClosed.
			eng = tc.mk()
			var sendDone, recvDone chan error
			if tc.blockSend {
				sendDone = make(chan error, 1)
				go func() { sendDone <- eng.Send(context.Background(), tc.sendMsg()) }()
			}
			if tc.blockRecv {
				recvDone = make(chan error, 1)
				go func() { _, err := eng.Recv(context.Background()); recvDone <- err }()
			}
			if sendDone != nil || recvDone != nil {
				time.Sleep(50 * time.Millisecond) // let the goroutines park
				select {
				case err := <-sendDone:
					require.Fail(t, "send returned before close", "%v", err)
				case err := <-recvDone:
					require.Fail(t, "recv returned before close", "%v", err)
				default:
				}
			}
			require.NoError(t, eng.Close())
			if sendDone != nil {
				select {
				case err := <-sendDone:
					require.ErrorIs(t, err, zmqerr.ErrSocketClosed, "parked send")
				case <-time.After(time.Second):
					require.Fail(t, "close did not unblock parked Send")
				}
			}
			if recvDone != nil {
				select {
				case err := <-recvDone:
					require.ErrorIs(t, err, zmqerr.ErrSocketClosed, "parked recv")
				case <-time.After(time.Second):
					require.Fail(t, "close did not unblock parked Recv")
				}
			}
		})
	}
}
