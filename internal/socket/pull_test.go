package socket_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

// TestPull_RecvFromPeer proves the PULL engine wires Recv to its
// FairQueue: one message sent by the peer end of the attached pipe comes
// out of the engine. Fairness, wake-on-attach and Close semantics are the
// FairQueue's and are tested in engine_test.go and conformance_test.go.
func TestPull_RecvFromPeer(t *testing.T) {
	t.Parallel()
	eng := socket.NewPullEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, pipe.PeerInfo{}, pipe.PeerInfo{})
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, b.Send(ctx, msg.FromBytes([]byte("work"))), "peer send")
	got, err := eng.Recv(ctx)
	require.NoError(t, err, "recv")
	require.Equal(t, "work", string(got.Bytes()))
}
