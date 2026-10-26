package socket_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/pipe"
	"github.com/exergy-dev/gozmq/internal/socket"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/msg"
)

// TestPush_SendReachesPeer proves the PUSH engine wires Send to its
// LoadBalancer: one message sent through the engine arrives at the peer
// end of the attached pipe. Distribution, HWM blocking and Close
// semantics are the LoadBalancer's and are tested in engine_test.go and
// conformance_test.go.
func TestPush_SendReachesPeer(t *testing.T) {
	t.Parallel()
	eng := socket.NewPushEngine()
	defer eng.Close()

	a, b := pipe.NewPair(1, 1, nil, nil)
	defer closePipes(a, b)
	mustAttach(t, eng, a)

	ctx := testutil.Ctx(t)
	require.NoError(t, eng.Send(ctx, msg.FromBytes([]byte("work"))), "send")
	got, err := b.RecvOr(ctx, nil)
	require.NoError(t, err, "peer recv")
	require.Equal(t, "work", string(got.Bytes()))
}
