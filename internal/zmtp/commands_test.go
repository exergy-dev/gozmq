package zmtp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// TestCommandCodec: the command header round-trips, names are checked
// byte-exact, ERROR reasons are clamped and parsed, libzmq's broken ERROR
// name is the one malformed encoding accepted, and subscription frames
// carry the flag byte.
func TestCommandCodec(t *testing.T) {
	t.Parallel()
	body := EncodeCommand(Command{Name: "PING", Body: []byte{0, 10, 'c'}})
	require.Equal(t, []byte{4, 'P', 'I', 'N', 'G', 0, 10, 'c'}, body)
	cmd, err := DecodeCommand(body)
	require.NoError(t, err)
	require.Equal(t, Command{Name: "PING", Body: []byte{0, 10, 'c'}}, cmd)

	payload, err := ExpectCommand(body, "PING")
	require.NoError(t, err)
	require.Equal(t, []byte{0, 10, 'c'}, payload)
	_, err = ExpectCommand(body, "PONG")
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	_, err = ExpectCommand([]byte{9}, "PONG")
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	for _, raw := range [][]byte{nil, {0}, {3, 'A', 'B'}, {2, 'A', 0x01}} {
		_, err := DecodeCommand(raw)
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "%v", raw)
	}
	require.Panics(t, func() { AppendCommandName(nil, string(make([]byte, 256))) })

	long := string(make([]byte, 300))
	reason, isErr, err := DecodeError(EncodeError(long))
	require.NoError(t, err)
	require.True(t, isErr)
	require.Len(t, reason, 255)
	reason, isErr, err = DecodeError(append([]byte{0x5E, 'R', 'R', 'O', 'R'}, 2, 'n', 'o'))
	require.NoError(t, err)
	require.True(t, isErr, "libzmq's broken ERROR name")
	require.Equal(t, "no", reason)
	_, isErr, err = DecodeError(append(AppendCommandName(nil, CmdError), 5, 'x'))
	require.True(t, isErr)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "truncated reason")
	_, isErr, err = DecodeError(body)
	require.False(t, isErr)
	require.NoError(t, err)

	require.Equal(t, []byte{1, 'a', 'b'}, SubscriptionFrame(true, []byte("ab")))
	require.Equal(t, []byte{0}, SubscriptionFrame(false, nil))
}
