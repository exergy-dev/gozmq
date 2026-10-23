package gozmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
)

// TestHWMZeroDoesNotBlockWithoutReceiver verifies that WithSendHWM(0) /
// WithRecvHWM(0) (unbounded) lets a PAIR sender enqueue freely with no
// active receiver, on both the inproc direct-pipe path and the TCP
// session path.
func TestHWMZeroDoesNotBlockWithoutReceiver(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		endpoint string
		count    int
	}{
		{"direct-inproc", "inproc://hwm-zero-direct", 1000},
		{"session-tcp", "tcp://127.0.0.1:0", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, server, client := newSocketPair(t, gozmq.Pair, tc.endpoint,
				gozmq.WithSendHWM(0), gozmq.WithRecvHWM(0))

			sendManyWithoutReceiver(t, client, tc.count)
			recvMany(t, server, tc.count)
		})
	}
}

func sendManyWithoutReceiver(t *testing.T, sock *gozmq.Socket, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := sock.SendMsg(ctx, gozmq.NewMsgFromBytes([]byte(fmt.Sprintf("%04d", i))))
		cancel()
		require.NoError(t, err, "send[%d]", i)
	}
}

func recvMany(t *testing.T, sock *gozmq.Socket, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		m, err := sock.RecvMsg(ctx)
		cancel()
		require.NoError(t, err, "recv[%d]", i)
		m.Release()
	}
}
