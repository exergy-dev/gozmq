package gozmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/exergy-dev/gozmq"
)

// TestRecvHWM0NoLeak: an HWM-0 inbound pipe whose session has ended with
// messages still queued must not keep any goroutine alive past
// Socket.Close, on either the bind (accepted session) or the connect
// (reconnector) side.
func TestRecvHWM0NoLeak(t *testing.T) {
	for _, pullBinds := range []bool{true, false} {
		t.Run(fmt.Sprintf("pullBinds=%v", pullBinds), func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			zctx := gozmq.NewContext()
			pull, err := zctx.NewSocket(gozmq.Pull, gozmq.WithRecvHWM(0))
			require.NoError(t, err)
			push, err := zctx.NewSocket(gozmq.Push)
			require.NoError(t, err)
			if pullBinds {
				require.NoError(t, pull.Bind("tcp://127.0.0.1:0"))
				require.NoError(t, push.Connect(pull.LastEndpoint()))
			} else {
				require.NoError(t, push.Bind("tcp://127.0.0.1:0"))
				require.NoError(t, pull.Connect(push.LastEndpoint()))
			}
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for i := 0; i < 100; i++ {
				require.NoError(t, push.SendBytes(c, []byte("x")))
			}
			// Let the messages reach pull's pipe, then end the session
			// from the push side while they are still unread.
			time.Sleep(100 * time.Millisecond)
			require.NoError(t, push.Close())
			time.Sleep(100 * time.Millisecond)
			require.NoError(t, pull.Close())
			require.NoError(t, zctx.Close())
		})
	}
}
