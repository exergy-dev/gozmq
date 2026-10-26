package security_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gozmq "github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/security"
)

func TestZapHandler_IntegratesWithPlainHandshake(t *testing.T) {
	t.Run("accept", func(t *testing.T) {
		var plainAuthCalls atomic.Int32
		handler := security.ZAPHandler(func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			require.Equal(t, "global", domain)
			require.Equal(t, "PLAIN", mech)
			require.Len(t, credentials, 2)
			require.Equal(t, "alice", string(credentials[0]))
			require.Equal(t, "hunter2", string(credentials[1]))
			require.NotEmpty(t, address, "address unexpectedly empty")
			return "alice", map[string]string{"Role": "admin"}, nil
		})

		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(handler))
		defer ctxZmq.Close()

		serverEvents := make(chan gozmq.Event, 16)
		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithPlainAuth(func(string, string) bool {
				plainAuthCalls.Add(1)
				return false
			}),
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(serverEvents, gozmq.EventAll))
		require.NoError(t, err)
		defer server.Close()
		require.NoError(t, server.Bind("tcp://127.0.0.1:0"))

		client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithPlainClient("alice", "hunter2"))
		require.NoError(t, err)
		defer client.Close()
		require.NoError(t, client.Connect(server.LastEndpoint()))

		ev := testutil.WaitEvent(t, serverEvents, 5*time.Second, isHandshakeEvent)
		require.Equal(t, gozmq.EventHandshakeSucceeded, ev.Type, "want success, got reason=%s", ev.Reason)
		require.Equal(t, int32(0), plainAuthCalls.Load(), "plain auth fallback should not be called")

		callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("secret"))))
		msg, err := server.RecvMsg(callCtx)
		require.NoError(t, err)
		require.Equal(t, "secret", string(msg.Bytes()))
	})

	t.Run("reject", func(t *testing.T) {
		handler := security.ZAPHandler(func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, zmqerr.ErrAuthFailed
		})

		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(handler))
		defer ctxZmq.Close()

		serverEvents := make(chan gozmq.Event, 16)
		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithPlainAuth(func(string, string) bool { return true }),
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(serverEvents, gozmq.EventAll))
		require.NoError(t, err)
		defer server.Close()
		require.NoError(t, server.Bind("tcp://127.0.0.1:0"))

		client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithPlainClient("alice", "wrong"))
		require.NoError(t, err)
		defer client.Close()
		require.NoError(t, client.Connect(server.LastEndpoint()))

		ev := testutil.WaitEvent(t, serverEvents, 5*time.Second, isHandshakeEvent)
		require.Equal(t, gozmq.EventHandshakeFailedAuth, ev.Type, "want auth failure, got reason=%s", ev.Reason)
	})
}

func TestZapHandler_IntegratesWithCurveHandshake(t *testing.T) {
	serverPub, serverSec := testutil.CurveKeys(t)
	clientPub, clientSec := testutil.CurveKeys(t)

	t.Run("accept", func(t *testing.T) {
		handler := security.ZAPHandler(func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
			require.Equal(t, "global", domain)
			require.Equal(t, "CURVE", mech)
			require.Len(t, credentials, 1)
			require.Equal(t, string(clientPub[:]), string(credentials[0]))
			require.NotEmpty(t, address, "address unexpectedly empty")
			return "client", nil, nil
		})

		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(handler))
		defer ctxZmq.Close()

		serverEvents := make(chan gozmq.Event, 16)
		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithCurveServer(serverSec[:]),
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(serverEvents, gozmq.EventAll))
		require.NoError(t, err)
		defer server.Close()
		require.NoError(t, server.Bind("tcp://127.0.0.1:0"))

		client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]))
		require.NoError(t, err)
		defer client.Close()
		require.NoError(t, client.Connect(server.LastEndpoint()))

		ev := testutil.WaitEvent(t, serverEvents, 5*time.Second, isHandshakeEvent)
		require.Equal(t, gozmq.EventHandshakeSucceeded, ev.Type, "want success, got reason=%s", ev.Reason)

		callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, client.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte("curve-secret"))))
		msg, err := server.RecvMsg(callCtx)
		require.NoError(t, err)
		require.Equal(t, "curve-secret", string(msg.Bytes()))
	})

	t.Run("reject", func(t *testing.T) {
		handler := security.ZAPHandler(func(string, string, string, [][]byte) (string, map[string]string, error) {
			return "", nil, zmqerr.ErrAuthFailed
		})

		ctxZmq := gozmq.NewContext(gozmq.WithZAPHandler(handler))
		defer ctxZmq.Close()

		serverEvents := make(chan gozmq.Event, 16)
		server, err := ctxZmq.NewSocket(gozmq.Pair,
			gozmq.WithCurveServer(serverSec[:]),
			gozmq.WithZAPDomain("global"),
			gozmq.WithMonitor(serverEvents, gozmq.EventAll))
		require.NoError(t, err)
		defer server.Close()
		require.NoError(t, server.Bind("tcp://127.0.0.1:0"))

		client, err := ctxZmq.NewSocket(gozmq.Pair, gozmq.WithCurveClient(serverPub[:], clientPub[:], clientSec[:]))
		require.NoError(t, err)
		defer client.Close()
		require.NoError(t, client.Connect(server.LastEndpoint()))

		ev := testutil.WaitEvent(t, serverEvents, 5*time.Second, isHandshakeEvent)
		require.Equal(t, gozmq.EventHandshakeFailedAuth, ev.Type, "want auth failure, got reason=%s", ev.Reason)
	})
}

func isHandshakeEvent(ev gozmq.Event) bool {
	return ev.Type == gozmq.EventHandshakeSucceeded || ev.Type == gozmq.EventHandshakeFailedAuth || ev.Type == gozmq.EventHandshakeFailedProtocol
}
