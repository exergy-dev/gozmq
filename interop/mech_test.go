//go:build interop

package interop

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/security"
)

// mech configures one security mechanism on both roles of a link: the gozmq
// options and the libzmq socket setup for each side.
type mech struct {
	// name is the ZMTP mechanism name: NULL, PLAIN or CURVE.
	name                     string
	gozmqServer, gozmqClient []gozmq.Option
	libServer, libClient     func(e *libEnd)
	// libZAP is what a libzmq server needs to vouch for the client; a
	// libzmq PLAIN server drops every connection without one.
	libZAP security.ZAPHandler
	// creds are the credential frames a ZAP request carries for the client.
	creds [][]byte
	keys  *curveKeys
}

func nullMech() *mech { return &mech{name: "NULL"} }

// The PLAIN credentials every accepting test agrees on.
const (
	plainUser = "alice"
	plainPass = "hunter2"
)

func plainAuth(user, pass string) bool { return user == plainUser && pass == plainPass }

// plainMech is PLAIN with the server accepting exactly what auth accepts and
// the client presenting (user, pass).
func plainMech(auth func(user, pass string) bool, user, pass string) *mech {
	return &mech{
		name:        "PLAIN",
		gozmqServer: []gozmq.Option{gozmq.WithPlainAuth(auth)},
		gozmqClient: []gozmq.Option{gozmq.WithPlainClient(user, pass)},
		libServer:   func(e *libEnd) { e.set(ZMQ_PLAIN_SERVER, 1) },
		libClient: func(e *libEnd) {
			err := e.s.SetOption(ZMQ_PLAIN_USERNAME, user)
			if err != nil && len(user) > 255 {
				// libzmq caps PLAIN credentials at 255 bytes in setsockopt, so
				// an oversized username never reaches the wire from this side.
				e.t.Skipf("libzmq refuses a %d-byte PLAIN username: %v", len(user), err)
			}
			require.NoError(e.t, err)
			e.set(ZMQ_PLAIN_PASSWORD, pass)
		},
		libZAP: func(_, _, mechName string, creds [][]byte) (string, map[string]string, error) {
			if mechName != "PLAIN" || len(creds) != 2 || !auth(string(creds[0]), string(creds[1])) {
				return "", nil, gozmq.ErrAuthFailed
			}
			return string(creds[0]), nil, nil
		},
		creds: [][]byte{[]byte(user), []byte(pass)},
	}
}

type curveKeys struct {
	serverPub, serverSec, clientPub, clientSec [32]byte
}

// curveMech is CURVE with fresh long-term keys for both sides.
func curveMech(t *testing.T) *mech {
	t.Helper()
	var k curveKeys
	k.serverPub, k.serverSec = testutil.CurveKeys(t)
	k.clientPub, k.clientSec = testutil.CurveKeys(t)
	return &mech{
		name:        "CURVE",
		gozmqServer: []gozmq.Option{gozmq.WithCurveServer(k.serverSec[:])},
		gozmqClient: []gozmq.Option{gozmq.WithCurveClient(k.serverPub[:], k.clientPub[:], k.clientSec[:])},
		libServer: func(e *libEnd) {
			e.set(ZMQ_CURVE_SERVER, 1)
			e.set(ZMQ_CURVE_SECRETKEY, k.serverSec[:])
			e.set(ZMQ_CURVE_PUBLICKEY, k.serverPub[:])
		},
		libClient: func(e *libEnd) {
			e.set(ZMQ_CURVE_SERVERKEY, k.serverPub[:])
			e.set(ZMQ_CURVE_PUBLICKEY, k.clientPub[:])
			e.set(ZMQ_CURVE_SECRETKEY, k.clientSec[:])
		},
		creds: [][]byte{k.clientPub[:]},
		keys:  &k,
	}
}
