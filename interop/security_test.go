//go:build interop

package interop

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/internal/testutil"
	"github.com/exergy-dev/gozmq/security"
)

// outcome is what a security row expects of the handshake.
type outcome int

const (
	wantSuccess    outcome = iota
	wantAuthFailed         // EventHandshakeFailedAuth, exactly
	wantAnyFailure         // any handshake outcome but success
	// wantNoDelivery makes no claim about monitor events: a bounded send
	// from the gozmq client must simply never succeed.
	wantNoDelivery
)

// noDeliveryWindow bounds a send that must not complete: well past the
// loopback handshake RTT, tight enough that a regression surfaces quickly.
const noDeliveryWindow = 500 * time.Millisecond

// TestSecurity: PLAIN and CURVE handshakes against libzmq in both roles,
// with good and bad credentials, and PLAIN carrying the other patterns.
func TestSecurity(t *testing.T) {
	t.Parallel()
	goodPlain := func(*testing.T) *mech { return plainMech(plainAuth, plainUser, plainPass) }
	rows := []struct {
		name    string
		mech    func(t *testing.T) *mech
		dirs    []direction // nil: both
		pattern pattern     // zero: PAIR round trip
		want    outcome
	}{
		{name: "plain", mech: goodPlain},
		{name: "plain-bad-password", want: wantAuthFailed,
			mech: func(*testing.T) *mech { return plainMech(plainAuth, plainUser, "wrong-password") }},
		{name: "plain-empty-username",
			mech: func(*testing.T) *mech {
				return plainMech(func(user, pass string) bool { return user == "" && pass == plainPass }, "", plainPass)
			}},
		// Only a libzmq client can try to present a 256-byte username, and
		// libzmq refuses it in setsockopt, so this row skips on 4.3.x.
		{name: "plain-long-username", dirs: []direction{gozmqBinds}, want: wantAnyFailure,
			mech: func(*testing.T) *mech {
				return plainMech(func(string, string) bool { return true }, strings.Repeat("u", 256), plainPass)
			}},
		{name: "plain-req-rep", mech: goodPlain, pattern: reqRepPattern},
		{name: "plain-dealer-router", mech: goodPlain, pattern: dealerRouterPattern},
		{name: "plain-pub-sub", mech: goodPlain, pattern: pubSubPattern},
		{name: "curve", mech: curveMech},
		// The client pins a server key that is not the server's: HELLO is
		// unreadable to libzmq and the handshake never completes.
		{name: "curve-wrong-server-key", dirs: []direction{libzmqBinds}, want: wantNoDelivery,
			mech: func(t *testing.T) *mech {
				m := curveMech(t)
				wrongPub, _ := testutil.CurveKeys(t)
				require.NotEqual(t, m.keys.serverPub, wrongPub, "key collision")
				m.gozmqClient = []gozmq.Option{gozmq.WithCurveClient(wrongPub[:], m.keys.clientPub[:], m.keys.clientSec[:])}
				return m
			}},
	}
	for _, row := range rows {
		p := row.pattern
		if p.body == nil {
			p = pairPattern
		}
		dirs := row.dirs
		if dirs == nil {
			dirs = bothDirections
		}
		for _, dir := range dirs {
			t.Run(row.name+"/"+dir.String(), func(t *testing.T) {
				t.Parallel()
				sp := p.spec
				sp.mech = row.mech(t)
				l := newLink(t, dir, sp)
				switch row.want {
				case wantSuccess:
					l.requireHandshake()
					p.body(t, l)
				case wantNoDelivery:
					require.Error(t, l.g.sendWithin(noDeliveryWindow, []byte("should-never-arrive")))
				default:
					ev := l.handshake()
					if row.want == wantAuthFailed {
						require.Equal(t, gozmq.EventHandshakeFailedAuth, ev)
					} else {
						require.NotEqual(t, gozmq.EventHandshakeSucceeded, ev)
					}
					l.requireZAPDone()
					if dir == libzmqBinds {
						require.Error(t, l.g.sendWithin(noDeliveryWindow, []byte("should-fail")), "send after a failed handshake")
					}
				}
			})
		}
	}
}

// TestZAP: each mechanism's credentials reach a ZAP handler with the right
// domain and shape, and the handler's verdict decides the handshake, with
// either implementation as the authenticating server.
func TestZAP(t *testing.T) {
	t.Parallel()
	mechs := []func(t *testing.T) *mech{
		func(*testing.T) *mech { return nullMech() },
		func(*testing.T) *mech { return plainMech(plainAuth, plainUser, plainPass) },
		curveMech,
	}
	for _, build := range mechs {
		for _, dir := range bothDirections {
			for _, accept := range []bool{true, false} {
				m := build(t)
				verdict := map[bool]string{true: "accept", false: "reject"}[accept]
				t.Run(strings.ToLower(m.name)+"/"+dir.String()+"/"+verdict, func(t *testing.T) {
					t.Parallel()
					var called atomic.Bool
					handler := security.ZAPHandler(func(domain, _, mechName string, creds [][]byte) (string, map[string]string, error) {
						called.Store(true)
						if !accept {
							return "", nil, gozmq.ErrAuthFailed
						}
						// Runs on the server's ZAP goroutine: record, do not FailNow.
						assert.Equal(t, "global", domain)
						assert.Equal(t, m.name, mechName)
						assert.True(t, framesEqual(m.creds, creds), "credentials %q, want %q", creds, m.creds)
						return "user", nil, nil
					})
					sp := pairSpec()
					sp.mech = m
					sp.serverOpts = []gozmq.Option{gozmq.WithZAPDomain("global")}
					sp.serverLib = func(e *libEnd) { e.set(ZMQ_ZAP_DOMAIN, "global") }
					if m.name == "PLAIN" {
						// The local callback votes the other way: the ZAP verdict must win.
						sp.serverOpts = append(sp.serverOpts, gozmq.WithPlainAuth(func(string, string) bool { return !accept }))
					}
					if dir == gozmqBinds {
						sp.ctxOpts = []gozmq.ContextOption{gozmq.WithZAPHandler(handler)}
					} else {
						sp.libZAP = handler
					}
					l := newLink(t, dir, sp)
					ev := l.handshake()
					switch {
					case accept:
						require.Equal(t, gozmq.EventHandshakeSucceeded, ev)
					case dir == gozmqBinds:
						require.Equal(t, gozmq.EventHandshakeFailedAuth, ev)
					default:
						require.NotEqual(t, gozmq.EventHandshakeSucceeded, ev)
					}
					l.requireZAPDone()
					require.True(t, called.Load(), "ZAP handler was not consulted")
				})
			}
		}
	}
}

// TestCurve_CorruptedWelcome flips one byte inside the cookie box of the
// libzmq server's WELCOME on the wire. Poly1305 must reject the box and the
// gozmq client's handshake must fail rather than silently succeed.
//
// WELCOME body per RFC 26 §2.2: 1-byte name length, "WELCOME" (7 bytes), a
// 16-byte nonce and the 144-byte cookie-and-S' box; offset 100 lands inside
// the box, which starts at offset 24.
func TestCurve_CorruptedWelcome(t *testing.T) {
	t.Parallel()
	m := curveMech(t)
	lCtx := NewCtx()
	t.Cleanup(func() { _ = lCtx.Close() })
	server := newLibEnd(t, lCtx, ZMQ_PAIR)
	m.libServer(server)
	require.NoError(t, server.s.Bind("tcp://127.0.0.1:0"))
	upstream, err := server.s.LastEndpoint()
	require.NoError(t, err)

	const welcomeBodyLen = 1 + 7 + 16 + 144
	proxy := newFlagSniffingProxy(t, upstream)
	proxy.SetMutator(func(clientToServer bool, frameIdx int, body []byte) []byte {
		if clientToServer || frameIdx != 0 {
			return nil // WELCOME is the first server→client frame
		}
		if !assert.Len(t, body, welcomeBodyLen, "WELCOME body (RFC 26 §2.2 shape)") {
			return nil
		}
		out := append([]byte(nil), body...)
		out[100] ^= 0x01
		return out
	})

	gCtx := gozmq.NewContext()
	t.Cleanup(func() { _ = gCtx.Close() })
	client := newGozmqEnd(t, gCtx, gozmq.Pair, m.gozmqClient...)
	require.NoError(t, client.s.Connect(proxy.Addr()))
	// Either FailedAuth or FailedProtocol satisfies the contract.
	require.NotEqual(t, gozmq.EventHandshakeSucceeded, client.handshake(), "handshake with a corrupted WELCOME")
}
