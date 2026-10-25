package curve

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/nacl/box"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
	"github.com/exergy-dev/gozmq/security"
)

// genPair is a tiny helper wrapping GenerateKeyPair with a fatal-on-error
// to keep test bodies uncluttered.
func genPair(t testing.TB) (Key, Key) {
	t.Helper()
	pub, sec, err := GenerateKeyPair()
	require.NoError(t, err, "GenerateKeyPair")
	return pub, sec
}

// TestCurveHandshakeEndToEnd drives a client and a server mechanism
// lock-step through the full HELLO → WELCOME → INITIATE → READY sequence,
// then exchanges user traffic in both directions through EncodeFrame /
// DecodeFrame. The handshake should converge to done=true on both sides
// and the metadata round-trips.
func TestCurveHandshakeEndToEnd(t *testing.T) {
	serverPub, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)

	clientMeta := map[string]string{
		"Socket-Type": "DEALER",
		"Identity":    "client-xyz",
	}
	serverMeta := map[string]string{
		"Socket-Type": "ROUTER",
		"Resource":    "/api",
	}

	client, err := NewCurveClient(serverPub, clientPub, clientSec, clientMeta)
	require.NoError(t, err, "NewCurveClient")
	server, err := NewCurveServer(serverSec, serverMeta)
	require.NoError(t, err, "NewCurveServer")

	require.False(t, client.AsServer(), "client.AsServer = true")
	require.True(t, server.AsServer(), "server.AsServer = false")
	require.Equal(t, "CURVE", client.Name())
	require.Equal(t, "CURVE", server.Name())

	// Step 1: client seeds -> HELLO.
	hello, done, err := client.NextHandshake(nil)
	require.NoError(t, err, "client HELLO")
	require.False(t, done, "client HELLO: done=true")
	require.NotEmpty(t, hello, "client HELLO: empty")

	// Server seed is passive; the session layer reads HELLO first.
	sOut, sDone, err := server.NextHandshake(nil)
	require.NoError(t, err, "server seed")
	require.Nil(t, sOut, "server seed: out should be nil")
	require.False(t, sDone, "server seed: done should be false")

	// Step 2: server ingests HELLO, emits WELCOME.
	welcome, done, err := server.NextHandshake(hello)
	require.NoError(t, err, "server WELCOME")
	require.False(t, done, "server WELCOME: done=true")
	require.NotEmpty(t, welcome, "server WELCOME: empty")

	// Step 3: client ingests WELCOME, emits INITIATE.
	initiate, done, err := client.NextHandshake(welcome)
	require.NoError(t, err, "client INITIATE")
	require.False(t, done, "client INITIATE: done=true")
	require.NotEmpty(t, initiate, "client INITIATE: empty")

	// Step 4: server ingests INITIATE, emits READY; done on server.
	ready, done, err := server.NextHandshake(initiate)
	require.NoError(t, err, "server READY")
	require.True(t, done, "server READY: done=false, want true")
	require.NotEmpty(t, ready, "server READY: empty")

	// Step 5: client ingests READY; done on client.
	extra, done, err := client.NextHandshake(ready)
	require.NoError(t, err, "client READY ingest")
	require.True(t, done, "client READY ingest: done=false, want true")
	require.Nil(t, extra, "client READY ingest: out should be nil")

	// Metadata round-tripped in both directions.
	cImpl := client.(*curveMech)
	sImpl := server.(*curveMech)
	require.Equal(t, serverMeta, cImpl.PeerMetadata(), "client peer meta")
	require.Equal(t, clientMeta, sImpl.PeerMetadata(), "server peer meta")
	// Server learned the client long-term key during INITIATE.
	require.Equal(t, clientPub, sImpl.peerPublic, "server did not recover client long-term public")

	// Traffic phase: exchange multiple frames in both directions and
	// assert round-trip fidelity.
	frames := [][]byte{
		[]byte("hello server"),
		nil,
		{},
		bytes.Repeat([]byte{0xAA}, 4096),
		[]byte("short"),
	}
	for i, f := range frames {
		// client -> server
		wireFlags, wire, err := client.EncodeFrame(0, f)
		require.NoError(t, err, "client EncodeFrame[%d]", i)
		gotFlags, got, err := server.DecodeFrame(wireFlags, wire)
		require.NoError(t, err, "server DecodeFrame[%d]", i)
		require.Equal(t, byte(0), gotFlags, "client->server frame %d flags", i)
		require.True(t, bytes.Equal(got, f), "client->server frame %d round-trip: got %x, want %x", i, got, f)
		// server -> client
		wireFlags2, wire2, err := server.EncodeFrame(0, f)
		require.NoError(t, err, "server EncodeFrame[%d]", i)
		gotFlags2, got2, err := client.DecodeFrame(wireFlags2, wire2)
		require.NoError(t, err, "client DecodeFrame[%d]", i)
		require.Equal(t, byte(0), gotFlags2, "server->client frame %d flags", i)
		require.True(t, bytes.Equal(got2, f), "server->client frame %d round-trip: got %x, want %x", i, got2, f)
	}
}

func TestCurveMessageFlagsRoundTrip(t *testing.T) {
	client, server := runHandshake(t)
	payload := []byte("multipart")

	wireFlags, wire, err := client.EncodeFrame(zmtp.FlagMore, payload)
	require.NoError(t, err, "client EncodeFrame")
	require.Zero(t, wireFlags&zmtp.FlagMore, "wire flags carried outer MORE: %#x", wireFlags)
	gotFlags, got, err := server.DecodeFrame(wireFlags, wire)
	require.NoError(t, err, "server DecodeFrame")
	require.NotZero(t, gotFlags&zmtp.FlagMore, "decoded flags missing MORE: %#x", gotFlags)
	require.Equal(t, payload, got, "decoded payload")
}

// openInnerFlags decrypts a MESSAGE body with the receiver's keys and
// returns the inner flags byte, without advancing the receiver's state.
func openInnerFlags(t *testing.T, recv *curveMech, wire []byte) byte {
	t.Helper()
	off := 1 + len(cmdMessage)
	nonce := withCounter(recv.decNonce, binary.BigEndian.Uint64(wire[off:off+8]))
	plain, ok := box.OpenAfterPrecomputation(nil, wire[off+8:], &nonce, &recv.shared)
	require.True(t, ok, "box open")
	require.NotEmpty(t, plain, "inner plaintext")
	return plain[0]
}

// sealMessage builds a MESSAGE body from sender with an arbitrary inner
// plaintext (flags byte included), bypassing EncodeFrame's flag mapping.
func sealMessage(sender *curveMech, inner []byte) []byte {
	counter := sender.ourShortNonce
	sender.ourShortNonce++
	nonce := withCounter(sender.encNonce, counter)
	body := append([]byte{byte(len(cmdMessage))}, cmdMessage...)
	body = binary.BigEndian.AppendUint64(body, counter)
	return box.SealAfterPrecomputation(body, inner, &nonce, &sender.shared)
}

// TestCurveMessageCommandFlag pins the libzmq wire format for traffic-phase
// commands (heartbeat PING/PONG): the COMMAND flag travels as bit 0x02 of
// the encrypted flags byte and the outer frame carries no COMMAND flag.
// libzmq drops the outer flags of a MESSAGE, so an outer-only COMMAND made
// gozmq's PINGs surface as user data there, and its inner 0x02 was
// rejected here as malformed, tearing the session down.
func TestCurveMessageCommandFlag(t *testing.T) {
	c, s := runHandshake(t)
	client, server := c.(*curveMech), s.(*curveMech)
	ping := zmtp.EncodeCommand(zmtp.Command{Name: "PING", Body: []byte{0x00, 0x0a}})

	for _, tc := range []struct {
		name  string
		flags uint8
		inner byte
	}{
		{"data", 0, 0x00},
		{"more", zmtp.FlagMore, innerFlagMore},
		{"command", zmtp.FlagCommand, innerFlagCommand},
		{"command+more", zmtp.FlagCommand | zmtp.FlagMore, innerFlagCommand | innerFlagMore},
	} {
		wireFlags, wire, err := client.EncodeFrame(tc.flags, ping)
		require.NoError(t, err, "%s: EncodeFrame", tc.name)
		require.Zero(t, wireFlags&(zmtp.FlagMore|zmtp.FlagCommand), "%s: outer flags %#x", tc.name, wireFlags)
		require.Equal(t, tc.inner, openInnerFlags(t, server, wire), "%s: inner flags", tc.name)

		gotFlags, got, err := server.DecodeFrame(wireFlags, wire)
		require.NoError(t, err, "%s: DecodeFrame", tc.name)
		require.Equal(t, tc.flags, gotFlags, "%s: decoded flags", tc.name)
		require.Equal(t, ping, got, "%s: decoded body", tc.name)
	}
}

// TestCurveMessageOuterFlagsIgnored: the outer wire flags of a MESSAGE are
// not authenticated, so they must not change how the plaintext is handled.
// Only the encrypted inner flags byte decides MORE and COMMAND, as in
// libzmq. An on-path attacker flipping the outer COMMAND bit would
// otherwise have a data frame consumed as a command.
func TestCurveMessageOuterFlagsIgnored(t *testing.T) {
	c, s := runHandshake(t)
	client, server := c.(*curveMech), s.(*curveMech)
	payload := []byte("\x04PING\x00\x0a")
	for _, outer := range []uint8{zmtp.FlagCommand, zmtp.FlagMore, zmtp.FlagCommand | zmtp.FlagMore} {
		wire := sealMessage(client, append([]byte{0x00}, payload...))
		gotFlags, got, err := server.DecodeFrame(outer, wire)
		require.NoError(t, err, "DecodeFrame outer=%#x", outer)
		require.Zero(t, gotFlags, "outer flags %#x must not leak into decoded flags", outer)
		require.Equal(t, payload, got, "decoded body")
	}
}

// TestCurveMessageUnknownInnerFlagRejected: inner bits other than MORE and
// COMMAND are undefined and treated as a malformed frame.
func TestCurveMessageUnknownInnerFlagRejected(t *testing.T) {
	c, s := runHandshake(t)
	client, server := c.(*curveMech), s.(*curveMech)
	wire := sealMessage(client, []byte{0x04, 'x'})
	_, _, err := server.DecodeFrame(0, wire)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "inner flags 0x04")
}

// TestCurveMetadataIsolated confirms Metadata returns a copy so external
// mutation can't leak into what the mechanism sends on the wire.
func TestCurveMetadataIsolated(t *testing.T) {
	_, serverSec := genPair(t)
	local := map[string]string{"Socket-Type": "ROUTER"}
	m, err := NewCurveServer(serverSec, local)
	require.NoError(t, err, "NewCurveServer")
	local["Socket-Type"] = "PUB" // mutation after construction
	got := m.Metadata()
	require.Equal(t, "ROUTER", got["Socket-Type"], "Metadata leaked caller mutation")
	got["Socket-Type"] = "SUB" // mutation of returned map
	again := m.Metadata()
	require.Equal(t, "ROUTER", again["Socket-Type"], "Metadata leaked return-map mutation")
}

// TestCurveEncodeBeforeHandshakeFails ensures EncodeFrame / DecodeFrame
// refuse to run until the handshake is complete.
func TestCurveEncodeBeforeHandshakeFails(t *testing.T) {
	serverPub, _ := genPair(t)
	clientPub, clientSec := genPair(t)
	m, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	require.NoError(t, err, "NewCurveClient")
	_, _, err = m.EncodeFrame(0, []byte("x"))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "EncodeFrame before handshake")
	_, _, err = m.DecodeFrame(0, []byte("x"))
	require.ErrorIs(t, err, zmqerr.ErrFSMViolation, "DecodeFrame before handshake")
}

// TestCurveHelloRejectsMalformed checks that invalid HELLO bodies surface
// ErrMalformedFrame / ErrAuthFailed per contract.
func TestCurveHelloRejectsMalformed(t *testing.T) {
	_, serverSec := genPair(t)
	server, err := NewCurveServer(serverSec, nil)
	require.NoError(t, err, "NewCurveServer")
	// Drive the server past its passive seed step so the next call
	// expects a HELLO.
	_, _, err = server.NextHandshake(nil)
	require.NoError(t, err, "server seed")

	t.Run("short-body", func(t *testing.T) {
		_, _, err := server.NextHandshake([]byte{0x05, 'H', 'E', 'L', 'L', 'O'})
		require.ErrorIs(t, err, zmqerr.ErrMalformedFrame)
	})
}

// TestCurveClientSeedRejectsNonNil: the client's first call must receive
// in=nil or the contract is violated.
func TestCurveClientSeedRejectsNonNil(t *testing.T) {
	serverPub, _ := genPair(t)
	clientPub, clientSec := genPair(t)
	c, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	require.NoError(t, err, "NewCurveClient")
	_, _, err = c.NextHandshake([]byte{0x00})
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "non-nil seed input")
}

// TestCurveMessageTampering flips a byte in a MESSAGE payload and asserts
// DecodeFrame returns ErrMalformedFrame.
func TestCurveMessageTampering(t *testing.T) {
	client, server := runHandshake(t)
	wireFlags, wire, err := client.EncodeFrame(0, []byte("hello"))
	require.NoError(t, err, "EncodeFrame")
	wire[len(wire)-4] ^= 0x80
	_, _, err = server.DecodeFrame(wireFlags, wire)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "tampered MESSAGE")
}

// TestCurveMessageReplayRejected captures a valid MESSAGE envelope, lets the
// peer accept it once, then re-injects the same bytes. The second DecodeFrame
// must fail with ErrMalformedFrame because the short-nonce counter has not
// advanced. Without the requireCounterAdvance guard the ciphertext opens
// successfully (same nonce, same key, same plaintext) and the replay slips
// through — the exact attack RFC 26 §3's monotonic counter protects against.
func TestCurveMessageReplayRejected(t *testing.T) {
	client, server := runHandshake(t)

	flags, wire, err := client.EncodeFrame(0, []byte("first"))
	require.NoError(t, err, "EncodeFrame")
	// Keep a pristine copy — DecodeFrame does not mutate its input but we
	// want replay to be byte-identical regardless.
	replay := append([]byte(nil), wire...)

	_, _, err = server.DecodeFrame(flags, wire)
	require.NoError(t, err, "first DecodeFrame")
	_, _, err = server.DecodeFrame(flags, replay)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "replayed MESSAGE")
}

// TestCurveMessageReorderRejected: a counter older than peerShortNonce must
// be rejected even if it carries a not-yet-seen ciphertext. Out-of-order
// delivery is impossible on the ZMTP transports we support (all stream-
// ordered) so rejecting is the safe choice and matches libzmq.
func TestCurveMessageReorderRejected(t *testing.T) {
	client, server := runHandshake(t)

	_, firstWire, err := client.EncodeFrame(0, []byte("first"))
	require.NoError(t, err, "EncodeFrame first")
	flags, secondWire, err := client.EncodeFrame(0, []byte("second"))
	require.NoError(t, err, "EncodeFrame second")

	// Deliver them out of order. The server accepts "second" (counter n+1),
	// advancing peerShortNonce to n+2. "first" (counter n) must then be
	// rejected as non-monotonic.
	_, _, err = server.DecodeFrame(flags, secondWire)
	require.NoError(t, err, "deliver second")
	_, _, err = server.DecodeFrame(flags, firstWire)
	require.ErrorIs(t, err, zmqerr.ErrMalformedFrame, "reordered MESSAGE")
}

// newMechPair creates a fresh client/server mechanism pair with random keys.
func newMechPair(t testing.TB) (client, server security.Mech) {
	t.Helper()
	serverPub, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)
	client, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	require.NoError(t, err, "NewCurveClient")
	server, err = NewCurveServer(serverSec, nil)
	require.NoError(t, err, "NewCurveServer")
	return client, server
}

// driveToWelcome drives a client/server pair through HELLO + server seed +
// WELCOME and returns the WELCOME bytes. Used by tests that need to inject
// faults after the WELCOME step.
func driveToWelcome(t testing.TB, client, server security.Mech) []byte {
	t.Helper()
	hello, _, err := client.NextHandshake(nil)
	require.NoError(t, err, "HELLO")
	_, _, err = server.NextHandshake(nil)
	require.NoError(t, err, "server seed")
	welcome, _, err := server.NextHandshake(hello)
	require.NoError(t, err, "WELCOME")
	return welcome
}

// runHandshake drives a fresh client/server pair through the five-step
// handshake and returns them ready for traffic-phase tests.
func runHandshake(t testing.TB) (security.Mech, security.Mech) {
	t.Helper()
	client, server := newMechPair(t)
	welcome := driveToWelcome(t, client, server)
	initiate, _, err := client.NextHandshake(welcome)
	require.NoError(t, err, "INITIATE")
	ready, _, err := server.NextHandshake(initiate)
	require.NoError(t, err, "READY")
	_, done, err := client.NextHandshake(ready)
	require.NoError(t, err, "READY ingest")
	require.True(t, done, "READY ingest: done=false")
	return client, server
}

// TestCurveWrongServerKey: supplying a bogus server public to the client
// makes WELCOME fail to decrypt.
func TestCurveWrongServerKey(t *testing.T) {
	_, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)
	var bogus Key
	for i := range bogus {
		bogus[i] = byte(i + 1)
	}
	client, err := NewCurveClient(bogus, clientPub, clientSec, nil)
	require.NoError(t, err, "NewCurveClient")
	server, err := NewCurveServer(serverSec, nil)
	require.NoError(t, err, "NewCurveServer")
	hello, _, err := client.NextHandshake(nil)
	require.NoError(t, err, "HELLO")
	// Server's parseHello will fail because it decrypts the signature box
	// with its own long-term secret expecting it to pair with c'. But the
	// client boxed using the BOGUS server key; the signature won't open.
	_, _, err = server.NextHandshake(nil)
	require.NoError(t, err, "server seed")
	_, _, err = server.NextHandshake(hello)
	require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "bogus-server-key HELLO")
}

// TestCurveClientConstructorValidatesSecretPublic ensures the constructor
// rejects a client key pair whose public doesn't pair with the secret.
func TestCurveClientConstructorValidatesSecretPublic(t *testing.T) {
	serverPub, _ := genPair(t)
	_, clientSec := genPair(t)
	var bogus Key
	for i := range bogus {
		bogus[i] = 0x42
	}
	_, err := NewCurveClient(serverPub, bogus, clientSec, nil)
	require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "constructor accepted mismatched public")
}

// TestCurve_NonceExhaustion_Send asserts EncodeFrame refuses to emit a
// frame once ourShortNonce has reached math.MaxUint64. The guard wraps
// zmqerr.ErrNonceExhausted so operators can detect the condition.
func TestCurve_NonceExhaustion_Send(t *testing.T) {
	client, _ := runHandshake(t)
	cImpl := client.(*curveMech)
	// Reach into the mechanism via the same-package accessor to pin the
	// send counter to MaxUint64. This is a white-box test — production
	// code has no need for such a hook. checkNonceCounter treats
	// MaxUint64 itself as "already exhausted," so the NEXT send must
	// fail without ever touching box.Seal.
	cImpl.ourShortNonce = math.MaxUint64

	_, _, err := client.EncodeFrame(0, []byte("should-not-emit"))
	require.ErrorIs(t, err, zmqerr.ErrNonceExhausted, "EncodeFrame at exhausted counter")
	// Counter must not have advanced past MaxUint64 (i.e. no wrap).
	require.Equal(t, uint64(math.MaxUint64), cImpl.ourShortNonce, "ourShortNonce advanced past MaxUint64")
}

// TestCurve_NonceExhaustion_Recv asserts DecodeFrame rejects a frame
// whose wire counter is MaxUint64. The peer reaching that value is a
// protocol violation (RFC 26 expects rekeying long before), so we tear
// the session down with the same ErrNonceExhausted sentinel.
func TestCurve_NonceExhaustion_Recv(t *testing.T) {
	client, server := runHandshake(t)

	// Our own Encode guard blocks us from legitimately producing a
	// MESSAGE with counter == MaxUint64, so craft the frame manually:
	// take the server's next encoded frame and overwrite its 8-byte
	// counter with 0xFF…FF. The client's DecodeFrame guard runs BEFORE
	// box.Open and must return ErrNonceExhausted synchronously — the
	// MAC would fail too, but the sentinel is specifically what we
	// want so operators can distinguish wrap from tampering.
	wireFlags, wire, err := server.EncodeFrame(0, []byte("nothing-important"))
	require.NoError(t, err, "server EncodeFrame setup")
	nameOff := 1 + len("MESSAGE") // layout: [len]"MESSAGE" | counter(8) | cipher
	for i := 0; i < 8; i++ {
		wire[nameOff+i] = 0xFF
	}

	_, _, err = client.DecodeFrame(wireFlags, wire)
	require.ErrorIs(t, err, zmqerr.ErrNonceExhausted, "client accepted MESSAGE with MaxUint64 counter")
}

// TestCurve_RejectsLowOrderClientLongKey: the server rejects an INITIATE
// whose advertised long-term client key is any low-order encoding (zero,
// the order-8 points, ...) before it tries to open the vouch.
func TestCurve_RejectsLowOrderClientLongKey(t *testing.T) {
	for name, k := range lowOrderEncodings(t) {
		client, server := newMechPair(t)
		welcome := driveToWelcome(t, client, server)
		client.(*curveMech).ourPublic = k // embedded in the INITIATE plaintext
		initiate, _, err := client.NextHandshake(welcome)
		require.NoError(t, err, "%s: client INITIATE build", name)
		_, _, err = server.NextHandshake(initiate)
		require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "%s: server accepted a low-order client key", name)
		require.NotEqual(t, stateDone, server.(*curveMech).state, "%s: server reached stateDone", name)
	}
}

// TestCurve_RejectsZeroClientEphemeralKey: the server must refuse a
// HELLO whose advertised client ephemeral (C') is all zero.
func TestCurve_RejectsZeroClientEphemeralKey(t *testing.T) {
	client, server := newMechPair(t)
	cImpl := client.(*curveMech)
	cImpl.ourEphPublic = Key{} // all-zero ephemeral public on the wire
	hello, _, err := client.NextHandshake(nil)
	require.NoError(t, err, "HELLO build")
	_, _, err = server.NextHandshake(nil)
	require.NoError(t, err, "server seed")
	_, _, err = server.NextHandshake(hello)
	require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "server accepted HELLO with zero C'")
}

// TestCurve_RejectsLowOrderServerKey: the client constructor must
// also reject each of the canonical Curve25519 low-order points.
func TestCurve_RejectsLowOrderServerKey(t *testing.T) {
	clientPub, clientSec := genPair(t)
	for name, pt := range lowOrderEncodings(t) {
		_, err := NewCurveClient(pt, clientPub, clientSec, nil)
		require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "low-order point %s", name)
	}
}

// TestCurveHandshakeTampering: a flipped byte anywhere under a handshake
// box (WELCOME box, INITIATE box, the cookie inside INITIATE) fails that
// step with ErrAuthFailed and leaves the server short of stateDone.
func TestCurveHandshakeTampering(t *testing.T) {
	for _, tc := range []struct {
		name     string
		initiate bool           // tamper with INITIATE (server side) rather than WELCOME (client side)
		mutate   func(b []byte) // the flip
		contains string         // extra error text, if any
	}{
		{"welcome box", false, func(b []byte) { b[32] ^= 0xFF }, ""},
		{"initiate box", true, func(b []byte) { b[len(b)-8] ^= 0x01 }, ""},
		// The cookie field starts after the command name; its first 16
		// bytes are the nonce, so +20 is inside the secretbox ciphertext.
		{"initiate cookie", true, func(b []byte) { b[1+len(cmdInitiate)+20] ^= 0xFF }, "cookie open failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := newMechPair(t)
			welcome := driveToWelcome(t, client, server)
			var err error
			if tc.initiate {
				var initiate []byte
				initiate, _, err = client.NextHandshake(welcome)
				require.NoError(t, err, "INITIATE build")
				tc.mutate(initiate)
				_, _, err = server.NextHandshake(initiate)
			} else {
				tc.mutate(welcome)
				_, _, err = client.NextHandshake(welcome)
			}
			require.ErrorIs(t, err, zmqerr.ErrAuthFailed)
			if tc.contains != "" {
				require.ErrorContains(t, err, tc.contains)
			}
			require.NotEqual(t, stateDone, server.(*curveMech).state, "server reached stateDone")
		})
	}
}

// TestCurveVouchWrongServerKeyRejected tampers the C'||S binding inside the
// vouch: the client is made to vouch for a DIFFERENT (but valid) server
// long-term key. The vouch box still opens — it is sealed to our ephemeral —
// but the server must reject the S half as not matching its own identity.
func TestCurveVouchWrongServerKeyRejected(t *testing.T) {
	client, server := newMechPair(t)
	welcome := driveToWelcome(t, client, server)

	// Swap the client's record of the server long-term public for another
	// valid key after WELCOME is consumed but before INITIATE is built;
	// buildInitiate embeds it as the S half of the vouch plaintext. Both
	// steps run inside one NextHandshake call (parseWelcome opens the box
	// with the genuine S), so drive them individually.
	cImpl := client.(*curveMech)
	cookie, err := cImpl.parseWelcome(welcome)
	require.NoError(t, err, "parseWelcome")
	otherPub, _ := genPair(t)
	cImpl.peerPublic = otherPub

	initiate, err := cImpl.buildInitiate(cookie)
	require.NoError(t, err, "INITIATE build")
	_, _, err = server.NextHandshake(initiate)
	require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "wrong-S vouch accepted")
	require.ErrorContains(t, err, "vouch S mismatch")
	require.NotEqual(t, stateDone, server.(*curveMech).state, "server reached stateDone despite wrong-S vouch")
}

// TestCurveZAP_RejectEmitsError asserts that a ZAP rejection during
// INITIATE returns an encoded ZMTP ERROR command body alongside the
// wrapped ErrAuthFailed, mirroring PLAIN's auth-failure path.
func TestCurveZAP_RejectEmitsError(t *testing.T) {
	client, server := newMechPair(t)
	server.(*curveMech).ConfigureZAP(func(string, string, string, [][]byte) (string, map[string]string, error) {
		return "", nil, zmqerr.ErrAuthFailed
	}, "global", "127.0.0.1:1")

	welcome := driveToWelcome(t, client, server)
	initiate, _, err := client.NextHandshake(welcome)
	require.NoError(t, err, "INITIATE build")

	out, done, err := server.NextHandshake(initiate)
	require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "server error")
	require.False(t, done, "rejected handshake must not be done")
	require.NotEmpty(t, out, "rejection must carry an ERROR command body")
	require.Equal(t, byte(5), out[0], "ERROR name-length byte")
	require.Equal(t, "ERROR", string(out[1:6]), "ERROR command name")
	require.NotEqual(t, stateDone, server.(*curveMech).state, "server reached stateDone despite ZAP rejection")
}

// TestCurveZAP_AcceptMergesMetadata asserts the user-id returned by the ZAP
// handler reaches the server's peer metadata as "User-Id", alongside the
// client's INITIATE properties.
func TestCurveZAP_AcceptMergesMetadata(t *testing.T) {
	serverPub, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)
	client, err := NewCurveClient(serverPub, clientPub, clientSec, map[string]string{
		"Socket-Type": "DEALER",
		"Identity":    "client-1",
	})
	require.NoError(t, err, "NewCurveClient")
	server, err := NewCurveServer(serverSec, nil)
	require.NoError(t, err, "NewCurveServer")

	server.(*curveMech).ConfigureZAP(func(domain, address, mech string, credentials [][]byte) (string, map[string]string, error) {
		require.Equal(t, "CURVE", mech)
		require.Len(t, credentials, 1)
		require.Equal(t, clientPub[:], credentials[0], "ZAP credential is not the client long-term public")
		return "alice", nil, nil
	}, "global", "127.0.0.1:1")

	welcome := driveToWelcome(t, client, server)
	initiate, _, err := client.NextHandshake(welcome)
	require.NoError(t, err, "INITIATE")
	_, done, err := server.NextHandshake(initiate)
	require.NoError(t, err, "READY")
	require.True(t, done, "server not done")

	peer := server.(*curveMech).PeerMetadata()
	require.Equal(t, "DEALER", peer["Socket-Type"], "client property lost")
	require.Equal(t, "alice", peer["User-Id"], "ZAP user-id not surfaced")
}

// TestInnerFlagConstants pins the relationships EncodeFrame/DecodeFrame's
// branchless MORE/COMMAND translation relies on.
func TestInnerFlagConstants(t *testing.T) {
	require.Equal(t, zmtp.FlagMore, innerFlagMore, "MORE bit")
	require.Equal(t, zmtp.FlagCommand>>1, innerFlagCommand, "COMMAND bit")
	require.Zero(t, zmtp.FlagMore&zmtp.FlagCommand)
}
