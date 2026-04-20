package curve

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/rkm/gozmq/internal/iface"
	"github.com/rkm/gozmq/internal/zmqerr"
)

// genPair is a tiny helper wrapping GenerateKeyPair with a fatal-on-error
// to keep test bodies uncluttered.
func genPair(t *testing.T) (Key, Key) {
	t.Helper()
	pub, sec, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
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
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	server, err := NewCurveServer(serverSec, serverMeta)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}

	if client.AsServer() {
		t.Fatal("client.AsServer = true")
	}
	if !server.AsServer() {
		t.Fatal("server.AsServer = false")
	}
	if client.Name() != "CURVE" || server.Name() != "CURVE" {
		t.Fatalf("Name: client=%q server=%q", client.Name(), server.Name())
	}

	// Step 1: client seeds -> HELLO.
	hello, done, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("client HELLO: %v", err)
	}
	if done {
		t.Fatal("client HELLO: done=true")
	}
	if len(hello) == 0 {
		t.Fatal("client HELLO: empty")
	}

	// Server seed is passive; the session layer reads HELLO first.
	sOut, sDone, err := server.NextHandshake(nil)
	if err != nil {
		t.Fatalf("server seed: %v", err)
	}
	if sOut != nil || sDone {
		t.Fatalf("server seed: out=%v done=%v, want nil,false", sOut, sDone)
	}

	// Step 2: server ingests HELLO, emits WELCOME.
	welcome, done, err := server.NextHandshake(hello)
	if err != nil {
		t.Fatalf("server WELCOME: %v", err)
	}
	if done {
		t.Fatal("server WELCOME: done=true")
	}
	if len(welcome) == 0 {
		t.Fatal("server WELCOME: empty")
	}

	// Step 3: client ingests WELCOME, emits INITIATE.
	initiate, done, err := client.NextHandshake(welcome)
	if err != nil {
		t.Fatalf("client INITIATE: %v", err)
	}
	if done {
		t.Fatal("client INITIATE: done=true")
	}
	if len(initiate) == 0 {
		t.Fatal("client INITIATE: empty")
	}

	// Step 4: server ingests INITIATE, emits READY; done on server.
	ready, done, err := server.NextHandshake(initiate)
	if err != nil {
		t.Fatalf("server READY: %v", err)
	}
	if !done {
		t.Fatal("server READY: done=false, want true")
	}
	if len(ready) == 0 {
		t.Fatal("server READY: empty")
	}

	// Step 5: client ingests READY; done on client.
	extra, done, err := client.NextHandshake(ready)
	if err != nil {
		t.Fatalf("client READY ingest: %v", err)
	}
	if !done {
		t.Fatal("client READY ingest: done=false, want true")
	}
	if extra != nil {
		t.Fatalf("client READY ingest: out=%v, want nil", extra)
	}

	// Metadata round-tripped in both directions.
	cImpl := client.(*curveMech)
	sImpl := server.(*curveMech)
	if !mapsEqual(cImpl.PeerMetadata(), serverMeta) {
		t.Fatalf("client peer meta: got %v want %v", cImpl.PeerMetadata(), serverMeta)
	}
	if !mapsEqual(sImpl.PeerMetadata(), clientMeta) {
		t.Fatalf("server peer meta: got %v want %v", sImpl.PeerMetadata(), clientMeta)
	}
	// Server learned the client long-term key during INITIATE.
	if sImpl.PeerLongTermPublic() != clientPub {
		t.Fatal("server did not recover client long-term public")
	}

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
		if err != nil {
			t.Fatalf("client EncodeFrame[%d]: %v", i, err)
		}
		gotFlags, got, err := server.DecodeFrame(wireFlags, wire)
		if err != nil {
			t.Fatalf("server DecodeFrame[%d]: %v", i, err)
		}
		if gotFlags != 0 {
			t.Fatalf("client->server frame %d flags: got %#x, want 0", i, gotFlags)
		}
		if !bytes.Equal(got, f) {
			t.Fatalf("client->server frame %d round-trip: got %x, want %x", i, got, f)
		}
		// server -> client
		wireFlags2, wire2, err := server.EncodeFrame(0, f)
		if err != nil {
			t.Fatalf("server EncodeFrame[%d]: %v", i, err)
		}
		gotFlags2, got2, err := client.DecodeFrame(wireFlags2, wire2)
		if err != nil {
			t.Fatalf("client DecodeFrame[%d]: %v", i, err)
		}
		if gotFlags2 != 0 {
			t.Fatalf("server->client frame %d flags: got %#x, want 0", i, gotFlags2)
		}
		if !bytes.Equal(got2, f) {
			t.Fatalf("server->client frame %d round-trip: got %x, want %x", i, got2, f)
		}
	}
}

func TestCurveMessageFlagsRoundTrip(t *testing.T) {
	client, server := runHandshake(t)
	payload := []byte("multipart")

	wireFlags, wire, err := client.EncodeFrame(iface.FlagMore, payload)
	if err != nil {
		t.Fatalf("client EncodeFrame: %v", err)
	}
	if wireFlags&iface.FlagMore != 0 {
		t.Fatalf("wire flags carried outer MORE: %#x", wireFlags)
	}
	gotFlags, got, err := server.DecodeFrame(wireFlags, wire)
	if err != nil {
		t.Fatalf("server DecodeFrame: %v", err)
	}
	if gotFlags&iface.FlagMore == 0 {
		t.Fatalf("decoded flags missing MORE: %#x", gotFlags)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("decoded payload = %q, want %q", got, payload)
	}
}

// TestCurveMetadataIsolated confirms Metadata returns a copy so external
// mutation can't leak into what the mechanism sends on the wire.
func TestCurveMetadataIsolated(t *testing.T) {
	_, serverSec := genPair(t)
	local := map[string]string{"Socket-Type": "ROUTER"}
	m, err := NewCurveServer(serverSec, local)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	local["Socket-Type"] = "PUB" // mutation after construction
	got := m.Metadata()
	if got["Socket-Type"] != "ROUTER" {
		t.Fatalf("Metadata leaked caller mutation: got %q", got["Socket-Type"])
	}
	got["Socket-Type"] = "SUB" // mutation of returned map
	again := m.Metadata()
	if again["Socket-Type"] != "ROUTER" {
		t.Fatalf("Metadata leaked return-map mutation: got %q", again["Socket-Type"])
	}
}

// TestCurveEncodeBeforeHandshakeFails ensures EncodeFrame / DecodeFrame
// refuse to run until the handshake is complete.
func TestCurveEncodeBeforeHandshakeFails(t *testing.T) {
	serverPub, _ := genPair(t)
	clientPub, clientSec := genPair(t)
	m, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	if _, _, err := m.EncodeFrame(0, []byte("x")); err == nil {
		t.Fatal("EncodeFrame before handshake: expected error")
	} else if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("want ErrFSMViolation, got %v", err)
	}
	if _, _, err := m.DecodeFrame(0, []byte("x")); err == nil {
		t.Fatal("DecodeFrame before handshake: expected error")
	} else if !errors.Is(err, zmqerr.ErrFSMViolation) {
		t.Fatalf("want ErrFSMViolation, got %v", err)
	}
}

// TestCurveHelloRejectsMalformed checks that invalid HELLO bodies surface
// ErrMalformedFrame / ErrAuthFailed per contract.
func TestCurveHelloRejectsMalformed(t *testing.T) {
	_, serverSec := genPair(t)
	server, err := NewCurveServer(serverSec, nil)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	// Drive the server past its passive seed step so the next call
	// expects a HELLO.
	_, _, err = server.NextHandshake(nil)
	if err != nil {
		t.Fatalf("server seed: %v", err)
	}

	t.Run("short-body", func(t *testing.T) {
		_, _, err := server.NextHandshake([]byte{0x05, 'H', 'E', 'L', 'L', 'O'})
		if err == nil || !errors.Is(err, zmqerr.ErrMalformedFrame) {
			t.Fatalf("want ErrMalformedFrame, got %v", err)
		}
	})
}

// TestCurveClientSeedRejectsNonNil: the client's first call must receive
// in=nil or the contract is violated.
func TestCurveClientSeedRejectsNonNil(t *testing.T) {
	serverPub, _ := genPair(t)
	clientPub, clientSec := genPair(t)
	c, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	_, _, err = c.NextHandshake([]byte{0x00})
	if err == nil {
		t.Fatal("want error on non-nil seed input")
	}
	if !errors.Is(err, zmqerr.ErrMalformedFrame) {
		t.Fatalf("want ErrMalformedFrame, got %v", err)
	}
}

// TestCurveWelcomeTampering flips a byte in WELCOME and asserts the
// client's box.Open fails with ErrAuthFailed.
func TestCurveWelcomeTampering(t *testing.T) {
	serverPub, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)
	client, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	server, err := NewCurveServer(serverSec, nil)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	hello, _, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("HELLO: %v", err)
	}
	if _, _, err := server.NextHandshake(nil); err != nil {
		t.Fatalf("server seed: %v", err)
	}
	welcome, _, err := server.NextHandshake(hello)
	if err != nil {
		t.Fatalf("WELCOME: %v", err)
	}
	// Flip a byte in the encrypted region (skip past command name + nonce
	// prefix; index 32 sits inside the welcome box).
	welcome[32] ^= 0xFF
	if _, _, err := client.NextHandshake(welcome); err == nil {
		t.Fatal("tampered WELCOME accepted")
	} else if !errors.Is(err, zmqerr.ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed, got %v", err)
	}
}

// TestCurveInitiateTampering flips a byte in INITIATE and asserts the
// server's box.Open fails with ErrAuthFailed.
func TestCurveInitiateTampering(t *testing.T) {
	serverPub, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)
	client, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	server, err := NewCurveServer(serverSec, nil)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	hello, _, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("HELLO: %v", err)
	}
	if _, _, err := server.NextHandshake(nil); err != nil {
		t.Fatalf("server seed: %v", err)
	}
	welcome, _, err := server.NextHandshake(hello)
	if err != nil {
		t.Fatalf("WELCOME: %v", err)
	}
	initiate, _, err := client.NextHandshake(welcome)
	if err != nil {
		t.Fatalf("INITIATE: %v", err)
	}
	// Flip a byte well past the fixed header into the initiate box.
	initiate[len(initiate)-8] ^= 0x01
	if _, _, err := server.NextHandshake(initiate); err == nil {
		t.Fatal("tampered INITIATE accepted")
	} else if !errors.Is(err, zmqerr.ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed, got %v", err)
	}
}

// TestCurveMessageTampering flips a byte in a MESSAGE payload and asserts
// DecodeFrame returns ErrMalformedFrame.
func TestCurveMessageTampering(t *testing.T) {
	client, server := runHandshake(t)
	wireFlags, wire, err := client.EncodeFrame(0, []byte("hello"))
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	wire[len(wire)-4] ^= 0x80
	if _, _, err := server.DecodeFrame(wireFlags, wire); err == nil {
		t.Fatal("tampered MESSAGE accepted")
	} else if !errors.Is(err, zmqerr.ErrMalformedFrame) {
		t.Fatalf("want ErrMalformedFrame, got %v", err)
	}
}

// runHandshake drives a fresh client/server pair through the five-step
// handshake and returns them ready for traffic-phase tests.
func runHandshake(t *testing.T) (iface.SecurityMech, iface.SecurityMech) {
	t.Helper()
	serverPub, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)
	client, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	server, err := NewCurveServer(serverSec, nil)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	hello, _, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("HELLO: %v", err)
	}
	if _, _, err := server.NextHandshake(nil); err != nil {
		t.Fatalf("server seed: %v", err)
	}
	welcome, _, err := server.NextHandshake(hello)
	if err != nil {
		t.Fatalf("WELCOME: %v", err)
	}
	initiate, _, err := client.NextHandshake(welcome)
	if err != nil {
		t.Fatalf("INITIATE: %v", err)
	}
	ready, _, err := server.NextHandshake(initiate)
	if err != nil {
		t.Fatalf("READY: %v", err)
	}
	if _, done, err := client.NextHandshake(ready); err != nil || !done {
		t.Fatalf("READY ingest: err=%v done=%v", err, done)
	}
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
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	server, err := NewCurveServer(serverSec, nil)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	hello, _, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("HELLO: %v", err)
	}
	// Server's parseHello will fail because it decrypts the signature box
	// with its own long-term secret expecting it to pair with c'. But the
	// client boxed using the BOGUS server key; the signature won't open.
	if _, _, err := server.NextHandshake(nil); err != nil {
		t.Fatalf("server seed: %v", err)
	}
	if _, _, err := server.NextHandshake(hello); err == nil {
		t.Fatal("bogus-server-key HELLO accepted")
	} else if !errors.Is(err, zmqerr.ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed, got %v", err)
	}
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
	if err == nil {
		t.Fatal("constructor accepted mismatched public")
	}
	if !errors.Is(err, zmqerr.ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed, got %v", err)
	}
}

// TestCurveSetPeerMetadataStores mirrors the NULL test: SetPeerMetadata is
// a pure store; nil clears.
func TestCurveSetPeerMetadataStores(t *testing.T) {
	_, serverSec := genPair(t)
	m, err := NewCurveServer(serverSec, nil)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	props := map[string]string{"Socket-Type": "REQ"}
	if err := m.SetPeerMetadata(props); err != nil {
		t.Fatalf("SetPeerMetadata: %v", err)
	}
	props["Socket-Type"] = "REP"
	impl := m.(*curveMech)
	if impl.PeerMetadata()["Socket-Type"] != "REQ" {
		t.Fatal("PeerMetadata not isolated from caller mutation")
	}
	if err := m.SetPeerMetadata(nil); err != nil {
		t.Fatalf("SetPeerMetadata(nil): %v", err)
	}
	if impl.PeerMetadata() != nil {
		t.Fatalf("PeerMetadata after nil-set: %v, want nil", impl.PeerMetadata())
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
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
	if err == nil {
		t.Fatal("EncodeFrame at exhausted counter: expected error")
	}
	if !errors.Is(err, zmqerr.ErrNonceExhausted) {
		t.Fatalf("want ErrNonceExhausted, got %v", err)
	}
	// Counter must not have advanced past MaxUint64 (i.e. no wrap).
	if cImpl.ourShortNonce != math.MaxUint64 {
		t.Fatalf("ourShortNonce advanced past MaxUint64: got %d", cImpl.ourShortNonce)
	}
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
	if err != nil {
		t.Fatalf("server EncodeFrame setup: %v", err)
	}
	nameOff := 1 + len("MESSAGE") // layout: [len]"MESSAGE" | counter(8) | cipher
	for i := 0; i < 8; i++ {
		wire[nameOff+i] = 0xFF
	}

	_, _, err = client.DecodeFrame(wireFlags, wire)
	if err == nil {
		t.Fatal("client accepted MESSAGE with MaxUint64 counter")
	}
	if !errors.Is(err, zmqerr.ErrNonceExhausted) {
		t.Fatalf("want ErrNonceExhausted, got %v", err)
	}
}

// TestCurve_RejectsZeroClientLongKey: the server must refuse an
// INITIATE whose client long-term public (C) field is all zero, even
// if the vouch would otherwise open. Without this check, a peer could
// complete the handshake with a key whose shared secret with any
// server is a small-subgroup element.
func TestCurve_RejectsZeroClientLongKey(t *testing.T) {
	serverPub, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)
	client, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	server, err := NewCurveServer(serverSec, nil)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	// Drive HELLO → WELCOME normally.
	hello, _, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("HELLO: %v", err)
	}
	if _, _, err := server.NextHandshake(nil); err != nil {
		t.Fatalf("server seed: %v", err)
	}
	welcome, _, err := server.NextHandshake(hello)
	if err != nil {
		t.Fatalf("WELCOME: %v", err)
	}
	// Before building INITIATE, overwrite the client's own long-term
	// public key to all zeros. buildInitiate will embed the zero key
	// in the INITIATE plaintext; the server must reject at the
	// validatePeerPublicKey check before attempting vouch open.
	cImpl := client.(*curveMech)
	cImpl.ourPublic = Key{}
	initiate, _, err := client.NextHandshake(welcome)
	if err != nil {
		t.Fatalf("client INITIATE build: %v", err)
	}
	_, _, err = server.NextHandshake(initiate)
	if err == nil {
		t.Fatal("server accepted zero client long-term key")
	}
	if !errors.Is(err, zmqerr.ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed, got %v", err)
	}
	// Server must not have reached stateDone.
	sImpl := server.(*curveMech)
	if sImpl.state == stateDone {
		t.Fatal("server reached stateDone despite invalid client key")
	}
}

// TestCurve_RejectsLowOrderClientLongKey: same as above but uses a
// known order-8 point rather than zero. Confirms the low-order list
// is consulted in addition to the all-zero fast-path.
func TestCurve_RejectsLowOrderClientLongKey(t *testing.T) {
	serverPub, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)
	client, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	server, err := NewCurveServer(serverSec, nil)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	hello, _, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("HELLO: %v", err)
	}
	if _, _, err := server.NextHandshake(nil); err != nil {
		t.Fatalf("server seed: %v", err)
	}
	welcome, _, err := server.NextHandshake(hello)
	if err != nil {
		t.Fatalf("WELCOME: %v", err)
	}
	// Inject the order-8 point A as the client's advertised C.
	cImpl := client.(*curveMech)
	cImpl.ourPublic = Key(lowOrderPoints[2])
	initiate, _, err := client.NextHandshake(welcome)
	if err != nil {
		t.Fatalf("client INITIATE build: %v", err)
	}
	_, _, err = server.NextHandshake(initiate)
	if err == nil {
		t.Fatal("server accepted order-8 client long-term key")
	}
	if !errors.Is(err, zmqerr.ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed, got %v", err)
	}
	sImpl := server.(*curveMech)
	if sImpl.state == stateDone {
		t.Fatal("server reached stateDone despite low-order client key")
	}
}

// TestCurve_RejectsZeroClientEphemeralKey: the server must refuse a
// HELLO whose advertised client ephemeral (C') is all zero.
func TestCurve_RejectsZeroClientEphemeralKey(t *testing.T) {
	serverPub, serverSec := genPair(t)
	clientPub, clientSec := genPair(t)
	client, err := NewCurveClient(serverPub, clientPub, clientSec, nil)
	if err != nil {
		t.Fatalf("NewCurveClient: %v", err)
	}
	server, err := NewCurveServer(serverSec, nil)
	if err != nil {
		t.Fatalf("NewCurveServer: %v", err)
	}
	cImpl := client.(*curveMech)
	cImpl.ourEphPublic = Key{} // all-zero ephemeral public on the wire
	hello, _, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("HELLO build: %v", err)
	}
	if _, _, err := server.NextHandshake(nil); err != nil {
		t.Fatalf("server seed: %v", err)
	}
	_, _, err = server.NextHandshake(hello)
	if err == nil {
		t.Fatal("server accepted HELLO with zero C'")
	}
	if !errors.Is(err, zmqerr.ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed, got %v", err)
	}
}

// TestCurve_RejectsZeroServerKey: the client constructor must fail
// fast when given an all-zero server public key, surfacing
// ErrAuthFailed without reaching the handshake.
func TestCurve_RejectsZeroServerKey(t *testing.T) {
	clientPub, clientSec := genPair(t)
	var zero Key
	_, err := NewCurveClient(zero, clientPub, clientSec, nil)
	if err == nil {
		t.Fatal("NewCurveClient accepted zero server key")
	}
	if !errors.Is(err, zmqerr.ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed, got %v", err)
	}
}

// TestCurve_RejectsLowOrderServerKey: the client constructor must
// also reject each of the canonical Curve25519 low-order points.
func TestCurve_RejectsLowOrderServerKey(t *testing.T) {
	clientPub, clientSec := genPair(t)
	for i, pt := range lowOrderPoints {
		_, err := NewCurveClient(Key(pt), clientPub, clientSec, nil)
		if err == nil {
			t.Fatalf("low-order point %d accepted", i)
		}
		if !errors.Is(err, zmqerr.ErrAuthFailed) {
			t.Fatalf("low-order point %d: want ErrAuthFailed, got %v", i, err)
		}
	}
}

// TestCurve_ValidatePeerPublicKey_Length_Happy exercises the validator
// directly — odd lengths must be rejected, and a fresh random key must
// pass.
func TestCurve_ValidatePeerPublicKey_Length_Happy(t *testing.T) {
	if err := validatePeerPublicKey(nil); err == nil {
		t.Fatal("nil key accepted")
	}
	if err := validatePeerPublicKey(make([]byte, 31)); err == nil {
		t.Fatal("31-byte key accepted")
	}
	pub, _ := genPair(t)
	if err := validatePeerPublicKey(pub[:]); err != nil {
		t.Fatalf("fresh random key rejected: %v", err)
	}
}
