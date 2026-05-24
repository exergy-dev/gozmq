package curve

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rkm/gozmq/internal/iface"
)

// deterministicReader is a non-cryptographic io.Reader derived from a
// splitmix64-style LCG seeded at construction. It lets us pin ephemeral
// keys and long-nonce randomness so the CURVE handshake produces
// reproducible wire bytes — the prerequisite for byte-exact fixtures.
type deterministicReader struct{ s uint64 }

func newDeterministicReader(seed uint64) io.Reader { return &deterministicReader{s: seed} }

func (r *deterministicReader) Read(p []byte) (int, error) {
	for i := range p {
		r.s = r.s*6364136223846793005 + 1442695040888963407
		p[i] = byte(r.s >> 56)
	}
	return len(p), nil
}

// fixedKeyPair derives a Curve25519 secret and its matching public from a
// seed. The secret is simply the first 32 bytes of the reader; the public
// is derived via derivePublic so the test's inputs line up with anything a
// later interop harness might produce from the same secret.
func fixedKeyPair(t *testing.T, seed uint64) (Key, Key) {
	t.Helper()
	var sec Key
	r := newDeterministicReader(seed)
	_, err := io.ReadFull(r, sec[:])
	require.NoError(t, err, "read secret")
	pub, err := derivePublic(sec)
	require.NoError(t, err, "derivePublic")
	return pub, sec
}

// TestCurveHandshakeDeterministic proves that identical inputs (long-term
// keys + RNG seed for ephemerals) produce identical wire bytes across the
// full handshake.
//
// Algorithm:
//  1. Build two independent (client, server) pairs sharing the same inputs.
//  2. Run both handshakes and record every wire command.
//  3. Assert the two recordings match byte-for-byte.
func TestCurveHandshakeDeterministic(t *testing.T) {
	serverPub, serverSec := fixedKeyPair(t, 0xA11CE00000000001)
	clientPub, clientSec := fixedKeyPair(t, 0xB0B0000000000002)

	run := func(rngSeed uint64) (hello, welcome, initiate, ready, msgCS, msgSC []byte) {
		cRand := newDeterministicReader(rngSeed)
		sRand := newDeterministicReader(rngSeed ^ 0xDEADBEEF)
		client, err := newMech(iface.RoleClient, clientSec, clientPub, serverPub, nil, cRand)
		require.NoError(t, err, "newMech client")
		server, err := newMech(iface.RoleServer, serverSec, Key{}, Key{}, nil, sRand)
		require.NoError(t, err, "newMech server")
		var done bool
		hello, done, err = client.NextHandshake(nil)
		require.NoError(t, err, "HELLO")
		require.False(t, done, "HELLO: done=true")
		_, _, err = server.NextHandshake(nil)
		require.NoError(t, err, "server seed")
		welcome, done, err = server.NextHandshake(hello)
		require.NoError(t, err, "WELCOME")
		require.False(t, done, "WELCOME: done=true")
		initiate, done, err = client.NextHandshake(welcome)
		require.NoError(t, err, "INITIATE")
		require.False(t, done, "INITIATE: done=true")
		ready, done, err = server.NextHandshake(initiate)
		require.NoError(t, err, "READY")
		require.True(t, done, "READY: done=false")
		_, done, err = client.NextHandshake(ready)
		require.NoError(t, err, "READY ingest")
		require.True(t, done, "READY ingest: done=false")
		// Traffic phase: one fixed frame per direction. Short-nonce
		// counters are monotonic across the whole session (RFC 26 §3),
		// so the first client MESSAGE carries counter=3 (after HELLO=1
		// and INITIATE=2) and the first server MESSAGE carries
		// counter=2 (after READY=1; WELCOME uses a long nonce).
		_, msgCS, err = client.EncodeFrame(0, []byte("ping"))
		require.NoError(t, err, "client MESSAGE")
		_, msgSC, err = server.EncodeFrame(0, []byte("pong"))
		require.NoError(t, err, "server MESSAGE")
		return hello, welcome, initiate, ready, msgCS, msgSC
	}

	h1, w1, i1, r1, mcs1, msc1 := run(0xF00D1234CAFE5678)
	h2, w2, i2, r2, mcs2, msc2 := run(0xF00D1234CAFE5678)

	require.Equal(t, h1, h2, "HELLO not deterministic")
	require.Equal(t, w1, w2, "WELCOME not deterministic")
	require.Equal(t, i1, i2, "INITIATE not deterministic")
	require.Equal(t, r1, r2, "READY not deterministic")
	require.Equal(t, mcs1, mcs2, "MESSAGE client->server not deterministic")
	require.Equal(t, msc1, msc2, "MESSAGE server->client not deterministic")

	// Surface sanity checks on the recorded shapes. These catch layout
	// regressions (e.g. an off-by-one in the padding) without requiring
	// us to pin the exact byte string, which depends on the RNG and on
	// the nacl/box version.
	assert.Len(t, h1, helloBodySize, "HELLO size")
	assert.Equal(t, "HELLO", string(h1[1:6]), "HELLO name")
	assert.Len(t, w1, welcomeBodySize, "WELCOME size")
	assert.Equal(t, "WELCOME", string(w1[1:8]), "WELCOME name")
	// INITIATE has variable metadata but a fixed header.
	assert.Equal(t, "INITIATE", string(i1[1:9]), "INITIATE name")
	assert.GreaterOrEqual(t, len(i1), 1+len("INITIATE")+cookieSize+8, "INITIATE truncated")
	assert.Equal(t, "READY", string(r1[1:6]), "READY name")
	assert.Equal(t, "MESSAGE", string(mcs1[1:8]), "MESSAGE name")
}

// TestCurveHandshakeGoldenBytes compares the deterministic handshake to a
// golden fixture under testdata/. The fixture is recorded on first run
// (guarded by GOZMQ_CURVE_REGEN=1); checked-in fixtures then freeze the
// wire layout against future patches.
//
// Wire format of the fixture file is hex-per-line, labelled:
//
//	HELLO <hex>
//	WELCOME <hex>
//	INITIATE <hex>
//	READY <hex>
//	MESSAGE-CS <hex>
//	MESSAGE-SC <hex>
//
// If the file is absent, the test is skipped rather than failed — this
// keeps a fresh checkout from being red before the fixture is generated.
// Run `GOZMQ_CURVE_REGEN=1 go test -run TestCurveHandshakeGoldenBytes
// ./security/curve/...` to (re)generate.
func TestCurveHandshakeGoldenBytes(t *testing.T) {
	serverPub, serverSec := fixedKeyPair(t, 0xA11CE00000000001)
	clientPub, clientSec := fixedKeyPair(t, 0xB0B0000000000002)

	cRand := newDeterministicReader(0x0123456789ABCDEF)
	sRand := newDeterministicReader(0xFEDCBA9876543210)

	client, err := newMech(iface.RoleClient, clientSec, clientPub, serverPub, nil, cRand)
	require.NoError(t, err, "client")
	server, err := newMech(iface.RoleServer, serverSec, Key{}, Key{}, nil, sRand)
	require.NoError(t, err, "server")
	hello, _, err := client.NextHandshake(nil)
	require.NoError(t, err, "HELLO")
	_, _, err = server.NextHandshake(nil)
	require.NoError(t, err, "server seed")
	welcome, _, err := server.NextHandshake(hello)
	require.NoError(t, err, "WELCOME")
	initiate, _, err := client.NextHandshake(welcome)
	require.NoError(t, err, "INITIATE")
	ready, _, err := server.NextHandshake(initiate)
	require.NoError(t, err, "READY")
	_, _, err = client.NextHandshake(ready)
	require.NoError(t, err, "READY ingest")
	_, msgCS, err := client.EncodeFrame(0, []byte("ping"))
	require.NoError(t, err, "MESSAGE-CS")
	_, msgSC, err := server.EncodeFrame(0, []byte("pong"))
	require.NoError(t, err, "MESSAGE-SC")

	goldenPath := filepath.Join("testdata", "handshake_golden.hex")

	built := new(bytes.Buffer)
	writeHex := func(label string, b []byte) {
		built.WriteString(label)
		built.WriteByte(' ')
		built.WriteString(hex.EncodeToString(b))
		built.WriteByte('\n')
	}
	writeHex("HELLO", hello)
	writeHex("WELCOME", welcome)
	writeHex("INITIATE", initiate)
	writeHex("READY", ready)
	writeHex("MESSAGE-CS", msgCS)
	writeHex("MESSAGE-SC", msgSC)

	if os.Getenv("GOZMQ_CURVE_REGEN") == "1" {
		require.NoError(t, os.MkdirAll(filepath.Dir(goldenPath), 0o755), "mkdir")
		require.NoError(t, os.WriteFile(goldenPath, built.Bytes(), 0o644), "write golden")
		t.Logf("regenerated %s", goldenPath)
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("no golden fixture at %s; run with GOZMQ_CURVE_REGEN=1 to generate", goldenPath)
			return
		}
		require.NoError(t, err, "read golden")
	}

	if !assert.Equal(t, bytes.TrimRight(want, "\n"), bytes.TrimRight(built.Bytes(), "\n"), "golden mismatch") {
		t.Logf("To regenerate after an intentional codec change:\n  GOZMQ_CURVE_REGEN=1 go test -run TestCurveHandshakeGoldenBytes ./security/curve/...")
	}
}

// TestCurveHandshakeRFC26ShapeBytes pins the non-cryptographic prefix of
// each handshake command — the portions that do not depend on random
// nonces or secret keys. This guards the layout (command names, version
// bytes, padding length) against regression without the golden-file
// machinery, so a patch that accidentally shifts a field layout is
// caught immediately.
func TestCurveHandshakeRFC26ShapeBytes(t *testing.T) {
	serverPub, serverSec := fixedKeyPair(t, 0xA11CE00000000001)
	clientPub, clientSec := fixedKeyPair(t, 0xB0B0000000000002)

	client, err := newMech(iface.RoleClient, clientSec, clientPub, serverPub, nil, newDeterministicReader(1))
	require.NoError(t, err, "client")
	server, err := newMech(iface.RoleServer, serverSec, Key{}, Key{}, nil, newDeterministicReader(2))
	require.NoError(t, err, "server")
	hello, _, err := client.NextHandshake(nil)
	require.NoError(t, err, "HELLO")
	// HELLO prefix: [5]"HELLO" + 0x01 0x00 + 72 zero bytes.
	wantPrefix := append([]byte{0x05}, []byte("HELLO")...)
	wantPrefix = append(wantPrefix, 0x01, 0x00)
	wantPrefix = append(wantPrefix, make([]byte, 72)...)
	assert.Equal(t, wantPrefix, hello[:len(wantPrefix)], "HELLO prefix mismatch")

	_, _, err = server.NextHandshake(nil)
	require.NoError(t, err, "server seed")
	welcome, _, err := server.NextHandshake(hello)
	require.NoError(t, err, "WELCOME")
	// WELCOME prefix: [7]"WELCOME".
	assert.Equal(t, append([]byte{0x07}, []byte("WELCOME")...), welcome[:8], "WELCOME prefix")

	initiate, _, err := client.NextHandshake(welcome)
	require.NoError(t, err, "INITIATE")
	assert.Equal(t, append([]byte{0x08}, []byte("INITIATE")...), initiate[:9], "INITIATE prefix")

	ready, _, err := server.NextHandshake(initiate)
	require.NoError(t, err, "READY")
	assert.Equal(t, append([]byte{0x05}, []byte("READY")...), ready[:6], "READY prefix")
	_, _, err = client.NextHandshake(ready)
	require.NoError(t, err, "READY ingest")

	// MESSAGE prefix: [7]"MESSAGE".
	_, msg, err := client.EncodeFrame(0, []byte{})
	require.NoError(t, err, "MESSAGE")
	assert.Equal(t, append([]byte{0x07}, []byte("MESSAGE")...), msg[:8], "MESSAGE prefix")
}
