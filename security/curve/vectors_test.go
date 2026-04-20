package curve

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

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
	if _, err := io.ReadFull(r, sec[:]); err != nil {
		t.Fatalf("read secret: %v", err)
	}
	pub, err := derivePublic(sec)
	if err != nil {
		t.Fatalf("derivePublic: %v", err)
	}
	return pub, sec
}

// TestCurveHandshakeDeterministic proves that identical inputs (long-term
// keys + RNG seed for ephemerals) produce identical wire bytes across the
// full handshake. This is the round-trip symmetry the plan's Wave 1
// "byte-exact agreement" mandate calls out; cross-impl interop against
// libzmq is deferred to Wave 3 C4 which will read captured fixtures.
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
		if err != nil {
			t.Fatalf("newMech client: %v", err)
		}
		server, err := newMech(iface.RoleServer, serverSec, Key{}, Key{}, nil, sRand)
		if err != nil {
			t.Fatalf("newMech server: %v", err)
		}
		var done bool
		hello, done, err = client.NextHandshake(nil)
		if err != nil || done {
			t.Fatalf("HELLO: err=%v done=%v", err, done)
		}
		if _, _, err := server.NextHandshake(nil); err != nil {
			t.Fatalf("server seed: %v", err)
		}
		welcome, done, err = server.NextHandshake(hello)
		if err != nil || done {
			t.Fatalf("WELCOME: err=%v done=%v", err, done)
		}
		initiate, done, err = client.NextHandshake(welcome)
		if err != nil || done {
			t.Fatalf("INITIATE: err=%v done=%v", err, done)
		}
		ready, done, err = server.NextHandshake(initiate)
		if err != nil || !done {
			t.Fatalf("READY: err=%v done=%v", err, done)
		}
		if _, done, err := client.NextHandshake(ready); err != nil || !done {
			t.Fatalf("READY ingest: err=%v done=%v", err, done)
		}
		// Traffic phase: one fixed frame per direction. Short-nonce
		// counters are monotonic across the whole session (RFC 26 §3),
		// so the first client MESSAGE carries counter=3 (after HELLO=1
		// and INITIATE=2) and the first server MESSAGE carries
		// counter=2 (after READY=1; WELCOME uses a long nonce).
		_, msgCS, err = client.EncodeFrame(0, []byte("ping"))
		if err != nil {
			t.Fatalf("client MESSAGE: %v", err)
		}
		_, msgSC, err = server.EncodeFrame(0, []byte("pong"))
		if err != nil {
			t.Fatalf("server MESSAGE: %v", err)
		}
		return hello, welcome, initiate, ready, msgCS, msgSC
	}

	h1, w1, i1, r1, mcs1, msc1 := run(0xF00D1234CAFE5678)
	h2, w2, i2, r2, mcs2, msc2 := run(0xF00D1234CAFE5678)

	if !bytes.Equal(h1, h2) {
		t.Fatalf("HELLO not deterministic:\n  run1=%x\n  run2=%x", h1, h2)
	}
	if !bytes.Equal(w1, w2) {
		t.Fatalf("WELCOME not deterministic:\n  run1=%x\n  run2=%x", w1, w2)
	}
	if !bytes.Equal(i1, i2) {
		t.Fatalf("INITIATE not deterministic:\n  run1=%x\n  run2=%x", i1, i2)
	}
	if !bytes.Equal(r1, r2) {
		t.Fatalf("READY not deterministic:\n  run1=%x\n  run2=%x", r1, r2)
	}
	if !bytes.Equal(mcs1, mcs2) {
		t.Fatalf("MESSAGE client->server not deterministic:\n  run1=%x\n  run2=%x", mcs1, mcs2)
	}
	if !bytes.Equal(msc1, msc2) {
		t.Fatalf("MESSAGE server->client not deterministic:\n  run1=%x\n  run2=%x", msc1, msc2)
	}

	// Surface sanity checks on the recorded shapes. These catch layout
	// regressions (e.g. an off-by-one in the padding) without requiring
	// us to pin the exact byte string, which depends on the RNG and on
	// the nacl/box version.
	if len(h1) != helloBodySize {
		t.Errorf("HELLO size %d != %d", len(h1), helloBodySize)
	}
	if string(h1[1:6]) != "HELLO" {
		t.Errorf("HELLO name: %q", h1[1:6])
	}
	if len(w1) != welcomeBodySize {
		t.Errorf("WELCOME size %d != %d", len(w1), welcomeBodySize)
	}
	if string(w1[1:8]) != "WELCOME" {
		t.Errorf("WELCOME name: %q", w1[1:8])
	}
	// INITIATE has variable metadata but a fixed header.
	if string(i1[1:9]) != "INITIATE" {
		t.Errorf("INITIATE name: %q", i1[1:9])
	}
	if len(i1) < 1+len("INITIATE")+cookieSize+8 {
		t.Errorf("INITIATE truncated: %d", len(i1))
	}
	if string(r1[1:6]) != "READY" {
		t.Errorf("READY name: %q", r1[1:6])
	}
	if string(mcs1[1:8]) != "MESSAGE" {
		t.Errorf("MESSAGE name: %q", mcs1[1:8])
	}
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
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	server, err := newMech(iface.RoleServer, serverSec, Key{}, Key{}, nil, sRand)
	if err != nil {
		t.Fatalf("server: %v", err)
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
	if _, _, err := client.NextHandshake(ready); err != nil {
		t.Fatalf("READY ingest: %v", err)
	}
	_, msgCS, err := client.EncodeFrame(0, []byte("ping"))
	if err != nil {
		t.Fatalf("MESSAGE-CS: %v", err)
	}
	_, msgSC, err := server.EncodeFrame(0, []byte("pong"))
	if err != nil {
		t.Fatalf("MESSAGE-SC: %v", err)
	}

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
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(goldenPath, built.Bytes(), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("regenerated %s", goldenPath)
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("no golden fixture at %s; run with GOZMQ_CURVE_REGEN=1 to generate", goldenPath)
			return
		}
		t.Fatalf("read golden: %v", err)
	}

	if !bytes.Equal(bytes.TrimRight(want, "\n"), bytes.TrimRight(built.Bytes(), "\n")) {
		t.Errorf("golden mismatch:\n--- want\n%s--- got\n%s", want, built.Bytes())
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
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	server, err := newMech(iface.RoleServer, serverSec, Key{}, Key{}, nil, newDeterministicReader(2))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	hello, _, err := client.NextHandshake(nil)
	if err != nil {
		t.Fatalf("HELLO: %v", err)
	}
	// HELLO prefix: [5]"HELLO" + 0x01 0x00 + 72 zero bytes.
	wantPrefix := append([]byte{0x05}, []byte("HELLO")...)
	wantPrefix = append(wantPrefix, 0x01, 0x00)
	wantPrefix = append(wantPrefix, make([]byte, 72)...)
	if !bytes.HasPrefix(hello, wantPrefix) {
		t.Errorf("HELLO prefix mismatch\n got: %s\nwant: %s",
			hex.EncodeToString(hello[:len(wantPrefix)]),
			hex.EncodeToString(wantPrefix))
	}

	if _, _, err := server.NextHandshake(nil); err != nil {
		t.Fatalf("server seed: %v", err)
	}
	welcome, _, err := server.NextHandshake(hello)
	if err != nil {
		t.Fatalf("WELCOME: %v", err)
	}
	// WELCOME prefix: [7]"WELCOME".
	if !bytes.HasPrefix(welcome, append([]byte{0x07}, []byte("WELCOME")...)) {
		t.Errorf("WELCOME prefix: %s", hex.EncodeToString(welcome[:8]))
	}

	initiate, _, err := client.NextHandshake(welcome)
	if err != nil {
		t.Fatalf("INITIATE: %v", err)
	}
	if !bytes.HasPrefix(initiate, append([]byte{0x08}, []byte("INITIATE")...)) {
		t.Errorf("INITIATE prefix: %s", hex.EncodeToString(initiate[:9]))
	}

	ready, _, err := server.NextHandshake(initiate)
	if err != nil {
		t.Fatalf("READY: %v", err)
	}
	if !bytes.HasPrefix(ready, append([]byte{0x05}, []byte("READY")...)) {
		t.Errorf("READY prefix: %s", hex.EncodeToString(ready[:6]))
	}
	if _, _, err := client.NextHandshake(ready); err != nil {
		t.Fatalf("READY ingest: %v", err)
	}

	// MESSAGE prefix: [7]"MESSAGE".
	_, msg, err := client.EncodeFrame(0, []byte{})
	if err != nil {
		t.Fatalf("MESSAGE: %v", err)
	}
	if !bytes.HasPrefix(msg, append([]byte{0x07}, []byte("MESSAGE")...)) {
		t.Errorf("MESSAGE prefix: %s", hex.EncodeToString(msg[:8]))
	}
}
