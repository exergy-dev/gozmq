package curve

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/security"
)

// lowOrderEncodings returns every 32-byte encoding of a Curve25519
// u-coordinate of small order: the canonical ones (0, 1, the two order-8
// points, p-1, and the non-canonical p ≡ 0 and p+1 ≡ 1), each also with bit
// 255 set, which RFC 7748 masks off before use.
func lowOrderEncodings(t testing.TB) map[string]Key {
	t.Helper()
	canonical := map[string]string{
		"zero":    "0000000000000000000000000000000000000000000000000000000000000000",
		"one":     "0100000000000000000000000000000000000000000000000000000000000000",
		"order8a": "e0eb7a7c3b41b8ae1656e3faf19fc46ada098deb9c32b1fd866205165f49b800",
		"order8b": "5f9c95bca3508c24b1d0b1559c83ef5b04445cc4581c8e86d8224eddd09f1157",
		"p-1":     "ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
		"p":       "edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
		"p+1":     "eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
	}
	out := make(map[string]Key, 2*len(canonical))
	for name, h := range canonical {
		b, err := hex.DecodeString(h)
		require.NoError(t, err)
		var k Key
		copy(k[:], b)
		out[name] = k
		k[31] |= 0x80
		out[name+"|bit255"] = k
	}
	return out
}

// TestCurve_ValidatePeerPublicKey: validatePeerPublicKey rejects wrong
// lengths and every low-order encoding, including those with the high bit
// set (the old table missed 31 zero bytes + 0x80), cross-checked against
// X25519's own low-order rejection; it accepts a real key in either encoding.
func TestCurve_ValidatePeerPublicKey(t *testing.T) {
	require.Error(t, validatePeerPublicKey(nil), "nil key accepted")
	require.Error(t, validatePeerPublicKey(make([]byte, 31)), "31-byte key accepted")
	_, sec := genPair(t)
	for name, k := range lowOrderEncodings(t) {
		_, xerr := curve25519.X25519(sec[:], k[:])
		require.Errorf(t, xerr, "%s: test vector is not low order", name)
		require.ErrorIsf(t, validatePeerPublicKey(k[:]), zmqerr.ErrAuthFailed, "%s accepted", name)
	}
	pub, _ := genPair(t)
	require.NoError(t, validatePeerPublicKey(pub[:]))
	pub[31] |= 0x80 // same point, non-canonical encoding: still fine
	require.NoError(t, validatePeerPublicKey(pub[:]))
}

// TestCurve_ForgedVouchLowOrderClientKey: without the high-bit check, a
// client presenting C = 0x00..80 (u = 0 after masking) can forge the vouch
// without any secret, because every ECDH with it yields the same shared key.
// The server must reject the INITIATE.
func TestCurve_ForgedVouchLowOrderClientKey(t *testing.T) {
	sPub, sSec := genPair(t)
	srv, err := newMech(security.RoleServer, sSec, Key{}, Key{}, nil, nil)
	require.NoError(t, err)
	_, cSec := genPair(t)
	cli, err := newMech(security.RoleClient, cSec, Key{}, sPub, nil, nil)
	require.NoError(t, err)

	hello, _, err := cli.NextHandshake(nil)
	require.NoError(t, err)
	_, _, err = srv.NextHandshake(nil)
	require.NoError(t, err)
	welcome, _, err := srv.NextHandshake(hello)
	require.NoError(t, err)
	cookie, err := cli.parseWelcome(welcome)
	require.NoError(t, err)

	var C Key
	C[31] = 0x80
	// The shared key with a low-order C is HSalsa20(0) whatever the secret.
	var k [32]byte
	box.Precompute(&k, (*[32]byte)(&C), (*[32]byte)(&cSec))
	var vouchR [16]byte
	vn := longNonce(prefixVouch, vouchR[:])
	vplain := append(append([]byte{}, cli.ourEphPublic[:]...), sPub[:]...)
	vouch := append(vouchR[:], box.SealAfterPrecomputation(nil, vplain, &vn, &k)...)
	initPlain := append(append(append([]byte{}, C[:]...), vouch...),
		security.EncodeMetadata(map[string]string{"Socket-Type": "DEALER"})...)
	counter := cli.ourShortNonce
	ib := sealShort(prefixInitiate, counter, initPlain, &cli.peerEphPublic, &cli.ourEphSecret)
	body := []byte{byte(len(cmdInitiate))}
	body = append(body, cmdInitiate...)
	body = append(body, cookie...)
	body = binary.BigEndian.AppendUint64(body, counter)
	body = append(body, ib...)

	_, done, err := srv.NextHandshake(body)
	require.ErrorIs(t, err, zmqerr.ErrAuthFailed, "server accepted a forged vouch for a low-order C")
	require.False(t, done)
	require.Equal(t, Key{}, srv.PeerLongTermPublic())
}

// TestCurve_ClientReportsServerError: a server that rejects the client via
// ZAP answers INITIATE with ERROR. The client must report an auth failure
// with the reason, not a malformed READY.
func TestCurve_ClientReportsServerError(t *testing.T) {
	client, server := newMechPair(t)
	server.(*curveMech).ConfigureZAP(func(string, string, string, [][]byte) (string, map[string]string, error) {
		return "", nil, zmqerr.ErrAuthFailed
	}, "global", "addr")
	welcome := driveToWelcome(t, client, server)
	initiate, _, err := client.NextHandshake(welcome)
	require.NoError(t, err)
	errCmd, _, serr := server.NextHandshake(initiate)
	require.ErrorIs(t, serr, zmqerr.ErrAuthFailed)

	_, done, cerr := client.NextHandshake(errCmd)
	require.False(t, done)
	require.ErrorIs(t, cerr, zmqerr.ErrAuthFailed)
	require.NotErrorIs(t, cerr, zmqerr.ErrMalformedFrame)
	require.Contains(t, cerr.Error(), "Authentication failure")
}

// TestCurve_ClientReportsErrorInsteadOfWelcome: libzmq may answer HELLO with
// ERROR; the client must surface it as an auth failure, tolerating libzmq's
// mis-encoded ERROR name the way PLAIN does.
func TestCurve_ClientReportsErrorInsteadOfWelcome(t *testing.T) {
	for name, body := range map[string][]byte{
		"well-formed":   errorCommand("Invalid key"),
		"libzmq broken": {0x5E, 'R', 'R', 'O', 'R', 0x03, '4', '0', '0'},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := newMechPair(t)
			_, _, err := client.NextHandshake(nil)
			require.NoError(t, err)
			_, _, err = client.NextHandshake(body)
			require.ErrorIs(t, err, zmqerr.ErrAuthFailed)
			require.NotErrorIs(t, err, zmqerr.ErrMalformedFrame)
		})
	}
}
