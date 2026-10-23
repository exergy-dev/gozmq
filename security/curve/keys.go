// Package curve implements the CurveZMQ (RFC 26) security mechanism for
// gozmq. The handshake is the five-step HELLO/WELCOME/INITIATE/READY/MESSAGE
// sequence; every post-handshake frame body is wrapped by the MESSAGE
// envelope.
//
// CurveZMQ is built on the NaCl crypto_box primitive (Curve25519 + XSalsa20
// + Poly1305). This package uses golang.org/x/crypto/nacl/box for the
// primitive operations and implements only the CurveZMQ-specific codec
// shapes (nonce construction, cookie boxing, envelope layout).
//
// This file holds the key-management helpers: the fixed-size Key type,
// generation via crypto/rand, and Z85 encode/decode for the human-readable
// libzmq key format (ZMQ RFC 32).
package curve

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/nacl/box"
)

// KeySize is the byte length of a Curve25519 public or secret key.
const KeySize = 32

// Key is a 32-byte Curve25519 public or secret key.
type Key [KeySize]byte

// GenerateKeyPair returns a fresh Curve25519 long-term keypair sourced from
// crypto/rand. The first return is the public key, the second the secret.
// This matches the ordering of libzmq's zmq_curve_keypair helper.
func GenerateKeyPair() (pub Key, priv Key, err error) {
	return generateKeyPair(rand.Reader)
}

// generateKeyPair is the rand-injectable variant used by tests.
func generateKeyPair(r io.Reader) (pub Key, priv Key, err error) {
	pubPtr, privPtr, err := box.GenerateKey(r)
	if err != nil {
		return Key{}, Key{}, fmt.Errorf("gozmq/security/curve: GenerateKeyPair: %w", err)
	}
	return Key(*pubPtr), Key(*privPtr), nil
}

// Z85 codec — ZMQ RFC 32.
//
// Z85 is a 5-char-per-4-byte base-85 encoding using a libzmq-specific
// alphabet. Input byte length must be a multiple of 4; output char length
// is a multiple of 5. (The 32-byte Curve keys encode to 40 chars.)

// z85Alphabet is the 85-character alphabet from RFC 32 §2. Index 0 → '0',
// ascending through digits, lower-case letters, upper-case letters, and
// then the punctuation set specified by the RFC.
var z85Alphabet = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#")

// z85Decoder is the inverse of z85Alphabet: z85Decoder[c] = index of c in
// the alphabet, or 0xFF if c is not a valid Z85 character.
var z85Decoder [256]byte

func init() {
	for i := range z85Decoder {
		z85Decoder[i] = 0xFF
	}
	for i, c := range z85Alphabet {
		z85Decoder[c] = byte(i)
	}
}

// Z85Encode encodes b using the libzmq Z85 alphabet. Input length must be
// a multiple of 4; callers encoding a 32-byte key get a 40-char result.
func Z85Encode(b []byte) string {
	if len(b)%4 != 0 {
		panic(fmt.Sprintf("gozmq/security/curve: Z85Encode: input length %d not a multiple of 4", len(b)))
	}
	outLen := len(b) / 4 * 5
	out := make([]byte, outLen)
	oi := 0
	for i := 0; i < len(b); i += 4 {
		// Big-endian uint32 over the 4-byte chunk. i+3 is always in
		// range: the loop steps by 4 and len(b)%4 == 0 is enforced by the
		// panic above.
		v := binary.BigEndian.Uint32(b[i : i+4])
		// Emit 5 base-85 digits most-significant first.
		out[oi+4] = z85Alphabet[v%85]
		v /= 85
		out[oi+3] = z85Alphabet[v%85]
		v /= 85
		out[oi+2] = z85Alphabet[v%85]
		v /= 85
		out[oi+1] = z85Alphabet[v%85]
		v /= 85
		out[oi+0] = z85Alphabet[v%85]
		oi += 5
	}
	return string(out)
}

// ErrInvalidZ85 is returned when Z85Decode is given input of wrong length or
// containing a character outside the RFC 32 alphabet.
var ErrInvalidZ85 = errors.New("gozmq/security/curve: invalid Z85 input")

// Z85Decode decodes a Z85 string into its byte form. Input length must be a
// multiple of 5; output length is 4/5 of that.
func Z85Decode(s string) ([]byte, error) {
	if len(s)%5 != 0 {
		return nil, fmt.Errorf("%w: length %d not a multiple of 5", ErrInvalidZ85, len(s))
	}
	outLen := len(s) / 5 * 4
	out := make([]byte, outLen)
	oi := 0
	for i := 0; i < len(s); i += 5 {
		var v uint64
		for j := 0; j < 5; j++ {
			d := z85Decoder[s[i+j]]
			if d == 0xFF {
				return nil, fmt.Errorf("%w: character %q at index %d", ErrInvalidZ85, s[i+j], i+j)
			}
			v = v*85 + uint64(d)
		}
		// The 32-bit value must fit; guard against overflow from a
		// 5-digit base-85 number > 2^32.
		if v > 0xFFFFFFFF {
			return nil, fmt.Errorf("%w: chunk at index %d overflows uint32", ErrInvalidZ85, i)
		}
		binary.BigEndian.PutUint32(out[oi:], uint32(v))
		oi += 4
	}
	return out, nil
}

// Z85EncodeKey is a convenience wrapper that encodes a 32-byte Key to its
// 40-character Z85 string.
func Z85EncodeKey(k Key) string { return Z85Encode(k[:]) }

// Z85DecodeKey is a convenience wrapper that decodes a 40-character Z85
// string into a 32-byte Key.
func Z85DecodeKey(s string) (Key, error) {
	var k Key
	if len(s) != 40 {
		return k, fmt.Errorf("%w: key must be 40 chars, got %d", ErrInvalidZ85, len(s))
	}
	b, err := Z85Decode(s)
	if err != nil {
		return k, err
	}
	copy(k[:], b)
	return k, nil
}
