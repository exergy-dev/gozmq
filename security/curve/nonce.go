// Nonce construction per CurveZMQ (RFC 26) §3.
//
// NaCl crypto_box nonces are 24 bytes. CurveZMQ uses two shapes:
//
//   - "short" nonces: a fixed 16-byte ASCII prefix followed by an 8-byte
//     big-endian counter (per-direction, starting at 1). Used for the HELLO,
//     INITIATE, READY, and MESSAGE commands.
//
//   - "long" nonces: an 8-byte ASCII prefix followed by 16 random bytes.
//     Used for the WELCOME command, the inner COOKIE box, and the inner
//     VOUCH box. RFC 26 §3 names these "long nonces" because the freshness
//     comes from random bytes rather than a counter.
//
// The prefix strings are literal; callers MUST pass the exact strings
// defined by the RFC (see the prefix* constants) or interop breaks.

package curve

import (
	"encoding/binary"
	"fmt"
)

// Nonce prefix strings from RFC 26 §3. These are the literal bytes written
// at the start of each nonce. Length is load-bearing: short nonce prefixes
// are 16 bytes, long nonce prefixes are 8 bytes. Any padding required to
// reach the prefix length is included here (dashes, following libzmq's
// canonical form).
const (
	// Short-nonce prefixes (16 bytes each; 8-byte counter fills the rest).
	prefixHello    = "CurveZMQHELLO---"
	prefixInitiate = "CurveZMQINITIATE"
	prefixReady    = "CurveZMQREADY---"
	prefixMessageC = "CurveZMQMESSAGEC" // client → server traffic
	prefixMessageS = "CurveZMQMESSAGES" // server → client traffic

	// Long-nonce prefixes (8 bytes each; 16 random bytes fill the rest).
	prefixWelcome = "WELCOME-"
	prefixCookie  = "COOKIE--"
	prefixVouch   = "VOUCH---"
)

// shortNonce builds a 24-byte NaCl nonce of the form prefix || u64be(counter).
// prefix must be exactly 16 bytes (the RFC's short-nonce prefix length); we
// panic on misuse because this is internal and any slippage is a bug.
func shortNonce(prefix string, counter uint64) [24]byte {
	if len(prefix) != 16 {
		panic(fmt.Sprintf("gozmq/security/curve: shortNonce prefix %q is %d bytes, want 16", prefix, len(prefix)))
	}
	var n [24]byte
	copy(n[:16], prefix)
	binary.BigEndian.PutUint64(n[16:], counter)
	return n
}

// longNonce builds a 24-byte NaCl nonce of the form prefix || random[16].
// prefix must be exactly 8 bytes; random must be at least 16 bytes long and
// the first 16 bytes are used.
//
// The "long nonce" name comes from RFC 26 §3: the counter half of the nonce
// is replaced with 16 random bytes, matching NaCl's recommendation for
// unique-per-message nonces when a counter is unavailable.
func longNonce(prefix string, random []byte) [24]byte {
	if len(prefix) != 8 {
		panic(fmt.Sprintf("gozmq/security/curve: longNonce prefix %q is %d bytes, want 8", prefix, len(prefix)))
	}
	if len(random) < 16 {
		panic(fmt.Sprintf("gozmq/security/curve: longNonce random too short: %d bytes, want >= 16", len(random)))
	}
	var n [24]byte
	copy(n[:8], prefix)
	copy(n[8:], random[:16])
	return n
}
