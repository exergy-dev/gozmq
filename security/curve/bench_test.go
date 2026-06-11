// Traffic-path benchmarks for the CURVE MESSAGE codec: EncodeFrame,
// DecodeFrame, and the combined round trip over a handshaked client/server
// mechanism pair. These measure the post-handshake hot path only — pure
// XSalsa20/Poly1305 seal/open plus envelope assembly; the per-frame ECDH is
// already hoisted into finishHandshake.
package curve

import (
	"fmt"
	"testing"

	"github.com/rkm/gozmq/internal/iface"
)

// benchSizes are the payload sizes exercised: a small control-plane-ish
// frame, a typical message, and a bulk frame.
var benchSizes = []int{64, 1024, 16 * 1024}

// benchPair returns a handshaked client/server pair as concrete *curveMech
// so benchmarks can reach the nonce counters (see BenchmarkDecodeFrame).
func benchPair(b *testing.B) (*curveMech, *curveMech) {
	b.Helper()
	client, server := runHandshake(b)
	return client.(*curveMech), server.(*curveMech)
}

func sizeName(n int) string {
	if n >= 1024 {
		return fmt.Sprintf("%dKiB", n/1024)
	}
	return fmt.Sprintf("%dB", n)
}

func BenchmarkEncodeFrame(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			client, _ := benchPair(b)
			payload := make([]byte, size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := client.EncodeFrame(iface.FlagMore, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecodeFrame(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			client, server := benchPair(b)
			payload := make([]byte, size)
			_, wire, err := client.EncodeFrame(0, payload)
			if err != nil {
				b.Fatal(err)
			}
			// Anti-replay would reject decoding the same wire frame twice
			// (the short-nonce counter must strictly advance), so rewind
			// the server's expected counter before every iteration. This
			// is benchmark-only state surgery; the wire bytes stay valid
			// because the nonce is derived from the counter inside wire.
			rewind := server.peerShortNonce
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				server.peerShortNonce = rewind
				if _, _, err := server.DecodeFrame(0, wire); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEncodeDecodeRoundTrip(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			client, server := benchPair(b)
			payload := make([]byte, size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, wire, err := client.EncodeFrame(iface.FlagMore, payload)
				if err != nil {
					b.Fatal(err)
				}
				if _, _, err := server.DecodeFrame(0, wire); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
