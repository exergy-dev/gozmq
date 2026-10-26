package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"strings"
	"time"

	"github.com/exergy-dev/gozmq"
	"github.com/exergy-dev/gozmq/security/curve"
	gztransport "github.com/exergy-dev/gozmq/transport"
)

// The CURVE keys are libzmq's documented test keypairs (zmq_curve(7)), so
// zmqperf.c can use the same Z85 strings and either side can be either
// library. They are public; never use them outside a benchmark.
const (
	curveServerPublic = "rq:rM>}U?@Lns47E1%kR.o@n%FcmmsL/@{H8]yf7"
	curveServerSecret = "JTKVSB%%)wK0E.X)V>+}o?pNmC{O&4W4b!Ni{Lh6"
	curveClientPublic = "Yne@$w-vo<fVvi]a<NY6T1ed:M$fCG*[IaLV{hID"
	curveClientSecret = "D:)Q[IlAW!ahhC2ac:9*A}h:p?([4%wOTJ%JR%cs"
)

// curveOption returns the CURVE role for a socket: the binding side is the
// server.
func curveOption(bind bool) gozmq.Option {
	key := func(z string) []byte {
		k, err := curve.Z85DecodeKey(z)
		check(err, "decode curve key")
		return k[:]
	}
	if bind {
		return gozmq.WithCurveServer(key(curveServerSecret))
	}
	return gozmq.WithCurveClient(key(curveServerPublic), key(curveClientPublic), key(curveClientSecret))
}

// registerTLS registers the tls+tcp transport on zctx when the endpoint
// uses it. Each process makes its own self-signed loopback certificate and
// the client side does not verify it: this is a throughput tool on
// loopback, not a security configuration.
func registerTLS(zctx *gozmq.Context, endpoint string) {
	if !strings.HasPrefix(endpoint, "tls+tcp://") {
		return
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	check(err, "tls key")
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "zmqperf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, pub, priv)
	check(err, "tls certificate")
	tr, err := gztransport.NewTLSTransport(&tls.Config{
		Certificates:       []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv}},
		InsecureSkipVerify: true, //nolint:gosec // loopback benchmark, see the doc comment
		MinVersion:         tls.VersionTLS12,
	})
	check(err, "tls transport")
	zctx.RegisterTransport(tr)
}
