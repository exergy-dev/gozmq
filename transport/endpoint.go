// Package transport defines the transport-layer contracts — [Transport],
// [Conn], and [Listener] — and provides the concrete implementations for
// the schemes gozmq ships: tcp, tcp6, ipc, and (opt-in) tls+tcp. inproc://
// is not a transport: the gozmq Context joins inproc sockets with an
// in-memory pipe and no ZMTP.
//
// Each transport turns an endpoint string into Dial / Listen operations and
// adapts the underlying primitive (a net.Conn) to the Conn / Listener
// contracts.
//
// Every gozmq.Context starts with tcp, tcp6 and ipc registered.
// Two cases call for registering one yourself via
// (*gozmq.Context).RegisterTransport:
//
//   - A built-in that needs caller-supplied configuration. TLS is the only
//     one: there is no reasonable default *tls.Config, so tls+tcp is not
//     preregistered — install it with [NewTLSTransport]. The TCP and IPC
//     transports expose their knobs (keepalive, socket buffer sizes, socket
//     file permissions) as struct fields for the same reason.
//   - A scheme gozmq does not ship. Implementing [Transport] out of tree is
//     supported: the interfaces here name only standard-library types, so a
//     QUIC or WebSocket carrier, or a fault-injecting test harness, plugs in
//     without forking. Everything above the transport — sessions, ZMTP
//     framing, security mechanisms, socket patterns — is unaffected by which
//     scheme carries the bytes.
package transport

import (
	"fmt"
	"net"
	"strings"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// splitEndpoint parses "scheme://address" into its parts. Returns
// ErrEndpointInvalid if the separator is missing.
func splitEndpoint(endpoint string) (scheme, address string, err error) {
	scheme, address, ok := strings.Cut(endpoint, "://")
	if !ok || scheme == "" {
		return "", "", fmt.Errorf("%w: %q (missing scheme://)", zmqerr.ErrEndpointInvalid, endpoint)
	}
	return scheme, address, nil
}

// parseTCPEndpoint parses a "scheme://host:port" endpoint into a net.Dial
// or net.Listen address. When dial is true, wildcard hosts ("*") and
// ephemeral ports ("0", "*" or empty) are rejected; when false, a "*" host
// expands to "" (all interfaces), a "*" port to "0", and ephemeral ports are
// left for the OS to resolve.
func parseTCPEndpoint(endpoint, expectedScheme string, dial bool) (string, error) {
	scheme, addr, err := splitEndpoint(endpoint)
	if err != nil {
		return "", err
	}
	if scheme != expectedScheme {
		return "", fmt.Errorf("%w: scheme %q passed to %s transport", zmqerr.ErrEndpointInvalid, scheme, expectedScheme)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("%w: %s", zmqerr.ErrEndpointInvalid, err.Error())
	}
	if dial {
		if host == "*" {
			return "", fmt.Errorf("%w: wildcard host %q not allowed on Dial", zmqerr.ErrEndpointInvalid, addr)
		}
		if port == "" || port == "0" || port == "*" {
			return "", fmt.Errorf("%w: port must be specified for Dial", zmqerr.ErrEndpointInvalid)
		}
	} else {
		if host == "*" {
			host = ""
		}
		if port == "*" { // libzmq: any free port
			port = "0"
		}
	}
	return net.JoinHostPort(host, port), nil
}
