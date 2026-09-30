package transport

import (
	"testing"
)

// Fuzz targets for the endpoint parsers. Each checks that parsing never
// panics and that an accepted value re-parses to itself, so the resolved
// form we log, report via Endpoint(), or hand back to the user is stable.

func FuzzSplitEndpoint(f *testing.F) {
	for _, s := range []string{"tcp://127.0.0.1:5555", "ipc:///tmp/x", "inproc://a", "://x", "tcp:/x", "a:://b", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, endpoint string) {
		scheme, addr, err := splitEndpoint(endpoint)
		if err != nil {
			return
		}
		if scheme == "" {
			t.Fatalf("empty scheme accepted for %q", endpoint)
		}
		if got := scheme + "://" + addr; got != endpoint {
			t.Fatalf("split(%q) = %q, %q; rejoins to %q", endpoint, scheme, addr, got)
		}
		s2, a2, err := splitEndpoint(scheme + "://" + addr)
		if err != nil || s2 != scheme || a2 != addr {
			t.Fatalf("re-split(%q) = %q, %q, %v", endpoint, s2, a2, err)
		}
	})
}

func FuzzParseTCPEndpoint(f *testing.F) {
	for _, s := range []string{
		"tcp://127.0.0.1:5555", "tcp://*:0", "tcp://*:*", "tcp://[::1]:1",
		"tcp://[fe80::1%eth0]:9", "tcp://host:", "tcp://:80", "tcp://a:b:c", "tcp6://[::]:1",
	} {
		f.Add(s, false)
		f.Add(s, true)
	}
	f.Fuzz(func(t *testing.T, endpoint string, dial bool) {
		addr, err := parseTCPEndpoint(endpoint, "tcp", dial)
		if err != nil {
			return
		}
		again, err := parseTCPEndpoint("tcp://"+addr, "tcp", dial)
		if err != nil {
			t.Fatalf("parse(%q, dial=%v) = %q, which does not re-parse: %v", endpoint, dial, addr, err)
		}
		if again != addr {
			t.Fatalf("parse(%q, dial=%v) = %q, re-parses to %q", endpoint, dial, addr, again)
		}
	})
}

func FuzzParseIPCAddress(f *testing.F) {
	for _, s := range []string{"ipc:///tmp/a.sock", "ipc://@abstract", "ipc://@", "ipc://", "ipc://rel/path", "tcp://x"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, endpoint string) {
		path, abstract, err := parseIPCAddress(endpoint)
		if err != nil {
			return
		}
		if path == "" || abstract != (path[0] == '@') {
			t.Fatalf("parse(%q) = %q, abstract=%v", endpoint, path, abstract)
		}
		p2, a2, err := parseIPCAddress("ipc://" + path)
		if err != nil || p2 != path || a2 != abstract {
			t.Fatalf("parse(%q) = %q; re-parse = %q, %v, %v", endpoint, path, p2, a2, err)
		}
	})
}
