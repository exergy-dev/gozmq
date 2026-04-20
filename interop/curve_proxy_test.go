//go:build interop

package interop

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// flagSniffingProxy is a minimal in-process TCP splicer that sits between
// two peers and records the ZMTP flags byte of every frame it forwards in
// each direction. It exists purely to diagnose wire-format disagreements
// in the CURVE cross-tests; it does not belong to any product path.
//
// The splicer trusts that both sides speak ZMTP 3.1 framing after the
// greeting. It forwards the 64-byte greeting verbatim, then shifts into
// frame-by-frame mode: read flags + length, log flags, forward header and
// body. No decoding of bodies occurs.
type flagSniffingProxy struct {
	t       *testing.T
	listen  net.Listener
	upstrm  string
	stopped chan struct{}

	mu        sync.Mutex
	clientOut []byte // flags bytes observed on client→server frames
	serverOut []byte // flags bytes observed on server→client frames
	active    []net.Conn

	// bodies keyed by (direction, frame index) — diagnostic aid, not
	// part of the main test path.
	clientBodies [][]byte
	serverBodies [][]byte

	// mutator, if non-nil, is invoked once per post-greeting frame body
	// before it is forwarded. The returned slice replaces the body on the
	// wire. It MUST preserve length — the length header has already been
	// forwarded by the time this fires. Use SetMutator to install.
	mutator func(clientToServer bool, frameIdx int, body []byte) []byte
}

func newFlagSniffingProxy(t *testing.T, upstream string) *flagSniffingProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &flagSniffingProxy{
		t:       t,
		listen:  ln,
		upstrm:  upstream,
		stopped: make(chan struct{}),
	}
	go p.accept()
	t.Cleanup(func() {
		_ = ln.Close()
		// Forcibly tear down the active conns so the pipe loops unblock
		// even if the peer sockets haven't closed yet. Without this the
		// cleanup deadlocks waiting for p.stopped because the server-side
		// pipe is still blocked in ReadFull on a still-open libzmq conn.
		p.mu.Lock()
		for _, c := range p.active {
			_ = c.Close()
		}
		p.mu.Unlock()
		<-p.stopped
	})
	return p
}

// Endpoint returns the tcp:// endpoint callers should connect to so that
// traffic routes through the proxy.
func (p *flagSniffingProxy) Endpoint() string {
	return "tcp://" + p.listen.Addr().String()
}

// SetMutator installs a per-frame body mutator. The mutator is called for
// every post-greeting frame body in both directions before the body is
// forwarded; the returned slice replaces the body on the wire and MUST be
// the same length as the input (the length header has already been
// written). Frame indices are per-direction and start at zero. Passing
// nil clears the mutator.
func (p *flagSniffingProxy) SetMutator(fn func(clientToServer bool, frameIdx int, body []byte) []byte) {
	p.mu.Lock()
	p.mutator = fn
	p.mu.Unlock()
}

func (p *flagSniffingProxy) accept() {
	defer close(p.stopped)
	conn, err := p.listen.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	// upstream is "tcp://host:port"; trim the scheme.
	addr := p.upstrm[len("tcp://"):]
	up, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	defer up.Close()

	p.mu.Lock()
	p.active = append(p.active, conn, up)
	p.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.pipe(conn, up, true) // client→server
	}()
	go func() {
		defer wg.Done()
		p.pipe(up, conn, false) // server→client
	}()
	wg.Wait()
}

// pipe forwards bytes from r to w, after the 64-byte greeting switches
// into frame mode and records the flags byte on each post-greeting frame.
//
// The greeting is sent by libzmq in two parts (11-byte signature, then
// 53-byte remainder) and the peer often withholds its second half until
// ours is forwarded. Stream forward byte-by-byte (small reads) until we
// have forwarded 64 bytes total, THEN switch to frame mode.
func (p *flagSniffingProxy) pipe(r, w net.Conn, clientToServer bool) {
	// Forward greeting, streaming small chunks so the peer's reply can
	// interleave with our forwarding.
	forwarded := 0
	buf := make([]byte, 64)
	for forwarded < 64 {
		n, err := r.Read(buf[:64-forwarded])
		if err != nil {
			return
		}
		if _, werr := w.Write(buf[:n]); werr != nil {
			return
		}
		forwarded += n
	}

	// Frame mode.
	frameIdx := 0
	for {
		hdr := make([]byte, 2)
		if _, err := io.ReadFull(r, hdr); err != nil {
			return
		}
		flags := hdr[0]
		p.mu.Lock()
		if clientToServer {
			p.clientOut = append(p.clientOut, flags)
		} else {
			p.serverOut = append(p.serverOut, flags)
		}
		p.mu.Unlock()

		var size uint64
		var full []byte
		if flags&0x02 != 0 { // LONG form
			more := make([]byte, 7)
			if _, err := io.ReadFull(r, more); err != nil {
				return
			}
			full = append(hdr, more...)
			for i := 0; i < 8; i++ {
				size = (size << 8) | uint64(full[1+i])
			}
		} else {
			size = uint64(hdr[1])
			full = hdr
		}
		body := make([]byte, size)
		if size > 0 {
			if _, err := io.ReadFull(r, body); err != nil {
				return
			}
		}
		p.mu.Lock()
		snap := append([]byte(nil), body...)
		if clientToServer {
			p.clientBodies = append(p.clientBodies, snap)
		} else {
			p.serverBodies = append(p.serverBodies, snap)
		}
		mutator := p.mutator
		p.mu.Unlock()
		if _, err := w.Write(full); err != nil {
			return
		}
		if size > 0 {
			out := body
			if mutator != nil {
				if m := mutator(clientToServer, frameIdx, body); m != nil {
					if len(m) != len(body) {
						p.t.Errorf("mutator returned %d bytes, expected %d (length must be preserved)", len(m), len(body))
						return
					}
					out = m
				}
			}
			if _, err := w.Write(out); err != nil {
				return
			}
		}
		frameIdx++
	}
}

// bodiesSnapshot returns copies of all recorded frame bodies.
func (p *flagSniffingProxy) bodiesSnapshot() (clientBodies, serverBodies [][]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cb := make([][]byte, len(p.clientBodies))
	for i, b := range p.clientBodies {
		cb[i] = append([]byte(nil), b...)
	}
	sb := make([][]byte, len(p.serverBodies))
	for i, b := range p.serverBodies {
		sb[i] = append([]byte(nil), b...)
	}
	return cb, sb
}

func (p *flagSniffingProxy) snapshot() (clientOut, serverOut []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.clientOut...), append([]byte(nil), p.serverOut...)
}

// formatFlags renders a byte slice of ZMTP flags as a comma-separated list
// of their symbolic names for easy test diagnostics.
func formatFlags(flags []byte) string {
	out := ""
	for i, f := range flags {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("0x%02x", f)
	}
	return out
}

