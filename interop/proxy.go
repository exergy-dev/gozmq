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

// SniffingProxy is a minimal in-process TCP splicer that sits between two
// peers and records the ZMTP flags byte of every frame it forwards in each
// direction. It exists purely for interop diagnostics and fault injection.
//
// The splicer trusts that both sides speak ZMTP 3.1 framing after the
// greeting. It forwards the 64-byte greeting verbatim, then shifts into
// frame-by-frame mode: read flags + length, optionally mutate or delay the
// body, then forward the header and body.
type SniffingProxy struct {
	t       testing.TB
	listen  net.Listener
	upstrm  string
	stopped chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once

	mu        sync.Mutex
	clientOut []byte
	serverOut []byte
	active    []net.Conn

	clientBodies [][]byte
	serverBodies [][]byte

	mutator func(clientToServer bool, frameIdx int, body []byte) []byte

	dropAfter  int
	delayAfter int
	delay      time.Duration
}

// NewSniffingProxy constructs a proxy bound to a random loopback port. Call
// Start before connecting a peer, and Stop to tear it down.
func NewSniffingProxy(t testing.TB, upstream string) *SniffingProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	return &SniffingProxy{
		t:          t,
		listen:     ln,
		upstrm:     upstream,
		stopped:    make(chan struct{}),
		dropAfter:  -1,
		delayAfter: -1,
	}
}

// Start begins accepting the single proxied connection.
func (p *SniffingProxy) Start() {
	p.startOnce.Do(func() {
		go p.accept()
	})
}

// Stop tears the proxy down and waits for its goroutines to exit.
func (p *SniffingProxy) Stop() {
	p.stopOnce.Do(func() {
		_ = p.listen.Close()
		p.mu.Lock()
		for _, c := range p.active {
			_ = c.Close()
		}
		p.mu.Unlock()
		<-p.stopped
	})
}

// Addr returns the tcp:// endpoint callers should connect to so that traffic
// routes through the proxy.
func (p *SniffingProxy) Addr() string {
	return "tcp://" + p.listen.Addr().String()
}

// Endpoint is a compatibility alias for Addr.
func (p *SniffingProxy) Endpoint() string { return p.Addr() }

// SetMutator installs a per-frame body mutator. The mutator is called for
// every post-greeting frame body in both directions before the body is
// forwarded; the returned slice replaces the body on the wire and must be the
// same length as the input.
func (p *SniffingProxy) SetMutator(fn func(clientToServer bool, frameIdx int, body []byte) []byte) {
	p.mu.Lock()
	p.mutator = fn
	p.mu.Unlock()
}

// SetDropAfter stops forwarding after the specified post-greeting frame index
// in each direction. Pass a negative value to disable dropping.
func (p *SniffingProxy) SetDropAfter(frameIdx int) {
	p.mu.Lock()
	p.dropAfter = frameIdx
	p.mu.Unlock()
}

// SetDelayAfter sleeps for d after forwarding the specified post-greeting
// frame index in each direction. Pass a negative frame index or a non-positive
// duration to disable delaying.
func (p *SniffingProxy) SetDelayAfter(frameIdx int, d time.Duration) {
	p.mu.Lock()
	p.delayAfter = frameIdx
	p.delay = d
	p.mu.Unlock()
}

func (p *SniffingProxy) accept() {
	defer close(p.stopped)
	conn, err := p.listen.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

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
		p.pipe(conn, up, true)
	}()
	go func() {
		defer wg.Done()
		p.pipe(up, conn, false)
	}()
	wg.Wait()
}

func (p *SniffingProxy) pipe(r, w net.Conn, clientToServer bool) {
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
		if flags&0x02 != 0 {
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
		dropAfter := p.dropAfter
		delayAfter := p.delayAfter
		delay := p.delay
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
		if frameIdx == dropAfter {
			return
		}
		if frameIdx == delayAfter && delay > 0 {
			time.Sleep(delay)
		}
		frameIdx++
	}
}

// bodiesSnapshot returns copies of all recorded frame bodies.
func (p *SniffingProxy) bodiesSnapshot() (clientBodies, serverBodies [][]byte) {
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

func (p *SniffingProxy) snapshot() (clientOut, serverOut []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.clientOut...), append([]byte(nil), p.serverOut...)
}

func newFlagSniffingProxy(t *testing.T, upstream string) *SniffingProxy {
	t.Helper()
	p := NewSniffingProxy(t, upstream)
	p.Start()
	t.Cleanup(func() { p.Stop() })
	return p
}

// formatFlags renders a byte slice of ZMTP flags as a comma-separated list of
// their symbolic names for easy test diagnostics.
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
