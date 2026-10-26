package main

import (
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/gozmq"
)

// STREAM carries raw bytes, so its two patterns pair a STREAM socket with a
// plain TCP peer that is always this Go code, whichever library the STREAM
// side is: stream_in measures STREAM receiving, stream_out STREAM sending.
// Byte boundaries are not message boundaries on a raw stream, so the
// receivers count bytes until size*count have arrived and report the rate
// in payload-sized units; the clock starts at the first chunk, which is not
// subtracted (at most one read of a run of many megabytes).

// tcpAddr turns "tcp://host:port" into "host:port" ("*" becomes all
// interfaces).
func tcpAddr(endpoint string) string {
	addr, ok := strings.CutPrefix(endpoint, "tcp://")
	if !ok {
		log.Fatalf("stream patterns need a tcp:// endpoint, not %q", endpoint)
	}
	if rest, ok := strings.CutPrefix(addr, "*:"); ok {
		return ":" + rest
	}
	return addr
}

// recvStream is stream_in's receiver: a STREAM socket counting the bytes a
// raw TCP writer sends.
func recvStream(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool, ready chan<- struct{}) thrResult {
	s := open(zctx, gozmq.Stream, cfg, bind)
	defer s.Close()
	signal(ready)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var progress atomic.Int64
	go watchdog(ctx, cancel, &progress, cfg.idle)

	total := int64(cfg.size) * int64(cfg.count)
	var got int64
	var start time.Time
	for got < total {
		m, err := s.RecvMsg(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Fatalf("stream recv: %v", err)
		}
		// [identity, data]; an empty data frame is a connect or
		// disconnect notification.
		if fr := m.Frames(); len(fr) == 2 && len(fr[1]) > 0 {
			if got == 0 {
				start = time.Now()
			}
			got += int64(len(fr[1]))
			progress.Store(got)
		}
		m.Release()
	}
	return streamResult(cfg, got, start)
}

func streamResult(cfg *config, got int64, start time.Time) thrResult {
	if cfg.size == 0 {
		return thrResult{received: cfg.count, elapsed: time.Since(start)}
	}
	return thrResult{received: int(got / int64(cfg.size)), elapsed: time.Since(start)}
}

// sendRawTCP is stream_in's sender: write size*count bytes of payload over
// one plain TCP connection in large writes, so the writer is never the
// bottleneck.
func sendRawTCP(ctx context.Context, _ *gozmq.Context, cfg *config, _ bool) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", tcpAddr(cfg.endpoint))
	check(err, "dial")
	defer conn.Close()
	writeBytes(conn, cfg)
}

// writeBytes writes the payload count times, batched into ~256 KiB writes.
func writeBytes(w io.Writer, cfg *config) {
	per := max(1, (256<<10)/max(cfg.size, 1))
	chunk := make([]byte, 0, per*cfg.size)
	for range per {
		chunk = append(chunk, cfg.payload...)
	}
	for left := cfg.count; left > 0; {
		n := min(left, per)
		_, err := w.Write(chunk[:n*cfg.size])
		check(err, "raw write")
		left -= n
	}
}

// recvRawTCP is stream_out's receiver: accept one plain TCP connection,
// write "go" so the STREAM peer learns this connection's identity, then
// count bytes.
func recvRawTCP(ctx context.Context, _ *gozmq.Context, cfg *config, _ bool, ready chan<- struct{}) thrResult {
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", tcpAddr(cfg.endpoint))
	check(err, "listen")
	defer l.Close()
	signal(ready)
	conn, err := l.Accept()
	check(err, "accept")
	defer conn.Close()
	_, err = conn.Write([]byte("go"))
	check(err, "write go")

	total := int64(cfg.size) * int64(cfg.count)
	buf := make([]byte, 256<<10)
	var got int64
	var start time.Time
	for got < total {
		_ = conn.SetReadDeadline(time.Now().Add(cfg.idle))
		n, err := conn.Read(buf)
		if n > 0 {
			if got == 0 {
				start = time.Now()
			}
			got += int64(n)
		}
		if err != nil {
			break
		}
	}
	return streamResult(cfg, got, start)
}

// sendStream is stream_out's sender: a STREAM socket that connects, learns
// the peer's identity from its "go", and sends [identity, payload] count
// times.
func sendStream(ctx context.Context, zctx *gozmq.Context, cfg *config, bind bool) {
	s := open(zctx, gozmq.Stream, cfg, bind)
	var id []byte
	for id == nil {
		m, err := s.RecvMsg(ctx)
		check(err, "stream recv go")
		if fr := m.Frames(); len(fr) == 2 && len(fr[1]) > 0 {
			id = append([]byte(nil), fr[0]...)
		}
		m.Release()
	}
	for i := range cfg.count {
		if err := s.SendMsg(ctx, gozmq.NewMsgFromFrames(id, cfg.payload)); err != nil {
			log.Fatalf("send[%d]: %v", i, err)
		}
	}
	check(s.Close(), "close")
}
