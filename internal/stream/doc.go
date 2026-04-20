// Package stream is the Wave-6 raw-bytes transport adapter that lets a
// Socket(Stream) carry non-ZMTP traffic end-to-end.
//
// Why this package exists. STREAM is the only ZMTP socket type that does
// NOT speak ZMTP on the wire: the peer is typically a plain HTTP/SMTP/line-
// oriented TCP service that has never heard of ZMQ. The standard
// internal/session layer unconditionally applies the ZMTP FrameCodec to
// every byte in both directions and therefore cannot host a STREAM
// connection — feeding "GET / HTTP/1.1" into ZMTP framing is a malformed-
// frame error. This package supplies the dedicated pump that replaces
// session.New when socketType == gozmq.Stream.
//
// Shape. One Adapter per peer conn. On construction it allocates a Pipe
// pair (engine<->adapter), spawns a reader goroutine that turns each
// conn.Read into a one-frame Msg on the pipe, and a writer goroutine that
// drains pipe Msgs straight to conn.Write. The adapter returns the engine-
// facing Pipe end — the gozmq.Socket then calls engine.AttachPipe with it,
// just like it would with session.Pipe() for ZMTP traffic.
//
// Boundary contract (pinned by socket/stream.go godoc):
//   - Inbound pipe Msg = one body frame = one conn.Read chunk (size is
//     whatever the OS returned).
//   - Outbound pipe Msg = one body frame = raw bytes to write verbatim.
//     Zero-length body means "close this peer" (STREAM half-close). The
//     streamEngine upstream collapses multi-frame bodies into a single
//     concatenated frame before handing it down the pipe.
//   - Peer disconnect / read error: close the pipe end; engine detaches
//     when it notices Closed().
//   - Both goroutines honor a context.Context so the owning Socket can
//     tear the adapter down cleanly on Close.
//
// Lifetime. The adapter owns the conn for its lifetime: shutdown (ctx
// cancel, reader EOF, writer empty-body, or Stop) closes the conn and the
// pipe ends, then the goroutines exit. Close is idempotent; it's safe
// for both reader/writer to race into shutdown.
package stream
