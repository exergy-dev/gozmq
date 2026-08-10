// Package zmtp implements the wire-level constructs of ZeroMQ Message
// Transport Protocol 3.1: the framing layer (flags + length + body), the
// command codec, and the READY metadata codec. Higher-level packages
// (security, internal/session) consume these via the iface contracts
// defined in internal/iface. The greeting and PING/PONG wire formats are
// handled inline by internal/session, which deliberately does not import
// this package.
package zmtp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/exergy-dev/gozmq/internal/bufpool"
	"github.com/exergy-dev/gozmq/internal/iface"
	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// MaxBodyUnlimited is a sentinel value for NewFrameCodec meaning "accept any
// length the spec permits" (i.e. up to 2^63-1 bytes). Sessions enforce a
// finite cap via WithMaxMessageSize; pass that cap directly. Tests typically
// use a small finite cap to exercise the rejection path.
const MaxBodyUnlimited int64 = 0

// validFlagBits is the set of bits the spec reserves; any bit outside this
// mask in a received flags byte is a protocol violation.
const validFlagBits uint8 = iface.FlagMore | iface.FlagLong | iface.FlagCommand

// frameCodec is the concrete implementation of iface.FrameCodec. It is
// stateless aside from its configured max body size; a single value is safe
// to share across many sessions because the io.Reader / io.Writer arguments
// supply all per-call state.
type frameCodec struct {
	maxBody int64
}

// NewFrameCodec returns a ZMTP 3.1 frame codec. maxBody bounds the size of
// any body the codec will accept on the read side; bodies larger than maxBody
// fail with an error wrapping zmqerr.ErrMalformedFrame so callers can treat
// the connection as poisoned. Pass MaxBodyUnlimited (0) to disable the cap;
// non-positive values other than 0 panic (programmer error).
func NewFrameCodec(maxBody int64) iface.FrameCodec {
	if maxBody < 0 {
		panic(fmt.Sprintf("gozmq/zmtp: NewFrameCodec maxBody=%d must be >= 0", maxBody))
	}
	return &frameCodec{maxBody: maxBody}
}

// ReadFrame implements iface.FrameCodec.
func (c *frameCodec) ReadFrame(r io.Reader, dst []byte) (uint8, []byte, error) {
	var hdr [1]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		// Surface EOF/UnexpectedEOF unwrapped so callers can use errors.Is on
		// io.EOF to detect clean peer close. Any other error is wrapped as a
		// malformed-frame condition because we cannot resync the stream.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("gozmq/zmtp: read flags: %w", err)
	}
	flags := hdr[0]
	if flags&^validFlagBits != 0 {
		return 0, nil, fmt.Errorf("gozmq/zmtp: reserved flag bits set: 0x%02x: %w", flags, zmqerr.ErrMalformedFrame)
	}

	var length uint64
	if flags&iface.FlagLong == 0 {
		var ln [1]byte
		if _, err := io.ReadFull(r, ln[:]); err != nil {
			return 0, nil, fmt.Errorf("gozmq/zmtp: read short length: %w", wrapEOF(err))
		}
		length = uint64(ln[0])
	} else {
		var ln [8]byte
		if _, err := io.ReadFull(r, ln[:]); err != nil {
			return 0, nil, fmt.Errorf("gozmq/zmtp: read long length: %w", wrapEOF(err))
		}
		length = binary.BigEndian.Uint64(ln[:])
		// A LONG frame whose length fits in one byte is permitted by the
		// spec but discouraged; we accept it for interop. The COMMAND flag
		// has no length restriction beyond maxBody.
	}

	// Reject lengths that would overflow Go's int on 32-bit hosts before
	// touching the body slice.
	if length > uint64(math.MaxInt) {
		return flags, nil, fmt.Errorf("gozmq/zmtp: frame length %d exceeds platform int: %w", length, zmqerr.ErrMalformedFrame)
	}
	if c.maxBody > 0 && length > uint64(c.maxBody) {
		return flags, nil, fmt.Errorf("gozmq/zmtp: frame length %d exceeds max %d: %w", length, c.maxBody, zmqerr.ErrMalformedFrame)
	}

	n := int(length)
	body := reuseOrAllocate(dst, n)
	if n > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return flags, nil, fmt.Errorf("gozmq/zmtp: read body (%d bytes): %w", n, wrapEOF(err))
		}
	}
	return flags, body, nil
}

// WriteFrame implements iface.FrameCodec. It always emits the smallest legal
// length encoding: bodies <256 bytes use the short form unless the LONG flag
// is forced by the caller. The caller-provided flags are written verbatim,
// except LONG is forced on for bodies that don't fit in a single byte.
func (c *frameCodec) WriteFrame(w io.Writer, flags uint8, body []byte) error {
	useLong, n, err := c.validateWriteFrame(flags, body)
	if err != nil {
		return err
	}
	var hdr [9]byte
	hl := packHeader(&hdr, flags, useLong, n)
	if _, err := w.Write(hdr[:hl]); err != nil {
		return fmt.Errorf("gozmq/zmtp: write header: %w", err)
	}
	if n > 0 {
		if _, err := w.Write(body); err != nil {
			return fmt.Errorf("gozmq/zmtp: write body: %w", err)
		}
	}
	return nil
}

// AppendFrameHeader implements iface.FrameHeaderAppender. It appends the
// ZMTP wire header (flags byte + 1- or 8-byte big-endian length) for a body
// of len(body) bytes onto scratch and returns the grown scratch plus the
// number of header bytes appended. The body itself is NOT written; the
// caller arranges the body slice in its net.Buffers tail separately.
//
// This is the allocation-free fast path used by the writer batching loop.
// Bodies <=255 bytes use the 2-byte short header; longer bodies (or callers
// that force FlagLong) use the 9-byte long header.
func (c *frameCodec) AppendFrameHeader(scratch []byte, flags uint8, body []byte) ([]byte, int, error) {
	useLong, n, err := c.validateWriteFrame(flags, body)
	if err != nil {
		return scratch, 0, err
	}
	var hdr [9]byte
	hl := packHeader(&hdr, flags, useLong, n)
	return append(scratch, hdr[:hl]...), hl, nil
}

// packHeader writes the ZMTP frame header (flags byte + 1- or 8-byte
// big-endian length) into buf and returns the header length: 2 for the short
// form, 9 for the long form. The caller guarantees useLong is consistent
// with n (via validateWriteFrame).
func packHeader(buf *[9]byte, flags uint8, useLong bool, n int) int {
	if useLong {
		buf[0] = flags | iface.FlagLong
		binary.BigEndian.PutUint64(buf[1:], uint64(n))
		return 9
	}
	buf[0] = flags
	buf[1] = byte(n)
	return 2
}

func (c *frameCodec) validateWriteFrame(flags uint8, body []byte) (useLong bool, n int, err error) {
	if flags&^validFlagBits != 0 {
		return false, 0, fmt.Errorf("gozmq/zmtp: reserved flag bits set: 0x%02x: %w", flags, zmqerr.ErrMalformedFrame)
	}
	n = len(body)
	if c.maxBody > 0 && int64(n) > c.maxBody {
		return false, 0, fmt.Errorf("gozmq/zmtp: body length %d exceeds max %d: %w", n, c.maxBody, zmqerr.ErrMalformedFrame)
	}
	return flags&iface.FlagLong != 0 || n > 255, n, nil
}

// reuseOrAllocate returns dst[:n] when dst has the capacity, otherwise a
// fresh buffer drawn from the size-tiered frame-body pool. The session
// reader may retain the returned slice beyond the next ReadFrame — in
// that case a runtime finalizer on the user-facing *Msg is responsible
// for returning pooled buffers to the pool.
func reuseOrAllocate(dst []byte, n int) []byte {
	if cap(dst) >= n {
		return dst[:n]
	}
	return bufpool.Get(n)
}

// wrapEOF normalizes "stream ended mid-frame" into ErrUnexpectedEOF wrapped
// with ErrMalformedFrame so callers using errors.Is(err, zmqerr.ErrMalformedFrame)
// catch the truncation. A clean io.EOF before the flags byte is handled in
// ReadFrame directly.
func wrapEOF(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %w", zmqerr.ErrMalformedFrame, io.ErrUnexpectedEOF)
	}
	return err
}
