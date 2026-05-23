package typed

import (
	"context"
	"errors"
	"fmt"
)

// ErrDecode wraps errors the dispatch loop observes while turning a
// received message into a value of type T (frame-shape mismatches,
// Codec.Unmarshal failures). Use errors.Is against ErrDecode inside a
// WithErrorHandler callback to tell decode failures from handler
// failures.
var ErrDecode = errors.New("typed: decode failed")

// ErrHandler wraps non-nil errors returned by the user callback passed
// to Handle or registered with a Router. Use errors.Is against
// ErrHandler to distinguish these from decode errors.
var ErrHandler = errors.New("typed: handler returned error")

// Handle repeatedly receives from r's socket, decodes the payload into
// a T via r's Codec, and invokes fn with the result. It blocks until:
//
//   - ctx is cancelled — Handle returns ctx.Err();
//   - RecvMsg on the socket returns a non-context error (e.g.
//     ErrSocketClosed) — Handle returns that error unchanged;
//   - the configured WithErrorHandler returns a non-nil error for a
//     decode failure or a handler failure — Handle returns that error.
//
// Socket-level errors are always fatal to the loop; only per-message
// decode failures and non-nil returns from fn are subject to the
// configured error policy. Without WithErrorHandler those are silently
// dropped and the loop continues.
//
// Panics in fn are not intercepted; they propagate to the caller.
func (r *Receiver[T]) Handle(
	ctx context.Context,
	fn func(context.Context, T) error,
	opts ...Option,
) error {
	o := apply(opts)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := r.sock.RecvMsg(ctx)
		if err != nil {
			return err
		}
		var v T
		var decodeErr error
		if m.NumFrames() != 1 {
			decodeErr = fmt.Errorf("%w: expected 1 frame, got %d: %w", ErrDecode, m.NumFrames(), ErrFrameCount)
		} else if err := r.codec.Unmarshal(m.Bytes(), &v); err != nil {
			decodeErr = fmt.Errorf("%w: %w", ErrDecode, err)
		}
		if decodeErr != nil {
			if stop := o.report(decodeErr); stop != nil {
				return stop
			}
			continue
		}
		if err := fn(ctx, v); err != nil {
			if stop := o.report(fmt.Errorf("%w: %w", ErrHandler, err)); stop != nil {
				return stop
			}
		}
	}
}

// Handle repeatedly receives from s's socket, decodes the [topic,
// payload] pair into a Message[T], and invokes fn. Stop conditions and
// error semantics match Receiver.Handle.
func (s *Subscriber[T]) Handle(
	ctx context.Context,
	fn func(context.Context, Message[T]) error,
	opts ...Option,
) error {
	o := apply(opts)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := s.sock.RecvMsg(ctx)
		if err != nil {
			return err
		}
		frames := m.Frames()
		var msg Message[T]
		var decodeErr error
		if len(frames) != 2 {
			decodeErr = fmt.Errorf("%w: expected [topic, payload], got %d frames: %w", ErrDecode, len(frames), ErrFrameCount)
		} else if err := s.codec.Unmarshal(frames[1], &msg.Value); err != nil {
			decodeErr = fmt.Errorf("%w: %w", ErrDecode, err)
		} else {
			msg.Topic = string(frames[0])
		}
		if decodeErr != nil {
			if stop := o.report(decodeErr); stop != nil {
				return stop
			}
			continue
		}
		if err := fn(ctx, msg); err != nil {
			if stop := o.report(fmt.Errorf("%w: %w", ErrHandler, err)); stop != nil {
				return stop
			}
		}
	}
}
