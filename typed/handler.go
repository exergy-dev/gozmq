package typed

import (
	"context"
	"errors"
	"fmt"

	"github.com/rkm/gozmq"
)

// ErrDecode wraps errors the dispatch loop observes while turning a
// received message into a value of type T (frame-shape mismatches,
// Codec.Unmarshal failures). Use errors.Is against ErrDecode inside a
// WithErrorHandler callback to tell decode failures from handler
// failures.
var ErrDecode = errors.New("typed: decode failed")

// ErrHandler wraps non-nil errors returned by the user callback passed
// to Handle or registered with a TopicMux. Use errors.Is against
// ErrHandler to distinguish these from decode errors.
var ErrHandler = errors.New("typed: handler returned error")

// Handle repeatedly receives from r's socket, decodes the payload into
// a T via r's Codec, and invokes fn with the result. See handleLoop for
// stop conditions and error semantics.
func (r *Receiver[T]) Handle(
	ctx context.Context,
	fn func(context.Context, T) error,
	opts ...Option,
) error {
	return handleLoop(ctx, r.sock, func(m *gozmq.Msg) (T, error) {
		var v T
		if m.NumFrames() != 1 {
			return v, fmt.Errorf("%w: expected 1 frame, got %d: %w", ErrDecode, m.NumFrames(), ErrFrameCount)
		}
		if err := r.codec.Unmarshal(m.Bytes(), &v); err != nil {
			return v, fmt.Errorf("%w: %w", ErrDecode, err)
		}
		return v, nil
	}, fn, apply(opts))
}

// handleLoop is the shared dispatch loop for Receiver.Handle and
// Subscriber.Handle. decode turns each received message into a V (or a
// decode error already wrapping ErrDecode); the loop owns ctx checks,
// RecvMsg, Release, error reporting, and fn invocation. It blocks until:
//
//   - ctx is cancelled — returns ctx.Err();
//   - RecvMsg returns a non-context error (e.g. ErrSocketClosed) —
//     returns that error unchanged;
//   - the configured WithErrorHandler returns a non-nil error for a
//     decode failure or a handler failure — returns that error.
//
// Socket-level errors are always fatal; per-message decode failures and
// non-nil returns from fn follow the configured error policy (dropped
// silently without WithErrorHandler). Panics in fn propagate.
func handleLoop[V any](
	ctx context.Context,
	sock *gozmq.Socket,
	decode func(*gozmq.Msg) (V, error),
	fn func(context.Context, V) error,
	o options,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := sock.RecvMsg(ctx)
		if err != nil {
			return err
		}
		v, decodeErr := decode(m)
		m.Release()
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

// Handle repeatedly receives [topic, payload] messages from s's socket,
// decodes each into a Message[T], and invokes fn. Stop conditions and
// error semantics match Receiver.Handle.
func (s *Subscriber[T]) Handle(
	ctx context.Context,
	fn func(context.Context, Message[T]) error,
	opts ...Option,
) error {
	return handleLoop(ctx, s.sock, func(m *gozmq.Msg) (Message[T], error) {
		var msg Message[T]
		frames := m.Frames()
		if len(frames) != 2 {
			return msg, fmt.Errorf("%w: expected [topic, payload], got %d frames: %w", ErrDecode, len(frames), ErrFrameCount)
		}
		if err := s.codec.Unmarshal(frames[1], &msg.Value); err != nil {
			return msg, fmt.Errorf("%w: %w", ErrDecode, err)
		}
		msg.Topic = string(frames[0])
		return msg, nil
	}, fn, apply(opts))
}
