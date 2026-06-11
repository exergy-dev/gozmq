// Package typed wraps a gozmq.Socket with generic helpers so callers can
// send and receive arbitrary Go values through a pluggable Codec instead
// of hand-rolling serialization and multipart framing at every call site.
//
// The package is purely a convenience layer on top of the core gozmq
// API — it owns no sockets, starts no goroutines, and never mutates the
// socket's send/recv path. All wrappers are safe for concurrent use to
// the same extent the underlying *gozmq.Socket is.
//
// There are two pairs of wrappers:
//
//   - Sender[T] / Receiver[T] for non-topic patterns (PUSH/PULL, REQ/REP,
//     DEALER/ROUTER, PAIR). The wire format is a single-frame payload.
//
//   - Publisher[T] / Subscriber[T] for PUB/SUB. The wire format is a
//     two-frame message [topic, payload], matching how ZMQ's built-in
//     subscription filter works (prefix match on frame 0).
package typed

import (
	"context"
	"errors"
	"fmt"

	"github.com/rkm/gozmq"
)

// Codec marshals Go values to bytes and back. Implementations must be
// safe for concurrent use; a single Codec may be shared across multiple
// wrappers and called from multiple goroutines.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// ErrFrameCount is returned when a received message's frame count does
// not match the layout the wrapper expects (1 frame for Sender/Receiver,
// 2 frames for Publisher/Subscriber).
var ErrFrameCount = errors.New("typed: unexpected frame count")

// Sender wraps a *gozmq.Socket to send values of type T as single-frame
// messages encoded by Codec.
type Sender[T any] struct {
	sock  *gozmq.Socket
	codec Codec
}

// NewSender returns a Sender bound to sock. sock must be a send-capable
// non-PUB socket (PUSH, REQ, REP, DEALER, ROUTER, PAIR).
func NewSender[T any](sock *gozmq.Socket, codec Codec) *Sender[T] {
	return &Sender[T]{sock: sock, codec: codec}
}

// Send marshals v and transmits it as a single-frame message.
func (s *Sender[T]) Send(ctx context.Context, v T) error {
	b, err := s.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("typed: marshal: %w", err)
	}
	return s.sock.SendMsg(ctx, gozmq.NewMsgFromBytes(b))
}

// Receiver wraps a *gozmq.Socket to receive single-frame messages and
// decode them into a value of type T via Codec.
type Receiver[T any] struct {
	sock  *gozmq.Socket
	codec Codec
}

// NewReceiver returns a Receiver bound to sock.
func NewReceiver[T any](sock *gozmq.Socket, codec Codec) *Receiver[T] {
	return &Receiver[T]{sock: sock, codec: codec}
}

// Recv blocks until a single-frame message arrives or ctx cancels, then
// unmarshals the payload into a T.
func (r *Receiver[T]) Recv(ctx context.Context) (T, error) {
	var zero T
	m, err := r.sock.RecvMsg(ctx)
	if err != nil {
		return zero, err
	}
	defer m.Release()
	if m.NumFrames() != 1 {
		return zero, fmt.Errorf("typed: expected 1 frame, got %d: %w", m.NumFrames(), ErrFrameCount)
	}
	var out T
	if err := r.codec.Unmarshal(m.Bytes(), &out); err != nil {
		return zero, fmt.Errorf("typed: unmarshal: %w", err)
	}
	return out, nil
}

// Message is a received value with its topic prefix attached.
type Message[T any] struct {
	Topic string
	Value T
}

// Publisher wraps a PUB/XPUB socket to send values of type T on a named
// topic. The wire format is a two-frame message: [topic, payload].
type Publisher[T any] struct {
	sock  *gozmq.Socket
	codec Codec
}

// NewPublisher returns a Publisher bound to sock.
func NewPublisher[T any](sock *gozmq.Socket, codec Codec) *Publisher[T] {
	return &Publisher[T]{sock: sock, codec: codec}
}

// Send marshals v and transmits it as [topic, payload].
func (p *Publisher[T]) Send(ctx context.Context, topic string, v T) error {
	b, err := p.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("typed: marshal: %w", err)
	}
	return p.sock.SendMsg(ctx, gozmq.NewMsgFromFrames([]byte(topic), b))
}

// Subscriber wraps a SUB/XSUB socket to receive two-frame [topic, payload]
// messages and decode the payload into a value of type T.
type Subscriber[T any] struct {
	sock  *gozmq.Socket
	codec Codec
}

// NewSubscriber returns a Subscriber bound to sock. The caller is still
// responsible for configuring topic filters via Subscribe (or via
// gozmq.WithSubscribe when constructing sock) — an unfiltered SUB socket
// receives nothing.
func NewSubscriber[T any](sock *gozmq.Socket, codec Codec) *Subscriber[T] {
	return &Subscriber[T]{sock: sock, codec: codec}
}

// Subscribe adds topic as a prefix filter on the underlying SUB socket.
func (s *Subscriber[T]) Subscribe(topic string) error {
	return s.sock.Subscribe([]byte(topic))
}

// Unsubscribe decrements the refcount for topic on the underlying SUB
// socket.
func (s *Subscriber[T]) Unsubscribe(topic string) error {
	return s.sock.Unsubscribe([]byte(topic))
}

// Recv blocks until a [topic, payload] message arrives or ctx cancels,
// then unmarshals the payload and returns it alongside the topic.
func (s *Subscriber[T]) Recv(ctx context.Context) (Message[T], error) {
	var zero Message[T]
	m, err := s.sock.RecvMsg(ctx)
	if err != nil {
		return zero, err
	}
	defer m.Release()
	frames := m.Frames()
	if len(frames) != 2 {
		return zero, fmt.Errorf("typed: expected [topic, payload], got %d frames: %w", len(frames), ErrFrameCount)
	}
	var v T
	if err := s.codec.Unmarshal(frames[1], &v); err != nil {
		return zero, fmt.Errorf("typed: unmarshal: %w", err)
	}
	return Message[T]{Topic: string(frames[0]), Value: v}, nil
}

// Option configures a Handle invocation or TopicMux.Run invocation.
type Option func(*options)

type options struct {
	onError func(error) error
}

// WithErrorHandler installs fn as the callback for every error the
// dispatch loop observes (decode failure, frame-count mismatch, or a
// non-nil return from the user handler). If fn returns nil the loop
// continues; if fn returns a non-nil error the loop stops and that
// error is surfaced to the caller.
//
// Default (no option): errors are dropped silently and the loop
// continues.
//
// Panics in the user handler are not intercepted — they propagate up
// through the loop and crash the caller's goroutine.
func WithErrorHandler(fn func(error) error) Option {
	return func(o *options) { o.onError = fn }
}

func apply(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func (o *options) report(err error) error {
	if err == nil {
		return nil
	}
	if o.onError == nil {
		return nil
	}
	return o.onError(err)
}
