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
//     DEALER, PAIR). The wire format is a single-frame payload, so ROUTER,
//     whose messages carry a leading identity frame, is not supported.
//
//   - Publisher[T] / Subscriber[T] for PUB/SUB. The wire format is a
//     two-frame message [topic, payload], matching how ZMQ's built-in
//     subscription filter works (prefix match on frame 0).
package typed

import (
	"context"
	"errors"
	"fmt"

	"github.com/exergy-dev/gozmq"
)

// Codec marshals Go values to bytes and back. Implementations must be
// safe for concurrent use; a single Codec may be shared across multiple
// wrappers and called from multiple goroutines. Unmarshal must not keep a
// reference to data: the wrappers release the received message, whose
// frames are pooled memory by default (gozmq.WithRecvBufferPool), as soon as it
// returns. encoding/json copies what it keeps.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// ErrFrameCount is returned when a received message's frame count does
// not match the layout the wrapper expects (1 frame for Sender/Receiver,
// 2 frames for Publisher/Subscriber).
var ErrFrameCount = errors.New("typed: unexpected frame count")

// wrapper is the socket and codec every wrapper type holds.
type wrapper struct {
	sock  *gozmq.Socket
	codec Codec
}

// marshal encodes v with the wrapper's codec.
func (w wrapper) marshal(v any) ([]byte, error) {
	b, err := w.codec.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("typed: marshal: %w", err)
	}
	return b, nil
}

// unmarshal decodes b into a T, returning the zero T on error.
func unmarshal[T any](c Codec, b []byte) (T, error) {
	var v T
	if err := c.Unmarshal(b, &v); err != nil {
		var zero T
		return zero, err
	}
	return v, nil
}

// recvDecode receives one message from sock, decodes it and releases it.
func recvDecode[V any](ctx context.Context, sock *gozmq.Socket, decode func(*gozmq.Msg) (V, error)) (V, error) {
	m, err := sock.RecvMsg(ctx)
	if err != nil {
		var zero V
		return zero, err
	}
	defer m.Release()
	return decode(m)
}

// topicPayload splits a [topic, payload] message, wrapping ErrDecode and
// ErrFrameCount when it has any other shape.
func topicPayload(m *gozmq.Msg) (topic, payload []byte, err error) {
	frames := m.Frames()
	if len(frames) != 2 {
		return nil, nil, fmt.Errorf("%w: expected [topic, payload], got %d frames: %w", ErrDecode, len(frames), ErrFrameCount)
	}
	return frames[0], frames[1], nil
}

// Sender wraps a *gozmq.Socket to send values of type T as single-frame
// messages encoded by Codec.
type Sender[T any] struct{ wrapper }

// NewSender returns a Sender bound to sock. sock must be a send-capable
// single-frame socket (PUSH, REQ, REP, DEALER, PAIR); ROUTER needs the
// identity envelope and is not supported.
func NewSender[T any](sock *gozmq.Socket, codec Codec) *Sender[T] {
	return &Sender[T]{wrapper{sock, codec}}
}

// Send marshals v and transmits it as a single-frame message.
func (s *Sender[T]) Send(ctx context.Context, v T) error {
	b, err := s.marshal(v)
	if err != nil {
		return err
	}
	return s.sock.SendMsg(ctx, gozmq.NewMsgFromBytes(b))
}

// Receiver wraps a *gozmq.Socket to receive single-frame messages and
// decode them into a value of type T via Codec.
type Receiver[T any] struct{ wrapper }

// NewReceiver returns a Receiver bound to sock.
func NewReceiver[T any](sock *gozmq.Socket, codec Codec) *Receiver[T] {
	return &Receiver[T]{wrapper{sock, codec}}
}

// Recv blocks until a single-frame message arrives or ctx cancels, then
// unmarshals the payload into a T. Frame-count and Codec failures wrap
// ErrDecode.
func (r *Receiver[T]) Recv(ctx context.Context) (T, error) {
	return recvDecode(ctx, r.sock, r.decode)
}

// decode turns a single-frame message into a T; shared by Recv and Handle.
func (r *Receiver[T]) decode(m *gozmq.Msg) (T, error) {
	if m.NumFrames() != 1 {
		var zero T
		return zero, fmt.Errorf("%w: expected 1 frame, got %d: %w", ErrDecode, m.NumFrames(), ErrFrameCount)
	}
	v, err := unmarshal[T](r.codec, m.Bytes())
	if err != nil {
		return v, fmt.Errorf("%w: %w", ErrDecode, err)
	}
	return v, nil
}

// Message is a received value with its topic prefix attached.
type Message[T any] struct {
	Topic string
	Value T
}

// Publisher wraps a PUB/XPUB socket to send values of type T on a named
// topic. The wire format is a two-frame message: [topic, payload].
type Publisher[T any] struct{ wrapper }

// NewPublisher returns a Publisher bound to sock.
func NewPublisher[T any](sock *gozmq.Socket, codec Codec) *Publisher[T] {
	return &Publisher[T]{wrapper{sock, codec}}
}

// Send marshals v and transmits it as [topic, payload].
func (p *Publisher[T]) Send(ctx context.Context, topic string, v T) error {
	b, err := p.marshal(v)
	if err != nil {
		return err
	}
	return p.sock.SendMsg(ctx, gozmq.NewMsgFromFrames([]byte(topic), b))
}

// Subscriber wraps a SUB/XSUB socket to receive two-frame [topic, payload]
// messages and decode the payload into a value of type T.
type Subscriber[T any] struct{ wrapper }

// NewSubscriber returns a Subscriber bound to sock. The caller is still
// responsible for configuring topic filters via Subscribe (or via
// gozmq.WithSubscribe when constructing sock) — an unfiltered SUB socket
// receives nothing.
func NewSubscriber[T any](sock *gozmq.Socket, codec Codec) *Subscriber[T] {
	return &Subscriber[T]{wrapper{sock, codec}}
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
// Frame-count and Codec failures wrap ErrDecode.
func (s *Subscriber[T]) Recv(ctx context.Context) (Message[T], error) {
	return recvDecode(ctx, s.sock, s.decode)
}

// decode turns a [topic, payload] message into a Message[T]; shared by
// Recv and Handle.
func (s *Subscriber[T]) decode(m *gozmq.Msg) (Message[T], error) {
	topic, payload, err := topicPayload(m)
	if err != nil {
		return Message[T]{}, err
	}
	v, err := unmarshal[T](s.codec, payload)
	if err != nil {
		return Message[T]{}, fmt.Errorf("%w: %w", ErrDecode, err)
	}
	return Message[T]{Topic: string(topic), Value: v}, nil
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
	if err == nil || o.onError == nil {
		return nil
	}
	return o.onError(err)
}
