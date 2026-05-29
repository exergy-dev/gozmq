package typed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/rkm/gozmq"
)

// ErrNoRouterMatch is fed to the Router's error handler when a received
// [topic, payload] message carries a topic that matches no registered
// prefix. Consumers can errors.Is against it in a WithErrorHandler
// callback.
var ErrNoRouterMatch = errors.New("typed: no handler registered for topic")

// ErrRouterRunning is returned by Register and Unregister after
// Router.Run has started. Registration must happen before Run.
var ErrRouterRunning = errors.New("typed: router is running")

// ErrTopicAlreadyRegistered is returned by Register when topic is
// already bound to a handler.
var ErrTopicAlreadyRegistered = errors.New("typed: topic already registered")

// Router dispatches incoming messages on a SUB/XSUB socket to typed
// handlers registered by topic prefix. Topic matching uses prefix
// semantics mirroring ZMQ SUB filtering: a handler registered for
// "orders." matches "orders.created", "orders.cancelled", etc. When
// multiple prefixes match, the longest one wins.
//
// Registration must happen before Run is called. Register or
// Unregister calls made after Run has started return ErrRouterRunning.
type Router struct {
	sock    *gozmq.Socket
	codec   Codec
	entries []routerEntry
	started atomic.Bool
}

type routerEntry struct {
	topic  string
	decode func([]byte) (any, error)
	invoke func(ctx context.Context, topic string, v any) error
}

// NewRouter returns a Router bound to sock. sock should be a SUB or
// XSUB socket; Register subscribes topic prefixes on it.
func NewRouter(sock *gozmq.Socket, codec Codec) *Router {
	return &Router{sock: sock, codec: codec}
}

// Register binds topic (a prefix) to handler and subscribes sock to
// that prefix. Returns ErrRouterRunning if Run has started, or
// ErrTopicAlreadyRegistered if topic is already bound.
func Register[T any](
	r *Router,
	topic string,
	handler func(context.Context, Message[T]) error,
) error {
	if r.started.Load() {
		return ErrRouterRunning
	}
	for _, e := range r.entries {
		if e.topic == topic {
			return fmt.Errorf("%w: %q", ErrTopicAlreadyRegistered, topic)
		}
	}

	entry := routerEntry{
		topic: topic,
		decode: func(b []byte) (any, error) {
			var v T
			if err := r.codec.Unmarshal(b, &v); err != nil {
				return nil, err
			}
			return v, nil
		},
		invoke: func(ctx context.Context, topic string, v any) error {
			return handler(ctx, Message[T]{Topic: topic, Value: v.(T)})
		},
	}

	if err := r.sock.Subscribe([]byte(topic)); err != nil {
		return err
	}

	// Insert preserving descending-length order so Run's linear scan
	// naturally picks the longest match first.
	inserted := false
	for i, e := range r.entries {
		if len(topic) > len(e.topic) {
			r.entries = slices.Insert(r.entries, i, entry)
			inserted = true
			break
		}
	}
	if !inserted {
		r.entries = append(r.entries, entry)
	}
	return nil
}

// Unregister removes the handler for topic and unsubscribes sock from
// that prefix. Returns ErrRouterRunning if Run has started. Unknown
// topics are a no-op.
func (r *Router) Unregister(topic string) error {
	if r.started.Load() {
		return ErrRouterRunning
	}
	for i, e := range r.entries {
		if e.topic == topic {
			r.entries = slices.Delete(r.entries, i, i+1)
			return r.sock.Unsubscribe([]byte(topic))
		}
	}
	return nil
}

// Run blocks and dispatches messages received on sock to the
// registered handlers. Shape / decode / handler errors are passed to
// the configured WithErrorHandler, with classification via ErrDecode,
// ErrHandler, and ErrNoRouterMatch. Socket-level errors are always
// fatal; ctx cancellation returns ctx.Err().
//
// Calling Run a second time on the same Router is supported only if
// the previous Run returned; concurrent Run calls are not supported.
func (r *Router) Run(ctx context.Context, opts ...Option) error {
	r.started.Store(true)
	o := apply(opts)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := r.sock.RecvMsg(ctx)
		if err != nil {
			return err
		}
		frames := m.Frames()
		if len(frames) != 2 {
			m.Release()
			if stop := o.report(fmt.Errorf("%w: expected [topic, payload], got %d frames: %w", ErrDecode, len(frames), ErrFrameCount)); stop != nil {
				return stop
			}
			continue
		}
		topic := string(frames[0])
		entry, ok := r.match(topic)
		if !ok {
			m.Release()
			if stop := o.report(fmt.Errorf("%w: %q", ErrNoRouterMatch, topic)); stop != nil {
				return stop
			}
			continue
		}
		v, err := entry.decode(frames[1])
		m.Release()
		if err != nil {
			if stop := o.report(fmt.Errorf("%w: topic %q: %w", ErrDecode, topic, err)); stop != nil {
				return stop
			}
			continue
		}
		if err := entry.invoke(ctx, topic, v); err != nil {
			if stop := o.report(fmt.Errorf("%w: topic %q: %w", ErrHandler, topic, err)); stop != nil {
				return stop
			}
		}
	}
}

func (r *Router) match(topic string) (routerEntry, bool) {
	for _, e := range r.entries {
		if strings.HasPrefix(topic, e.topic) {
			return e, true
		}
	}
	return routerEntry{}, false
}
