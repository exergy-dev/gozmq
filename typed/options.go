package typed

// Option configures a Handle invocation or Router.Run invocation.
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

// apply folds opts into a fresh options value.
func apply(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// report feeds err to the configured error handler (if any) and returns
// the error the loop should surface to the caller — nil to continue,
// non-nil to stop. A nil incoming err is a no-op and always continues.
func (o *options) report(err error) error {
	if err == nil {
		return nil
	}
	if o.onError == nil {
		return nil
	}
	return o.onError(err)
}
