package gozmq

import (
	"bytes"
	"fmt"
	"log/slog"
	"time"

	"github.com/exergy-dev/gozmq/security"
)

// Option configures a Socket at construction time. The option type is opaque;
// construct values via the With* helpers. Options that are inapplicable to
// the socket type are rejected with an error at Socket construction rather
// than silently ignored: pattern-specific options (WithRouterMandatory,
// WithXPubVerbose, WithSubscribe) on the wrong pattern, and ZMTP-layer
// options (security, heartbeat, identity, handshake timeout, ZAP domain)
// on STREAM sockets, which bypass ZMTP entirely.
type Option interface {
	apply(*socketConfig) error
}

// socketConfig is the concrete aggregate that every Option mutates. Defaults
// are set in newSocketConfig.
type socketConfig struct {
	identity             []byte
	sendHWM              int
	recvHWM              int
	linger               time.Duration
	reconnectInterval    time.Duration
	reconnectIntervalMax time.Duration
	heartbeatInterval    time.Duration
	heartbeatTimeout     time.Duration
	maxMessageSize       int64
	routerMandatory      bool
	xpubVerbose          bool
	subscriptions        [][]byte
	handshakeTimeout     time.Duration
	zapDomain            string

	// Set-flags for options whose zero/default values are
	// indistinguishable from "not configured" — needed so
	// validateSocketConfig can reject explicit use on STREAM sockets.
	heartbeatSet        bool
	handshakeTimeoutSet bool

	plainAuth       func(user, pass string) bool
	plainClientUser string
	plainClientPass string
	plainClient     bool

	newSecurityMech func(role security.Role, meta map[string]string) (security.Mech, error)

	curveServer    bool
	curveServerKey []byte
	curvePublicKey []byte
	curveSecretKey []byte

	monitor     chan<- Event
	monitorMask EventType

	// logger sinks internal diagnostic events (handshake failures,
	// reconnect attempts, heartbeat timeouts). nil lets the library
	// pick slog.Default; set via WithLogger for reproducible test
	// logs and structured context.
	logger *slog.Logger
}

func newSocketConfig() *socketConfig {
	return &socketConfig{
		sendHWM:              1000,
		recvHWM:              1000,
		linger:               time.Second,
		reconnectInterval:    100 * time.Millisecond,
		reconnectIntervalMax: 30 * time.Second,
		heartbeatInterval:    30 * time.Second,
		heartbeatTimeout:     90 * time.Second,
		maxMessageSize:       64 << 20,
		handshakeTimeout:     30 * time.Second,
	}
}

type optionFunc func(*socketConfig) error

func (f optionFunc) apply(c *socketConfig) error { return f(c) }

func newOption(fn func(*socketConfig) error) Option {
	return optionFunc(fn)
}

// WithIdentity sets the ROUTER/DEALER identity advertised during the ZMTP
// handshake. The identity is the peer-visible name by which a ROUTER
// addresses this DEALER (or by which a ROUTER peer appears in the leading
// frame of a routed message). Identity bytes must be either empty (library
// assigns a random identity, default) or 1..255 bytes long with a non-zero
// leading byte; longer or zero-prefixed identities are rejected by ZMTP.
// The slice is copied defensively.
func WithIdentity(id []byte) Option {
	return newOption(func(c *socketConfig) error {
		c.identity = bytes.Clone(id)
		return nil
	})
}

// WithSendHWM sets the outbound high-water mark — the maximum number of
// messages queued per peer pipe before backpressure engages. Default is
// 1000. Zero disables the limit (unbounded growth; not recommended for
// untrusted peers). When the HWM is reached, Send behavior depends on the
// socket pattern: blocking sockets (PAIR, PUSH, DEALER, REQ, REP, ROUTER)
// wait for a Recv on the peer to drain queue space, while PUB/XPUB drop
// the message for the full peer and continue the fan-out (the drop is
// counted in [Socket.Metrics]).
func WithSendHWM(n int) Option {
	return newOption(func(c *socketConfig) error { c.sendHWM = n; return nil })
}

// WithRecvHWM sets the inbound high-water mark — the maximum number of
// messages buffered per peer pipe before the reader goroutine stops
// accepting frames from the wire and exerts TCP backpressure on the peer.
// Default is 1000. Zero disables the limit.
func WithRecvHWM(n int) Option {
	return newOption(func(c *socketConfig) error { c.recvHWM = n; return nil })
}

// WithLinger sets the grace period Socket.Close waits for outbound pipes
// to flush queued messages to the wire. Default is 1 second. Zero
// discards any queued messages immediately on Close. A negative duration
// blocks Close indefinitely until every peer has drained, matching
// libzmq's infinite-linger behavior — this can deadlock Close if a peer
// never drains or reconnects, so pair it with a parallel shutdown
// watchdog if you need it.
func WithLinger(d time.Duration) Option {
	return newOption(func(c *socketConfig) error { c.linger = d; return nil })
}

// WithReconnectInterval sets the base delay between reconnection attempts
// on a client-side socket after a dial failure or post-handshake
// disconnect. Default is 100ms. The actual delay grows exponentially and
// is jittered by 20% before each attempt, capped by
// [WithReconnectIntervalMax].
func WithReconnectInterval(d time.Duration) Option {
	return newOption(func(c *socketConfig) error { c.reconnectInterval = d; return nil })
}

// WithReconnectIntervalMax caps the reconnection backoff. Default is 30
// seconds. Zero removes the cap: backoff doubles without bound, so after
// a long outage a socket may wait arbitrarily long between attempts.
// NOTE this diverges from libzmq, where ZMQ_RECONNECT_IVL_MAX=0 (its
// default) means "no exponential backoff, constant interval".
func WithReconnectIntervalMax(d time.Duration) Option {
	return newOption(func(c *socketConfig) error { c.reconnectIntervalMax = d; return nil })
}

// WithHeartbeat configures the ZMTP 3.1 heartbeat. interval is how often
// PING commands are sent on an otherwise-idle session; timeout is the
// duration without any inbound frame (data or PONG) after which the peer
// is considered dead and the session is torn down. Defaults: 30s interval,
// 90s timeout. A timeout of 0 means 3× interval; an interval of 0
// disables heartbeats. Heartbeats are disabled automatically when the peer
// advertises ZMTP 3.0 in the greeting. Heartbeating is the only reliable
// way to detect a half-open TCP connection (e.g. a peer killed without a
// FIN, or a NAT that dropped the flow) — disabling it is discouraged on
// long-lived connections.
func WithHeartbeat(interval, timeout time.Duration) Option {
	return newOption(func(c *socketConfig) error {
		c.heartbeatInterval = interval
		c.heartbeatTimeout = timeout
		c.heartbeatSet = true
		return nil
	})
}

// WithMaxMessageSize sets the per-message byte limit. Default is 64 MiB.
// It is enforced on both directions, with different surfacing:
//
//   - Send side: SendMsg/SendBytes measure every frame up front and
//     return a synchronous error wrapping [ErrMalformedFrame] on
//     overflow; the session is NOT torn down.
//   - Recv side: a peer whose frame — or whose accumulated multipart
//     message — exceeds the limit has its session torn down with
//     [ErrMalformedFrame]. The violation surfaces via a monitor
//     EventDisconnected rather than a synchronous Recv error, because
//     the read loop detects it asynchronously of the application's
//     call path.
//
// Pass 0 to disable the limit — opt-in to unlimited size, NOT
// recommended for untrusted peers because a hostile sender can force
// unbounded allocation.
func WithMaxMessageSize(n int64) Option {
	return newOption(func(c *socketConfig) error { c.maxMessageSize = n; return nil })
}

// WithRouterMandatory controls ROUTER.Send behaviour for unroutable
// destinations (leading identity frame not matching any connected peer).
// Default is false: unroutable messages are dropped silently, matching
// libzmq. When true, Send returns [ErrRouteUnknown] instead, which is the
// safer default for request/response services that need to observe
// delivery failures.
func WithRouterMandatory(enabled bool) Option {
	return newOption(func(c *socketConfig) error { c.routerMandatory = enabled; return nil })
}

// WithXPubVerbose controls subscription deduplication on XPUB. Default is
// false: XPUB surfaces a SUBSCRIBE or CANCEL to the application only when
// it is the first/last reference to that prefix across all connected SUB
// peers (the refcounted view). When true, every SUBSCRIBE/CANCEL from any
// peer is forwarded to the application as a discrete message — useful for
// applications that track subscriber churn or implement their own
// authorization on subscriptions.
func WithXPubVerbose(enabled bool) Option {
	return newOption(func(c *socketConfig) error { c.xpubVerbose = enabled; return nil })
}

// WithSubscribe adds a subscription prefix to a SUB (or XSUB) socket. May
// be passed multiple times to subscribe to disjoint prefixes. An empty
// prefix subscribes to every message. Subscriptions are refcounted:
// calling Subscribe or passing WithSubscribe for the same prefix twice
// requires two matching Unsubscribe calls before the prefix is actually
// removed from the filter and a CANCEL is sent upstream to connected
// publishers. This enables independent components on the same socket to
// manage their own subscriptions without interfering.
func WithSubscribe(prefix []byte) Option {
	return newOption(func(c *socketConfig) error {
		c.subscriptions = append(c.subscriptions, bytes.Clone(prefix))
		return nil
	})
}

// WithPlainAuth makes the socket the PLAIN server and installs its credential
// validator. The callback is invoked for each handshake with the credentials
// the client sent; returning true allows the session, false rejects it with
// [ErrAuthFailed]. When the Context has a [WithZAPHandler], that handler
// decides instead. As with libzmq's ZMQ_PLAIN_SERVER, the PLAIN role is
// independent of Bind/Connect: the socket is the server on every connection.
// It cannot be combined with [WithPlainClient] or the CURVE options.
//
// PLAIN credentials traverse the wire in cleartext — only use this mechanism
// on a trusted link (inproc, ipc, loopback) or wrapped in tls+tcp://. For
// untrusted networks prefer [WithCurveServer] / [WithCurveClient].
func WithPlainAuth(fn func(user, pass string) bool) Option {
	return newOption(func(c *socketConfig) error { c.plainAuth = fn; return nil })
}

// WithPlainClient makes the socket the PLAIN client, presenting (username,
// password) to the server during the ZMTP handshake. The PLAIN role is
// independent of Bind/Connect: the socket is the client on every connection,
// including those it accepts. It cannot be combined with [WithPlainAuth] or
// the CURVE options. Credentials are sent in cleartext; wrap in tls+tcp:// or
// switch to CURVE for any network you do not trust.
func WithPlainClient(username, password string) Option {
	return newOption(func(c *socketConfig) error {
		c.plainClient = true
		c.plainClientUser = username
		c.plainClientPass = password
		return nil
	})
}

// WithZAPDomain sets the RFC 27 ZAP domain presented to the Context's
// [WithZAPHandler] when it authorizes this socket's peers. PLAIN and CURVE
// servers consult the handler whatever the domain; NULL sockets consult it,
// for the connections they accept, only when a domain is set, as in libzmq,
// so a handler installed for PLAIN or CURVE does not start vetting every NULL
// socket. The default is the empty domain.
func WithZAPDomain(domain string) Option {
	return newOption(func(c *socketConfig) error {
		c.zapDomain = domain
		return nil
	})
}

// WithCurveServer configures the socket as the server side of a CURVE
// (CurveZMQ) handshake using secretKey as its long-term private key.
// secretKey must be 32 raw bytes (decode from Z85 via the
// security/curve package if you store keys in the standard textual
// format). CURVE provides forward-secret AEAD encryption and mutual
// authentication against each connecting client's long-term public key.
// Clients must present a matching public key via [WithCurveClient].
func WithCurveServer(secretKey []byte) Option {
	return newOption(func(c *socketConfig) error {
		c.curveServer = true
		c.curveSecretKey = bytes.Clone(secretKey)
		return nil
	})
}

// WithCurveClient configures the socket as the client side of a CURVE
// handshake. serverKey is the expected server long-term public key
// (clients authenticate the server by this pinned key);
// publicKey/secretKey is this client's long-term keypair, which the
// server uses to authenticate the client. All three arguments must be
// 32 raw bytes each; decode from Z85 first if stored textually. The
// resulting channel is forward-secret (ephemeral session keys) and
// mutually authenticated.
func WithCurveClient(serverKey, publicKey, secretKey []byte) Option {
	return newOption(func(c *socketConfig) error {
		c.curveServer = false
		c.curveServerKey = bytes.Clone(serverKey)
		c.curvePublicKey = bytes.Clone(publicKey)
		c.curveSecretKey = bytes.Clone(secretKey)
		return nil
	})
}

// WithSecurityMech installs a caller-supplied ZMTP security mechanism,
// bypassing the built-in NULL / PLAIN / CURVE selection entirely. It is
// mutually exclusive with [WithPlainAuth], [WithPlainClient],
// [WithCurveServer], and [WithCurveClient].
//
// Reach for this only when the shipped mechanisms genuinely do not fit —
// a private protocol variant, or a fault-injecting test double. A
// [security.Mech] sits directly on the wire, so a mistake in it is a
// security bug rather than a correctness bug, and a mechanism gozmq does
// not ship will not interoperate with libzmq or any other ZMTP peer. If
// what you actually need is control over *which* peers are allowed rather
// than how the handshake works, use [WithZAPHandler] instead and keep the
// vetted cryptography.
//
// newMech is called once per session, with the session's role and with the
// local metadata gozmq advertises in READY (Socket-Type, and Identity when
// [WithIdentity] is set). The mechanism must include those properties in
// its own Metadata, and report the peer's through PeerMetadata once the
// handshake is done; the simplest way is to pass the map straight through to
// [security.NewMechBase], embed the result, and call its MergePeerMetadata.
// Returning an error fails the session — gozmq never falls back to a weaker
// mechanism.
//
// Each inbound handshake frame is capped at 64 KiB, whatever
// [WithMaxMessageSize] says, so an unauthenticated peer cannot make the
// socket allocate more. A socket with a custom mechanism never takes the
// inproc direct-pipe shortcut; every connection runs the handshake.
func WithSecurityMech(newMech func(role security.Role, meta map[string]string) (security.Mech, error)) Option {
	return newOption(func(c *socketConfig) error {
		if newMech == nil {
			return fmt.Errorf("gozmq: WithSecurityMech requires a non-nil constructor")
		}
		c.newSecurityMech = newMech
		return nil
	})
}

// WithHandshakeTimeout caps the total duration of the ZMTP handshake,
// covering the greeting exchange plus the security mechanism handshake
// (NULL, PLAIN, or CURVE) and the READY metadata exchange. Default is 30
// seconds. On timeout the session is torn down with [ErrHandshakeTimeout]
// and, on a client socket, the reconnector retries with the configured
// backoff.
func WithHandshakeTimeout(d time.Duration) Option {
	return newOption(func(c *socketConfig) error {
		c.handshakeTimeout = d
		c.handshakeTimeoutSet = true
		return nil
	})
}

// WithMonitor registers a channel to receive lifecycle [Event]s whose
// Type bit is set in mask. Use [EventAll] to receive every event kind.
// The channel must be buffered: events are delivered non-blocking, and
// when the channel is full the event is dropped rather than blocking
// the socket's hot path. A buffer of 64-256 events is recommended;
// consumers that lag materially are responsible for their own drops.
func WithMonitor(ch chan<- Event, mask EventType) Option {
	return newOption(func(c *socketConfig) error {
		c.monitor = ch
		c.monitorMask = mask
		return nil
	})
}

// WithLogger sets the [*slog.Logger] used for internal diagnostics
// emitted by this socket and its child sessions — handshake failures,
// reconnect attempts, heartbeat loss, and teardown anomalies. Default is
// [slog.Default]. Passing nil resets to the process default. This option
// is optional; it exists primarily so tests can capture structured log
// output without mutating the process-wide default logger.
func WithLogger(l *slog.Logger) Option {
	return newOption(func(c *socketConfig) error { c.logger = l; return nil })
}

func nonNeg[T int | int64 | time.Duration](name string, v T) error {
	if v < 0 {
		return fmt.Errorf("gozmq: %s requires n >= 0", name)
	}
	return nil
}

func validateSocketConfig(t SocketType, cfg *socketConfig) error {
	for _, check := range []error{
		nonNeg("WithSendHWM", cfg.sendHWM),
		nonNeg("WithRecvHWM", cfg.recvHWM),
		nonNeg("WithMaxMessageSize", cfg.maxMessageSize),
		nonNeg("WithHandshakeTimeout", cfg.handshakeTimeout),
		nonNeg("WithReconnectInterval", cfg.reconnectInterval),
		nonNeg("WithReconnectIntervalMax", cfg.reconnectIntervalMax),
		nonNeg("WithHeartbeat interval", cfg.heartbeatInterval),
		nonNeg("WithHeartbeat timeout", cfg.heartbeatTimeout),
	} {
		if check != nil {
			return check
		}
	}
	if len(cfg.identity) > 0 {
		if len(cfg.identity) > 255 {
			return fmt.Errorf("gozmq: WithIdentity requires 1..255 bytes")
		}
		if cfg.identity[0] == 0 {
			return fmt.Errorf("gozmq: WithIdentity requires a non-zero leading byte")
		}
	}
	// At most one mechanism per socket: otherwise newMechanism would silently
	// pick one (CURVE over PLAIN), or fail every session rather than NewSocket
	// (PLAIN server plus client).
	plainSet := cfg.plainAuth != nil || cfg.plainClient
	curveSet := len(cfg.curveSecretKey) > 0 || len(cfg.curvePublicKey) > 0 || len(cfg.curveServerKey) > 0 || cfg.curveServer
	switch {
	case cfg.newSecurityMech != nil && plainSet:
		return fmt.Errorf("gozmq: WithSecurityMech is mutually exclusive with the PLAIN options")
	case cfg.newSecurityMech != nil && curveSet:
		return fmt.Errorf("gozmq: WithSecurityMech is mutually exclusive with the CURVE options")
	case cfg.plainAuth != nil && cfg.plainClient:
		return fmt.Errorf("gozmq: WithPlainAuth and WithPlainClient are mutually exclusive")
	case plainSet && curveSet:
		return fmt.Errorf("gozmq: the PLAIN and CURVE options are mutually exclusive")
	}
	if cfg.routerMandatory && t != Router {
		return fmt.Errorf("gozmq: WithRouterMandatory only valid on ROUTER sockets")
	}
	if cfg.xpubVerbose && t != XPub {
		return fmt.Errorf("gozmq: WithXPubVerbose only valid on XPUB sockets")
	}
	if len(cfg.subscriptions) > 0 && t != Sub && t != XSub {
		return fmt.Errorf("gozmq: WithSubscribe only valid on SUB/XSUB sockets")
	}
	if t == Stream {
		// STREAM bypasses ZMTP entirely: no handshake, no security, no
		// heartbeats, no identity exchange. Silently ignoring these
		// options would be a security hazard (e.g. WithPlainAuth on a
		// raw-TCP listener that authenticates nobody).
		switch {
		case cfg.newSecurityMech != nil:
			return fmt.Errorf("gozmq: WithSecurityMech is not applicable to STREAM sockets (no ZMTP handshake)")
		case plainSet:
			return fmt.Errorf("gozmq: PLAIN security options are not applicable to STREAM sockets (no ZMTP handshake)")
		case curveSet:
			return fmt.Errorf("gozmq: CURVE security options are not applicable to STREAM sockets (no ZMTP handshake)")
		case len(cfg.identity) > 0:
			return fmt.Errorf("gozmq: WithIdentity is not applicable to STREAM sockets (no ZMTP handshake)")
		case cfg.heartbeatSet:
			return fmt.Errorf("gozmq: WithHeartbeat is not applicable to STREAM sockets (no ZMTP heartbeats)")
		case cfg.handshakeTimeoutSet:
			return fmt.Errorf("gozmq: WithHandshakeTimeout is not applicable to STREAM sockets (no ZMTP handshake)")
		case cfg.zapDomain != "":
			return fmt.Errorf("gozmq: WithZAPDomain is not applicable to STREAM sockets (no ZMTP handshake)")
		}
	}
	if err := validateCurveKey("secret", cfg.curveSecretKey); err != nil {
		return err
	}
	if err := validateCurveKey("public", cfg.curvePublicKey); err != nil {
		return err
	}
	if err := validateCurveKey("server", cfg.curveServerKey); err != nil {
		return err
	}
	if cfg.curveServer {
		if len(cfg.curveSecretKey) != 32 {
			return fmt.Errorf("gozmq: WithCurveServer requires a 32-byte secret key")
		}
	} else if len(cfg.curveSecretKey) > 0 || len(cfg.curvePublicKey) > 0 || len(cfg.curveServerKey) > 0 {
		if len(cfg.curveSecretKey) != 32 || len(cfg.curvePublicKey) != 32 || len(cfg.curveServerKey) != 32 {
			return fmt.Errorf("gozmq: WithCurveClient requires 32-byte server, public, and secret keys")
		}
	}
	return nil
}

// validateCurveKey asserts that k is either empty (option not set) or
// exactly 32 bytes (a complete Curve25519 key). It returns a wrapped
// error naming which key role failed the check so callers can locate
// the misconfigured option.
func validateCurveKey(name string, k []byte) error {
	if len(k) == 0 || len(k) == 32 {
		return nil
	}
	return fmt.Errorf("gozmq: CURVE %s key must be 32 bytes, got %d", name, len(k))
}
