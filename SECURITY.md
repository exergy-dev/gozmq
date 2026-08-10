# Security policy

`gozmq` implements the ZMTP 3.1 wire protocol along with the PLAIN, CURVE,
and ZAP security mechanisms, and it parses untrusted input from the network
by design. Vulnerability reports are taken seriously.

## Reporting a vulnerability

**Do not open a public issue for a security problem.**

Report privately through GitHub's
[private vulnerability reporting](https://github.com/exergy-dev/gozmq/security/advisories/new)
for this repository. That opens a draft advisory visible only to you and the
maintainers.

Please include:

- the affected version or commit;
- the socket pattern, transport, and security mechanism involved;
- what an attacker can achieve (read plaintext, bypass authentication, crash
  a peer, exhaust memory, …), and what position they need to be in
  (on-path, a connecting peer, a subscriber);
- a reproducer if you have one — a Go test or a captured byte sequence is
  ideal, since most of this surface is reachable from a raw socket.

You should get an acknowledgement within **3 business days** and an initial
assessment within **10 business days**. If a fix is warranted we will agree a
disclosure date with you; the default is to publish the advisory and the
patched release together.

## Supported versions

| Version | Supported |
| --- | :---: |
| 1.x (latest minor) | ✅ |
| 1.x (older minors) | ❌ |
| Pre-1.0 tags | ❌ |

Security fixes land on the latest minor. There is no long-term-support
branch; upgrading within v1 is a non-breaking change by the compatibility
promise.

## Scope

In scope — please report:

- Anything that lets a peer read or forge traffic it should not: a CURVE
  handshake flaw, nonce reuse, a MAC that is not actually verified.
- Authentication or authorization bypass in PLAIN, CURVE, or the ZAP path.
- Memory exhaustion or panics reachable from a remote peer, including
  malformed greetings, commands, frames, or metadata. Frame-size limits that
  fail to bound allocation count here.
- Anything that causes plaintext to be sent on a socket configured for
  CURVE, or a silent downgrade from a configured mechanism to a weaker one.
- TLS transport misconfiguration that weakens the connection below what the
  supplied `*tls.Config` asks for.

Out of scope — these are documented behaviours, not vulnerabilities:

- **NULL is the default mechanism.** An unauthenticated, unencrypted socket
  on an untrusted network is a deployment choice; see the `Security` section
  of the package documentation.
- **PLAIN sends credentials in cleartext.** This is what RFC 24 specifies.
  Use it under `tls+tcp://`, or use CURVE.
- **inproc bypasses ZAP.** Direct-pipe connections between NULL sockets in
  one Context skip the session layer and therefore the ZAP hook, matching
  libzmq. Documented in `docs/COMPATIBILITY.md`.
- **STREAM performs no handshake and no security.** It is a raw TCP bridge
  by definition.
- **A custom `security.Mech` installed via `WithSecurityMech`.** You own the
  wire format of anything you implement yourself.
- Denial of service that requires an already-authenticated peer to
  misbehave, unless it costs the victim disproportionately more than the
  attacker.

## Hardening notes

For deployments on untrusted networks:

- Prefer **CURVE** over PLAIN. If you must use PLAIN, put it under
  `tls+tcp://`.
- Compare PLAIN credentials in constant time — `crypto/subtle.ConstantTimeCompare`,
  not `==`. The callback you pass to `WithPlainAuth` is your code.
- Keep `WithMaxMessageSize` at a finite value. Setting it to 0 removes the
  ceiling and lets a hostile sender force unbounded allocation.
- Keep heartbeats enabled on long-lived connections; they are the only
  reliable way to detect a half-open TCP connection.
- Set `WithHandshakeTimeout` to something tight if you are exposed to the
  open internet, so half-finished handshakes cannot pin resources.
