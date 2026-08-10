# Contributing to gozmq

Thanks for looking. This is a wire-protocol library, so the bar for changes
is mostly about not breaking interoperability — the rest is ordinary Go.

## Getting set up

Go 1.22 or newer. Everything except the interop harness is pure Go with no
external dependencies:

```sh
git clone https://github.com/exergy-dev/gozmq
cd gozmq
go build ./...
go test ./...
```

The interop suite talks to a real libzmq through cgo and is behind a build
tag, so it needs the library installed:

```sh
sudo apt-get install libzmq3-dev      # Debian/Ubuntu
brew install zeromq                   # macOS

go test -tags interop -race ./interop/...
```

Install the linter (CI runs the same config):

```sh
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
golangci-lint run
```

## Before opening a pull request

```sh
go build ./... && CGO_ENABLED=0 go build ./...   # cgo-free is the point
go vet ./...
golangci-lint run
go test -race -count=1 ./...
go test -tags interop -race ./interop/...        # if you touched the wire
```

## Things worth knowing

**CGO_ENABLED=0 must always build.** The entire reason this library exists is
to be a pure-Go ZeroMQ. The only cgo in the repository is the build-tagged
interop harness. A dependency that needs cgo is not acceptable.

**Wire changes need interop coverage.** If you touch framing, the greeting,
a command codec, or a security mechanism, add a case under `interop/` that
proves a real libzmq still understands us and we still understand it. Golden
byte vectors are better than round-trip-with-ourselves tests, which can pass
while both sides are wrong in the same way.

**Divergences from libzmq get documented.** If your change makes gozmq behave
differently from libzmq in any observable way, add a row to
`docs/COMPATIBILITY.md`. An undocumented divergence is a bug even when the
behaviour is the better one.

**Public API changes.** The v1 compatibility promise covers everything
exported outside `internal/`, with the documented exception of the plumbing
methods in the `msg` package. Additions are fine in a minor release;
anything breaking has to wait for v2. No exported signature may name a type
from `internal/` — the contracts live in `transport`, `security`, and `msg`
precisely so this stays true.

**Performance claims need numbers.** `perf/` has both a Go benchmark suite
and libzmq-comparable CLI tools. If a change is for speed, post before/after
`benchstat` output. Do **not** benchmark with `-race`; it multiplies
synchronisation cost by roughly 5× and makes the numbers meaningless.

**Goroutine leaks fail the build.** Every package's `TestMain` calls
`testutil.VerifyTestMain`, which runs goleak. If a new goroutine can outlive
`Socket.Close`, tests will say so.

**Long-horizon changes want a soak run.** If you touch teardown, reconnect,
or anything that allocates per connection:

```sh
GOZMQ_SOAK_DURATION=2m go test -tags soak -run TestSoak -v ./soak/...
```

Nightly CI runs this for ten minutes. It exists because it found a real
per-socket leak that the ordinary suite could not see.

## Commit messages and PRs

Conventional-commit prefixes (`fix:`, `feat:`, `perf:`, `docs:`, `refactor:`,
`build:`, `test:`); `!` for a breaking change. Explain *why* in the body — the
diff already shows what. Add a `CHANGELOG.md` entry under `[Unreleased]` for
anything user-visible.

## Reporting bugs

Include the socket pattern, transport, and security mechanism, plus a
reproducer if you can. If it involves a real libzmq peer, say which version.

Security issues go through `SECURITY.md`, not the public issue tracker.
