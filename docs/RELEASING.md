# Releasing

`gozmq` follows [Semantic Versioning](https://semver.org/). After v1.0.0 the
compatibility promise covers everything exported outside `internal/`.

Go modules make a tag permanent — the proxy caches it and it cannot be
withdrawn. Work the checklist.

## Pre-flight

Everything green on the release commit:

```sh
go mod tidy && git diff --exit-code go.mod go.sum
go build ./... && CGO_ENABLED=0 go build ./...
go vet ./...
golangci-lint run
gofmt -l .                                        # must print nothing
go test -race -count=1 ./...
go test -tags interop -race ./interop/...         # needs libzmq3-dev
GOZMQ_SOAK_DURATION=10m go test -tags soak -run TestSoak ./soak/...
```

Confirm the last **nightly** run passed — the long fuzz and repeated race
passes only run there, and they are the checks most likely to catch what a
single PR run misses.

Cross-compilation, since the pure-Go claim is the headline feature:

```sh
for target in linux/386 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 freebsd/amd64; do
  GOOS=${target%/*} GOARCH=${target#*/} CGO_ENABLED=0 go build ./... \
    || echo "FAILED: $target"
done
```

## Public API review

No exported signature anywhere outside `internal/` may name an internal
type. This must print nothing:

```sh
for p in . ./msg ./transport ./typed ./typed/codecjson ./security ./security/curve; do
  go doc -all "$p" | grep -E "^(func|type|const|var| ) .*\b(pipe|session|socket|zmqerr|zmtp|bufpool)\."
done
```

Then read the diff of the public surface against the previous tag and
confirm it matches the intended version bump:

```sh
go doc -all ./... > /tmp/api-new.txt
git stash && go doc -all ./... > /tmp/api-old.txt && git stash pop
diff /tmp/api-old.txt /tmp/api-new.txt
```

Anything removed or changed in place is a **major** bump. Additions are a
**minor**. Nothing but fixes is a **patch**.

## Documentation

- Roll `CHANGELOG.md`: rename `[Unreleased]` to `[X.Y.Z] - YYYY-MM-DD` and
  open a fresh empty `[Unreleased]`.
- Every breaking change listed, with before/after.
- `docs/COMPATIBILITY.md` matches shipped behaviour, including any new
  divergence.
- `README.md` version references and performance numbers are current.
- Every example still runs: `for d in examples/*/; do go run "./$d" || break; done`
  (some are interactive — check the output, do not just check exit codes).

## Tag and publish

```sh
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

Warm the module proxy and confirm the tag resolves:

```sh
GOPROXY=proxy.golang.org go list -m github.com/exergy-dev/gozmq@vX.Y.Z
```

Check that pkg.go.dev has rendered the new version, and specifically that
`Msg`, `SocketMetrics`, `transport.Transport`, and `security.Mech` all show
their full method sets and fields — the whole point of the pre-1.0 API work
was that none of them render as bare aliases to internal types.

Then write the GitHub release notes from the CHANGELOG section.

## After the release

- Confirm `go get github.com/exergy-dev/gozmq@latest` resolves to the new
  tag from a clean module cache.
- If a released tag turns out to be broken, do **not** delete it — the proxy
  has it forever. Use `retract` in `go.mod` and ship the fix as the next
  patch.
