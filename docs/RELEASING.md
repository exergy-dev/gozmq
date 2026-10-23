# Releasing

`gozmq` follows [Semantic Versioning](https://semver.org/). After v1.0.0 the
compatibility promise covers everything exported outside `internal/`.

Go modules make a tag permanent — the proxy caches it and it cannot be
withdrawn. Work the checklist.

## Pre-flight

Run the pre-PR checklist in `CONTRIBUTING.md` on the release commit, plus:

```sh
GOZMQ_SOAK_DURATION=10m go test -tags soak -run TestSoak -timeout 60m ./soak/...   # three tests, 10m each
for target in linux/386 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 freebsd/amd64; do
  GOOS=${target%/*} GOARCH=${target#*/} CGO_ENABLED=0 go build ./... || echo "FAILED: $target"
done
```

If the release contains performance work, run `perf/benchgate.sh vPREVIOUS`
and record the result in `perf/REPORT.md`. Confirm the last nightly run
passed: the long fuzz and repeated race passes only run there.

## Public API review

No exported signature anywhere outside `internal/` may name an internal
type. This must print nothing (the error sentinels are `var X = zmqerr.X`
aliases by design and are excluded):

```sh
for p in . ./msg ./transport ./typed ./typed/codecjson ./security ./security/curve; do
  go doc -all "$p" | grep -E "^\s*(func|type|const|var)?\s*[A-Z][A-Za-z0-9_]*(\(|\[| ).*\b(pipe|session|socket|zmqerr|zmtp|bufpool|testutil)\.[A-Z]" \
    | grep -v "= zmqerr\."
done
```

Then read the diff of the public surface against the previous tag and
confirm it matches the intended version bump:

```sh
git worktree add /tmp/gozmq-prev vPREVIOUS
for p in . ./msg ./transport ./typed ./typed/codecjson ./security ./security/curve; do
  diff <(cd /tmp/gozmq-prev && go doc -all "$p") <(go doc -all "$p")
done
git worktree remove /tmp/gozmq-prev
```

(`apidiff` from `golang.org/x/exp/cmd/apidiff` gives the same answer with a
compatible/incompatible verdict per change.)

Anything removed or changed in place is a **major** bump. Additions are a
**minor**. Nothing but fixes is a **patch**.

## Documentation

- Roll `CHANGELOG.md`: rename `[Unreleased]` to `[X.Y.Z] - YYYY-MM-DD` and
  open a fresh empty `[Unreleased]`.
- Every breaking change listed, with before/after, and summarised at the
  top of the section.
- `docs/COMPATIBILITY.md` matches shipped behaviour, including any new
  divergence.
- `README.md` version references are current.
- Every example still runs: `for d in examples/*/; do go run "./$d" || break; done`.

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
their full method sets and fields — the whole point of the earlier API cleanup
was that none of them render as bare aliases to internal types.

Then write the GitHub release notes from the CHANGELOG section.

## After the release

- Confirm `go get github.com/exergy-dev/gozmq@latest` resolves to the new
  tag from a clean module cache.
- If a released tag turns out to be broken, do **not** delete it — the proxy
  has it forever. Use `retract` in `go.mod` and ship the fix as the next
  patch.
