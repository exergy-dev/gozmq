#!/usr/bin/env bash
# benchgate.sh: compare benchmarks of the working tree against a base commit.
#
# Usage: perf/benchgate.sh <base-commit> [bench-regex] [count]
#
#   bench-regex  -test.bench regex for ./perf (default '.')
#   count        interleaved runs per side (default 10)
#   ROOT_RE      env: -test.bench regex for the root package (default
#                'Poller'); set it empty to skip the root package
#
# The base commit is checked out in a temporary git worktree. Test binaries
# for ./perf and the root package are built from base and from the current
# working tree (uncommitted changes included), then run interleaved (base,
# candidate, base, ...) with GOMAXPROCS=8, -benchmem and -benchtime 1s, and
# compared with benchstat.
#
# Pass rule for a candidate:
#   - no statistically significant (p<0.05) sec/op regression above 5%;
#   - allocs/op never higher;
#   - B/op within 5%.
# Re-run any flagged benchmark with count 16 before calling it a regression:
#   perf/benchgate.sh <base-commit> '<flagged regex>' 16
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
	echo "usage: $0 <base-commit> [bench-regex] [count]" >&2
	exit 2
fi
BASE=$1
RE=${2:-.}
COUNT=${3:-10}
ROOT_RE=${ROOT_RE-Poller}

BENCHSTAT=$(command -v benchstat || true)
if [[ -z $BENCHSTAT && -x $HOME/go/bin/benchstat ]]; then
	BENCHSTAT=$HOME/go/bin/benchstat
fi
if [[ -z $BENCHSTAT ]]; then
	echo "benchstat not found (go install golang.org/x/perf/cmd/benchstat@latest)" >&2
	exit 1
fi

REPO=$(git rev-parse --show-toplevel)
BASE_SHA=$(git -C "$REPO" rev-parse --verify "$BASE^{commit}")
TMP=$(mktemp -d)
BASE_TREE=$TMP/base

cleanup() {
	git -C "$REPO" worktree remove --force "$BASE_TREE" >/dev/null 2>&1 || true
	rm -rf "$TMP"
}
trap cleanup EXIT

git -C "$REPO" worktree add --detach --quiet "$BASE_TREE" "$BASE_SHA"

# build <tree> <side>: compile both test binaries of one side into $TMP.
build() {
	echo "building $2 ($1)" >&2
	(cd "$1" && go test -c -o "$TMP/$2-perf.test" ./perf)
	if [[ -n $ROOT_RE ]]; then
		(cd "$1" && go test -c -o "$TMP/$2-root.test" .)
	fi
}
build "$BASE_TREE" base
build "$REPO" candidate

# run <dir> <binary> <regex> <out>: one benchmark pass, appended to <out>.
run() {
	(cd "$1" && GOMAXPROCS=8 "$2" -test.run '^$' -test.bench "$3" \
		-test.benchmem -test.count 1 -test.benchtime 1s) >>"$4"
}

mkdir -p "$TMP/perf" "$TMP/root"
for ((i = 1; i <= COUNT; i++)); do
	echo "run $i/$COUNT" >&2
	run "$BASE_TREE/perf" "$TMP/base-perf.test" "$RE" "$TMP/perf/base"
	run "$REPO/perf" "$TMP/candidate-perf.test" "$RE" "$TMP/perf/candidate"
	if [[ -n $ROOT_RE ]]; then
		run "$BASE_TREE" "$TMP/base-root.test" "$ROOT_RE" "$TMP/root/base"
		run "$REPO" "$TMP/candidate-root.test" "$ROOT_RE" "$TMP/root/candidate"
	fi
done

echo "== ./perf  base=$BASE_SHA  candidate=working tree"
(cd "$TMP/perf" && "$BENCHSTAT" base candidate)
if [[ -n $ROOT_RE ]]; then
	echo
	echo "== root package  base=$BASE_SHA  candidate=working tree"
	(cd "$TMP/root" && "$BENCHSTAT" base candidate)
fi
