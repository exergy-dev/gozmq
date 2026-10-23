#!/usr/bin/env bash
# fuzz.sh <fuzztime>: run every Fuzz* target in the module for <fuzztime>.
set -euo pipefail
for pkg in $(go list ./...); do
	for t in $(go test -list '^Fuzz' "$pkg" 2>/dev/null | grep '^Fuzz' || true); do
		echo "::group::$pkg $t"
		go test -run '^$' -fuzz "^${t}$" -fuzztime="$1" "$pkg"
		echo "::endgroup::"
	done
done
