package perf_test

import (
	"testing"

	"github.com/exergy-dev/gozmq/internal/testutil"
)

// TestMain wires in goleak via the shared helper so every benchmark's
// setup/teardown is leak-checked alongside the rest of the module (design
// §10.5). Standalone main.go binaries under perf/local_lat, remote_lat,
// etc. run to completion and are exempt from goleak.
func TestMain(m *testing.M) { testutil.VerifyMain(m) }
