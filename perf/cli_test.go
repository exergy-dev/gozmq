package perf_test

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/internal/testutil"
)

// TestCLI runs each zmqperf pair end to end over a loopback port: the local
// side binds and the remote side drives a small count, and both must exit
// cleanly with the libzmq-style banner and summary lines (and, for the
// throughput pair, the requested profiles).
func TestCLI(t *testing.T) {
	bin := buildZmqperf(t)
	for _, tc := range []struct {
		local, remote string
		wantRemote    []string // in the remote's output
		wantLocal     []string // in the local's output
	}{
		{"local_lat", "remote_lat", []string{"message size: 32 [B]", "roundtrip count: 100", "average latency:", "roundtrip rate:"}, nil},
		{"local_thr", "remote_thr", nil, []string{"message size: 32 [B]", "message count: 100", "mean throughput:", "[msg/s]", "[Mb/s]"}},
	} {
		t.Run(tc.local, func(t *testing.T) {
			port := freeTCPPort(t)
			profiles := t.TempDir()
			args := func(side string, ep string) []string {
				a := []string{side, "-endpoint", ep, "-size", "32", "-count", "100"}
				if tc.local == "local_thr" {
					a = append(a, "-cpuprofile", filepath.Join(profiles, side+".cpu"), "-memprofile", filepath.Join(profiles, side+".mem"))
				}
				return a
			}
			localOut, remoteOut := &bytes.Buffer{}, &bytes.Buffer{}
			local := exec.Command(bin, args(tc.local, fmt.Sprintf("tcp://*:%d", port))...)
			local.Stdout, local.Stderr = localOut, localOut
			require.NoError(t, local.Start())
			localDone := testutil.Async(local.Wait)
			time.Sleep(100 * time.Millisecond) // let the bind land

			remote := exec.Command(bin, args(tc.remote, fmt.Sprintf("tcp://127.0.0.1:%d", port))...)
			remote.Stdout, remote.Stderr = remoteOut, remoteOut
			require.NoError(t, remote.Run(), "%s: %s", tc.remote, remoteOut)
			select {
			case err := <-localDone:
				require.NoError(t, err, "%s: %s", tc.local, localOut)
			case <-time.After(30 * time.Second):
				_ = local.Process.Kill()
				t.Fatalf("%s did not exit:\n%s", tc.local, localOut)
			}
			for _, want := range tc.wantRemote {
				require.Contains(t, remoteOut.String(), want)
			}
			for _, want := range tc.wantLocal {
				require.Contains(t, localOut.String(), want)
			}
			if tc.local == "local_thr" {
				for _, p := range []string{"local_thr.cpu", "local_thr.mem", "remote_thr.cpu", "remote_thr.mem"} {
					info, err := os.Stat(filepath.Join(profiles, p))
					require.NoError(t, err, "profile %s", p)
					require.NotZero(t, info.Size(), "profile %s is empty", p)
				}
			}
		})
	}
}

// TestCLIPatterns runs every -pattern of zmqperf once with gozmq on both
// sides: each throughput pattern two-process over tcp and in one process
// over inproc where it exists, and each latency pattern the same way, plus
// one CURVE and one fan-out run. compare.sh drives the same commands with
// zmqperf.c on either side; this keeps the Go half of that contract tested.
func TestCLIPatterns(t *testing.T) {
	bin := buildZmqperf(t)
	type run struct {
		kind, pattern string
		extra         []string
		inproc        bool
	}
	runs := []run{
		{"thr", "push_pull", nil, true},
		{"thr", "pub_sub", []string{"-wait", "200ms"}, true},
		{"thr", "pub_sub", []string{"-wait", "200ms", "-prefix", "aaaa"}, false},
		{"thr", "xpub_xsub", nil, true},
		{"thr", "pair", nil, true},
		{"thr", "dealer_router", nil, true},
		{"thr", "router_dealer", nil, true},
		{"thr", "stream_in", nil, false},
		{"thr", "stream_out", nil, false},
		{"thr", "push_pull", []string{"-curve"}, false},
		{"lat", "req_rep", nil, true},
		{"lat", "req_router", nil, true},
		{"lat", "dealer_rep", nil, true},
		{"lat", "dealer_router", nil, true},
		{"lat", "pair", nil, true},
		{"lat", "req_rep", []string{"-curve"}, false},
	}
	for _, r := range runs {
		name := r.kind + "/" + r.pattern
		if len(r.extra) > 0 {
			name += "/" + r.extra[len(r.extra)-1]
		}
		t.Run(name, func(t *testing.T) {
			port := freeTCPPort(t)
			common := append([]string{"-pattern", r.pattern, "-size", "64", "-count", "200"}, r.extra...)
			local, remote := exec.Command(bin, append([]string{"local_" + r.kind, "-endpoint", fmt.Sprintf("tcp://127.0.0.1:%d", port)}, common...)...),
				exec.Command(bin, append([]string{"remote_" + r.kind, "-endpoint", fmt.Sprintf("tcp://127.0.0.1:%d", port)}, common...)...)
			localOut, remoteOut := &bytes.Buffer{}, &bytes.Buffer{}
			local.Stdout, local.Stderr = localOut, localOut
			remote.Stdout, remote.Stderr = remoteOut, remoteOut
			require.NoError(t, local.Start())
			localDone := testutil.Async(local.Wait)
			time.Sleep(100 * time.Millisecond)
			require.NoError(t, remote.Run(), "remote: %s", remoteOut)
			select {
			case err := <-localDone:
				require.NoError(t, err, "local: %s", localOut)
			case <-time.After(30 * time.Second):
				_ = local.Process.Kill()
				t.Fatalf("local did not exit:\n%s", localOut)
			}
			want, out := "mean throughput:", localOut
			if r.kind == "lat" {
				want, out = "average latency:", remoteOut
			}
			require.Contains(t, out.String(), want)
			if r.kind == "thr" {
				require.Contains(t, out.String(), "received: 200\n")
			}
			if r.inproc {
				inOut, err := exec.Command(bin, append([]string{"inproc_" + r.kind}, common...)...).CombinedOutput()
				require.NoError(t, err, "inproc: %s", inOut)
				require.Contains(t, string(inOut), want)
			}
		})
	}
	t.Run("thr/pub_sub/fanout", func(t *testing.T) {
		out, err := exec.Command(bin, "inproc_thr", "-pattern", "pub_sub", "-subs", "3", "-size", "64", "-count", "200").CombinedOutput()
		require.NoError(t, err, "%s", out)
		require.Contains(t, string(out), "received: 200\n")
	})
}

// buildZmqperf builds perf/zmqperf into a temp dir with CGO off and returns
// the binary's path.
func buildZmqperf(t *testing.T) string {
	t.Helper()
	target := filepath.Join(t.TempDir(), "zmqperf")
	cmd := exec.Command("go", "build", "-o", target, "./perf/zmqperf")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	cmd.Dir = ".." // tests run in perf/
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build perf/zmqperf: %s", out)
	return target
}

// freeTCPPort asks the kernel for an unused port and releases it; the short
// window before the CLI binds is tolerated by the tests' 100 ms settle.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
