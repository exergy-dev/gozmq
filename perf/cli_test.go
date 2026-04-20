package perf_test

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestCLI_LocalLat_RemoteLat exercises the two latency-measurement binaries
// end-to-end: local_lat binds REP on a random port, remote_lat runs REQ
// against it for a small count. We assert both exit cleanly and that
// remote_lat emits the libzmq-style banner + summary lines.
//
// The binaries are built via `go build` to a temp dir at test-time; this
// exercises the real main packages (not just the import graph) without
// requiring the user to have run `go install` first.
func TestCLI_LocalLat_RemoteLat(t *testing.T) {
	bin := buildCLIs(t, "local_lat", "remote_lat")
	port := freeTCPPort(t)
	endpoint := fmt.Sprintf("tcp://127.0.0.1:%d", port)
	bindEndpoint := fmt.Sprintf("tcp://*:%d", port)

	// Server side: local_lat binds, echoes -count messages, exits.
	serverOut := &bytes.Buffer{}
	server := exec.Command(bin["local_lat"], "-endpoint", bindEndpoint, "-size", "32", "-count", "100")
	server.Stdout = serverOut
	server.Stderr = serverOut
	if err := server.Start(); err != nil {
		t.Fatalf("start local_lat: %v", err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Wait() }()

	// Give the bind a beat to land before we connect.
	time.Sleep(100 * time.Millisecond)

	clientOut := &bytes.Buffer{}
	client := exec.Command(bin["remote_lat"], "-endpoint", endpoint, "-size", "32", "-count", "100")
	client.Stdout = clientOut
	client.Stderr = clientOut
	if err := client.Run(); err != nil {
		t.Fatalf("remote_lat failed: %v\noutput:\n%s", err, clientOut.String())
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("local_lat exit: %v\noutput:\n%s", err, serverOut.String())
		}
	case <-time.After(10 * time.Second):
		_ = server.Process.Kill()
		t.Fatalf("local_lat did not exit within 10s\noutput:\n%s", serverOut.String())
	}

	out := clientOut.String()
	for _, want := range []string{
		"message size: 32 [B]",
		"roundtrip count: 100",
		"average latency:",
		"roundtrip rate:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("remote_lat output missing %q:\n%s", want, out)
		}
	}
}

// TestCLI_LocalThr_RemoteThr exercises the throughput binaries end-to-end.
// local_thr binds PULL, remote_thr connects PUSH and blasts -count
// messages. local_thr reports mean throughput; remote_thr exits silently.
func TestCLI_LocalThr_RemoteThr(t *testing.T) {
	bin := buildCLIs(t, "local_thr", "remote_thr")
	profiles := t.TempDir()
	port := freeTCPPort(t)
	endpoint := fmt.Sprintf("tcp://127.0.0.1:%d", port)
	bindEndpoint := fmt.Sprintf("tcp://*:%d", port)
	localCPU := filepath.Join(profiles, "local.cpu")
	localMem := filepath.Join(profiles, "local.mem")
	remoteCPU := filepath.Join(profiles, "remote.cpu")
	remoteMem := filepath.Join(profiles, "remote.mem")

	serverOut := &bytes.Buffer{}
	server := exec.Command(bin["local_thr"],
		"-endpoint", bindEndpoint,
		"-size", "32",
		"-count", "100",
		"-cpuprofile", localCPU,
		"-memprofile", localMem,
	)
	server.Stdout = serverOut
	server.Stderr = serverOut
	if err := server.Start(); err != nil {
		t.Fatalf("start local_thr: %v", err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Wait() }()

	time.Sleep(100 * time.Millisecond)

	clientOut := &bytes.Buffer{}
	client := exec.Command(bin["remote_thr"],
		"-endpoint", endpoint,
		"-size", "32",
		"-count", "100",
		"-cpuprofile", remoteCPU,
		"-memprofile", remoteMem,
	)
	client.Stdout = clientOut
	client.Stderr = clientOut
	if err := client.Run(); err != nil {
		t.Fatalf("remote_thr failed: %v\noutput:\n%s", err, clientOut.String())
	}
	t.Logf("remote_thr output:\n%s", clientOut.String())

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("local_thr exit: %v\noutput:\n%s", err, serverOut.String())
		}
	case <-time.After(30 * time.Second):
		_ = server.Process.Kill()
		t.Fatalf("local_thr did not exit within 30s\noutput:\n%s", serverOut.String())
	}

	out := serverOut.String()
	for _, want := range []string{
		"message size: 32 [B]",
		"message count: 100",
		"mean throughput:",
		"[msg/s]",
		"[Mb/s]",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("local_thr output missing %q:\n%s", want, out)
		}
	}
	for _, path := range []string{localCPU, localMem, remoteCPU, remoteMem} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("profile %s not created: %v", path, err)
		}
		if info.Size() == 0 {
			t.Fatalf("profile %s is empty", path)
		}
	}
}

// buildCLIs builds each named binary in perf/<name>/ into a tempdir and
// returns {name: absolute path}. Uses `go build`; skips the test if the
// go toolchain is unavailable.
func buildCLIs(t *testing.T, names ...string) map[string]string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go toolchain not available: %v", err)
	}
	dir := t.TempDir()
	out := make(map[string]string, len(names))
	for _, n := range names {
		target := filepath.Join(dir, n)
		cmd := exec.Command(goBin, "build", "-o", target, "./perf/"+n)
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		// Run from the module root. The tests run from the perf/ dir by
		// default, so step up one level to find ./perf/<name>.
		cmd.Dir = ".."
		buf := &bytes.Buffer{}
		cmd.Stdout = buf
		cmd.Stderr = buf
		if err := cmd.Run(); err != nil {
			t.Fatalf("go build perf/%s: %v\n%s", n, err, buf.String())
		}
		out[n] = target
	}
	return out
}

// freeTCPPort returns a currently-unused ephemeral TCP port by asking the
// kernel and immediately closing the listener. There's a tiny race window
// between this call and the CLI binding; the surrounding tests tolerate
// that via retries in the 100ms sleep. For a two-process loopback test
// this is the standard idiom.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
