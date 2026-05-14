//go:build contract

/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Tier 3 contract-test harness — runs real drbd-utils binaries inside
// a thin Docker container against loopback files. See
// docs/test-strategy.md "Tier 3" section for the why.
//
// The harness is Linux-only by policy (per docs/test-strategy.md):
// even when macOS docker-desktop happens to run the commands, the
// production target is Linux and contract pins should match
// production. CI's ubuntu-latest runner is the contracted platform;
// tests skip on other GOOS values via t.Skip.

package contract

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

// ImageTag is the fixed Docker image tag the harness builds and runs.
// Hard-coded so a local re-run and CI hit the same artefact without
// extra plumbing.
const ImageTag = "blockstor-drbd-contract:local"

// LoopFileSize is the size in MiB of the loopback file fed to drbdmeta
// as the metadata device. 64 MiB is the smallest size that comfortably
// holds a v09 internal metadata block for a multi-peer resource
// without drbdmeta refusing on "data area too small".
const LoopFileSize = 64

var (
	buildOnce sync.Once
	buildErr  error
)

// SkipIfNotLinux short-circuits the test on non-Linux platforms (the
// macOS dev path). Contract tests need the Linux kernel's loopback
// driver inside the container.
func SkipIfNotLinux(t *testing.T) {
	t.Helper()

	if runtime.GOOS != "linux" {
		t.Skip("contract tests require Linux + docker (skipping on " + runtime.GOOS + ")")
	}
}

// EnsureImage builds the contract-test Docker image once per process.
// Subsequent callers reuse the cached result. The Dockerfile lives
// next to this file; we resolve its path at runtime via runtime.Caller
// so `go test` from any working directory finds it.
func EnsureImage(t *testing.T) {
	t.Helper()

	buildOnce.Do(func() {
		_, thisFile, _, ok := runtime.Caller(0)
		if !ok {
			buildErr = errors.New("contract: cannot resolve harness.go path via runtime.Caller")

			return
		}

		dir := filepath.Dir(thisFile)

		// `docker info` doubles as a "docker daemon reachable" probe;
		// a missing socket here surfaces a clear error rather than
		// the more cryptic "docker: command not found / cannot
		// connect" you get from `docker build` directly.
		if err := exec.Command("docker", "info").Run(); err != nil {
			buildErr = fmt.Errorf("contract: docker not available (%w)", err)

			return
		}

		cmd := exec.Command("docker", "build", "-t", ImageTag, "-f", filepath.Join(dir, "Dockerfile"), dir)
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("contract: docker build failed: %w\n%s", err, string(out))

			return
		}
	})

	if buildErr != nil {
		t.Fatalf("EnsureImage: %v", buildErr)
	}
}

// LoopDevicePath is the in-container path tests pass to drbdmeta as
// the metadata device. The harness creates a fresh 64-MiB regular
// file at this path inside the container before running each
// drbdmeta invocation; drbdmeta is happy with a regular file (it
// only needs seekable storage), which avoids losetup +
// --privileged. We don't bind-mount from the host because docker
// desktop on macOS converts file bind-mounts to directories, and
// even on Linux a container-local file is cheaper to allocate than
// a tmp file on the host fs.
const LoopDevicePath = "/tmp/loop0.img"

// RunResult captures the outcome of one docker run invocation.
type RunResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// RunDrbdmeta execs `drbdmeta <args...>` inside the contract-test
// container against a freshly-created 64-MiB regular file at
// LoopDevicePath. The file is allocated inside the container (no
// host bind-mount needed), then drbdmeta runs in the same shell
// invocation. Args should reference LoopDevicePath as the device
// parameter, e.g. `--force 0 v09 /tmp/loop0.img internal create-md 15`.
//
// Returns separate stdout/stderr/exit so the caller can assert each.
// Drbdmeta writes prompts to stdout and diagnostic / error messages
// to stderr; both matter for byte-shape regression pins. We pipe
// `yes` to stdin so the create-md confirmation prompt unblocks.
func RunDrbdmeta(t *testing.T, args ...string) RunResult {
	t.Helper()

	SkipIfNotLinux(t)
	EnsureImage(t)

	// dd allocates the loopback file inside the container, then we
	// chain through `yes | drbdmeta …` so the confirmation prompt
	// auto-answers. Container is --rm; one fresh allocation per
	// test by construction (each `docker run` is a new filesystem).
	script := "dd if=/dev/zero of=" + LoopDevicePath +
		" bs=1M count=" + itoaInternal(LoopFileSize) + " 2>/dev/null && " +
		"yes | drbdmeta " + joinShellArgs(args)

	dockerArgs := []string{
		"run", "--rm",
		ImageTag,
		"sh", "-c", script,
	}

	return runDocker(t, "", dockerArgs)
}

// itoaInternal is a tiny strconv.Itoa replacement; we avoid the
// extra import to keep this file dependency-minimal.
func itoaInternal(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[i:])
}

// joinShellArgs single-quote-escapes each argument for `sh -c`. The
// values we pass (paths, hex GIs, integers) never contain single
// quotes, but a strict quoter keeps drbdmeta's `:`-delimited GI
// tuples from being misparsed by the shell.
func joinShellArgs(args []string) string {
	var sb []byte

	for i, a := range args {
		if i > 0 {
			sb = append(sb, ' ')
		}

		sb = append(sb, '\'')

		for j := 0; j < len(a); j++ {
			if a[j] == '\'' {
				sb = append(sb, []byte(`'\''`)...)
			} else {
				sb = append(sb, a[j])
			}
		}

		sb = append(sb, '\'')
	}

	return string(sb)
}

// RunDrbdmetaChain executes several drbdmeta invocations inside ONE
// container against the SAME freshly-allocated loopback file. Tests
// that need a "create-md then set-gi" sequence (where set-gi must
// see the metadata create-md wrote) call this instead of RunDrbdmeta
// — separate containers would each get a fresh zero-filled file.
//
// Returns the result of the LAST invocation; earlier calls are
// setup. A non-zero exit from an earlier call still propagates
// because the script uses `&&` between commands — the final
// RunResult reflects "everything chained succeeded" or "the failing
// step (sh aborted there)".
func RunDrbdmetaChain(t *testing.T, calls [][]string) RunResult {
	t.Helper()

	SkipIfNotLinux(t)
	EnsureImage(t)

	if len(calls) == 0 {
		t.Fatalf("RunDrbdmetaChain: empty calls slice")
	}

	var script []byte

	script = append(script, []byte("dd if=/dev/zero of="+LoopDevicePath+
		" bs=1M count="+itoaInternal(LoopFileSize)+" 2>/dev/null")...)

	for _, call := range calls {
		script = append(script, []byte(" && yes | drbdmeta ")...)
		script = append(script, []byte(joinShellArgs(call))...)
	}

	dockerArgs := []string{
		"run", "--rm",
		ImageTag,
		"sh", "-c", string(script),
	}

	return runDocker(t, "", dockerArgs)
}

// RunDrbdadm execs `drbdadm <args...>` inside the contract-test
// container with the supplied .res file mounted at
// /etc/drbd.d/<basename>.res. The resource name passed to drbdadm
// should match the .res file's `resource <name> { ... }` header.
// hostname is forwarded to docker as --hostname so drbdadm's
// "which `on <node>` block is mine?" logic resolves to a real entry
// in the .res — otherwise drbdadm errors with "not defined in your
// config (for this host)" because the random container hostname
// doesn't match any of the resource's hosts.
//
// drbdadm's `dump` subcommand parses the file and re-emits it on
// stdout; exit 0 means our ConfFileBuilder output is syntactically
// valid drbd-9. No loopback file is needed for the parser pass.
func RunDrbdadm(t *testing.T, resFile, hostname string, args ...string) RunResult {
	t.Helper()

	SkipIfNotLinux(t)
	EnsureImage(t)

	abs, err := filepath.Abs(resFile)
	if err != nil {
		t.Fatalf("resolve res file: %v", err)
	}

	dockerArgs := []string{
		"run", "--rm",
		"--hostname", hostname,
		"-v", abs + ":/etc/drbd.d/" + filepath.Base(abs),
		ImageTag,
		"drbdadm",
	}
	dockerArgs = append(dockerArgs, args...)

	return runDocker(t, "", dockerArgs)
}

// RunDocker is the single docker-shellout site so stdin / stdout /
// stderr / exit-code wiring is identical across both subcommand
// helpers above. Exported so tests that need to chain multiple
// drbd-utils calls inside ONE container (e.g. create-md + set-gi
// against the same loopback) can build their own `docker run` args
// without re-implementing the exec plumbing.
//
// args is the FULL argv after `docker` — typically starting with
// `run --rm -i -v <file>:/dev/loop0 blockstor-drbd-contract:local
// sh -c "<chained-commands>"`. stdin is fed in unchanged; pass ""
// for no stdin.
func RunDocker(t *testing.T, stdin string, args []string) RunResult {
	t.Helper()

	return runDocker(t, stdin, args)
}

// runDocker is the unexported worker used by both the high-level
// helpers (RunDrbdmeta / RunDrbdadm) and the exported RunDocker.
func runDocker(t *testing.T, stdin string, args []string) RunResult {
	t.Helper()

	var stdout, stderr bytes.Buffer

	cmd := exec.Command("docker", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if stdin != "" {
		cmd.Stdin = bytes.NewBufferString(stdin + "\n")
	} else {
		cmd.Stdin = io.LimitReader(bytes.NewReader(nil), 0)
	}

	err := cmd.Run()

	exitCode := 0

	var ee *exec.ExitError
	if errors.As(err, &ee) {
		exitCode = ee.ExitCode()
	} else if err != nil {
		// Non-exit error (e.g. docker binary missing / IO error) —
		// surface immediately; the contract pin is meaningless if
		// docker itself misbehaved.
		t.Fatalf("docker run: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}

	return RunResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}
}
