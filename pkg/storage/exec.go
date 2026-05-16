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

package storage

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
)

// Exec is a context-aware shell-out abstraction. Production uses
// RealExec which wraps os/exec; tests substitute FakeExec to assert what
// was called and inject canned output without root.
type Exec interface {
	// Run invokes name with the given args under the supplied context
	// and returns combined stdout+stderr. Non-zero exit codes turn into
	// non-nil errors that wrap the original exec error.
	Run(ctx context.Context, name string, args ...string) ([]byte, error)

	// RunWithStdin is Run plus a stdin reader. Required for callers
	// (notably pkg/luks) that must keep secrets out of argv and off
	// any `sh -c` pipeline — the previous LUKS path leaked the
	// passphrase via a `printf %q | cryptsetup …` shell expression
	// (Bug 175, P1). A nil stdin is equivalent to plain Run.
	RunWithStdin(ctx context.Context, stdin io.Reader,
		name string, args ...string) ([]byte, error)
}

// RealExec is the production implementation backed by os/exec.
type RealExec struct{}

// Run executes name with args, capturing stdout. Stderr is folded into
// the returned error so callers can include it in surfaced diagnostics
// without losing the original error chain.
func (e RealExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return e.RunWithStdin(ctx, nil, name, args...)
}

// RunWithStdin is Run plus an optional stdin reader. Used by callers
// that must keep secrets off argv (pkg/luks, Bug 175): cryptsetup
// reads the keyfile from stdin via `--key-file -` rather than
// composing a `sh -c 'printf %q | …'` pipeline whose `%q` form does
// not escape shell command substitution.
func (RealExec) RunWithStdin(
	ctx context.Context,
	stdin io.Reader,
	name string,
	args ...string,
) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)

	var (
		stdout bytes.Buffer
		stderr bytes.Buffer
	)

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if stdin != nil {
		cmd.Stdin = stdin
	}

	err := cmd.Run()
	if err != nil {
		return stdout.Bytes(), errors.Wrapf(err, "%s %s: %s",
			name, strings.Join(args, " "), strings.TrimSpace(stderr.String()))
	}

	return stdout.Bytes(), nil
}

// FakeExec is the test implementation. Programmes can pre-register
// canned responses via Expect and inspect Calls afterwards.
type FakeExec struct {
	mu sync.Mutex

	// Calls records every Run / RunWithStdin invocation in order.
	Calls []FakeCall

	// Stdins parallels Calls: Stdins[i] is the string read from the
	// stdin reader supplied to RunWithStdin for Calls[i], or empty
	// if Run (no stdin) was used. Tests use this to assert secrets
	// land on stdin instead of argv (Bug 175 LUKS hardening).
	Stdins []string

	// Responses maps "name arg1 arg2 …" → canned output.
	// Falling back to empty stdout + nil error if absent.
	Responses map[string]FakeResponse
}

// FakeCall records one invocation of FakeExec.Run.
type FakeCall struct {
	Name string
	Args []string
}

// FakeResponse is the canned output for a Responses entry.
type FakeResponse struct {
	Stdout []byte
	Err    error
}

// NewFakeExec returns a FakeExec ready for use.
func NewFakeExec() *FakeExec {
	return &FakeExec{Responses: map[string]FakeResponse{}}
}

// Run looks up a pre-registered response (full command line) and falls
// back to an empty success if none was registered.
func (f *FakeExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return f.RunWithStdin(ctx, nil, name, args...)
}

// RunWithStdin records the call and the contents read from stdin (if
// any), looks up a canned response, and returns it. Stdin contents
// are buffered into Stdins[i] aligned to Calls[i] so secret-on-stdin
// tests (Bug 175 LUKS hardening) can assert delivery without secrets
// touching argv.
func (f *FakeExec) RunWithStdin(
	_ context.Context,
	stdin io.Reader,
	name string,
	args ...string,
) ([]byte, error) {
	var stdinStr string

	if stdin != nil {
		buf, err := io.ReadAll(stdin)
		if err != nil {
			return nil, errors.Wrap(err, "FakeExec: read stdin")
		}

		stdinStr = string(buf)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.Calls = append(f.Calls, FakeCall{Name: name, Args: append([]string(nil), args...)})
	f.Stdins = append(f.Stdins, stdinStr)

	key := name
	if len(args) > 0 {
		key += " " + strings.Join(args, " ")
	}

	if resp, ok := f.Responses[key]; ok {
		return resp.Stdout, resp.Err
	}

	return nil, nil
}

// StdinFor returns the stdin string recorded for the i-th call, or
// empty if i is out of range. Used by tests that need to assert what
// secret material reached cryptsetup via stdin.
func (f *FakeExec) StdinFor(i int) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	if i < 0 || i >= len(f.Stdins) {
		return ""
	}

	return f.Stdins[i]
}

// Expect registers a canned response. Match is exact on the command line.
func (f *FakeExec) Expect(cmdline string, resp FakeResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.Responses[cmdline] = resp
}

// CommandLines returns the recorded calls as space-joined command lines —
// convenient for ContainsAll-style assertions in tests.
func (f *FakeExec) CommandLines() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.Calls))
	for i := range f.Calls {
		line := f.Calls[i].Name
		if len(f.Calls[i].Args) > 0 {
			line += " " + strings.Join(f.Calls[i].Args, " ")
		}

		out = append(out, line)
	}

	return out
}

// Reset clears recorded calls but keeps registered responses. Useful in
// table-driven tests where each row reuses the same FakeExec.
func (f *FakeExec) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.Calls = nil
	f.Stdins = nil
}
