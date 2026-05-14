//go:build integration

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

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// CLI invokes the upstream `linstor` Python client against the
// blockstor REST URL. We intentionally exec the real CLI binary
// instead of mocking it — the Tier 2 contract pins the wire shape
// blockstor returns, and the cheapest way to catch regressions
// against the actual Python parser (which has crashed us before
// with `xml.etree.ElementTree.ParseError` etc.) is to make it
// parse our responses for real.
type CLI struct {
	// URL is the controllers base URL — e.g. http://127.0.0.1:NNNN.
	URL string

	// Binary, if non-empty, overrides the binary name `linstor`.
	// Tests should rarely set this; CI installs the canonical
	// `linstor-client` Debian package.
	Binary string

	// Timeout caps each invocation. Defaults to 30s if zero.
	Timeout time.Duration
}

// ErrLinstorBinaryMissing is returned (via t.Fatal) when no
// `linstor` binary is on PATH. Carries an actionable message so
// the operator running `go test` locally without the CLI knows
// what to install.
var ErrLinstorBinaryMissing = errors.New("linstor binary not on PATH; install `linstor-client` (Debian) or skip with -short")

// Run invokes `linstor --controllers <url> --machine-readable
// <args...>` and returns stdout. The test fails on any of:
//   - non-zero exit
//   - python traceback in stderr (matched by the same pattern
//     `tests/e2e/client-compat.sh` uses)
//   - HTTPConnectionPool / xml ParseError fragments that the
//     Python json/REST layer emits when blockstor returns
//     something it can't decode (Bug-59 class).
//
// stderr is folded into the failure message so the operator sees
// what went wrong without re-running with -v.
func (c *CLI) Run(t *testing.T, args ...string) []byte {
	t.Helper()

	binary := c.binary()

	_, err := exec.LookPath(binary)
	if err != nil {
		t.Fatalf("%v (looked for %q): %v", ErrLinstorBinaryMissing, binary, err)
	}

	full := append([]string{"--controllers", c.URL, "--machine-readable"}, args...)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, binary, full...)

	var (
		stdout bytes.Buffer
		stderr bytes.Buffer
	)

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	checkFatalErrors(t, args, err, stderr.String())

	return stdout.Bytes()
}

// JSON runs the CLI and json.Unmarshals stdout into a flat list
// of objects. The `--machine-readable` envelope is actually
// nested: `[[obj, obj, ...]]` (outer array = "one response per
// command", inner array = the resource list itself). We flatten
// one level deep so tests get the natural `[]map` shape.
//
// For single-object endpoints (e.g. `controller version`) the CLI
// emits `[{...}]`; this helper detects that and returns the flat
// shape too.
func (c *CLI) JSON(t *testing.T, args ...string) []map[string]any {
	t.Helper()

	const logCap = 512

	out := c.Run(t, args...)

	// Try the most common case first: an array whose first element
	// is itself an array (the LINSTOR list-envelope).
	var nested [][]map[string]any

	errNested := json.Unmarshal(out, &nested)
	if errNested == nil {
		flat := make([]map[string]any, 0)
		for _, sub := range nested {
			flat = append(flat, sub...)
		}

		return flat
	}

	// Fall back to the flat shape: `[obj, obj, ...]`.
	var flat []map[string]any

	errFlat := json.Unmarshal(out, &flat)
	if errFlat == nil {
		return flat
	}

	t.Fatalf("linstor %s: unmarshal JSON (len=%d) into either [[obj...]] or [obj...]: stdout: %s",
		strings.Join(args, " "), len(out), truncateForLog(out, logCap))

	return nil
}

func (c *CLI) binary() string {
	if c.Binary != "" {
		return c.Binary
	}

	return "linstor"
}

func (c *CLI) timeout() time.Duration {
	const defaultTimeout = 30 * time.Second

	if c.Timeout > 0 {
		return c.Timeout
	}

	return defaultTimeout
}

// checkFatalErrors centralises the failure-mode classifier the
// Run wrapper applies. Pulled out so the hot path stays readable
// and so we can extend it later with new "this means linstor is
// broken" patterns without churning Run's signature.
func checkFatalErrors(t *testing.T, args []string, err error, stderrText string) {
	t.Helper()

	// Patterns lifted from tests/e2e/client-compat.sh — the
	// historical "linstor exited 0 but blew up the parser" class.
	fatalPatterns := []string{
		"Traceback (most recent call last)",
		"xml.etree.ElementTree.ParseError",
		"HTTPConnectionPool",
		"json.decoder.JSONDecodeError",
	}

	for _, p := range fatalPatterns {
		if strings.Contains(stderrText, p) {
			t.Fatalf("linstor %s: stderr contains %q\nstderr: %s",
				strings.Join(args, " "), p, stderrText)
		}
	}

	if err != nil {
		t.Fatalf("linstor %s: %v\nstderr: %s",
			strings.Join(args, " "), err, stderrText)
	}
}

// truncateForLog clamps the dumped bytes so a runaway HTML error
// page doesn't blow up CI logs.
func truncateForLog(buf []byte, limit int) string {
	if len(buf) <= limit {
		return string(buf)
	}

	return string(buf[:limit]) + "...[truncated]"
}
