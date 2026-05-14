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

// Package safety_test runs source-level static-analysis regression
// guards. Each test greps the repo for a forbidden pattern and
// fails if the match-set wanders outside a small, audited allowlist.
//
// Why static analysis: the two patterns we guard here (force-stripping
// finalizers from reconciler code; controller-side `linstor node lost`)
// have caused real production damage in past sessions. Both leave no
// runtime signal — DRBD ports stay allocated in kernel, the satellite
// cleanly accepts the deletion — so we need a *compile-time-equivalent*
// gate. Unit-test pinning of every reconciler's behaviour would be
// too brittle; a one-line grep is cheap and catches every new
// reconciler the moment it lands.
//
// References:
//   - tests/scenarios/05-drbd-state-recovery.md §5.27 (finalizer strip)
//   - tests/scenarios/05-drbd-state-recovery.md §5.28 (NodeLost)
package safety_test

import (
	"bufio"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// allowedFinalizerStripFiles lists the production files that are
// legitimately allowed to remove finalizer entries. Each entry is a
// repo-relative path (forward slashes); the test fails if any other
// file matches the forbidden pattern. Audit before adding entries.
//
// All four listed sites delete only the caller's OWN finalizer key
// (via `slices.DeleteFunc(... == OurFinalizerConst)`) after a
// successful local teardown — never blanket-strip the slice. That is
// the contract; force-strip is the bug.
func allowedFinalizerStripFiles() map[string]string {
	return map[string]string{
		"internal/controller/resource_controller.go":  "legacy controller-side finalizer cleanup on rolling upgrade",
		"pkg/satellite/controllers/resource.go":       "satellite teardown completion (own finalizer)",
		"pkg/satellite/controllers/storagepool.go":    "storage-pool deregistration (own finalizer)",
		"pkg/satellite/controllers/physicaldevice.go": "physical-device attach cleanup (own finalizer)",
		"pkg/satellite/controllers/snapshot.go":       "snapshot teardown completion (own finalizer, Bug 64)",
	}
}

// allowedNodeLostFiles lists the production files that are
// legitimately allowed to reference `linstor node lost` semantics.
// The only legitimate match is the REST handler — `node lost` is an
// explicit operator-mediated action (it drops the Node CRD and all
// its Resources irreversibly). Any controller-side reference would
// mean a reconciler / heartbeat watchdog could fire it on its own,
// which is exactly the "never auto-lose" guarantee this rail
// protects.
func allowedNodeLostFiles() map[string]string {
	return map[string]string{
		"pkg/rest/node_lifecycle.go": "explicit operator-invoked REST handler",
	}
}

// TestNoForceStripFinalizers scans the production tree for patterns
// that would strip finalizers wholesale (`Finalizers = nil`,
// `Finalizers = []string{}`, `metadata.finalizers: []`, …).
// Test fixtures setting up Finalizers as part of an object literal
// are excluded by the `_test.go` filter (grep only inspects the
// non-test tree). Any match outside `allowedFinalizerStripFiles` is
// a regression of this session's force-strip incident — DRBD kernel
// state survives the CRD deletion, ports stay reserved, and the
// next placement collides.
//
// Scenario 5.27.
func TestNoForceStripFinalizers(t *testing.T) {
	root := repoRoot(t)

	// Patterns: literal `Finalizers = nil`, `Finalizers = []string{}`,
	// `SetFinalizers(nil)`, and `slices.DeleteFunc(...Finalizers...)` —
	// the last is allowed only when removing a single known key (the
	// allowlist sites). To keep the grep simple, we surface every
	// occurrence and rely on the allowlist for whitelisting.
	pattern := `(Finalizers\s*=\s*nil|Finalizers\s*=\s*\[\]string\{\}|SetFinalizers\(nil\)|slices\.DeleteFunc\([^)]*Finalizers)`

	matches := gitGrep(t, root, pattern,
		"--", "pkg/", "cmd/", "internal/",
		":(exclude)*_test.go",
		":(exclude)tests/safety/*.go",
	)

	allowed := allowedFinalizerStripFiles()

	var violations []string

	for _, m := range matches {
		if _, ok := allowed[m.file]; ok {
			continue
		}

		violations = append(violations,
			m.file+":"+m.line+" — "+m.text)
	}

	if len(violations) > 0 {
		t.Fatalf("force-strip-finalizers regression — unauthorised matches outside the audited allowlist:\n  %s\n\n"+
			"If a new site legitimately needs to remove its OWN finalizer key (never blanket-strip!), add it to\n"+
			"allowedFinalizerStripFiles in this file along with a one-line justification.",
			strings.Join(violations, "\n  "))
	}
}

// TestNoControllerSideNodeLost scans cmd/controller, cmd/apiserver,
// pkg/satellite, and internal/controller for references to
// `NodeLost`, `node.lost`, `node_lost`, or `/v1/nodes/.../lost`.
// Anything in those trees is a controller-side path that could
// auto-fire the destructive lost-node flow. Operator intent only.
//
// Scenario 5.28.
func TestNoControllerSideNodeLost(t *testing.T) {
	root := repoRoot(t)

	pattern := `(NodeLost|node[._]lost|/v1/nodes/[^[:space:]]+/lost)`

	matches := gitGrep(t, root, pattern,
		"--",
		"cmd/controller", "cmd/apiserver",
		"pkg/satellite", "internal/controller",
		":(exclude)*_test.go",
	)

	allowed := allowedNodeLostFiles()

	var violations []string

	for _, m := range matches {
		if _, ok := allowed[m.file]; ok {
			continue
		}

		violations = append(violations,
			m.file+":"+m.line+" — "+m.text)
	}

	if len(violations) > 0 {
		t.Fatalf("controller-side NodeLost regression — `linstor node lost` is operator-only:\n  %s\n\n"+
			"The REST handler in pkg/rest/node_lifecycle.go is the single legitimate caller (operator intent).\n"+
			"Controllers MUST NOT fire NodeLost from a retry loop — it irreversibly drops Node CRDs.",
			strings.Join(violations, "\n  "))
	}
}

// repoRoot returns the repo-root path or skips the test when we are
// not running inside a git checkout (e.g. a vendored build).
func repoRoot(t *testing.T) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skipf("not in a git repo (git rev-parse failed: %v) — skipping static-analysis safety rails", err)
	}

	return strings.TrimSpace(string(out))
}

// grepMatch is one line of `git grep -n` output, decomposed.
type grepMatch struct {
	file string // repo-relative, forward-slash
	line string
	text string
}

// gitGrep runs `git grep -nE <pattern> <args>` from `root` and
// returns parsed matches. `git grep` exits 1 when no lines match
// (and 0 when matches are present); we treat both as success and
// only surface other exit codes as test errors.
func gitGrep(t *testing.T, root, pattern string, args ...string) []grepMatch {
	t.Helper()

	cmdArgs := append([]string{"grep", "-nE", pattern}, args...)
	cmd := exec.CommandContext(t.Context(), "git", cmdArgs...)
	cmd.Dir = root

	out, err := cmd.Output()
	if err != nil {
		// `git grep` returns exit-code 1 when there are no matches.
		// That's the success path for the safety rails.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return nil
		}

		stderr := ""
		if errors.As(err, &exitErr) {
			stderr = string(exitErr.Stderr)
		}

		t.Fatalf("git grep failed: %v\nstderr: %s", err, stderr)
	}

	var matches []grepMatch

	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := scanner.Text()

		// `git grep -n` output: `<path>:<lineno>:<text>`. Path may
		// contain colons only on Windows; we normalise to forward
		// slashes via filepath.ToSlash but split on first two colons
		// from the left.
		path, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}

		lineno, text, ok := strings.Cut(rest, ":")
		if !ok {
			continue
		}

		matches = append(matches, grepMatch{
			file: filepath.ToSlash(path),
			line: lineno,
			text: strings.TrimSpace(text),
		})
	}

	return matches
}
