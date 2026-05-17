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

package k8s_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// This file is the preventive-hardening extension (audit item v25 #235)
// of the v15 source-text guard in typed_field_carry_across_test.go.
// The original walked only pkg/store/k8s/. The same wholesale-
// Annotations / Labels / Finalizers / OwnerReferences write that the
// Resources.Update fix (item #210) addressed could land in pkg/rest/
// (REST handlers writing Secrets / CRDs) or pkg/satellite/
// reconcilers and slip past the original guard entirely.
//
// This sibling test extends the scope. It reuses the same
// `objectMetaWriteRegexp` + `isNilGuardInitializer` from
// typed_field_carry_across_test.go (test packages share file scope
// when in the same package) and additionally recognises two safe
// patterns that show up in reconciler code but not in the store
// layer:
//
//   - `X.Field = append(X.Field, ...)` — additive, preserves every
//     existing entry. The canonical kubebuilder pattern for stamping
//     a finalizer or an OwnerReference.
//   - `X.Field = slices.DeleteFunc(X.Field, ...)` — selective remove
//     that preserves every non-matching entry. The canonical pattern
//     for stripping a finalizer on the delete path.
//
// Both are semantically incompatible with the wipe class (which
// destroyed the satellite-stamped key on every routine modify);
// recognising them as safe keeps the allow-list small and focused on
// genuinely unusual cases.
//
// If a future PR adds a wholesale write to any walked directory that
// is neither a nil-guard init, an additive append, a selective
// DeleteFunc, nor allow-listed below, this test fails with the
// file:line of the offending write and the same actionable error
// message as the original guard.

// extendedObjectMetaWriteAllowList enumerates {dir, file,
// line-substring} triples the extended audit accepts as deliberately
// safe. Empty by default — every existing write in the walked dirs
// is either a nil-guard init, an additive append, or a selective
// DeleteFunc (verified by the audit on the baseline commit). Add an
// entry only with a justification comment explaining why the write
// cannot regress the wipe class.
//
//nolint:gochecknoglobals // package-level test data
var extendedObjectMetaWriteAllowList = []extendedObjectMetaWriteAllowListEntry{}

type extendedObjectMetaWriteAllowListEntry struct {
	dir           string // path relative to module root, e.g. "pkg/rest"
	file          string
	lineSubstring string
}

// extendedWalkRoots are the additional directories the preventive
// guard now covers. Listed relative to the module root.
//
// pkg/store/k8s/ is intentionally NOT here — the original
// TestNoWholesaleObjectMetaWrite still owns that directory and
// carries its own allow-list with the package-local justification
// comments. Walking it again here would double-report and confuse
// the failure messages.
//
//nolint:gochecknoglobals // package-level test data
var extendedWalkRoots = []string{
	"pkg/rest",
	"pkg/satellite",
	"pkg/satellite/controllers",
}

// TestNoWholesaleObjectMetaWriteExtended is the preventive guard:
// walk every non-test `*.go` file under each extendedWalkRoots
// entry and reject wholesale assignments to mutable ObjectMeta
// map/slice fields, applying the same safety classification as the
// original guard plus the two reconciler-idiomatic safe patterns
// documented above.
//
// If this test fails with "wholesale ObjectMeta write at file:line",
// the author MUST either:
//
//   - Route the write through a merge helper (the safe pattern)
//     that preserves operator-stamped keys.
//   - Switch to a per-key write (`m["k"] = v`).
//   - Use the additive `append` or selective `slices.DeleteFunc`
//     pattern documented in the file header.
//   - Add an `extendedObjectMetaWriteAllowList` entry with a
//     justification comment explaining why the write cannot wipe a
//     satellite-stamped key.
func TestNoWholesaleObjectMetaWriteExtended(t *testing.T) {
	t.Parallel()

	moduleRoot := moduleRootDir(t)

	var offenders []string

	for _, rel := range extendedWalkRoots {
		dir := filepath.Join(moduleRoot, rel)

		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read walked dir %q: %v", dir, err)
		}

		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}

			path := filepath.Join(dir, name)

			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %q: %v", path, err)
			}

			lines := strings.Split(string(data), "\n")

			for i, raw := range lines {
				trimmed := strings.TrimSpace(raw)

				if !objectMetaWriteRegexp.MatchString(raw) {
					continue
				}

				if isNilGuardInitializer(lines, i) {
					continue
				}

				if isAdditiveAppend(trimmed) {
					continue
				}

				if isSelectiveDeleteFunc(trimmed) {
					continue
				}

				if isExtendedAllowListed(rel, name, trimmed) {
					continue
				}

				offenders = append(offenders, formatExtendedOffender(rel, name, i+1, trimmed))
			}
		}
	}

	for _, o := range offenders {
		t.Errorf("extended ObjectMeta guard: wholesale write at %s\n"+
			"  Route this through a merge helper (the safe pattern),\n"+
			"  switch to a per-key write (`m[\"k\"] = v`),\n"+
			"  use `append`/`slices.DeleteFunc` if additive/selective,\n"+
			"  or add an `extendedObjectMetaWriteAllowList` entry with a justification.", o)
	}
}

// TestExtendedObjectMetaWriteAllowListEntriesExist mirrors the
// original allow-list drift check: every entry must still match a
// real source line so a stale entry can't silently grandfather a
// future wipe-class write at a different line.
func TestExtendedObjectMetaWriteAllowListEntriesExist(t *testing.T) {
	t.Parallel()

	moduleRoot := moduleRootDir(t)

	for _, entry := range extendedObjectMetaWriteAllowList {
		path := filepath.Join(moduleRoot, entry.dir, entry.file)

		data, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("extended allow-list references %q/%q which does not exist: %v",
				entry.dir, entry.file, err)

			continue
		}

		if !strings.Contains(string(data), entry.lineSubstring) {
			t.Errorf("extended allow-list entry for %q/%q with substring %q no longer matches any line — "+
				"remove the stale entry or update the substring.",
				entry.dir, entry.file, entry.lineSubstring)
		}
	}
}

// isAdditiveAppend reports whether `trimmed` is an assignment whose
// RHS starts with `append(<same-lhs>, ...)`. Such a write preserves
// every existing entry of the LHS slice and only adds new ones — it
// is the canonical kubebuilder pattern for stamping a finalizer or an
// OwnerReference and is semantically incompatible with the wipe class.
//
// The match is intentionally narrow: the RHS must literally call
// `append` with the LHS as its first argument. A construction like
// `X.Field = append(otherSlice, ...)` would replace the LHS contents
// and is correctly NOT recognised as additive.
func isAdditiveAppend(trimmed string) bool {
	eqIdx := strings.Index(trimmed, "=")
	if eqIdx <= 0 {
		return false
	}

	lhs := strings.TrimSpace(trimmed[:eqIdx])

	rhs := strings.TrimSpace(trimmed[eqIdx+1:])
	// `=[^=]` regex already excluded `==`, but a defensive guard
	// keeps the check robust to future regex tweaks.
	if strings.HasPrefix(rhs, "=") {
		return false
	}

	want := "append(" + lhs + ","
	want2 := "append(" + lhs + " ,"

	return strings.HasPrefix(rhs, want) || strings.HasPrefix(rhs, want2)
}

// isSelectiveDeleteFunc reports whether `trimmed` is an assignment
// whose RHS starts with `slices.DeleteFunc(<same-lhs>, ...)`. Such a
// write preserves every non-matching entry of the LHS slice and only
// removes the ones the predicate selects — the canonical pattern for
// stripping a finalizer on the delete path, semantically the inverse
// of the additive append but equally incompatible with the wipe class.
func isSelectiveDeleteFunc(trimmed string) bool {
	eqIdx := strings.Index(trimmed, "=")
	if eqIdx <= 0 {
		return false
	}

	lhs := strings.TrimSpace(trimmed[:eqIdx])

	rhs := strings.TrimSpace(trimmed[eqIdx+1:])
	if strings.HasPrefix(rhs, "=") {
		return false
	}

	want := "slices.DeleteFunc(" + lhs + ","
	want2 := "slices.DeleteFunc(" + lhs + " ,"

	return strings.HasPrefix(rhs, want) || strings.HasPrefix(rhs, want2)
}

// isExtendedAllowListed reports whether the trimmed source line at
// (rel-dir, file) matches any entry in the extended allow-list.
func isExtendedAllowListed(relDir, file, trimmed string) bool {
	for _, entry := range extendedObjectMetaWriteAllowList {
		if entry.dir == relDir && entry.file == file && strings.Contains(trimmed, entry.lineSubstring) {
			return true
		}
	}

	return false
}

// formatExtendedOffender renders a stable "<rel-dir>/<file>:<line>:
// <source>" string for the failure message so the author can jump
// straight to the offending write.
func formatExtendedOffender(relDir, file string, line int, src string) string {
	const maxSrc = 120

	if len(src) > maxSrc {
		src = src[:maxSrc] + "..."
	}

	return relDir + "/" + file + ":" + itoa(line) + ": " + src
}

// moduleRootDir resolves the on-disk module root by walking up from
// this test file's directory until it finds a `go.mod`. Keeps the
// extended walker hermetic: no `go list`, no reliance on cwd, no
// hard-coded module-root assumptions — mirrors the
// `packageSourceDir` pattern of the original guard.
func moduleRootDir(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("module root (go.mod) not found walking up from %q", filepath.Dir(file))
		}

		dir = parent
	}
}
