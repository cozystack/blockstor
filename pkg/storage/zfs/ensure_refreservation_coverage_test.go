// SPDX-License-Identifier: Apache-2.0

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

package zfs_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEnsureRefreservationCoverage is the source-text guard for the
// Bug 252+253+254+255 class. Any function in pkg/storage/zfs that
// mutates a dataset's volsize OR materialises a new dataset (via
// `zfs create -V`, `zfs clone`, `zfs receive`, or `zfs set volsize=`)
// MUST also call ensureRefreservation in its body — otherwise the
// thick guarantee is silently downgraded on every path that touches
// the dataset's allocation.
//
// This is the structural fix: future audits don't have to remember
// every mutation site; the test catches a new mutation that forgets
// the helper.
//
// Allow-list functions are explicit and tiny: only test helpers and
// the helper itself can mutate without re-invoking the helper.
func TestEnsureRefreservationCoverage(t *testing.T) {
	// ensureProvisioned runs ensureRefreservation as its last step,
	// so calling it satisfies the rule.
	helperCalls := []string{"ensureRefreservation(", "ensureProvisioned("}

	// Markers that indicate a dataset-mutating ZFS operation. Any
	// function body containing one of these strings MUST also
	// contain one of helperCalls.
	//
	// Markers cover both the literal-argv shape (`"zfs", "clone"`,
	// `"zfs", "recv"`) and the dynamic-argv shape used by CreateVolume
	// (`[]string{"create"}`). They also catch the volsize-setting
	// `"volsize="` literal used by ResizeVolume's `zfs set volsize=...`.
	mutationMarkers := []string{
		`"clone"`,
		`"recv"`,
		`"receive"`,
		`"create"`,
		`"volsize="`,
	}

	// Functions exempt from the rule: the helpers themselves + any
	// test helpers (none currently).
	allow := map[string]bool{
		"ensureRefreservation": true,
		"ensureProvisioned":    true,
	}

	for fnName, body := range zfsFuncBodies(t) {
		if allow[fnName] {
			continue
		}

		hitMarker := firstContained(body, mutationMarkers)
		if hitMarker == "" {
			continue
		}

		if firstContained(body, helperCalls) == "" {
			t.Errorf("Bug 252+253+254+255 guard: %s contains dataset-mutating "+
				"marker %q but does NOT call ensureRefreservation — every path that mutates volsize "+
				"or materialises a new dataset must end on ensureRefreservation so "+
				"the thick guarantee is preserved",
				fnName, hitMarker)
		}
	}
}

// TestOwnerMarkerCoverage is the same guard for the ownership marker
// (issue 264): every function that materialises a zvol, or finds one
// already there while provisioning it, must leave the marker on it,
// either in the materialising command itself (ownerAssignment) or
// through ensureProvisioned. A path that forgets it creates a zvol the
// orphan sweeper can never reclaim; the guard keeps the next new
// path from forgetting.
func TestOwnerMarkerCoverage(t *testing.T) {
	materialising := []string{`"clone"`, `"recv"`, `"receive"`, `"create"`}
	markerWriters := []string{"ownerAssignment", "ensureProvisioned("}

	for fnName, body := range zfsFuncBodies(t) {
		hitMarker := firstContained(body, materialising)
		if hitMarker == "" {
			continue
		}

		if firstContained(body, markerWriters) == "" {
			t.Errorf("issue 264 guard: %s materialises a dataset (%q) without writing "+
				"the ownership marker", fnName, hitMarker)
		}
	}
}

// firstContained returns the first of needles found in body, or "".
func firstContained(body string, needles []string) string {
	for _, needle := range needles {
		if strings.Contains(body, needle) {
			return needle
		}
	}

	return ""
}

// zfsFuncBodies maps every function declared in the package's non-test
// sources to the source text of its body.
func zfsFuncBodies(t *testing.T) map[string]string {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	fset := token.NewFileSet()
	bodies := map[string]string{}

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		path := filepath.Join(".", name)

		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}

		fileNode, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		for _, decl := range fileNode.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			bodyStart := fset.Position(fn.Body.Lbrace).Offset
			bodyEnd := fset.Position(fn.Body.Rbrace).Offset

			if bodyStart < 0 || bodyEnd > len(src) || bodyStart >= bodyEnd {
				continue
			}

			bodies[fn.Name.Name] = string(src[bodyStart:bodyEnd])
		}
	}

	return bodies
}
