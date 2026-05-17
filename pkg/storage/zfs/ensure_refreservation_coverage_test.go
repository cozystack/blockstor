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
	const (
		helperCall = "ensureRefreservation("
	)

	// Markers that indicate a dataset-mutating ZFS operation. Any
	// function body containing one of these strings MUST also
	// contain helperCall.
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

	// Functions exempt from the rule: the helper itself + any test
	// helpers (none currently).
	allow := map[string]bool{
		"ensureRefreservation": true,
	}

	pkgDir := "."

	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	fset := token.NewFileSet()

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		path := filepath.Join(pkgDir, name)

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

			fnName := fn.Name.Name
			if allow[fnName] {
				continue
			}

			// Slice the source range for this function body.
			bodyStart := fset.Position(fn.Body.Lbrace).Offset
			bodyEnd := fset.Position(fn.Body.Rbrace).Offset

			if bodyStart < 0 || bodyEnd > len(src) || bodyStart >= bodyEnd {
				continue
			}

			body := string(src[bodyStart:bodyEnd])

			var hitMarker string

			for _, marker := range mutationMarkers {
				if strings.Contains(body, marker) {
					hitMarker = marker

					break
				}
			}

			if hitMarker == "" {
				continue
			}

			if !strings.Contains(body, helperCall) {
				t.Errorf("Bug 252+253+254+255 guard: %s contains dataset-mutating "+
					"marker %q but does NOT call %s — every path that mutates volsize "+
					"or materialises a new dataset must end on ensureRefreservation so "+
					"the thick guarantee is preserved",
					fnName, hitMarker, helperCall)
			}
		}
	}
}
