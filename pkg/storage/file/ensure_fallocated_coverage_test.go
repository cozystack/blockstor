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

package file_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEnsureFallocatedCoverage is the source-text guard for the
// Bug 247+250+256 class. CreateVolume and ResizeVolume in the FILE
// provider MUST call ensureFallocated so the thick space guarantee is
// preserved on every path that can observe an existing-but-historically-
// sparse backing file — including the idempotent-skip / no-grow
// short-circuit branches.
//
// This is the structural fix preventing v36+ from finding this class
// again: any future refactor of CreateVolume/ResizeVolume that drops the
// ensureFallocated call fails this test.
func TestEnsureFallocatedCoverage(t *testing.T) {
	const (
		helperCall = "ensureFallocated("
	)

	// Required: these functions must call the helper.
	required := []string{"CreateVolume", "ResizeVolume"}

	pkgDir := "."

	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	fset := token.NewFileSet()

	// Aggregate function bodies across the package so a future refactor
	// that splits a function into helpers is still covered (we just need
	// the helper call to appear in the "owning" function body).
	bodies := map[string]string{}

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

			bodyStart := fset.Position(fn.Body.Lbrace).Offset
			bodyEnd := fset.Position(fn.Body.Rbrace).Offset

			if bodyStart < 0 || bodyEnd > len(src) || bodyStart >= bodyEnd {
				continue
			}

			bodies[fn.Name.Name] = string(src[bodyStart:bodyEnd])
		}
	}

	// reachesHelper walks a function body looking for either the
	// direct helper call or a call to a package-local helper whose own
	// body contains the helper call. One level of indirection is
	// enough: ResizeVolume can call ensureFallocated directly, while
	// CreateVolume can delegate to createBackingFile which then calls
	// the helper. Deeper chains would just make the guard harder to
	// reason about for no real win.
	var reachesHelper func(body string, seen map[string]bool) bool

	reachesHelper = func(body string, seen map[string]bool) bool {
		if strings.Contains(body, helperCall) {
			return true
		}

		for name, b := range bodies {
			if seen[name] {
				continue
			}

			callMarker := "p." + name + "("
			if !strings.Contains(body, callMarker) {
				continue
			}

			seen[name] = true
			if reachesHelper(b, seen) {
				return true
			}
		}

		return false
	}

	for _, fnName := range required {
		body, ok := bodies[fnName]
		if !ok {
			t.Errorf("Bug 247+250+256 guard: %s not found in FILE provider source",
				fnName)

			continue
		}

		if !reachesHelper(body, map[string]bool{fnName: true}) {
			t.Errorf("Bug 247+250+256 guard: %s must reach %s (directly or via a "+
				"package-local helper) — every path that can observe an existing-"+
				"but-historically-sparse backing file (including idempotent-skip / "+
				"no-grow short-circuits) must end on ensureFallocated so the thick "+
				"guarantee is preserved",
				fnName, helperCall)
		}
	}
}
