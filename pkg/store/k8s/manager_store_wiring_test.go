// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
)

// A store built on a manager's cached client without the manager's direct
// reader answers the reads that decide a node's fate from a cache, and says
// nothing while it does.
//
// That is not hypothetical: the controller binary served the LINSTOR surface
// from exactly such a store while the apiserver's had the reader, so `n lost`
// refused on a cached ONLINE in one topology and read the API server in the
// other. No test could tell, because the integration harness built its store
// the apiserver's way on a manager wired like the controller's.
//
// NewFromManager makes the pairing one decision. This is the check that it
// stays the only way a manager-backed binary takes it — the same shape the
// repository already uses to pin a recipe against the file it can silently
// drop.
func TestManagerBackedStoresTakeTheDirectReader(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)

	for _, dir := range []string{"cmd", "tests/integration/harness"} {
		walkGoFiles(t, filepath.Join(root, dir), func(path string, file *ast.File, fset *token.FileSet) {
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}

				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}

				if sel.Sel.Name != "New" && sel.Sel.Name != "NewWithAPIReader" {
					return true
				}

				pkg, ok := sel.X.(*ast.Ident)
				if !ok || !strings.Contains(pkg.Name, "k8s") {
					return true
				}

				for _, arg := range call.Args {
					if !mentionsManager(arg) {
						continue
					}

					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s:%d builds a manager-backed store with %s.%s; use %s.NewFromManager(mgr) "+
						"so the direct reader cannot be left out",
						rel, fset.Position(call.Pos()).Line, pkg.Name, sel.Sel.Name, pkg.Name)

					break
				}

				return true
			})
		})
	}
}

// mentionsManager reports whether an argument reads something off a manager,
// which is what makes the call a manager-backed construction rather than the
// CLI's uncached one.
func mentionsManager(arg ast.Expr) bool {
	found := false

	ast.Inspect(arg, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		if sel.Sel.Name == "GetClient" || sel.Sel.Name == "GetAPIReader" {
			found = true
		}

		return true
	})

	return found
}

func walkGoFiles(t *testing.T, dir string, visit func(string, *ast.File, *token.FileSet)) {
	t.Helper()

	fset := token.NewFileSet()

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}

		parsed, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return errors.Wrapf(perr, "parse %s", path)
		}

		visit(path, parsed, fset)

		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	// The package lives at pkg/store/k8s, so the module root is three up.
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve the module root: %v", err)
	}

	return root
}
