// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
)

// storePackagePath is the import path whose constructors this check guards.
const storePackagePath = "github.com/cozystack/blockstor/pkg/store/k8s"

// A store built on a manager's cached client without the manager's direct
// reader answers the reads that decide a node's fate from a cache, and says
// nothing while it does. The controller binary served the LINSTOR surface from
// exactly such a store while the apiserver's had the reader.
//
// The structural fix is that NewManager returns the store, built from the
// manager's own client and reader, so a manager-backed binary has one call and
// no second constructor to leave out. What this check stops is the way back:
// taking a manager's client (or reader) and handing it to New or
// NewWithAPIReader directly. It resolves the store package by import path, not
// by the name it happens to be imported under; it follows a manager's client
// through the local variables it is assigned to; and it walks the whole module
// rather than the directories a store is built in today.
func TestManagerBackedStoresComeFromNewManager(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)

	var findings []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			switch d.Name() {
			case ".git", "bin", "vendor", "third_party", "testdata", ".work", "node_modules":
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		rel, _ := filepath.Rel(root, path)

		got, perr := managerStoreViolations(path, nil)
		if perr != nil {
			return errors.Wrapf(perr, "analyse %s", rel)
		}

		for _, line := range got {
			findings = append(findings, rel+":"+strconv.Itoa(line))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk the module: %v", err)
	}

	for _, where := range findings {
		t.Errorf("%s builds a store from a manager's client or reader; take the store "+
			"NewManager returns, so the indexes and the direct reader cannot be left out", where)
	}
}

// The check is only as good as the spellings it recognises, so it is pinned on
// the ones a person actually reaches for: the direct call, an import alias with
// nothing store-like in it, the client through a local variable, and the
// reader alone. Each must be caught, and a CLI-shaped uncached client must not.
func TestManagerStoreCheckRecognisesTheSpellings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		src  string
		want int
	}{
		{
			name: "direct",
			src: `package x
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func f(mgr interface{ GetClient() any }) { _ = storek8s.New(mgr.GetClient()) }`,
			want: 1,
		},
		{
			name: "aliasWithoutK8s",
			src: `package x
import persistence "github.com/cozystack/blockstor/pkg/store/k8s"
func f(mgr interface{ GetClient() any }) { _ = persistence.New(mgr.GetClient()) }`,
			want: 1,
		},
		{
			name: "throughALocal",
			src: `package x
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func f(mgr interface{ GetClient() any }) {
	c := mgr.GetClient()
	cached := c
	_ = storek8s.New(cached)
}`,
			want: 1,
		},
		{
			name: "readerOnly",
			src: `package x
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func f(mgr interface{ GetAPIReader() any }, c any) { _ = storek8s.NewWithAPIReader(c, mgr.GetAPIReader()) }`,
			want: 1,
		},
		{
			name: "uncachedCLIClient",
			src: `package x
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func f(c any) { _ = storek8s.New(c) }`,
			want: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := managerStoreViolations("probe.go", []byte(tc.src))
			if err != nil {
				t.Fatalf("analyse: %v", err)
			}

			if len(got) != tc.want {
				t.Errorf("found %d violation(s), want %d", len(got), tc.want)
			}
		})
	}
}

// managerStoreViolations returns the lines where a store constructor is handed
// a value taken from a manager. src overrides reading path when non-nil.
func managerStoreViolations(path string, src []byte) ([]int, error) {
	fset := token.NewFileSet()

	// A nil []byte boxed into ParseFile's `any` is not a nil interface, and
	// would be parsed as an empty file rather than read from path.
	var source any
	if src != nil {
		source = src
	}

	file, err := parser.ParseFile(fset, path, source, parser.SkipObjectResolution)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	// The one sanctioned construction: NewManager builds the store from its own
	// manager inside the store package.
	inStorePackage := file.Name.Name == "k8s" &&
		strings.HasSuffix(filepath.ToSlash(filepath.Dir(path)), "pkg/store/k8s")

	storeLocal := ""

	for _, imp := range file.Imports {
		importPath, _ := strconv.Unquote(imp.Path.Value)
		if importPath != storePackagePath {
			continue
		}

		storeLocal = "k8s"
		if imp.Name != nil {
			storeLocal = imp.Name.Name
		}
	}

	if storeLocal == "" && !inStorePackage {
		return nil, nil
	}

	var lines []int

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		if inStorePackage && fn.Name.Name == "NewManager" {
			continue
		}

		derived := managerDerivedLocals(fn.Body)

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isStoreConstructor(call, storeLocal, inStorePackage) {
				return true
			}

			for _, arg := range call.Args {
				if takesFromManager(arg, derived) {
					lines = append(lines, fset.Position(call.Pos()).Line)

					break
				}
			}

			return true
		})
	}

	return lines, nil
}

func isStoreConstructor(call *ast.CallExpr, storeLocal string, inStorePackage bool) bool {
	name := ""

	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		pkg, ok := fun.X.(*ast.Ident)
		if !ok || storeLocal == "" || pkg.Name != storeLocal {
			return false
		}

		name = fun.Sel.Name
	case *ast.Ident:
		if !inStorePackage {
			return false
		}

		name = fun.Name
	default:
		return false
	}

	return name == "New" || name == "NewWithAPIReader"
}

// managerDerivedLocals is every local that holds, directly or through another
// local, a value a manager handed out.
func managerDerivedLocals(body *ast.BlockStmt) map[string]bool {
	derived := map[string]bool{}

	for changed := true; changed; {
		changed = false

		ast.Inspect(body, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok || len(assign.Lhs) != len(assign.Rhs) {
				return true
			}

			for i, lhs := range assign.Lhs {
				ident, ok := lhs.(*ast.Ident)
				if !ok || derived[ident.Name] {
					continue
				}

				if takesFromManager(assign.Rhs[i], derived) {
					derived[ident.Name] = true
					changed = true
				}
			}

			return true
		})
	}

	return derived
}

// takesFromManager reports whether an expression is, or contains, a manager's
// client or reader.
func takesFromManager(expr ast.Expr, derived map[string]bool) bool {
	found := false

	ast.Inspect(expr, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.SelectorExpr:
			if node.Sel.Name == "GetClient" || node.Sel.Name == "GetAPIReader" {
				found = true
			}
		case *ast.Ident:
			if derived[node.Name] {
				found = true
			}
		}

		return !found
	})

	return found
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
