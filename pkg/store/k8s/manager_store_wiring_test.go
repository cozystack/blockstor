// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
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
// no second constructor to leave out. What this check stops is the way back.
//
// Outside the store package, production code may call New or NewWithAPIReader
// only at the call sites listed in uncachedStoreConstructions, each of which
// builds over a client with no cache behind it. Any other call fails whatever
// its arguments look like, so a helper that receives the manager's client as a
// parameter is caught at the helper rather than escaping through it. Tests and
// the store package itself are held to the narrower rule: no constructor may be
// handed a manager's client or reader, followed through local variables and
// resolved by import path rather than by the name the package is imported
// under. The walk covers the whole module, skipping only what the Go toolchain
// itself ignores.
func TestManagerBackedStoresComeFromNewManager(t *testing.T) {
	t.Parallel()

	// The production call sites outside the store package allowed to build a
	// store from a client, keyed by module-relative file and enclosing
	// function, with the reason each one is safe.
	uncachedStoreConstructions := map[string]string{
		"cmd/blockstor/main.go:openStore": "the native CLI builds a plain client with no informer behind it",
	}

	findings, unused, err := storeConstructionFindings(repoRoot(t), uncachedStoreConstructions)
	if err != nil {
		t.Fatalf("walk the module: %v", err)
	}

	for _, where := range findings {
		t.Errorf("%s builds a store outside NewManager; take the store NewManager returns, "+
			"or, for a client with no cache behind it, add the call site to "+
			"uncachedStoreConstructions with the reason", where)
	}

	for _, site := range unused {
		t.Errorf("uncachedStoreConstructions lists %s, which no longer builds a store; "+
			"drop the entry so it cannot sanction a later call under the same name", site)
	}
}

// storeConstructionFindings walks the module under root and returns every
// violation as file:line, plus the allowlist entries no call site used.
func storeConstructionFindings(root string, allowed map[string]string) ([]string, []string, error) {
	var findings []string

	used := map[string]bool{}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			if path != root && goToolchainIgnores(d.Name()) {
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)

		src, rerr := os.ReadFile(path)
		if rerr != nil {
			return errors.Wrapf(rerr, "read %s", rel)
		}

		got, perr := storeConstructionViolations(rel, src, allowed, used)
		if perr != nil {
			return errors.Wrapf(perr, "analyse %s", rel)
		}

		for _, line := range got {
			findings = append(findings, rel+":"+strconv.Itoa(line))
		}

		return nil
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "walk")
	}

	var unused []string

	for site := range allowed {
		if !used[site] {
			unused = append(unused, site)
		}
	}

	sort.Strings(unused)

	return findings, unused, nil
}

// goToolchainIgnores is the set of directories `go build ./...` does not
// descend into: vendor, testdata, and names starting with a dot or an
// underscore. Nothing else is skipped. A name like third_party or bin is
// ordinary to the toolchain, so a construction planted there compiles and has
// to be seen.
func goToolchainIgnores(name string) bool {
	return name == "vendor" || name == "testdata" ||
		strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
}

// The walk skips only what the toolchain skips. A violation planted under
// third_party, which the previous skip list named, compiles and was invisible.
func TestManagerStoreCheckWalksWhatTheToolchainBuilds(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const violation = `package x
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func f(c any) { _ = storek8s.New(c) }
`

	for _, dir := range []string{"third_party/lib", "bin/tool", "node_modules/pkg", "testdata/fixture", "_scratch", ".work"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}

		if err := os.WriteFile(filepath.Join(root, dir, "planted.go"), []byte(violation), 0o600); err != nil {
			t.Fatalf("plant %s: %v", dir, err)
		}
	}

	findings, _, err := storeConstructionFindings(root, nil)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Strings(findings)

	want := []string{"bin/tool/planted.go:3", "node_modules/pkg/planted.go:3", "third_party/lib/planted.go:3"}
	if strings.Join(findings, " ") != strings.Join(want, " ") {
		t.Errorf("findings = %v, want %v: the walk must see every directory the toolchain "+
			"builds and skip only the ones it ignores", findings, want)
	}
}

// The check is only as good as the spellings it recognises, so it is pinned on
// the ones a person actually reaches for. In test code, which is held to the
// manager-derived rule: the direct call, an import alias with nothing
// store-like in it, the client through a local variable, and the reader alone
// are caught, and a CLI-shaped uncached client is not. In production code: a
// helper that takes the client as a parameter is caught, and so is any call
// site not on the allowlist, while the listed one passes.
func TestManagerStoreCheckRecognisesTheSpellings(t *testing.T) {
	t.Parallel()

	allowed := map[string]string{"cmd/blockstor/main.go:openStore": "probe"}

	for _, tc := range []struct {
		name string
		file string
		src  string
		want int
	}{
		{
			name: "direct",
			file: "x/probe_test.go",
			src: `package x
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func f(mgr interface{ GetClient() any }) { _ = storek8s.New(mgr.GetClient()) }`,
			want: 1,
		},
		{
			name: "aliasWithoutK8s",
			file: "x/probe_test.go",
			src: `package x
import persistence "github.com/cozystack/blockstor/pkg/store/k8s"
func f(mgr interface{ GetClient() any }) { _ = persistence.New(mgr.GetClient()) }`,
			want: 1,
		},
		{
			name: "throughALocal",
			file: "x/probe_test.go",
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
			file: "x/probe_test.go",
			src: `package x
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func f(mgr interface{ GetAPIReader() any }, c any) { _ = storek8s.NewWithAPIReader(c, mgr.GetAPIReader()) }`,
			want: 1,
		},
		{
			name: "uncachedClientInATest",
			file: "x/probe_test.go",
			src: `package x
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func f(c any) { _ = storek8s.New(c) }`,
			want: 0,
		},
		{
			name: "helperTakesTheClient",
			file: "cmd/controller/main.go",
			src: `package main
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func build(c any) any { return storek8s.New(c) }
func run(mgr interface{ GetClient() any }) { _ = build(mgr.GetClient()) }`,
			want: 1,
		},
		{
			name: "unlistedCallSite",
			file: "cmd/blockstor/main.go",
			src: `package main
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func openOtherStore(c any) any { return storek8s.New(c) }`,
			want: 1,
		},
		{
			name: "listedCallSite",
			file: "cmd/blockstor/main.go",
			src: `package main
import storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
func openStore(c any) any { return storek8s.New(c) }`,
			want: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := storeConstructionViolations(tc.file, []byte(tc.src), allowed, map[string]bool{})
			if err != nil {
				t.Fatalf("analyse: %v", err)
			}

			if len(got) != tc.want {
				t.Errorf("found %d violation(s), want %d", len(got), tc.want)
			}
		})
	}
}

// storeConstructionViolations returns the lines of rel (a module-relative,
// slash-separated path) where a store is built against the rules above, and
// records in used the allowlist entries it matched.
func storeConstructionViolations(
	rel string, src []byte, allowed map[string]string, used map[string]bool,
) ([]int, error) {
	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	// The one sanctioned construction: NewManager builds the store from its own
	// manager inside the store package.
	inStorePackage := file.Name.Name == "k8s" && path.Dir(rel) == "pkg/store/k8s"

	// Production code outside the store package may build a store only at a
	// listed call site. Tests and the store package keep the narrower rule.
	allowlistRule := !inStorePackage && !strings.HasSuffix(rel, "_test.go")

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

		site := rel + ":" + funcDeclName(fn)
		derived := managerDerivedLocals(fn.Body)

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isStoreConstructor(call, storeLocal, inStorePackage) {
				return true
			}

			if allowlistRule {
				if _, listed := allowed[site]; listed {
					used[site] = true
				} else {
					lines = append(lines, fset.Position(call.Pos()).Line)
				}

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

// funcDeclName names a function the way the allowlist keys it: Name for a
// function, Type.Name for a method.
func funcDeclName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}

	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}

	if ident, ok := recv.(*ast.Ident); ok {
		return ident.Name + "." + fn.Name.Name
	}

	return fn.Name.Name
}

func isStoreConstructor(call *ast.CallExpr, storeLocal string, inStorePackage bool) bool {
	var name string

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
