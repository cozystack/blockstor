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

package naming_test

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// The guard follows values that hold a LINSTOR object name from where they
// enter (a source) to where they are compared, folded, used as a map key,
// searched for or used to address a CR (a sink), and fails on a sink that
// naming.Key or naming.Same is not on. It works on types and on where a
// value comes from, not on what a variable is called, so a rename does not
// hide a site from it.

const module = "github.com/cozystack/blockstor"

// sourceFields are the fields that hold an object's name or a reference to
// another object, on the types of the packages in sourcePackages.
func sourceFields() map[string]bool {
	return map[string]bool{
		"NodeName": true, "ResourceName": true, "ResourceDefinitionName": true, "RscName": true,
		"ResourceGroupName": true, "StoragePoolName": true, "PoolName": true, "Nodes": true,
		"NodeNameList": true, "SnapshotName": true, "PeerName": true, "NodeA": true, "NodeB": true,
		"Resources": true,
	}
}

// storeParams are the parameters of the store's methods that take an
// object name. Whatever a caller passes there is a name, whatever it is
// called and wherever it came from.
func storeParams() map[string]bool {
	return map[string]bool{"name": true, "node": true, "nodeName": true, "pool": true, "rdName": true, "snapName": true}
}

// pathKeys are the REST path parameters that carry an object name.
func pathKeys() map[string]bool {
	return map[string]bool{
		"node": true, "nodeA": true, "nodeB": true, "src": true, "dst": true, "rd": true, "rg": true,
		"pool": true, "snap": true, "target": true, "name": true,
	}
}

// flagKeys are the CLI flags that carry an object name.
func flagKeys() map[string]bool {
	return map[string]bool{
		"from-resource": true, "from-snapshot": true, "migrate-from": true, "pool-name": true,
		"resource-group": true, "storage-pool": true, "storage-pools": true, "to-resource": true,
	}
}

// sourcePackages hold the types whose name fields are sources: the wire API,
// the CRDs' specs, the REST surface, the CLI's parsed arguments and the
// store's own types.
func sourcePackages() map[string]bool {
	return map[string]bool{
		module + "/pkg/api/v1": true, module + "/api/v1alpha1": true, module + "/pkg/rest": true,
		module + "/internal/cli": true, module + "/pkg/store": true,
	}
}

// namedTypes are the wire types whose own Name is an object name, in the
// spelling it was registered under. A CRD's own Name is its CR name, a key,
// and so is not a source.
func namedTypes() map[string]bool {
	return map[string]bool{
		"Node": true, "ResourceDefinition": true, "ResourceGroup": true, "Resource": true,
		"ResourceWithVolumes": true, "Snapshot": true, "StoragePool": true, "StoragePoolDefinition": true,
	}
}

type guard struct {
	pkgs    []*packages.Package
	tainted map[types.Object]bool
	keyed   map[types.Object]bool
	returns map[*types.Func]bool
	decls   map[*types.Func]*ast.FuncDecl
	info    map[*ast.FuncDecl]*types.Info
}

func newGuard(t *testing.T, dir string, patterns ...string) *guard {
	t.Helper()

	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Dir: dir,
	}

	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		t.Fatalf("load %v: %v", patterns, err)
	}

	for _, pkg := range pkgs {
		for _, e := range pkg.Errors {
			t.Fatalf("load %s: %v", pkg.PkgPath, e)
		}
	}

	g := &guard{
		pkgs: pkgs, tainted: map[types.Object]bool{}, keyed: map[types.Object]bool{}, returns: map[*types.Func]bool{},
		decls: map[*types.Func]*ast.FuncDecl{}, info: map[*ast.FuncDecl]*types.Info{},
	}

	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, d := range file.Decls {
				if fn, ok := d.(*ast.FuncDecl); ok && fn.Body != nil {
					if obj, ok := pkg.TypesInfo.Defs[fn.Name].(*types.Func); ok {
						g.decls[obj] = fn
						g.info[fn] = pkg.TypesInfo
					}
				}
			}
		}
	}

	g.propagate()

	return g
}

func isString(info *types.Info, e ast.Expr) bool {
	t := info.TypeOf(e)
	if t == nil {
		return false
	}

	b, ok := t.Underlying().(*types.Basic)

	return ok && b.Info()&types.IsString != 0
}

func isConstant(info *types.Info, e ast.Expr) bool {
	if id, ok := e.(*ast.Ident); ok && id.Name == "nil" {
		return true
	}

	tv, ok := info.Types[e]

	return ok && tv.Value != nil
}

// calleeOf is the function a call statically calls, if any.
func calleeOf(info *types.Info, call *ast.CallExpr) *types.Func {
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		f, _ := info.Uses[fun].(*types.Func)

		return f
	case *ast.SelectorExpr:
		if sel, ok := info.Selections[fun]; ok {
			f, _ := sel.Obj().(*types.Func)

			return f
		}

		f, _ := info.Uses[fun.Sel].(*types.Func)

		return f
	}

	return nil
}

func funcName(f *types.Func) string {
	if f == nil || f.Pkg() == nil {
		return ""
	}

	if recv := f.Type().(*types.Signature).Recv(); recv != nil {
		t := recv.Type()
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}

		if n, ok := t.(*types.Named); ok {
			return f.Pkg().Path() + "." + n.Obj().Name() + "." + f.Name()
		}
	}

	return f.Pkg().Path() + "." + f.Name()
}

// isKeyCall reports whether e is a call to naming.Key.
func isKeyCall(info *types.Info, e ast.Expr) bool {
	call, ok := ast.Unparen(e).(*ast.CallExpr)

	return ok && funcName(calleeOf(info, call)) == module+"/pkg/naming.Key"
}

func stringLit(info *types.Info, e ast.Expr) (string, bool) {
	tv, ok := info.Types[e]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		return "", false
	}

	return constant.StringVal(tv.Value), true
}

// isSourceCall reports whether a call returns a name a client sent: a path
// parameter or a query list that names objects.
func isSourceCall(info *types.Info, call *ast.CallExpr) bool {
	switch funcName(calleeOf(info, call)) {
	case "net/http.Request.PathValue":
		key, ok := stringLit(info, call.Args[0])

		return ok && pathKeys()[key]
	case module + "/pkg/rest.multiValueQuery":
		return true
	}

	return false
}

// isFlagSource reports whether e reads a CLI flag that names an object.
func isFlagSource(info *types.Info, e *ast.IndexExpr) bool {
	sel, ok := ast.Unparen(e.X).(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Values" {
		return false
	}

	selection, ok := info.Selections[sel]
	if !ok || selection.Obj().Pkg() == nil || selection.Obj().Pkg().Path() != module+"/internal/cli" {
		return false
	}

	key, ok := stringLit(info, e.Index)

	return ok && flagKeys()[key]
}

// isKey reports whether e holds a key: naming.Key's result, a value only
// ever assigned one, or a CR's own name.
func (g *guard) isKey(info *types.Info, e ast.Expr) bool {
	switch x := ast.Unparen(e).(type) {
	case *ast.CallExpr:
		return isKeyCall(info, x)
	case *ast.Ident:
		obj := info.Uses[x]

		return obj != nil && g.keyed[obj] && !g.tainted[obj]
	case *ast.SelectorExpr:
		selection, ok := info.Selections[x]
		if !ok || selection.Kind() != types.FieldVal {
			return false
		}

		f := selection.Obj()

		return f.Name() == "Name" && f.Pkg() != nil && f.Pkg().Path() == "k8s.io/apimachinery/pkg/apis/meta/v1"
	}

	return false
}

// nameLike reports whether e holds an object name, raw or as a key.
func (g *guard) nameLike(info *types.Info, e ast.Expr) bool {
	return g.isTainted(info, e) || g.isKey(info, e)
}

// isSourceField reports whether a selector reads a name field of a source
// type.
func (g *guard) isSourceField(info *types.Info, sel *ast.SelectorExpr) bool {
	selection, ok := info.Selections[sel]
	if !ok || selection.Kind() != types.FieldVal {
		return false
	}

	recv := types.Unalias(selection.Recv())
	if p, ok := recv.(*types.Pointer); ok {
		recv = types.Unalias(p.Elem())
	}

	named, ok := recv.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}

	// A satellite controller is configured with the node it runs on, in
	// the spelling the operator gave; the kernel names it also holds are
	// keys already.
	if named.Obj().Pkg().Path() == module+"/pkg/satellite/controllers" {
		return sel.Sel.Name == "NodeName"
	}

	if !sourcePackages()[named.Obj().Pkg().Path()] {
		return false
	}

	name := sel.Sel.Name
	if name == "Name" {
		return named.Obj().Pkg().Path() == module+"/pkg/api/v1" && namedTypes()[named.Obj().Name()]
	}

	return sourceFields()[name]
}

// isTainted reports whether e may hold an object name in a spelling of its
// own: read from a source, or derived from one.
func (g *guard) isTainted(info *types.Info, e ast.Expr) bool {
	switch x := ast.Unparen(e).(type) {
	case *ast.Ident:
		obj := info.Uses[x]
		if obj == nil {
			obj = info.Defs[x]
		}

		return obj != nil && g.tainted[obj]
	case *ast.SelectorExpr:
		return g.isSourceField(info, x)
	case *ast.IndexExpr:
		return isFlagSource(info, x) || g.isTainted(info, x.X)
	case *ast.SliceExpr:
		return g.isTainted(info, x.X)
	case *ast.StarExpr:
		return g.isTainted(info, x.X)
	case *ast.BinaryExpr:
		return x.Op == token.ADD && (g.isTainted(info, x.X) || g.isTainted(info, x.Y))
	case *ast.CallExpr:
		if isKeyCall(info, x) {
			return false
		}

		if isSourceCall(info, x) {
			return true
		}

		if tv, ok := info.Types[x.Fun]; ok && tv.IsType() && len(x.Args) == 1 {
			return g.isTainted(info, x.Args[0])
		}

		f := calleeOf(info, x)
		if f != nil && g.returns[f] {
			return true
		}

		// A method of a value that holds names, such as url.Values.Get,
		// returns one of them.
		if sel, ok := ast.Unparen(x.Fun).(*ast.SelectorExpr); ok && f != nil && isString(info, x) {
			return g.isTainted(info, sel.X)
		}

		switch funcName(f) {
		case "strings.TrimSpace", "strings.ToLower", "strings.ToUpper", "strings.Trim", "strings.TrimPrefix",
			"strings.TrimSuffix", "strings.Split", "strings.Fields":
			return len(x.Args) > 0 && g.isTainted(info, x.Args[0])
		}
	}

	return false
}

func (g *guard) mark(obj types.Object) bool {
	if obj == nil || g.tainted[obj] {
		return false
	}

	g.tainted[obj] = true

	return true
}

// propagate follows sources through assignments, ranges, calls and returns
// until nothing changes.
func (g *guard) propagate() {
	for changed := true; changed; {
		changed = false

		for fn, decl := range g.decls {
			info := g.info[decl]

			ast.Inspect(decl.Body, func(n ast.Node) bool {
				switch s := n.(type) {
				case *ast.AssignStmt:
					changed = g.assign(info, s.Lhs, s.Rhs) || changed
				case *ast.ValueSpec:
					lhs := make([]ast.Expr, len(s.Names))
					for i, id := range s.Names {
						lhs[i] = id
					}

					changed = g.assign(info, lhs, s.Values) || changed
				case *ast.RangeStmt:
					// A map's keys are not its values: ranging over a
					// map of names yields its values only.
					if g.isTainted(info, s.X) && s.Value != nil {
						changed = g.markExpr(info, s.Value) || changed
					}
				case *ast.ReturnStmt:
					if len(s.Results) > 0 && g.isTainted(info, s.Results[0]) && !g.returns[fn] {
						g.returns[fn] = true
						changed = true
					}
				case *ast.CallExpr:
					changed = g.passArgs(info, s) || changed
				}

				return true
			})
		}
	}
}

func identObj(info *types.Info, e ast.Expr) types.Object {
	id, ok := ast.Unparen(e).(*ast.Ident)
	if !ok || id.Name == "_" {
		return nil
	}

	if obj := info.Defs[id]; obj != nil {
		return obj
	}

	return info.Uses[id]
}

func (g *guard) markExpr(info *types.Info, e ast.Expr) bool {
	return g.mark(identObj(info, e))
}

func (g *guard) markKeyed(info *types.Info, e ast.Expr) bool {
	obj := identObj(info, e)
	if obj == nil || g.keyed[obj] {
		return false
	}

	g.keyed[obj] = true

	return true
}

func (g *guard) assign(info *types.Info, lhs, rhs []ast.Expr) bool {
	changed := false

	switch {
	case len(lhs) == len(rhs):
		for i := range lhs {
			if g.isTainted(info, rhs[i]) {
				changed = g.markExpr(info, lhs[i]) || changed
			} else if g.isKey(info, rhs[i]) {
				changed = g.markKeyed(info, lhs[i]) || changed
			}
		}
	case len(rhs) == 1 && len(lhs) > 0:
		if g.isTainted(info, rhs[0]) {
			changed = g.markExpr(info, lhs[0])
		}
	}

	return changed
}

// passArgs taints the parameters a call passes names into and, the other
// way, the arguments a call passes as names: to the store, or to a
// parameter that holds names.
func (g *guard) passArgs(info *types.Info, call *ast.CallExpr) bool {
	f := calleeOf(info, call)
	if f == nil {
		return false
	}

	changed := false

	if f.Pkg() != nil && f.Pkg().Path() == module+"/pkg/store" {
		sig, _ := f.Type().(*types.Signature)
		for i, arg := range call.Args {
			if sig != nil && i < sig.Params().Len() && storeParams()[sig.Params().At(i).Name()] &&
				isString(info, arg) && !g.isKey(info, arg) {
				changed = g.markExpr(info, arg) || changed
			}
		}
	}

	decl, ok := g.decls[f]
	if !ok {
		return changed
	}

	calleeInfo := g.info[decl]

	var params []types.Object

	for _, field := range decl.Type.Params.List {
		for _, id := range field.Names {
			params = append(params, calleeInfo.Defs[id])
		}
	}

	for i, arg := range call.Args {
		if i >= len(params) {
			continue
		}

		switch {
		case g.isTainted(info, arg):
			changed = g.mark(params[i]) || changed
		case g.tainted[params[i]] && isString(info, arg) && !g.isKey(info, arg):
			changed = g.markExpr(info, arg) || changed
		}
	}

	return changed
}

// sameShape reports whether two operands read one field of two elements of
// one collection, or of a comparator's two arguments: one source, not two
// spellings.
func sameShape(a, b ast.Expr) bool {
	sa, oka := ast.Unparen(a).(*ast.SelectorExpr)
	sb, okb := ast.Unparen(b).(*ast.SelectorExpr)

	if !oka || !okb || sa.Sel.Name != sb.Sel.Name {
		return false
	}

	if pa, ok := sa.X.(*ast.Ident); ok {
		if pb, ok := sb.X.(*ast.Ident); ok {
			switch pa.Name + "," + pb.Name {
			case "a,b", "left,right", "x,y", "lhs,rhs":
				return true
			}
		}
	}

	ia, oka := sa.X.(*ast.IndexExpr)
	ib, okb := sb.X.(*ast.IndexExpr)

	return oka && okb && fmt.Sprint(ia.X) == fmt.Sprint(ib.X)
}

func isCRAddress(info *types.Info, lit *ast.CompositeLit) bool {
	t := info.TypeOf(lit)
	if t == nil {
		return false
	}

	// client.ObjectKey is an alias of types.NamespacedName.
	named, ok := types.Unalias(t).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}

	switch named.Obj().Pkg().Path() + "." + named.Obj().Name() {
	case "k8s.io/apimachinery/pkg/types.NamespacedName", "k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta":
		return true
	}

	return false
}

type finding struct {
	file string
	line int
	code string
}

// violations reports, keyed "path: code", every sink a source reaches
// without naming.Key or naming.Same, in the files under root.
func (g *guard) violations(root string) []string {
	found := g.findings(root)
	out := make([]string, 0, len(found))

	for _, f := range found {
		out = append(out, f.file+": "+f.code)
	}

	sort.Strings(out)

	return slices.Compact(out)
}

func (g *guard) findings(root string) []finding {
	var out []finding

	for _, pkg := range g.pkgs {
		info := pkg.TypesInfo

		for _, file := range pkg.Syntax {
			path := pkg.Fset.File(file.Pos()).Name()

			rel, err := filepath.Rel(root, path)
			if err != nil {
				continue
			}

			rel = filepath.ToSlash(rel)
			if strings.HasSuffix(rel, "_test.go") || excluded(rel) {
				continue
			}

			src, err := os.ReadFile(path)
			if err != nil {
				continue
			}

			code := func(n ast.Node) string {
				return strings.Join(strings.Fields(string(src[pkg.Fset.Position(n.Pos()).Offset:pkg.Fset.Position(n.End()).Offset])), " ")
			}

			ast.Inspect(file, func(n ast.Node) bool {
				if hit := g.sink(info, n); hit != nil {
					out = append(out, finding{file: rel, line: pkg.Fset.Position(hit.Pos()).Line, code: code(hit)})
				}

				return true
			})
		}
	}

	return out
}

// rawPair reports whether two operands both hold object names and at least
// one of them is raw: two spellings compared as they are.
func (g *guard) rawPair(info *types.Info, x, y ast.Expr) bool {
	return g.isTainted(info, x) && g.nameLike(info, y) || g.isTainted(info, y) && g.nameLike(info, x)
}

// sink returns the node to report when n is a sink a source reaches.
func (g *guard) sink(info *types.Info, n ast.Node) ast.Node {
	switch e := n.(type) {
	case *ast.BinaryExpr:
		if e.Op != token.EQL && e.Op != token.NEQ || !isString(info, e.X) || !isString(info, e.Y) {
			return nil
		}

		if isConstant(info, e.X) || isConstant(info, e.Y) || sameShape(e.X, e.Y) {
			return nil
		}

		if g.rawPair(info, e.X, e.Y) {
			return e
		}
	case *ast.IndexExpr:
		m, ok := info.TypeOf(e.X).Underlying().(*types.Map)
		if !ok {
			return nil
		}

		if k, ok := m.Key().Underlying().(*types.Basic); !ok || k.Info()&types.IsString == 0 {
			return nil
		}

		if bin, ok := ast.Unparen(e.Index).(*ast.BinaryExpr); ok && bin.Op == token.ADD {
			return nil
		}

		if !isKeyCall(info, e.Index) && !isConstant(info, e.Index) && g.isTainted(info, e.Index) {
			return e
		}
	case *ast.CallExpr:
		switch funcName(calleeOf(info, e)) {
		case "slices.Contains", "slices.Index":
			if len(e.Args) == 2 && !isConstant(info, e.Args[1]) && g.rawPair(info, e.Args[0], e.Args[1]) {
				return e
			}
		case "strings.EqualFold", "strings.ToLower", "strings.ToUpper":
			for _, arg := range e.Args {
				if g.isTainted(info, arg) {
					return e
				}
			}
		}
	case *ast.CompositeLit:
		if !isCRAddress(info, e) {
			return nil
		}

		for _, elt := range e.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}

			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Name" && !isKeyCall(info, kv.Value) && g.isTainted(info, kv.Value) {
				return kv
			}
		}
	}

	return nil
}

// excludedDirs are the trees the rule does not reach, and why.
func excludedDirs() map[string]string {
	return map[string]string{
		"pkg/naming": "defines the key",
		"pkg/store/k8s": "the store backend: it resolves a caller's spelling to the CR name, " +
			"and its scoped reads ask in one given spelling at a time, as the field selector does",
		"pkg/store/storetest": "the conformance suite, which writes spellings on purpose",
		"pkg/linstormigrate": "reads one LINSTOR database dump, whose rows name each other in the " +
			"spelling LINSTOR stored, and compares them only with each other; the names it " +
			"writes go through k8s.Name",
		"pkg/drbd":    "kernel side: compares names DRBD reports back with each other",
		"third_party": "vendored code",
	}
}

// excludedFiles are single files out of reach, and why.
func excludedFiles() map[string]string {
	return map[string]string{
		"pkg/satellite/reconciler.go": "works on the intent the dispatcher built and on the kernel; " +
			"every name in it comes from that one intent",
	}
}

func excluded(rel string) bool {
	if _, ok := excludedFiles()[rel]; ok {
		return true
	}

	for dir := range excludedDirs() {
		if strings.HasPrefix(rel, dir+"/") {
			return true
		}
	}

	return false
}

// allowed are reviewed sites the rule reports whose names all come from one
// source and must stay as they are, and why.
func allowed() map[string]string {
	oneListing := "keys and lookups both come from one listing, so they spell each name alike"

	return map[string]string{
		"cmd/satellite/coldstart_owned.go: healthy[name]": "" +
			"kernel resource names on both sides, from one drbdsetup listing",
		"internal/cli/definition.go: seen[key]": "" +
			"dedups the replicas of one definition, one per node, so no key meets another spelling of itself",
		"internal/cli/handlers.go: seen[name]":                       oneListing,
		"internal/cli/handlers.go: sizes[name]":                      oneListing,
		"internal/cli/view/resource.go: in.VolumeSizesKib[res.Name]": oneListing,
		"pkg/rest/node_connections.go: conn[key]": "" +
			"the key is one storedPairKeys read off the same map",
		"pkg/rest/node_connections.go: ctrlConfig.Spec.NodeConnections[pairKeyIn(ctrlConfig.Spec.NodeConnections, src, dst)]": "" +
			"pairKeyIn finds the pair's stored key by naming.Same",
		"pkg/rest/physical_storage.go: entry.Nodes[devs[i].NodeName]": "" +
			"a wire map whose keys are shown; filled and read from one device listing",
		"pkg/rest/query_size_info.go: result[rgs[i].Name]": "" +
			"a wire map whose keys are shown, one entry per stored group",
		"pkg/rest/resource_connections.go: rd.Props[key]": "" +
			"pairPropKey finds the pair's stored prop by naming.Same",
		"pkg/rest/snapshots.go: seen[key]": "" +
			"dedups the replicas of one definition, one per node, so no key meets another spelling of itself",
		"pkg/store/clone_snapshot.go: out.InUse[snaps[i].Name]": "" +
			"keys are shown in the operator's message; filled and read from one listing",
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}

	return root
}

// Code above the store compares and indexes object names only as keys. Every
// earlier fix to name handling fixed one more place that compared two
// spellings as they were, or folded case where the store hashes; this keeps
// the class from coming back. A hit is fixed with naming.Key or naming.Same,
// or, when both sides come from one source and must stay as they are, added
// to allowed with the reason.
func TestNamesAreComparedAsKeys(t *testing.T) {
	t.Parallel()

	root := moduleRoot(t)
	reviewed := allowed()
	seen := map[string]bool{}

	for _, v := range newGuard(t, root, "./...").violations(root) {
		seen[v] = true

		if _, ok := reviewed[v]; !ok {
			t.Errorf("%s: compare or key the name with naming.Key or naming.Same", v)
		}
	}

	for v := range reviewed {
		if !seen[v] {
			t.Errorf("allowed entry %q matches nothing any more; remove it", v)
		}
	}
}

// The guard follows where a name comes from, not what it is called: every
// planted shape is caught under names no vocabulary would guess, and the
// right ways to compare are left alone.
func TestTheGuardCatchesPlantedShapes(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(moduleRoot(t), "pkg", "naming", "testdata", "planted")

	src, err := os.ReadFile(filepath.Join(dir, "planted.go"))
	if err != nil {
		t.Fatal(err)
	}

	var want []string

	for i, line := range strings.Split(string(src), "\n") {
		if strings.HasSuffix(line, "// want") {
			want = append(want, strconv.Itoa(i+1))
		}
	}

	found := newGuard(t, dir, ".").findings(dir)
	got := make([]string, 0, len(found))

	for _, f := range found {
		got = append(got, strconv.Itoa(f.line))
	}

	sort.Strings(got)
	sort.Strings(want)

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("reported lines %v, want %v", got, want)
	}
}

func TestInventory(t *testing.T) {
	if os.Getenv("NAMING_INVENTORY") == "" {
		t.Skip("set NAMING_INVENTORY=1 to print every hit")
	}

	root := moduleRoot(t)

	for _, v := range newGuard(t, root, "./...").violations(root) {
		t.Log(v)
	}
}
