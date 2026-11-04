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
// enter to where two spellings could meet, and fails where one gets there
// without naming.Key or naming.Same on the way. It works on types and on
// where a value comes from, not on what a variable is called.
//
// A name enters (a source) as: a name field of a wire, CRD, REST, CLI,
// store, satellite intent or DRBD configuration type (sourceFields, and Name
// on the types in namedTypes); a
// REST path parameter or query list that names objects (pathKeys,
// multiValueQuery); a CLI flag that names one (flagKeys); the node a
// satellite controller is configured with; and anything passed to a store
// method's name parameter (storeParams), whatever it is called.
//
// It travels through: assignment and declaration; range over a slice, over
// a map's values, and over the keys of a map (a variable or a field) that
// something keys by a raw name, maps.Keys of one included; indexing, slicing, dereference and type assertion;
// concatenation, fmt.Sprint*, strings.Join, path.Join and the strings
// trimming, cutting, splitting and case helpers; append, and slice, array and map
// literals; a write into a slice or map element, which taints the whole
// collection; string fields of this module's structs other than the
// source types, through composite literals and assignments, per field
// rather than per value; string conversions; calls, into the parameters of
// a declared function, a closure bound to a local variable or called in
// place, the function a slices.*Func call is given, and every method in
// the tree that implements a method of one of this module's interfaces;
// and back out of a call through its returns, with a parameter the callee
// returns standing for whatever that one call passes there, so a helper
// that returns what it is given returns a name only to callers that give
// it one. It runs to a fixpoint. naming.Key's result and a CR's
// metadata.name are keys, not raw names. Only values that can hold a
// string are tainted; a struct is followed field by field.
//
// It reports (a sink): == and != between two names, one of them raw, the
// other raw or a key (naming.Key's result, a helper that returns one, a CR's
// metadata.name or GetName()); a switch on a name with a case of another; strings.HasPrefix, HasSuffix,
// Contains, CutPrefix and CutSuffix, and slices.Contains and Index, between
// two names; strings.EqualFold, ToLower and ToUpper on a raw name; a map
// indexed, a map literal keyed, or a map entry deleted, by a raw name or a
// key built from one;
// and a CR addressed by a raw name. One field read off two elements of the
// same collection, by index or as the two arguments of the comparator a
// slices sort is given, is one source and not a pair.
//
// It does not see: channels, package-level variables, pointers aliased
// across calls, reflection, method values, function values other than the
// closures above, interfaces declared outside this module (error, Stringer
// and the like), calls into code outside this module other than the ones
// listed, generic code whose type parameter hides the string, and a name
// that leaves the process and comes back (a file, a kernel listing, a CR
// read by its key). A parameter is raw for every caller once one caller
// passes a raw name, and a field for every value once one value holds one,
// which errs towards reporting.

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
		"pool": true, "snap": true, "target": true,
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
// the CRDs' specs, the REST surface, the CLI's parsed arguments, the
// store's own types, the intent the dispatcher hands a satellite and the
// DRBD configuration a satellite renders from it.
func sourcePackages() map[string]bool {
	return map[string]bool{
		module + "/pkg/api/v1": true, module + "/api/v1alpha1": true, module + "/pkg/rest": true,
		module + "/internal/cli": true, module + "/pkg/store": true, module + "/pkg/satellite/intent": true,
		module + "/pkg/drbd": true,
	}
}

// namedTypes are the types whose own Name is an object name as some client
// spelled it: the wire types, in the spelling they were registered under,
// and an intent's peer, under the spelling its row stores. A CRD's own Name
// is its CR name, a key, and so is not a source.
func namedTypes() map[string]bool {
	out := map[string]bool{module + "/pkg/satellite/intent.DesiredPeer": true}

	for _, t := range []string{
		"Node", "ResourceDefinition", "ResourceGroup", "Resource", "ResourceWithVolumes", "Snapshot",
		"StoragePool", "StoragePoolDefinition",
	} {
		out[module+"/pkg/api/v1."+t] = true
	}

	return out
}

// fn is a body the guard walks: a declared function or method, or a
// function literal.
type fn struct {
	typ  *ast.FuncType
	body *ast.BlockStmt
	info *types.Info
}

type guard struct {
	pkgs    []*packages.Package
	tainted map[types.Object]labels
	reached map[types.Object]bool
	// mapKeys are the maps, variables or fields, some write keys by a raw
	// name; ranging over their keys yields names.
	mapKeys map[types.Object]bool
	keyed   map[types.Object]bool
	// returns and fns are keyed by *types.Func or *ast.FuncLit.
	returns map[any]labels
	// keyReturns are the functions that return a key.
	keyReturns map[any]bool
	fns        map[any]*fn
	// litOf is the literal a local variable is bound to.
	litOf map[types.Object]*ast.FuncLit
	// methods are the concrete methods with a body, by name, for calls
	// through an interface.
	methods map[string][]*types.Func
	// siblings pairs the two parameters of a comparator a sort is given:
	// two elements of one collection.
	siblings map[types.Object]types.Object
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
		pkgs: pkgs, tainted: map[types.Object]labels{}, reached: map[types.Object]bool{}, mapKeys: map[types.Object]bool{}, keyed: map[types.Object]bool{},
		returns: map[any]labels{}, keyReturns: map[any]bool{}, fns: map[any]*fn{}, litOf: map[types.Object]*ast.FuncLit{},
		methods: map[string][]*types.Func{}, siblings: map[types.Object]types.Object{},
	}

	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			g.collect(pkg.TypesInfo, file)
		}
	}

	g.propagate()

	return g
}

// collect records the bodies in file, and which local variables are bound
// to a function literal.
func (g *guard) collect(info *types.Info, file *ast.File) {
	for _, d := range file.Decls {
		decl, ok := d.(*ast.FuncDecl)
		if !ok || decl.Body == nil {
			continue
		}

		obj, ok := info.Defs[decl.Name].(*types.Func)
		if !ok {
			continue
		}

		g.fns[obj] = &fn{typ: decl.Type, body: decl.Body, info: info}

		if decl.Recv != nil {
			g.methods[obj.Name()] = append(g.methods[obj.Name()], obj)
		}
	}

	bind := func(lhs []ast.Expr, rhs []ast.Expr) {
		if len(lhs) != len(rhs) {
			return
		}

		for i := range rhs {
			if lit, ok := ast.Unparen(rhs[i]).(*ast.FuncLit); ok {
				if obj := identObj(info, lhs[i]); obj != nil {
					g.litOf[obj] = lit
				}
			}
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			g.pairComparator(info, x)
		case *ast.FuncLit:
			g.fns[x] = &fn{typ: x.Type, body: x.Body, info: info}
		case *ast.AssignStmt:
			bind(x.Lhs, x.Rhs)
		case *ast.ValueSpec:
			lhs := make([]ast.Expr, len(x.Names))
			for i, id := range x.Names {
				lhs[i] = id
			}

			bind(lhs, x.Values)
		}

		return true
	})
}

// pairComparator records the parameters of a comparator a sort of one
// collection is given as siblings.
func (g *guard) pairComparator(info *types.Info, call *ast.CallExpr) {
	switch funcName(calleeOf(info, call)) {
	case "slices.SortFunc", "slices.SortStableFunc", "slices.IsSortedFunc", "slices.MinFunc", "slices.MaxFunc",
		"slices.CompactFunc":
	default:
		return
	}

	lit, ok := ast.Unparen(call.Args[len(call.Args)-1]).(*ast.FuncLit)
	if !ok {
		return
	}

	var params []types.Object

	for _, field := range lit.Type.Params.List {
		for _, id := range field.Names {
			params = append(params, info.Defs[id])
		}
	}

	if len(params) == 2 && params[0] != nil && params[1] != nil {
		g.siblings[params[0]] = params[1]
		g.siblings[params[1]] = params[0]
	}
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

// calleeOf is the function a call statically names, if any: for a call
// through an interface, the interface's method.
func calleeOf(info *types.Info, call *ast.CallExpr) *types.Func {
	var f *types.Func

	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		f, _ = info.Uses[fun].(*types.Func)
	case *ast.SelectorExpr:
		if sel, ok := info.Selections[fun]; ok {
			f, _ = sel.Obj().(*types.Func)
		} else {
			f, _ = info.Uses[fun.Sel].(*types.Func)
		}
	case *ast.IndexExpr:
		if id, ok := fun.X.(*ast.Ident); ok {
			f, _ = info.Uses[id].(*types.Func)
		}
	}

	if f == nil {
		return nil
	}

	return f.Origin()
}

func funcName(f *types.Func) string {
	if f == nil || f.Pkg() == nil {
		return ""
	}

	if recv := f.Type().(*types.Signature).Recv(); recv != nil {
		t := types.Unalias(recv.Type())
		if p, ok := t.(*types.Pointer); ok {
			t = types.Unalias(p.Elem())
		}

		if n, ok := t.(*types.Named); ok {
			return f.Pkg().Path() + "." + n.Obj().Name() + "." + f.Name()
		}
	}

	return f.Pkg().Path() + "." + f.Name()
}

// callees are the bodies a call may run: a literal called in place or
// through the variable bound to it, a declared function, or every method in
// the tree that implements the interface method called.
func (g *guard) callees(info *types.Info, call *ast.CallExpr) []any {
	fun := ast.Unparen(call.Fun)
	if lit, ok := fun.(*ast.FuncLit); ok {
		return []any{lit}
	}

	if id, ok := fun.(*ast.Ident); ok {
		if lit := g.litOf[info.Uses[id]]; lit != nil {
			return []any{lit}
		}
	}

	f := calleeOf(info, call)
	if f == nil {
		return nil
	}

	if _, ok := g.fns[f]; ok {
		return []any{f}
	}

	// Only the module's own interfaces: every error in the tree is not
	// what one call of Error runs.
	recv := f.Type().(*types.Signature).Recv()
	if recv == nil || !types.IsInterface(recv.Type()) || f.Pkg() == nil || !strings.HasPrefix(f.Pkg().Path(), module) {
		return nil
	}

	iface, _ := recv.Type().Underlying().(*types.Interface)
	if iface == nil {
		return nil
	}

	var out []any

	for _, m := range g.methods[f.Name()] {
		rt := types.Unalias(m.Type().(*types.Signature).Recv().Type())
		if p, ok := rt.(*types.Pointer); ok {
			rt = p.Elem()
		}

		if types.Implements(rt, iface) || types.Implements(types.NewPointer(rt), iface) {
			out = append(out, m)
		}
	}

	return out
}

func (g *guard) params(key any) []types.Object {
	f := g.fns[key]
	if f == nil || f.typ.Params == nil {
		return nil
	}

	var out []types.Object

	for _, field := range f.typ.Params.List {
		for _, id := range field.Names {
			out = append(out, f.info.Defs[id])
		}

		if len(field.Names) == 0 {
			out = append(out, nil)
		}
	}

	return out
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
		if isKeyCall(info, x) || funcName(calleeOf(info, x)) == "k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta.GetName" {
			return true
		}

		// A helper that returns naming.Key's result returns a key, unless
		// it returns a raw name too.
		callees := g.callees(info, x)
		for _, c := range callees {
			if !g.keyReturns[c] {
				return false
			}
		}

		return len(callees) > 0 && !g.isTainted(info, x)
	case *ast.Ident:
		obj := info.Uses[x]

		return obj != nil && g.keyed[obj] && !g.raw(g.tainted[obj])
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
func isSourceField(info *types.Info, sel *ast.SelectorExpr) bool {
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
		return namedTypes()[named.Obj().Pkg().Path()+"."+named.Obj().Name()]
	}

	return sourceFields()[name]
}

// passThrough are the calls whose result holds whatever names their
// arguments do.
func passThrough() map[string]bool {
	return map[string]bool{
		"strings.TrimSpace": true, "strings.ToLower": true, "strings.ToUpper": true, "strings.Trim": true,
		"strings.TrimPrefix": true, "strings.TrimSuffix": true, "strings.TrimLeft": true, "strings.TrimRight": true,
		"strings.Split": true, "strings.SplitN": true, "strings.Cut": true, "strings.CutPrefix": true,
		"strings.CutSuffix": true, "strings.Fields": true, "strings.Join": true,
		"fmt.Sprintf": true, "fmt.Sprint": true, "path.Join": true, "slices.Clone": true, "slices.Sorted": true,
	}
}

// labels is where a value's names may come from: src, for a source, or a
// parameter of the function it sits in, for whatever a caller passes there.
// Keeping parameters apart lets a helper that returns what it is given
// return a name to the callers that give it one and to no other.
type labels map[any]bool

type srcLabel struct{}

func union(sets ...labels) labels {
	var out labels

	for _, s := range sets {
		for l := range s {
			if out == nil {
				out = labels{}
			}

			out[l] = true
		}
	}

	return out
}

// raw reports whether a value with these labels may hold a name as some
// caller spelled it: it comes from a source, or from a parameter some call
// passes one into.
func (g *guard) raw(set labels) bool {
	for l := range set {
		if l == (srcLabel{}) {
			return true
		}

		if p, ok := l.(types.Object); ok && g.reached[p] {
			return true
		}
	}

	return false
}

// isTainted reports whether e may hold an object name in a spelling of its
// own: read from a source, or derived from one.
func (g *guard) isTainted(info *types.Info, e ast.Expr) bool {
	return g.raw(g.labelsOf(info, e))
}

func (g *guard) labelsOf(info *types.Info, e ast.Expr) labels {
	switch x := ast.Unparen(e).(type) {
	case *ast.Ident:
		obj := info.Uses[x]
		if obj == nil {
			obj = info.Defs[x]
		}

		return g.tainted[obj]
	case *ast.SelectorExpr:
		if isSourceField(info, x) {
			return labels{srcLabel{}: true}
		}

		if selection, ok := info.Selections[x]; ok && selection.Kind() == types.FieldVal {
			return g.tainted[selection.Obj()]
		}
	case *ast.IndexExpr:
		if isFlagSource(info, x) {
			return labels{srcLabel{}: true}
		}

		return g.labelsOf(info, x.X)
	case *ast.SliceExpr:
		return g.labelsOf(info, x.X)
	case *ast.StarExpr:
		return g.labelsOf(info, x.X)
	case *ast.TypeAssertExpr:
		return g.labelsOf(info, x.X)
	case *ast.BinaryExpr:
		if x.Op == token.ADD {
			return union(g.labelsOf(info, x.X), g.labelsOf(info, x.Y))
		}
	case *ast.CompositeLit:
		if _, ok := info.TypeOf(x).Underlying().(*types.Struct); ok {
			return nil
		}

		var out labels

		for _, elt := range x.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				elt = kv.Value
			}

			out = union(out, g.labelsOf(info, elt))
		}

		return out
	case *ast.CallExpr:
		return g.callLabels(info, x)
	}

	return nil
}

func (g *guard) argLabels(info *types.Info, args []ast.Expr) labels {
	var out labels

	for _, arg := range args {
		out = union(out, g.labelsOf(info, arg))
	}

	return out
}

func (g *guard) anyTainted(info *types.Info, args []ast.Expr) bool {
	return g.raw(g.argLabels(info, args))
}

func (g *guard) callLabels(info *types.Info, call *ast.CallExpr) labels {
	if isKeyCall(info, call) {
		return nil
	}

	if isSourceCall(info, call) {
		return labels{srcLabel{}: true}
	}

	if tv, ok := info.Types[call.Fun]; ok && tv.IsType() && len(call.Args) == 1 {
		return g.labelsOf(info, call.Args[0])
	}

	if id, ok := ast.Unparen(call.Fun).(*ast.Ident); ok {
		if b, ok := info.Uses[id].(*types.Builtin); ok {
			if b.Name() == "append" {
				return g.argLabels(info, call.Args)
			}

			return nil
		}
	}

	var out labels

	for _, key := range g.callees(info, call) {
		out = union(out, g.substitute(info, key, call))
	}

	f := calleeOf(info, call)
	if funcName(f) == "maps.Keys" && len(call.Args) == 1 && g.mapKeys[mapObj(info, call.Args[0])] {
		return union(out, labels{srcLabel{}: true})
	}

	if passThrough()[funcName(f)] {
		return union(out, g.argLabels(info, call.Args))
	}

	// A method of a value that holds names outside the tree, such as
	// url.Values.Get, returns one of them.
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok && f != nil && g.fns[f] == nil && isString(info, call) {
		return union(out, g.labelsOf(info, sel.X))
	}

	return out
}

// substitute is what a call returns: what its callee returns, with each of
// the callee's parameters replaced by what this call passes there.
func (g *guard) substitute(info *types.Info, key any, call *ast.CallExpr) labels {
	ret := g.returns[key]
	if len(ret) == 0 {
		return nil
	}

	params := g.params(key)

	var out labels

	for l := range ret {
		obj, _ := l.(types.Object)

		i := -1
		if obj != nil {
			i = slices.Index(params, obj)
		}

		if i < 0 {
			// A source, or the parameter of a function a closure sits
			// in, which means the same in the caller.
			out = union(out, labels{l: true})

			continue
		}

		if i == len(params)-1 && len(call.Args) > len(params) {
			out = union(out, g.argLabels(info, call.Args[i:]))
		} else if i < len(call.Args) {
			out = union(out, g.labelsOf(info, call.Args[i]))
		}
	}

	return out
}

// holdsStrings reports whether a value of t can hold a name: a string, or a
// pointer, slice, array or map of them. A struct is never a name; its
// fields are followed one by one.
func holdsStrings(t types.Type) bool {
	switch u := t.Underlying().(type) {
	case *types.Basic:
		return u.Info()&types.IsString != 0
	case *types.Pointer:
		return holdsStrings(u.Elem())
	case *types.Slice:
		return holdsStrings(u.Elem())
	case *types.Array:
		return holdsStrings(u.Elem())
	case *types.Map:
		return holdsStrings(u.Elem())
	}

	return false
}

// add puts set into obj's labels. A field is shared by every value of its
// type, so it takes only whether the value is raw, never a parameter that
// means something in one function alone.
func (g *guard) add(obj types.Object, set labels) bool {
	if obj == nil || len(set) == 0 || !holdsStrings(obj.Type()) {
		return false
	}

	if v, ok := obj.(*types.Var); ok && v.IsField() {
		if !g.raw(set) {
			return false
		}

		set = labels{srcLabel{}: true}
	}

	cur := g.tainted[obj]
	changed := false

	for l := range set {
		if !cur[l] {
			if cur == nil {
				cur = labels{}
				g.tainted[obj] = cur
			}

			cur[l] = true
			changed = true
		}
	}

	return changed
}

// taintableField reports whether writes taint a field: a string field of a
// struct of this module whose name fields are not already declared as
// sources. A collection field would taint every element of every value
// for one write, which says nothing.
func taintableField(obj types.Object) bool {
	if obj.Pkg() == nil || !strings.HasPrefix(obj.Pkg().Path(), module) || sourcePackages()[obj.Pkg().Path()] {
		return false
	}

	b, ok := obj.Type().Underlying().(*types.Basic)

	return ok && b.Info()&types.IsString != 0
}

// propagate follows sources through assignments, ranges, literals, calls
// and returns until nothing changes.
func (g *guard) propagate() {
	for key := range g.fns {
		for _, p := range g.params(key) {
			if p != nil {
				g.add(p, labels{p: true})
			}
		}
	}

	for changed := true; changed; {
		changed = false

		for key, f := range g.fns {
			info := f.info

			ast.Inspect(f.body, func(n ast.Node) bool {
				switch s := n.(type) {
				case *ast.FuncLit:
					// Walked as a body of its own.
					return false
				case *ast.AssignStmt:
					changed = g.assign(info, s.Lhs, s.Rhs) || changed
				case *ast.ValueSpec:
					lhs := make([]ast.Expr, len(s.Names))
					for i, id := range s.Names {
						lhs[i] = id
					}

					changed = g.assign(info, lhs, s.Values) || changed
				case *ast.RangeStmt:
					// A map's values are names when it holds names, and
					// its keys when something keyed it by one.
					if s.Value != nil {
						changed = g.markExpr(info, s.Value, g.labelsOf(info, s.X)) || changed
					}

					if s.Key != nil && isStringMap(info, s.X) && g.mapKeys[mapObj(info, s.X)] {
						changed = g.markExpr(info, s.Key, labels{srcLabel{}: true}) || changed
					}
				case *ast.ReturnStmt:
					if !g.keyReturns[key] && len(s.Results) > 0 && g.isKey(info, s.Results[0]) {
						g.keyReturns[key] = true
						changed = true
					}

					set := g.argLabels(info, s.Results)
					if len(set) > 0 {
						before := len(g.returns[key])
						g.returns[key] = union(g.returns[key], set)
						changed = len(g.returns[key]) != before || changed
					}
				case *ast.CompositeLit:
					changed = g.fields(info, s) || changed
				case *ast.CallExpr:
					changed = g.passArgs(info, s) || changed
				}

				return true
			})
		}
	}
}

// fields taints the fields a struct literal sets from a name.
func (g *guard) fields(info *types.Info, lit *ast.CompositeLit) bool {
	st, ok := info.TypeOf(lit).Underlying().(*types.Struct)
	if !ok {
		return false
	}

	changed := false

	for i, elt := range lit.Elts {
		field := (*types.Var)(nil)
		value := elt

		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			value = kv.Value

			if id, ok := kv.Key.(*ast.Ident); ok {
				field, _ = info.Uses[id].(*types.Var)
			}
		} else if i < st.NumFields() {
			field = st.Field(i)
		}

		if field != nil && taintableField(field) {
			changed = g.add(field, g.labelsOf(info, value)) || changed
		}
	}

	return changed
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

// target is what a write to e taints: a variable, a field of this module's
// structs, or the whole collection an element is written into.
func target(info *types.Info, e ast.Expr) types.Object {
	switch x := ast.Unparen(e).(type) {
	case *ast.Ident:
		return identObj(info, x)
	case *ast.SelectorExpr:
		if selection, ok := info.Selections[x]; ok && selection.Kind() == types.FieldVal && taintableField(selection.Obj()) {
			return selection.Obj()
		}
	case *ast.IndexExpr:
		return target(info, x.X)
	case *ast.StarExpr:
		return target(info, x.X)
	}

	return nil
}

func (g *guard) markExpr(info *types.Info, e ast.Expr, set labels) bool {
	return g.add(target(info, e), set)
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
			if set := g.labelsOf(info, rhs[i]); len(set) > 0 {
				changed = g.markExpr(info, lhs[i], set) || changed
			} else if g.isKey(info, rhs[i]) {
				changed = g.markKeyed(info, lhs[i]) || changed
			}
		}
	case len(rhs) == 1 && len(lhs) > 0:
		set := g.labelsOf(info, rhs[0])
		for i := range lhs {
			changed = g.markExpr(info, lhs[i], set) || changed
		}
	}

	for i := range lhs {
		ix, ok := ast.Unparen(lhs[i]).(*ast.IndexExpr)
		if !ok || !isStringMap(info, ix.X) || !g.rawKey(info, ix.Index) {
			continue
		}

		if m := mapObj(info, ix.X); m != nil && !g.mapKeys[m] {
			g.mapKeys[m] = true
			changed = true
		}
	}

	return changed
}

// mapObj is the variable or field a map expression reads.
func mapObj(info *types.Info, e ast.Expr) types.Object {
	switch x := ast.Unparen(e).(type) {
	case *ast.Ident:
		return identObj(info, x)
	case *ast.SelectorExpr:
		if selection, ok := info.Selections[x]; ok && selection.Kind() == types.FieldVal {
			return selection.Obj()
		}
	}

	return nil
}

func (g *guard) reach(p types.Object) bool {
	if p == nil || g.reached[p] {
		return false
	}

	g.reached[p] = true

	return true
}

// passArgs records which parameters some call passes a raw name into and,
// the other way, taints what a call hands the store as a name.
func (g *guard) passArgs(info *types.Info, call *ast.CallExpr) bool {
	changed := false
	f := calleeOf(info, call)

	if f != nil && f.Pkg() != nil && f.Pkg().Path() == module+"/pkg/store" {
		sig, _ := f.Type().(*types.Signature)
		for i, arg := range call.Args {
			if sig != nil && i < sig.Params().Len() && storeParams()[sig.Params().At(i).Name()] &&
				isString(info, arg) && !g.isKey(info, arg) {
				changed = g.markExpr(info, arg, labels{srcLabel{}: true}) || changed
			}
		}
	}

	// slices.ContainsFunc(names, func(n string) bool { ... }) hands the
	// function each element.
	if name := funcName(f); strings.HasPrefix(name, "slices.") && strings.HasSuffix(name, "Func") &&
		len(call.Args) >= 2 && g.isTainted(info, call.Args[0]) {
		fake := &ast.CallExpr{Fun: call.Args[len(call.Args)-1]}
		for _, key := range g.callees(info, fake) {
			if params := g.params(key); len(params) > 0 {
				changed = g.reach(params[0]) || changed
			}
		}
	}

	for _, key := range g.callees(info, call) {
		params := g.params(key)

		for i, arg := range call.Args {
			p := min(i, len(params)-1)
			if p >= 0 && g.isTainted(info, arg) {
				changed = g.reach(params[p]) || changed
			}
		}
	}

	return changed
}

// sameShape reports whether two operands read one field off elements of one
// collection, by index or as the two arguments of a sort's comparator: one
// source, not two spellings.
func (g *guard) sameShape(info *types.Info, a, b ast.Expr) bool {
	sa, oka := ast.Unparen(a).(*ast.SelectorExpr)
	sb, okb := ast.Unparen(b).(*ast.SelectorExpr)

	if !oka || !okb || sa.Sel.Name != sb.Sel.Name {
		return false
	}

	if pa, pb := identObj(info, sa.X), identObj(info, sb.X); pa != nil && g.siblings[pa] == pb {
		return true
	}

	ia, oka := ast.Unparen(sa.X).(*ast.IndexExpr)
	ib, okb := ast.Unparen(sb.X).(*ast.IndexExpr)

	if !oka || !okb || types.ExprString(ia.X) != types.ExprString(ib.X) {
		return false
	}

	ra, rb := root(info, ia.X), root(info, ib.X)

	return ra != nil && ra == rb
}

// root is the variable an expression such as s.items or list reads from.
func root(info *types.Info, e ast.Expr) types.Object {
	switch x := ast.Unparen(e).(type) {
	case *ast.Ident:
		return identObj(info, x)
	case *ast.SelectorExpr:
		return root(info, x.X)
	}

	return nil
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
				if decl, ok := n.(*ast.FuncDecl); ok {
					if _, skip := excludedFuncs()[rel+":"+funcKey(decl)]; skip {
						return false
					}
				}

				for _, hit := range g.sinks(info, n) {
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
	if isConstant(info, x) || isConstant(info, y) || g.sameShape(info, x, y) {
		return false
	}

	return g.isTainted(info, x) && g.nameLike(info, y) || g.isTainted(info, y) && g.nameLike(info, x)
}

// rawKey reports whether e, used as a map key, is a raw name or built from
// one.
func (g *guard) rawKey(info *types.Info, e ast.Expr) bool {
	return !isConstant(info, e) && !isKeyCall(info, e) && g.isTainted(info, e)
}

func isStringMap(info *types.Info, e ast.Expr) bool {
	t := info.TypeOf(e)
	if t == nil {
		return false
	}

	m, ok := t.Underlying().(*types.Map)
	if !ok {
		return false
	}

	k, ok := m.Key().Underlying().(*types.Basic)

	return ok && k.Info()&types.IsString != 0
}

// sinks returns the nodes to report when n is a sink a source reaches.
func (g *guard) sinks(info *types.Info, n ast.Node) []ast.Node {
	switch e := n.(type) {
	case *ast.BinaryExpr:
		if (e.Op == token.EQL || e.Op == token.NEQ) && isString(info, e.X) && isString(info, e.Y) && g.rawPair(info, e.X, e.Y) {
			return []ast.Node{e}
		}
	case *ast.SwitchStmt:
		if e.Tag == nil || !isString(info, e.Tag) {
			return nil
		}

		var out []ast.Node

		for _, stmt := range e.Body.List {
			for _, c := range stmt.(*ast.CaseClause).List {
				if g.rawPair(info, e.Tag, c) {
					out = append(out, c)
				}
			}
		}

		return out
	case *ast.IndexExpr:
		if isStringMap(info, e.X) && g.rawKey(info, e.Index) {
			return []ast.Node{e}
		}
	case *ast.CallExpr:
		return g.callSinks(info, e)
	case *ast.CompositeLit:
		return g.literalSinks(info, e)
	}

	return nil
}

func (g *guard) callSinks(info *types.Info, call *ast.CallExpr) []ast.Node {
	if id, ok := ast.Unparen(call.Fun).(*ast.Ident); ok {
		if b, ok := info.Uses[id].(*types.Builtin); ok && b.Name() == "delete" && len(call.Args) == 2 &&
			isStringMap(info, call.Args[0]) && g.rawKey(info, call.Args[1]) {
			return []ast.Node{call}
		}
	}

	switch funcName(calleeOf(info, call)) {
	case "slices.Contains", "slices.Index", "strings.HasPrefix", "strings.HasSuffix", "strings.Contains",
		"strings.CutPrefix", "strings.CutSuffix":
		if len(call.Args) == 2 && g.rawPair(info, call.Args[0], call.Args[1]) {
			return []ast.Node{call}
		}
	case "strings.EqualFold", "strings.ToLower", "strings.ToUpper":
		if g.anyTainted(info, call.Args) {
			return []ast.Node{call}
		}
	}

	return nil
}

func (g *guard) literalSinks(info *types.Info, lit *ast.CompositeLit) []ast.Node {
	var out []ast.Node

	switch {
	case isStringMap(info, lit):
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok && g.rawKey(info, kv.Key) {
				out = append(out, kv)
			}
		}
	case isCRAddress(info, lit):
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}

			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Name" && !isKeyCall(info, kv.Value) && g.isTainted(info, kv.Value) {
				out = append(out, kv)
			}
		}
	}

	return out
}

// excludedDirs are the trees the rule does not reach, and why.
func excludedDirs() map[string]string {
	return map[string]string{
		"pkg/naming": "defines the key",
		"pkg/store/k8s": "the store backend: it resolves a caller's spelling to the CR name, " +
			"and its scoped reads ask in one given spelling at a time, as the field selector does",
		"pkg/store/storetest": "the conformance suite, which writes spellings on purpose",
		"third_party":         "vendored code",
	}
}

// excludedFuncs are single functions out of reach, as "path:Func" or
// "path:Type.Method", and why. The rest of each file is checked.
func excludedFuncs() map[string]string {
	device := "the in-memory store backend for physical devices, keyed by the device name and asked " +
		"for a node in one spelling, as the Kubernetes one is"
	fake := "the scripted command runner tests drive the storage backends with, keyed by the exact " +
		"command line a test expects"

	kernel := "names the kernel reports, compared and keyed only with each other"

	return map[string]string{
		"pkg/drbd/drbdadm.go:Adm.Day0SiblingSetConnected":                                      kernel,
		"pkg/drbd/drbdsetup_show.go:parseShowJSON":                                             kernel,
		"pkg/store/inmemory_physicaldevice.go:inMemoryPhysicalDevices.List":                    device,
		"pkg/store/inmemory_physicaldevice.go:inMemoryPhysicalDevices.ListForNode":             device,
		"pkg/store/inmemory_physicaldevice.go:inMemoryPhysicalDevices.Get":                     device,
		"pkg/store/inmemory_physicaldevice.go:inMemoryPhysicalDevices.PatchPhysicalDeviceSpec": device,
		"pkg/store/inmemory_physicaldevice.go:inMemoryPhysicalDevices.Delete":                  device,
		"pkg/storage/exec.go:FakeExec.RunWithStdin":                                            fake,
	}
}

func funcKey(decl *ast.FuncDecl) string {
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return decl.Name.Name
	}

	t := decl.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}

	if idx, ok := t.(*ast.IndexExpr); ok {
		t = idx.X
	}

	if id, ok := t.(*ast.Ident); ok {
		return id.Name + "." + decl.Name.Name
	}

	return decl.Name.Name
}

func excluded(rel string) bool {
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
	oneIntent := "option keys the dispatcher writes and the satellite reads back by the intent's own peer " +
		"names, the one list both sides iterate"
	shownKeys := "a wire map whose keys are shown, filled from one listing"
	satIntent := "the satellite's map of one intent's peers, written and read by the intent's own peer names"
	pools := "the satellite resolves a provider under the pool name the StoragePool registered; a replica " +
		"naming its pool in another spelling fails loudly as not registered, and keying the map would change " +
		"the pool names its other readers see"

	return map[string]string{
		"pkg/rest/node_connections.go: delete(conn, key)": "" +
			"the key is one storedPairKeys read off the same map",
		"pkg/rest/resource_connections.go: delete(rd.Props, key)": "" +
			"pairPropKey finds the pair's stored prop by naming.Same",
		"pkg/satellite/reconciler.go: opts[\"peer.\"+peer+\".address\"]":  oneIntent,
		"pkg/satellite/reconciler.go: opts[\"peer.\"+peer+\".diskless\"]": oneIntent,
		"pkg/satellite/reconciler.go: opts[\"peer.\"+peer+\".node-id\"]":  oneIntent,
		"pkg/satellite/reconciler.go: opts[\"peer.\"+peer+\".port\"]":     oneIntent,
		"pkg/satellite/reconciler.go: out[peer]":                          satIntent,
		"pkg/satellite/reconciler.go: delete(r.cfg.Providers, pool)":      pools,
		"pkg/satellite/reconciler.go: r.cfg.Providers[pool]":              pools,
		"pkg/satellite/reconciler.go: r.resourceToPool[name]": "" +
			"keyed by the resource name the intent carries, the definition's CR name, on write and read",
		"pkg/dispatcher/dispatcher.go: drbdOpts[\"peer.\"+peer+\".address\"]":  oneIntent,
		"pkg/dispatcher/dispatcher.go: drbdOpts[\"peer.\"+peer+\".diskless\"]": oneIntent,
		"pkg/dispatcher/dispatcher.go: drbdOpts[\"peer.\"+peer+\".node-id\"]":  oneIntent,
		"pkg/dispatcher/dispatcher.go: drbdOpts[\"peer.\"+peer+\".port\"]":     oneIntent,
		"pkg/rest/snapshots.go: objRefs[\"CreateTimestamp/\"+node.NodeName]":   shownKeys,
		"pkg/rest/snapshots.go: objRefs[\"SnapshotUuid/\"+node.NodeName]":      shownKeys,
		"pkg/satellite/kernel_cleanup.go: expectedSet[n]": "" +
			"the intent's peer names, against kernel peer slots the satellite named from the same intent",
		"pkg/store/cascade.go: slices.Contains(spellings, spelling)": "" +
			"the store's own list of the spellings it asks under, which differ on purpose; deduplicated as written",
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
		"pkg/store/clone_snapshot.go: l.InUse[snapshot]": "" +
			"reads back the keys of the same map it ranges over",
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
