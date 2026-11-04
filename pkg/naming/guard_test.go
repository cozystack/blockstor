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
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// refField is a field that names another object: a replica's definition and
// node, a definition's group, a pool's node, a snapshot's definition and nodes.
// The object it names may be registered, and other rows may store it, in
// another spelling, so these fields are where spellings meet.
var refField = regexp.MustCompile(`^(NodeName|ResourceName|ResourceDefinitionName|RscName|ResourceGroupName|StoragePoolName|PoolName|Nodes|NodeNameList|SnapshotName|PeerName)$`)

// refVar is a variable that carries such a reference, as the code names one.
var refVar = regexp.MustCompile(`^(node|nodeName|rd|rdName|rgName|pool|poolName|snapName|peer|peerName|removedNode|takeoverNode|targetNode|srcRD|srcNode|wireName|selfNode)$`)

// objectBase is the leaf of a value whose own Name is a node, definition,
// group, pool or snapshot name: nodes[i].Name, rd.Name, groups[i].Name.
var objectBase = regexp.MustCompile(`(?i)^(nodes?|n|node\w*|rds?|def|defs|definitions?|rgs?|groups?|resourcegroups?|pools?|snaps?|snapshots?|peers?)$`)

// objectName is any expression that holds a LINSTOR object name, for the
// fold rule: a reference, or the Name of an object.
var objectName = regexp.MustCompile(`(?i)^(node|nodes|nodename|rd|rdname|rg|rgname|resourcename|resourcedefinitionname|rscname|resourcegroupname|groupname|pool|poolname|storagepoolname|snap|snapname|snapshotname|peer|peername|name|wanted|want|candidate|needle|.*marker)$`)

// leaf names the last identifier an expression ends in: x for x, Sel for
// a.b.Sel, the indexed value for v[i], the function for f(...).
func leaf(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.IndexExpr:
		return leaf(e.X)
	case *ast.StarExpr:
		return leaf(e.X)
	case *ast.ParenExpr:
		return leaf(e.X)
	case *ast.CallExpr:
		return leaf(e.Fun)
	}

	return ""
}

// isRef reports whether expr reads a reference: a refField, or a refVar.
func isRef(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.SelectorExpr:
		if e.Sel.Name == "Name" {
			return objectBase.MatchString(leaf(e.X))
		}

		return refField.MatchString(e.Sel.Name)
	case *ast.Ident:
		return refVar.MatchString(e.Name)
	case *ast.ParenExpr:
		return isRef(e.X)
	case *ast.BinaryExpr:
		return e.Op == token.ADD && (isRef(e.X) || isRef(e.Y))
	}

	return false
}

// isObjectName reports whether expr may hold an object name, for the fold
// rule.
func isObjectName(expr ast.Expr) bool {
	if bin, ok := expr.(*ast.BinaryExpr); ok && bin.Op == token.ADD {
		return isObjectName(bin.X) || isObjectName(bin.Y)
	}

	return isRef(expr) || objectName.MatchString(leaf(expr))
}

// keyedVars holds, for the function being walked, the variables assigned
// from naming.Key: they hold a key already.
type keyedVars map[string]bool

// collectKeyed records every variable fn assigns from a naming.Key call.
func collectKeyed(fn ast.Node) keyedVars {
	out := keyedVars{}

	ast.Inspect(fn, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != len(assign.Rhs) {
			return true
		}

		for i, lhs := range assign.Lhs {
			if id, ok := lhs.(*ast.Ident); ok && isKeyed(assign.Rhs[i], nil) {
				out[id.Name] = true
			}
		}

		return true
	})

	return out
}

// isKeyed reports whether expr is already a key: a call to naming.Key, a
// variable assigned from one, nil, or a string literal.
func isKeyed(expr ast.Expr, keyed keyedVars) bool {
	switch e := expr.(type) {
	case *ast.BasicLit:
		return true
	case *ast.Ident:
		return e.Name == "nil" || keyed[e.Name]
	case *ast.ParenExpr:
		return isKeyed(e.X, keyed)
	case *ast.CallExpr:
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok {
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "naming" && sel.Sel.Name == "Key" {
				return true
			}
		}
	}

	return false
}

// sameShape reports whether two operands read the same field of two
// elements of one collection, as a sort comparator does: out[i].X and
// out[j].X hold names of one source, and comparing them is not mixing
// spellings.
func sameShape(a, b ast.Expr) bool {
	sa, oka := a.(*ast.SelectorExpr)
	sb, okb := b.(*ast.SelectorExpr)

	if !oka || !okb || sa.Sel.Name != sb.Sel.Name {
		return false
	}

	if pa, ok := sa.X.(*ast.Ident); ok {
		if pb, ok := sb.X.(*ast.Ident); ok && isComparatorPair(pa.Name, pb.Name) {
			return true
		}
	}

	ia, oka := sa.X.(*ast.IndexExpr)
	ib, okb := sb.X.(*ast.IndexExpr)

	return oka && okb && fmt.Sprint(ia.X) == fmt.Sprint(ib.X)
}

// isComparatorPair reports whether two names are the parameter pair a sort
// comparator is written with.
func isComparatorPair(a, b string) bool {
	switch a + "," + b {
	case "a,b", "left,right", "x,y", "lhs,rhs":
		return true
	}

	return false
}

func callee(call *ast.CallExpr) string {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		if pkg, ok := sel.X.(*ast.Ident); ok {
			return pkg.Name + "." + sel.Sel.Name
		}
	}

	return ""
}

// violations walks the Go sources under the repository's pkg, internal and
// cmd trees and returns every place that folds case on a name, compares two
// name spellings as they are, or indexes a map by one, keyed
// "path: code".
func violations(t *testing.T, root string) []string {
	t.Helper()

	var out []string

	fset := token.NewFileSet()

	for _, dir := range []string{"pkg", "internal", "cmd"} {
		if _, err := os.Stat(filepath.Join(root, dir)); os.IsNotExist(err) {
			continue
		}

		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)

			if d.IsDir() {
				if excludedDir(rel) {
					return filepath.SkipDir
				}

				return nil
			}

			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.HasPrefix(d.Name(), "zz_generated") {
				return nil
			}

			if _, skip := excludedFiles()[rel]; skip {
				return nil
			}

			src, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("read %s: %w", rel, err)
			}

			file, err := parser.ParseFile(fset, path, src, 0)
			if err != nil {
				return fmt.Errorf("parse %s: %w", rel, err)
			}

			code := func(n ast.Node) string {
				return strings.Join(strings.Fields(string(src[fset.Position(n.Pos()).Offset:fset.Position(n.End()).Offset])), " ")
			}

			var keyed keyedVars

			ast.Inspect(file, func(n ast.Node) bool {
				switch e := n.(type) {
				case *ast.FuncDecl:
					keyed = collectKeyed(e)
				case *ast.CallExpr:
					switch callee(e) {
					case "slices.Contains", "slices.Index":
						if len(e.Args) == 2 && isRef(e.Args[1]) && !isKeyed(e.Args[1], keyed) {
							out = append(out, rel+": "+code(e))
						}
					case "strings.EqualFold", "strings.ToLower", "strings.ToUpper":
						for _, arg := range e.Args {
							if isObjectName(arg) {
								out = append(out, rel+": "+code(e))

								break
							}
						}
					}
				case *ast.BinaryExpr:
					if e.Op != token.EQL && e.Op != token.NEQ {
						return true
					}

					if isKeyed(e.X, keyed) || isKeyed(e.Y, keyed) || !isRef(e.X) && !isRef(e.Y) || sameShape(e.X, e.Y) {
						return true
					}

					out = append(out, rel+": "+code(e))
				case *ast.IndexExpr:
					// A key built by joining a name with literal text is an
					// option or a label, written and read the same way.
					if bin, ok := e.Index.(*ast.BinaryExpr); ok && bin.Op == token.ADD {
						return true
					}

					if isRef(e.Index) && !isKeyed(e.Index, keyed) {
						out = append(out, rel+": "+code(e))
					}
				}

				return true
			})

			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	sort.Strings(out)

	return out
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
		"pkg/store/inmemory.go":                     "the in-memory store backend, keyed as stored",
		"pkg/store/inmemory_physicaldevice.go":      "the in-memory store backend, asked in one spelling as the k8s one is",
		"pkg/store/inmemory_resource_definition.go": "the in-memory store backend, keyed as stored",
		"pkg/store/inmemory_resource_group.go":      "the in-memory store backend, keyed as stored",
		"pkg/store/inmemory_storage_pool.go":        "the in-memory store backend, asked in one spelling as the k8s one is",
		"pkg/store/inmemory_volume_definition.go":   "the in-memory store backend, keyed as stored",
		"pkg/satellite/reconciler.go": "works on the intent the dispatcher built and on the kernel; " +
			"every name in it comes from that one intent",
	}
}

func excludedDir(rel string) bool {
	_, ok := excludedDirs()[rel]

	return ok
}

// allowed are reviewed sites the rule matches that compare or key names of
// one source, and why each is right as it is.
func allowed() map[string]string {
	return map[string]string{
		"internal/cli/view/resource.go: res.LayerObject.Drbd.Connections[peer]": "" +
			"peer ranges over the keys of the same reported connection map",
		"pkg/rest/physical_storage.go: entry.Nodes[devs[i].NodeName]": "" +
			"a wire map whose keys are shown; filled and read from one device listing",
		"pkg/rest/query_size_info.go: result[rgs[i].Name]": "" +
			"a wire map whose keys are shown, one entry per stored group",
		"pkg/satellite/controllers/observer.go: o.connCache[ev.ResourceName]": "" +
			"caches keyed by the kernel's resource name, written and read from kernel events",
		"pkg/satellite/controllers/observer.go: o.lifecycleCache[ev.ResourceName]": "" +
			"caches keyed by the kernel's resource name, written and read from kernel events",
		"pkg/satellite/controllers/observer.go: o.resCache[ev.ResourceName]": "" +
			"caches keyed by the kernel's resource name, written and read from kernel events",
		"pkg/satellite/controllers/observer.go: o.volCache[ev.ResourceName]": "" +
			"caches keyed by the kernel's resource name, written and read from kernel events",
		"pkg/satellite/controllers/observer.go: sig.PerConnection[peerName]": "" +
			"keyed by the kernel's peer name, written and read from kernel events",
		"pkg/satellite/controllers/physicaldevice_discovery.go: existing.Labels[blockstoriov1alpha1.PhysicalDeviceLabelNode] != p.NodeName": "" +
			"keeps the label's stored spelling in step with the device row it labels",
		"pkg/store/clone_snapshot.go: out.InUse[snaps[i].Name]": "" +
			"keys are shown in the operator's message; filled and read from one listing",
	}
}

// Code above the store compares and indexes object names only as keys. Every
// earlier fix to name handling fixed one more place that compared two
// spellings as they were, or folded case where the store hashes; this keeps
// the class from coming back. A hit is fixed with naming.Key or naming.Same,
// or, when both sides come from one source and must stay as they are, added
// to allowed with the reason.
func TestNamesAreComparedAsKeys(t *testing.T) {
	t.Parallel()

	seen := map[string]bool{}
	reviewed := allowed()

	for _, v := range violations(t, filepath.Join("..", "..")) {
		seen[v] = true

		if _, ok := reviewed[v]; !ok {
			t.Errorf("%s: compare or index object names through naming.Key or naming.Same", v)
		}
	}

	for v := range reviewed {
		if !seen[v] {
			t.Errorf("allowed entry %q matches nothing any more; remove it", v)
		}
	}
}

// The rule finds each shape it exists for.
func TestTheGuardFindsEveryShape(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dir := filepath.Join(root, "pkg", "planted")

	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	src := `package planted

import (
	"slices"
	"strings"
)

type replica struct{ NodeName, ResourceName string }

func fold(r replica, node string) bool { return strings.EqualFold(r.NodeName, node) }

func raw(r replica, node string) bool { return r.NodeName == node }

func keyed(m map[string]int, r replica) int { return m[r.ResourceName] }

func contains(nodes []string, r replica) bool { return slices.Contains(nodes, r.NodeName) }

func fine(m map[string]int, r replica, node string) bool {
	k := naming.Key(r.NodeName)

	return naming.Same(r.NodeName, node) && m[k] > 0 && m[naming.Key(r.ResourceName)] > 0
}
`

	if err := os.WriteFile(filepath.Join(dir, "planted.go"), []byte(src), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := violations(t, root)

	want := []string{
		"pkg/planted/planted.go: m[r.ResourceName]",
		"pkg/planted/planted.go: r.NodeName == node",
		"pkg/planted/planted.go: slices.Contains(nodes, r.NodeName)",
		"pkg/planted/planted.go: strings.EqualFold(r.NodeName, node)",
	}

	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("violations\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestInventory(t *testing.T) {
	if os.Getenv("NAMING_INVENTORY") == "" {
		t.Skip("set NAMING_INVENTORY=1 to print every hit")
	}

	for _, v := range violations(t, filepath.Join("..", "..")) {
		t.Log(v)
	}
}
