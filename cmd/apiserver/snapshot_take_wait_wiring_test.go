// SPDX-License-Identifier: Apache-2.0

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Every rest.Server this binary builds holds a snapshot create's answer
// until the take is over, as LINSTOR does. Left unset, the create answers
// before the take, linstor-csi caches the "not ready" view it reads right
// after for 30s, and every VolumeSnapshot waits out that cache.
func TestRESTServerWaitsForTheSnapshotTake(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	servers := 0

	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok || !isRESTServer(lit.Type) {
			return true
		}

		servers++

		if !setsSnapshotTakeWait(lit) {
			t.Errorf("rest.Server at %s does not set SnapshotTakeWait: rest.SnapshotTakeWait", fset.Position(lit.Lbrace))
		}

		return true
	})

	if servers == 0 {
		t.Fatal("main.go builds no rest.Server, so this guard checks nothing")
	}
}

func setsSnapshotTakeWait(lit *ast.CompositeLit) bool {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "SnapshotTakeWait" {
			continue
		}

		sel, ok := kv.Value.(*ast.SelectorExpr)
		if !ok {
			return false
		}

		pkg, ok := sel.X.(*ast.Ident)

		return ok && pkg.Name == "rest" && sel.Sel.Name == "SnapshotTakeWait"
	}

	return false
}
