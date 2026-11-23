// SPDX-License-Identifier: Apache-2.0

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Every rest.Server this binary builds reads the passphrase Secret through
// the manager's API reader. Left unset, the server falls back to the cached
// client, whose first Secret read starts a cluster-wide Secret informer that
// the namespaced Role does not allow to list, and every passphrase read hangs.
func TestRESTServerReadsSecretsPastTheCache(t *testing.T) {
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

		if !setsSecretReaderToAPIReader(lit) {
			t.Errorf("rest.Server at %s does not set SecretReader: mgr.GetAPIReader()", fset.Position(lit.Lbrace))
		}

		return true
	})

	if servers == 0 {
		t.Fatal("main.go builds no rest.Server, so this guard checks nothing")
	}
}

func isRESTServer(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	pkg, ok := sel.X.(*ast.Ident)

	return ok && pkg.Name == "rest" && sel.Sel.Name == "Server"
}

func setsSecretReaderToAPIReader(lit *ast.CompositeLit) bool {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "SecretReader" {
			continue
		}

		call, ok := kv.Value.(*ast.CallExpr)
		if !ok {
			return false
		}

		sel, ok := call.Fun.(*ast.SelectorExpr)

		return ok && sel.Sel.Name == "GetAPIReader"
	}

	return false
}
