// SPDX-License-Identifier: Apache-2.0

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The constant being loopback proves nothing if the flag is registered with
// a literal instead: `":3370"` back at the flag call binds every interface
// again. So the registration itself has to name defaultPlainBindAddress.
func TestPlainListenerFlagUsesTheLoopbackDefault(t *testing.T) {
	t.Parallel()

	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	found := false

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) < 3 {
			return true
		}

		name, ok := call.Args[1].(*ast.BasicLit)
		if !ok || name.Value != `"rest-bind-address"` {
			return true
		}

		found = true

		if ident, ok := call.Args[2].(*ast.Ident); !ok || ident.Name != "defaultPlainBindAddress" {
			t.Errorf("rest-bind-address is registered with a default other than defaultPlainBindAddress")
		}

		return true
	})

	if !found {
		t.Fatal("no rest-bind-address flag registration found in main.go")
	}
}
