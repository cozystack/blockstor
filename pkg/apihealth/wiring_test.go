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

package apihealth_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestDaemonsLoadTheirConfigThroughAPIHealth: a daemon that loads its
// config any other way builds its clients with client-go's 45 s health
// check, and a hard reset of the node whose API server it talks to
// freezes it for longer than the grace period. The daemons are the
// long-running binaries the node monitor depends on: the satellite
// writes the heartbeat, the controller judges it from its cache, the
// apiserver answers LINSTOR clients from its cache.
func TestDaemonsLoadTheirConfigThroughAPIHealth(t *testing.T) {
	t.Parallel()

	for _, daemon := range []string{"controller", "apiserver", "satellite"} {
		files, err := filepath.Glob(filepath.Join("..", "..", "cmd", daemon, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("cmd/%s: no sources (%v)", daemon, err)
		}

		fset := token.NewFileSet()
		viaAPIHealth := 0

		for _, name := range files {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}

			file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}

			ast.Inspect(file, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "GetConfig" && sel.Sel.Name != "GetConfigOrDie") {
					return true
				}

				if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "apihealth" {
					viaAPIHealth++

					return true
				}

				t.Errorf("%s loads the API config without apihealth", fset.Position(sel.Pos()))

				return true
			})
		}

		if viaAPIHealth == 0 {
			t.Errorf("cmd/%s never loads its API config through apihealth.GetConfig", daemon)
		}
	}
}
