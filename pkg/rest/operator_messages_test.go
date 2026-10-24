package rest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// internalTrackerRef matches the project's internal tracker spellings,
// "Bug 174", "Bug-020" and "Phase 12". They mean nothing to an operator
// reading an error, an event or a log line, so no string literal in the
// shipped code carries one; comments may.
var internalTrackerRef = regexp.MustCompile(`\bBug[- ][0-9]|\bPhase [0-9]`)

func TestShippedStringsCarryNoInternalTrackerNumbers(t *testing.T) {
	t.Parallel()

	var hits []string

	for _, root := range []string{"../../pkg", "../../internal", "../../cmd"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			fset := token.NewFileSet()

			file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if parseErr != nil {
				return fmt.Errorf("parse %s: %w", path, parseErr)
			}

			ast.Inspect(file, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}

				value, unquoteErr := strconv.Unquote(lit.Value)
				if unquoteErr != nil {
					value = lit.Value
				}

				if internalTrackerRef.MatchString(value) {
					hits = append(hits, fset.Position(lit.Pos()).String()+": "+value)
				}

				return true
			})

			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	for _, hit := range hits {
		t.Errorf("string literal carries an internal tracker number: %s", hit)
	}
}
