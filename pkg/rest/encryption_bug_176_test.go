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

// Bug 176 — P2 SECURITY: passphrase compare uses non-constant-time
// `==` / `!=`. Three sites in `pkg/rest/encryption.go` compare the
// operator-supplied passphrase against the stored value via byte-wise
// `==` / `!=`:
//
//   - line 157  (handlePassphraseCreate, idempotent re-create branch)
//   - line 260  (handlePassphraseEnter,  mismatch branch)
//   - line 436  (handlePassphraseModify, old_passphrase auth branch)
//
// Go's `string ==` short-circuits at the first differing byte (memcmp
// underneath). A network attacker who can probe the apiserver
// thousands of times can correlate response latency with the
// best-match-prefix length and recover the stored passphrase
// character-by-character (timing oracle, Kocher '96, Lawson '07 on
// HMAC checks). The standard remedy is `crypto/subtle.ConstantTimeCompare`,
// which scans both inputs end-to-end regardless of the first mismatch.
//
// These tests pin the fix with two complementary strategies:
//
//   - Source-grep / AST asserts: the easy-to-rely-on signal — if the
//     compare in encryption.go reverts to `==` / `!=` on a passphrase
//     variable, the source-level test fires immediately, regardless
//     of CI host noise. The grep accepts the canonical compare
//     pattern (`subtle.ConstantTimeCompare(...) == 1`).
//   - Happy-path regression guards: a correct constant-time switch
//     must still reject wrong passphrases (PATCH→401, PUT→401) and
//     accept right ones (POST idempotent→200, PATCH→200, PUT→200).
//     If a refactor breaks the comparison semantics (e.g.
//     `ConstantTimeCompare == 0` vs `== 1` mix-up), the regression
//     guards catch it.

package rest

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// encryptionGoPath is the file under test. Kept as a constant so the
// source-grep tests can be moved alongside other rest-package files
// without churn.
const encryptionGoPath = "encryption.go"

// readEncryptionSource slurps `encryption.go` from disk. The tests
// run with the package directory as cwd (standard `go test` layout),
// so a plain relative path works.
func readEncryptionSource(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	path := filepath.Join(wd, encryptionGoPath)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	return string(raw)
}

// TestBug176PassphraseCompareUsesConstantTime asserts that the file
// imports `crypto/subtle` and uses `subtle.ConstantTimeCompare` for
// passphrase verification. This is the positive signal: if the fix
// is in place, the import and the compare call MUST both appear.
//
// AST-based so that a stray "crypto/subtle" mention in a comment
// doesn't false-positive — only a real ImportSpec + CallExpr count.
func TestBug176PassphraseCompareUsesConstantTime(t *testing.T) {
	src := readEncryptionSource(t)

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, encryptionGoPath, src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", encryptionGoPath, err)
	}

	// Import check: `crypto/subtle` must be in the import list.
	var hasSubtleImport bool

	for _, imp := range file.Imports {
		// imp.Path.Value is the quoted string literal, e.g. `"crypto/subtle"`.
		if imp.Path != nil && imp.Path.Value == `"crypto/subtle"` {
			hasSubtleImport = true

			break
		}
	}

	if !hasSubtleImport {
		t.Fatalf("encryption.go does not import crypto/subtle (Bug 176 — passphrase compare must use subtle.ConstantTimeCompare)")
	}

	// Call check: count `subtle.ConstantTimeCompare(...)` invocations.
	// We expect at least 3 — one per compare site (lines 157/260/436
	// pre-fix). Allow more in case the refactor splits a branch.
	var ctcCalls int

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}

		if pkg.Name == "subtle" && sel.Sel.Name == "ConstantTimeCompare" {
			ctcCalls++
		}

		return true
	})

	if ctcCalls < 3 {
		t.Fatalf("subtle.ConstantTimeCompare call count = %d; want >= 3 (one per compare site: create/enter/modify)", ctcCalls)
	}
}

// TestBug176PassphraseCompareRejectsBytewiseEquality is the negative
// signal: the source MUST NOT contain byte-wise `==` / `!=` compares
// on a `passphrase` / `have` / `want` variable. We grep on the post-
// `proofOfKnowledge()` variable names (`have`, `want`,
// `req.OldPassphrase`, `req.proofOfKnowledge()`) for the canonical
// pre-fix patterns.
//
// A regression that reverts the constant-time switch to a plain
// equality compare lights this up immediately.
func TestBug176PassphraseCompareRejectsBytewiseEquality(t *testing.T) {
	src := readEncryptionSource(t)

	// Each pattern is the canonical pre-fix shape. Listed verbatim so
	// the failure message points at the exact pre-fix form a reviewer
	// might accidentally reintroduce.
	bannedPatterns := []*regexp.Regexp{
		// `if have == want` / `if have != want` (and swap).
		regexp.MustCompile(`\bif\s+have\s*==\s*want\b`),
		regexp.MustCompile(`\bif\s+have\s*!=\s*want\b`),
		regexp.MustCompile(`\bif\s+want\s*==\s*have\b`),
		regexp.MustCompile(`\bif\s+want\s*!=\s*have\b`),

		// `if have == req.proofOfKnowledge()` / `if have != req.proofOfKnowledge()`.
		regexp.MustCompile(`\bif\s+have\s*==\s*req\.proofOfKnowledge\(\)`),
		regexp.MustCompile(`\bif\s+have\s*!=\s*req\.proofOfKnowledge\(\)`),

		// `if have == req.OldPassphrase` / `if have != req.OldPassphrase`.
		regexp.MustCompile(`\bif\s+have\s*==\s*req\.OldPassphrase\b`),
		regexp.MustCompile(`\bif\s+have\s*!=\s*req\.OldPassphrase\b`),
	}

	// Drop comments so a documentation block describing the bug
	// (e.g. the file header comment) doesn't false-positive.
	stripped := stripGoComments(src)

	for _, pat := range bannedPatterns {
		if loc := pat.FindStringIndex(stripped); loc != nil {
			ctx := stripped[loc[0]:loc[1]]
			t.Errorf("encryption.go contains banned bytewise compare %q (matched %q) — Bug 176 requires subtle.ConstantTimeCompare",
				pat.String(), ctx)
		}
	}
}

// stripGoComments removes // line comments and /* block */ comments
// from Go source so the bytewise-equality grep doesn't false-positive
// on documentation that describes the pre-fix bug shape. Naive
// implementation is fine — encryption.go has no raw-string literals
// containing comment-like substrings.
func stripGoComments(src string) string {
	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "", src, parser.ParseComments)
	if err != nil {
		return src
	}

	out := []byte(src)

	for _, cg := range file.Comments {
		start := fset.Position(cg.Pos()).Offset
		end := fset.Position(cg.End()).Offset

		if start < 0 || end > len(out) || start >= end {
			continue
		}

		// Replace comment bytes with spaces so offsets in the rest of
		// the file stay aligned (not strictly required for the regex
		// scan, but cheap and tidy).
		for i := start; i < end; i++ {
			if out[i] != '\n' {
				out[i] = ' '
			}
		}
	}

	return string(out)
}

// TestBug176WrongPassphraseStillRejected is the happy-path regression
// guard: after the constant-time switch, a PATCH with the wrong
// proof-of-knowledge MUST still return 401 (W13 contract), and a PUT
// with the wrong old_passphrase MUST still return 401 (W14 contract).
// A bad refactor that mixed up `ConstantTimeCompare == 1` vs `== 0`
// would invert the auth surface — this test catches that.
func TestBug176WrongPassphraseStillRejected(t *testing.T) {
	srv := newSecretPathServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	const seed = "right-passphrase-12345"

	// Seed a known passphrase via POST.
	createBody, _ := json.Marshal(map[string]string{"new_passphrase": seed})

	createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)

	_ = createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("seed POST: got %d, want 201", createResp.StatusCode)
	}

	// PATCH with the WRONG passphrase MUST 401. Differs from the seed
	// at byte 0 to make sure the constant-time compare isn't
	// accidentally short-circuiting on the first byte.
	wrongPatch, _ := json.Marshal(map[string]string{"new_passphrase": "Xrong-passphrase-12345"})

	patchResp := httpPatch(t, base+"/v1/encryption/passphrase", wrongPatch)
	defer func() { _ = patchResp.Body.Close() }()

	if patchResp.StatusCode != http.StatusUnauthorized {
		raw, _ := io.ReadAll(patchResp.Body)
		t.Fatalf("PATCH wrong: got %d, want 401 (Bug 176 regression — auth surface, body=%q)",
			patchResp.StatusCode, string(raw))
	}

	// PUT with WRONG old_passphrase MUST 401.
	wrongPut, _ := json.Marshal(map[string]string{
		"old_passphrase": "right-passphrase-12340", // diverges at last byte
		"new_passphrase": "rotated-passphrase",
	})

	putResp := httpPut(t, base+"/v1/encryption/passphrase", wrongPut)
	defer func() { _ = putResp.Body.Close() }()

	if putResp.StatusCode != http.StatusUnauthorized {
		raw, _ := io.ReadAll(putResp.Body)
		t.Fatalf("PUT wrong old: got %d, want 401 (Bug 176 regression — auth surface, body=%q)",
			putResp.StatusCode, string(raw))
	}
}

// TestBug176RightPassphraseStillAccepted is the other half of the
// regression guard: a correct passphrase MUST still pass every
// gate. POST same-value → 200 (idempotent), PATCH right → 200 (W13),
// PUT old=right, new=fresh → 200 (W14).
func TestBug176RightPassphraseStillAccepted(t *testing.T) {
	srv := newSecretPathServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	const (
		seed  = "right-passphrase-67890"
		fresh = "next-passphrase-67890"
	)

	// Seed.
	createBody, _ := json.Marshal(map[string]string{"new_passphrase": seed})

	createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)

	_ = createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("seed POST: got %d, want 201", createResp.StatusCode)
	}

	// POST same value → 200 (idempotent re-create branch, encryption.go line 157).
	idemResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)

	_ = idemResp.Body.Close()

	if idemResp.StatusCode != http.StatusOK {
		t.Fatalf("idempotent POST: got %d, want 200 (Bug 176 — line 157 compare)", idemResp.StatusCode)
	}

	// PATCH right → 200 (line 260 compare).
	enterResp := httpPatch(t, base+"/v1/encryption/passphrase", createBody)

	_ = enterResp.Body.Close()

	if enterResp.StatusCode != http.StatusOK {
		t.Fatalf("PATCH right: got %d, want 200 (Bug 176 — line 260 compare)", enterResp.StatusCode)
	}

	// PUT right-old/fresh-new → 200 (line 436 compare).
	putBody, _ := json.Marshal(map[string]string{
		"old_passphrase": seed,
		"new_passphrase": fresh,
	})

	putResp := httpPut(t, base+"/v1/encryption/passphrase", putBody)

	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT right-old: got %d, want 200 (Bug 176 — line 436 compare)", putResp.StatusCode)
	}
}

// TestBug176SubtleCompareCallSites pins WHICH variables the
// ConstantTimeCompare calls operate on. A regression that swaps in
// `subtle.ConstantTimeCompare` somewhere but accidentally leaves a
// stray `==`/`!=` on the auth branch (e.g. by adding a compare on a
// non-passphrase variable) wouldn't be caught by the count test
// alone. This test asserts each ConstantTimeCompare argument is a
// passphrase-shaped expression (`[]byte(have)`, `[]byte(want)`,
// `[]byte(req.OldPassphrase)`, `[]byte(req.proofOfKnowledge())`).
func TestBug176SubtleCompareCallSites(t *testing.T) {
	src := readEncryptionSource(t)

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, encryptionGoPath, src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", encryptionGoPath, err)
	}

	// Canonical operand shapes the fix may use. Stored as substrings
	// because ast.Print's output of a `[]byte(x)` conversion is
	// stable enough to substring-match without false positives.
	allowedOperands := []string{
		"have",
		"want",
		"req.OldPassphrase",
		"req.proofOfKnowledge()",
	}

	var sawProofOfKnowledge, sawOldPassphrase, sawHave bool

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "subtle" || sel.Sel.Name != "ConstantTimeCompare" {
			return true
		}

		if len(call.Args) != 2 {
			t.Errorf("subtle.ConstantTimeCompare with %d args; want 2", len(call.Args))

			return true
		}

		argText := nodeText(fset, src, call.Args[0]) + " " + nodeText(fset, src, call.Args[1])

		var matched bool

		for _, op := range allowedOperands {
			if strings.Contains(argText, op) {
				matched = true

				break
			}
		}

		if !matched {
			t.Errorf("subtle.ConstantTimeCompare(%s) operates on non-passphrase variable", argText)
		}

		// Track which compare sites we covered.
		if strings.Contains(argText, "req.proofOfKnowledge()") {
			sawProofOfKnowledge = true
		}

		if strings.Contains(argText, "req.OldPassphrase") {
			sawOldPassphrase = true
		}

		if strings.Contains(argText, "have") && strings.Contains(argText, "want") {
			sawHave = true
		}

		return true
	})

	if !sawHave {
		t.Errorf("no subtle.ConstantTimeCompare(have, want) site found (Bug 176 — line 157 handlePassphraseCreate)")
	}

	if !sawProofOfKnowledge {
		t.Errorf("no subtle.ConstantTimeCompare(..., req.proofOfKnowledge()) site found (Bug 176 — line 260 handlePassphraseEnter)")
	}

	if !sawOldPassphrase {
		t.Errorf("no subtle.ConstantTimeCompare(..., req.OldPassphrase) site found (Bug 176 — line 436 handlePassphraseModify)")
	}
}

// nodeText returns the source-text slice for an AST node, used by
// the call-site test to substring-match operand shapes without
// re-printing via the go/printer package (which would normalise
// whitespace and lose the original shape).
func nodeText(fset *token.FileSet, src string, n ast.Node) string {
	start := fset.Position(n.Pos()).Offset
	end := fset.Position(n.End()).Offset

	if start < 0 || end > len(src) || start >= end {
		return ""
	}

	return src[start:end]
}
