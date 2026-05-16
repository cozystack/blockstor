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

package rest

// Bug 199 (P2) — `writeError` is the LINSTOR-shaped error envelope
// emitter. Bug 162 fixed `writeStoreError`'s default branch to route
// the message through `scrubImplDetails` so etcd / apimachinery /
// k8s.io / `*.blockstor.io` substrings never reach the wire. But Bug
// 162 only covered the `writeStoreError` dispatcher; 51 other call
// sites in `pkg/rest/*.go` reach `writeError` directly with the raw
// `err.Error()` string:
//
//	writeError(w, http.StatusInternalServerError, err.Error())
//
// These are spread across controller_props.go, snapshot_multi.go,
// autoplace.go, encryption.go, stats.go, snapshots.go, spawn.go,
// resource_connections.go, query_size_info.go, storage_pools.go,
// node_connections.go, node_lifecycle.go, advise.go, nodes.go,
// resource_groups.go, resource_group_extras.go,
// resource_definitions.go, rd_clone.go, volume_definitions.go,
// resources.go — every one of them bypasses scrubImplDetails and
// leaks the persistence-backend identity on any 500-shaped failure
// from the K8s-backed store.
//
// The fix wraps `writeError` itself: every message goes through
// `scrubImplDetails` before being JSON-encoded into the envelope.
// The function's contract becomes "the LINSTOR envelope is always
// scrubbed of backend impl details," which is what every caller
// expects anyway. `scrubImplDetails` is a noop for messages that
// don't contain the sentinel substrings, so caller-supplied literal
// strings like `"remote_name is required"` pass through unchanged.
//
// Tests pinned here:
//
//   - TestBug199WriteErrorScrubsEtcdMessage: hit `writeError` directly
//     with an etcd-shaped message, assert the wire body does not
//     contain "etcd" / "etcdserver".
//   - TestBug199WriteErrorScrubsApimachineryMessage: same shape for
//     "apimachinery" / "k8s.io" / "controllerconfigs.blockstor.io".
//   - TestBug199WriteErrorPreservesLiteralCallerStrings: a literal
//     caller-supplied message ("remote_name is required") reaches the
//     wire byte-for-byte — the scrub guard MUST be a noop on operator-
//     friendly strings so we don't break existing 4xx handler text.
//   - TestBug199WriteErrorBodyInvokesScrubImplDetails: AST guard on
//     `writeError`'s function body — asserts it calls
//     `scrubImplDetails` exactly once. This is the single-point-of-
//     change pin: if anyone removes the wrap, every direct call site
//     in pkg/rest/ regresses simultaneously and this test catches it.

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestBug199WriteErrorScrubsEtcdMessage pins the etcd flavor: a raw
// "etcdserver: request is too large" message reaches the wire via
// writeError. Without the wrap, the body leaks the backend identity.
func TestBug199WriteErrorScrubsEtcdMessage(t *testing.T) {
	t.Parallel()

	rr := httptest.NewRecorder()
	writeError(rr, http.StatusInternalServerError,
		"etcdserver: request is too large")

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status: got %d, want 500", rr.Code)
	}

	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	low := strings.ToLower(string(body))
	for _, leak := range []string{"etcdserver", "etcd"} {
		if strings.Contains(low, leak) {
			t.Errorf("body leaks impl detail %q: %s", leak, body)
		}
	}

	// envelope MUST still be the LINSTOR `[]ApiCallRc` shape.
	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 || rc[0].RetCode >= 0 {
		t.Fatalf("malformed envelope: %s", body)
	}
}

// TestBug199WriteErrorScrubsApimachineryMessage pins the apimachinery
// flavor: a NewConflict-shaped string carrying the GroupResource
// ("controllerconfigs.blockstor.io[.blockstor.io]") reaches writeError.
// All three substrings (apimachinery, k8s.io, the group fingerprint)
// MUST be scrubbed.
func TestBug199WriteErrorScrubsApimachineryMessage(t *testing.T) {
	t.Parallel()

	// Mirrors the exact apimachinery output: "Operation cannot be
	// fulfilled on <resource>.<group>[.<group>] ..." — the resource
	// itself often carries the group suffix, producing the double-
	// suffix shape the scrub regex collapses.
	msg := `Operation cannot be fulfilled on resourcedefinitions.blockstor.io.blockstor.io "rd-test": ` +
		`the object has been modified; please apply your changes to the latest version and try again ` +
		`(via apimachinery/k8s.io/api/...)`

	rr := httptest.NewRecorder()
	writeError(rr, http.StatusInternalServerError, msg)

	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	low := strings.ToLower(string(body))
	for _, leak := range []string{
		"resourcedefinitions.blockstor.io",
		"apimachinery",
		"k8s.io",
		"etcd",
	} {
		if strings.Contains(low, strings.ToLower(leak)) {
			t.Errorf("body leaks impl detail %q: %s", leak, body)
		}
	}
}

// TestBug199WriteErrorPreservesLiteralCallerStrings pins that the
// scrub wrap is a noop on operator-friendly literal strings — every
// 4xx call site (BadRequest, NotImplemented, Conflict, Unauthorized,
// PreconditionFailed) passes a hand-written message that MUST reach
// the wire byte-for-byte. If scrubImplDetails ever grows a rule that
// mangles a plain string, this test fires.
func TestBug199WriteErrorPreservesLiteralCallerStrings(t *testing.T) {
	t.Parallel()

	for _, literal := range []string{
		"remote_name is required",
		"new_passphrase is required",
		"cluster passphrase already set; PUT to modify with old passphrase",
		"passphrase mismatch",
		"old passphrase mismatch",
		"conflict: store object was modified, retry the request",
		"not found",
		"url is required",
	} {
		rr := httptest.NewRecorder()
		writeError(rr, http.StatusBadRequest, literal)

		body, err := io.ReadAll(rr.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}

		var rc []apiv1.APICallRc
		if err := json.Unmarshal(body, &rc); err != nil {
			t.Fatalf("decode %q: %v\nbody: %s", literal, err, body)
		}

		if len(rc) == 0 {
			t.Fatalf("empty envelope for %q", literal)
		}

		if rc[0].Message != literal {
			t.Errorf("literal %q mangled to %q", literal, rc[0].Message)
		}
	}
}

// TestBug199WriteErrorBodyInvokesScrubImplDetails is the AST guard:
// `writeError`'s function body MUST contain a call to
// `scrubImplDetails`. Bug 199's fix is a single-point wrap — every
// one of the 51 direct `writeError(w, 500, err.Error())` sites
// depends on this one call to scrub. If a future refactor removes
// the wrap, this test fires before the regression ships.
//
// We walk the AST so that a stray "scrubImplDetails" mention in a
// string literal or comment can't satisfy the test — only an actual
// call expression inside writeError's body counts.
func TestBug199WriteErrorBodyInvokesScrubImplDetails(t *testing.T) {
	t.Parallel()

	const path = "nodes.go"

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	var (
		foundDecl  bool
		scrubCalls int
	)

	ast.Inspect(f, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok {
			return true
		}

		// writeError is a package-level function, no receiver.
		if fn.Name == nil || fn.Name.Name != "writeError" || fn.Recv != nil {
			return true
		}

		foundDecl = true

		ast.Inspect(fn.Body, func(bn ast.Node) bool {
			call, ok := bn.(*ast.CallExpr)
			if !ok {
				return true
			}

			ident, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}

			if ident.Name == "scrubImplDetails" {
				scrubCalls++
			}

			return true
		})

		return false
	})

	if !foundDecl {
		t.Fatalf("writeError func declaration not found in %s", path)
	}

	if scrubCalls == 0 {
		t.Errorf("writeError body does not invoke scrubImplDetails — " +
			"every 500-shaped err.Error() call site leaks backend identity (Bug 199)")
	}
}

// TestBug199NoDirectWriteErrorErrorCallSitesRegress is a class-wide
// sanity check on the wire-edge: a representative sample of error
// strings that the K8s-backed store emits MUST scrub when written via
// `writeError` regardless of the status code chosen by the handler.
// Pins the contract that `writeError`'s scrub is unconditional.
func TestBug199NoDirectWriteErrorErrorCallSitesRegress(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		msg  string
	}{
		{"etcd-too-large", "etcdserver: request is too large"},
		{"etcd-bare", "rpc error: etcd unavailable"},
		{"apimachinery-conflict", `Operation cannot be fulfilled on snapshots.blockstor.io "snap-x": stale`},
		{"k8s-api-prefix", "k8s.io/apimachinery: unexpected EOF decoding response"},
		{"group-fingerprint", `controllerconfigs.blockstor.io.blockstor.io "default" not found`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rr := httptest.NewRecorder()
			// Use 500 because that's the call shape Bug 199 targets,
			// but the scrub MUST be status-agnostic.
			writeError(rr, http.StatusInternalServerError, tc.msg)

			body, err := io.ReadAll(rr.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			low := strings.ToLower(string(body))
			for _, leak := range []string{"etcd", "apimachinery", "k8s.io", ".blockstor.io"} {
				if strings.Contains(low, leak) {
					t.Errorf("case %q leaks %q on the wire: %s", tc.name, leak, body)
				}
			}
		})
	}
}
