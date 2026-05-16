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

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 177 (P3) — `deleteWithRollback.run` doc says capture runs BEFORE
// the pre-walk; the actual call order in the function body is the
// opposite (pre-walk first, then capture). The mismatch is cosmetic
// but the comment is the only auditable spec we have for the helper
// — let it drift and future copy-pasters will reproduce the wrong
// ordering elsewhere. Pin the contract: doc and code MUST agree.
//
// We parse delete_toctou.go with `go/parser` and walk the AST so a
// stray "captureStoragePool" mention in a string literal can't satisfy
// the test — only the function comment and the actual call sequence
// inside the run() body count.
func TestBug177CommentAndCodeAgreeOnOrder(t *testing.T) {
	t.Parallel()

	const path = "delete_toctou.go"

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	var (
		doc          string
		callSequence []string
	)

	ast.Inspect(f, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok {
			return true
		}

		if fn.Name == nil || fn.Name.Name != "run" {
			return true
		}

		if fn.Recv == nil || len(fn.Recv.List) == 0 {
			return true
		}

		if fn.Doc != nil {
			doc = fn.Doc.Text()
		}

		// Walk the function body and collect d.<selector>() call names
		// in source order. We only care about the four orchestrator
		// callbacks the helper invokes.
		ast.Inspect(fn.Body, func(bn ast.Node) bool {
			call, ok := bn.(*ast.CallExpr)
			if !ok {
				return true
			}

			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}

			ident, ok := sel.X.(*ast.Ident)
			if !ok || ident.Name != "d" {
				return true
			}

			switch sel.Sel.Name {
			case "refuseIfReferenced", "capture", "remove", "rolledBackIfRaced":
				callSequence = append(callSequence, sel.Sel.Name)
			}

			return true
		})

		return false
	})

	if doc == "" {
		t.Fatalf("run() has no docstring; the doc/code contract is unauditable")
	}

	if len(callSequence) < 2 {
		t.Fatalf("run() body has fewer than two orchestrator calls: %v", callSequence)
	}

	// The actual call order in the body. The first two entries pin
	// the doc/code agreement we care about.
	firstCall := callSequence[0]
	secondCall := callSequence[1]

	// What does the doc claim about the order of capture vs the
	// pre-walk? Match the two key phrases: "BEFORE the pre-walk" or
	// "AFTER the pre-walk".
	switch {
	case strings.Contains(doc, "capture deliberately runs BEFORE the pre-walk"):
		if firstCall != "capture" || secondCall != "refuseIfReferenced" {
			t.Fatalf(
				"doc says capture runs BEFORE the pre-walk but code runs %s -> %s; "+
					"fix the comment to match the code (pre-walk first, then capture) "+
					"or reorder the code to match the comment",
				firstCall, secondCall)
		}
	case strings.Contains(doc, "capture deliberately runs AFTER the pre-walk"):
		if firstCall != "refuseIfReferenced" || secondCall != "capture" {
			t.Fatalf(
				"doc says capture runs AFTER the pre-walk but code runs %s -> %s",
				firstCall, secondCall)
		}
	default:
		t.Fatalf(
			"run() docstring does not mention capture vs pre-walk ordering "+
				"in the canonical form (\"capture deliberately runs BEFORE/AFTER the pre-walk\"); "+
				"doc was:\n%s", doc)
	}
}

// failingCreateNodeStore wraps a NodeStore and forces Create() to
// return a fixed error — emulates the context.Canceled / connection
// drop the race-loser path hits when the client has already
// disconnected.
type failingCreateNodeStore struct {
	store.NodeStore

	createErr error
	creates   atomic.Int32
}

func (f *failingCreateNodeStore) Create(_ context.Context, _ *apiv1.Node) error {
	f.creates.Add(1)

	return f.createErr
}

// twoPhaseResourceStore wraps a ResourceStore and returns a different
// List() result on the first call vs subsequent calls. The pre-walk
// in handleNodeDelete (refuseNodeDeleteIfReferenced) sees an empty
// list and lets the Delete through; the post-walk in
// rollbackNodeDeleteIfRaced sees the racing replica and forces the
// rollback path.
type twoPhaseResourceStore struct {
	store.ResourceStore

	withRef []apiv1.Resource
	calls   atomic.Int32
}

func (t *twoPhaseResourceStore) List(_ context.Context) ([]apiv1.Resource, error) {
	n := t.calls.Add(1)
	if n == 1 {
		// Pre-walk: pretend no replicas reference the node so the
		// delete is allowed to proceed.
		return []apiv1.Resource{}, nil
	}

	// Post-walk: the racing `r c` has now landed. Return the
	// referencing replica so the rollback path fires.
	return t.withRef, nil
}

// bug178NodeStore composites the two flaky views with the real
// InMemory backing for everything else.
type bug178NodeStore struct {
	store.Store

	nodes     store.NodeStore
	resources store.ResourceStore
}

func (b *bug178NodeStore) Nodes() store.NodeStore {
	if b.nodes != nil {
		return b.nodes
	}

	return b.Store.Nodes()
}

func (b *bug178NodeStore) Resources() store.ResourceStore {
	if b.resources != nil {
		return b.resources
	}

	return b.Store.Resources()
}

// TestBug178RollbackErrorSurfacesIn5xx — when the racing-delete
// rollback path tries to Create() the captured Node back and the
// store returns an error (context.Canceled from a client
// disconnect, an apimachinery 5xx from a flaked apiserver, …), the
// handler MUST NOT write the misleading 409 "still referenced"
// envelope. The Node is GONE — the operator gets told it's still
// there. The fix surfaces a 500 envelope citing
// "rollback failed; cluster state may be inconsistent" + the
// original primary name so the operator knows to verify cluster
// state and possibly restore by hand.
func TestBug178RollbackErrorSurfacesIn5xx(t *testing.T) {
	t.Parallel()

	const nodeName = "bug178-node"

	inner := store.NewInMemory()
	ctx := t.Context()

	if err := inner.Nodes().Create(ctx, &apiv1.Node{Name: nodeName, Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// The racing replica that materialises between pre-walk and
	// post-walk. We never put it in the real store — the
	// twoPhaseResourceStore fabricates it from List() so the
	// rollback path is forced even though the InMemory store has
	// no actual resource row.
	racingResource := apiv1.Resource{
		Name:     "rd-bug178",
		NodeName: nodeName,
	}

	st := &bug178NodeStore{
		Store: inner,
		nodes: &failingCreateNodeStore{
			NodeStore: inner.Nodes(),
			createErr: errors.New("context canceled"),
		},
		resources: &twoPhaseResourceStore{
			ResourceStore: inner.Resources(),
			withRef:       []apiv1.Resource{racingResource},
		},
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/"+nodeName)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500 (rollback Create failed) — "+
			"the handler swallowed the restore error and wrote 409 "+
			"despite the Node being gone", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("empty envelope; expected one APICallRc entry with rollback-failed message")
	}

	rc := rcs[0]

	if rc.RetCode >= 0 {
		t.Errorf("RetCode = %d, want a negative MASK_ERROR-bearing value", rc.RetCode)
	}

	if !strings.Contains(strings.ToLower(rc.Message), "rollback failed") {
		t.Errorf("Message = %q, want a phrase containing 'rollback failed'", rc.Message)
	}

	if !strings.Contains(strings.ToLower(rc.Message), "inconsistent") {
		t.Errorf("Message = %q, want a phrase mentioning the cluster may be 'inconsistent'", rc.Message)
	}

	if !strings.Contains(rc.Cause, nodeName) {
		t.Errorf("Cause = %q, want the original primary name %q embedded so "+
			"the operator can find the lost object", rc.Cause, nodeName)
	}

	if !strings.Contains(strings.ToLower(rc.Correc), "verify") {
		t.Errorf("Correc = %q, want a verify/restore guidance line", rc.Correc)
	}

	// Verify the Node really is gone — the rollback Create errored
	// out, so the deleted primary is lost from the cluster's view.
	// This is the dangerous state the 500 envelope warns about.
	_, err := inner.Nodes().Get(ctx, nodeName)
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("inner.Nodes().Get(%q): got %v, want ErrNotFound — "+
			"the test setup expects the Node to be gone after the racing-delete path", nodeName, err)
	}

	if !strings.Contains(strings.ToLower(rc.Message+rc.Cause), "rollback") {
		t.Errorf("envelope does NOT cite the rollback failure; the misleading 409 " +
			"\"still referenced\" path has not been replaced")
	}
}

// TestBug178HappyPathRollback — regression guard: when the
// rollback path's restore Create SUCCEEDS, the handler must
// continue to surface the 409 + FAIL_IN_USE envelope (matching
// Bug 174). Without this guard we'd be free to "fix" Bug 178 by
// always returning 500, which would break the happy-path TOCTOU
// close.
func TestBug178HappyPathRollback(t *testing.T) {
	t.Parallel()

	const nodeName = "bug178-happy"

	inner := store.NewInMemory()
	ctx := t.Context()

	if err := inner.Nodes().Create(ctx, &apiv1.Node{Name: nodeName, Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	racingResource := apiv1.Resource{
		Name:     "rd-happy",
		NodeName: nodeName,
	}

	st := &bug178NodeStore{
		Store: inner,
		// No failingCreateNodeStore — Create restores cleanly via
		// the real InMemory store.
		resources: &twoPhaseResourceStore{
			ResourceStore: inner.Resources(),
			withRef:       []apiv1.Resource{racingResource},
		},
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/"+nodeName)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 — the rollback Create succeeded, "+
			"the handler should surface the FAIL_IN_USE envelope the pre-walk "+
			"would have written", resp.StatusCode)
	}

	// The Node must have been restored by the rollback Create.
	got, err := inner.Nodes().Get(ctx, nodeName)
	if err != nil {
		t.Fatalf("inner.Nodes().Get(%q): %v — the rollback Create did not "+
			"restore the captured Node", nodeName, err)
	}

	if got.Name != nodeName {
		t.Errorf("restored node name = %q, want %q", got.Name, nodeName)
	}
}
