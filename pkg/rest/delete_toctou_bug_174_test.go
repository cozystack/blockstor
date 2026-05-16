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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 174 (P2) — TOCTOU race in `n d` and `rg d`.
//
// `handleNodeDelete` (pkg/rest/nodes.go) and `handleRGDelete`
// (pkg/rest/resource_groups.go) carried the exact pre-condition-
// then-delete shape Bug 145 closed for `sp d`. The Bug 92 / W02
// referencing-resources gate ran a pre-Delete walk and refused
// with 409 + FAIL_IN_USE / FAIL_EXISTS_RSC_DFN when the count was
// non-zero — but the gate was check-then-write with no atomicity:
// a concurrent `r c <node>` / `rd c --resource-group <rg>` could
// slip between the Get and the Delete, the pre-walk saw an empty
// set, and the handler dropped the Node / RG out from under the
// just-persisted dependent.
//
// Symptom: orphan Resource CRDs whose NodeName points at a
// deleted Node, orphan RD CRDs whose ResourceGroupName points at
// a deleted RG. The satellite reconciler then waits forever on a
// gRPC peer that's no longer registered, and the spawn / rd-list
// path silently falls back to DfltRscGrp.
//
// Fix shape mirrors Bug 145's SP-delete close: capture the
// pre-Delete object via Get, run Delete, re-walk the
// pre-condition, restore the captured object if the re-walk
// fired, return the same 409 envelope the pre-walk would have
// emitted. The capture/rollback is lifted into a shared helper
// (tryDeleteWithRollback in pkg/rest/delete_toctou.go) so the
// SP-delete handler converges on the same machinery.

// TestBug174NodeDeleteRollsBackOnRaceWithResourceCreate fires 50
// pairs of (n d X, r c X.poke174.<rd>). Either ordering is
// acceptable: n-d wins → r-c hits the Bug 94 unknown-node gate
// and refuses; r-c wins → n-d's post-Delete re-walk sees the
// reference, restores the Node, returns 409. What's NOT
// acceptable: any Resource persisted on a NodeName whose Node
// row no longer exists. That orphan is the Bug 174 symptom.
func TestBug174NodeDeleteRollsBackOnRaceWithResourceCreate(t *testing.T) {
	t.Parallel()

	const pairs = 50

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed per-pair (Node, RD) so each goroutine pair races against
	// its own keys. Independent keys keep the test stable under
	// `-race` and isolate the assertion to "no orphan per pair".
	for i := range pairs {
		nodeName := fmt.Sprintf("n174-%d", i)
		rdName := fmt.Sprintf("rd174-%d", i)

		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: nodeName, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", nodeName, err)
		}

		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
			t.Fatalf("seed RD %s: %v", rdName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	var wg sync.WaitGroup

	wg.Add(pairs * 2)

	for i := range pairs {
		nodeName := fmt.Sprintf("n174-%d", i)
		rdName := fmt.Sprintf("rd174-%d", i)

		// Goroutine A: r c — try to persist a Resource pinned to the
		// node. Either succeeds (n d's post-Delete re-walk catches the
		// race and rolls back) or 4xx's on the Bug 94 unknown-node gate
		// (n d won the race).
		go func() {
			defer wg.Done()

			body, _ := json.Marshal([]apiv1.ResourceCreate{{
				Resource: apiv1.Resource{NodeName: nodeName},
			}})

			resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
			_ = resp.Body.Close()
		}()

		// Goroutine B: n d — try to drop the node. Either succeeds
		// (no racing Resource persisted yet) or 409's on the post-
		// Delete re-walk that catches a racing r c.
		go func() {
			defer wg.Done()

			resp := httpDelete(t, base+"/v1/nodes/"+nodeName)
			_ = resp.Body.Close()
		}()
	}

	wg.Wait()

	// Walk every persisted Resource — each MUST have a live Node
	// row matching its NodeName. An orphan is the bug.
	resources, err := st.Resources().List(ctx)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	for _, res := range resources {
		_, err := st.Nodes().Get(ctx, res.NodeName)
		if errors.Is(err, store.ErrNotFound) {
			t.Errorf("orphan Resource %s/%s references deleted Node %q (Bug 174)",
				res.Name, res.NodeName, res.NodeName)

			continue
		}

		if err != nil {
			t.Errorf("lookup Node %s for resource %s: %v",
				res.NodeName, res.Name, err)
		}
	}
}

// TestBug174RGDeleteRollsBackOnRaceWithRDCreate fires 50 pairs of
// (rg d X, rd c rd-X --resource-group X). Either ordering is
// acceptable: rg-d wins → rd-c hits the Bug 134 unknown-RG gate
// and refuses; rd-c wins → rg-d's post-Delete re-walk sees the
// referencing RD, restores the RG, returns 409. What's NOT
// acceptable: any RD persisted with a ResourceGroupName whose RG
// row no longer exists.
func TestBug174RGDeleteRollsBackOnRaceWithRDCreate(t *testing.T) {
	t.Parallel()

	const pairs = 50

	st := store.NewInMemory()
	ctx := t.Context()

	for i := range pairs {
		rgName := fmt.Sprintf("rg174-%d", i)

		if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: rgName}); err != nil {
			t.Fatalf("seed RG %s: %v", rgName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	var wg sync.WaitGroup

	wg.Add(pairs * 2)

	for i := range pairs {
		rgName := fmt.Sprintf("rg174-%d", i)
		rdName := fmt.Sprintf("rd174rg-%d", i)

		// Goroutine A: rd c <rd> --resource-group <rg>. Either
		// succeeds (rg d's post-Delete re-walk catches the race and
		// rolls back) or 404's on the Bug 134 unknown-RG gate (rg d
		// won the race).
		go func() {
			defer wg.Done()

			body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
				ResourceDefinition: apiv1.ResourceDefinition{
					Name:              rdName,
					ResourceGroupName: rgName,
				},
			})

			resp := httpPost(t, base+"/v1/resource-definitions", body)
			_ = resp.Body.Close()
		}()

		// Goroutine B: rg d — try to drop the RG. Either succeeds
		// (no racing RD persisted yet) or 409's on the post-Delete
		// re-walk that catches a racing rd c.
		go func() {
			defer wg.Done()

			resp := httpDelete(t, base+"/v1/resource-groups/"+rgName)
			_ = resp.Body.Close()
		}()
	}

	wg.Wait()

	// Walk every persisted RD — each that pinned a ResourceGroupName
	// MUST have a live RG row matching it.
	rds, err := st.ResourceDefinitions().List(ctx)
	if err != nil {
		t.Fatalf("list RDs: %v", err)
	}

	for _, rd := range rds {
		if rd.ResourceGroupName == "" {
			continue
		}

		_, err := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)
		if errors.Is(err, store.ErrNotFound) {
			t.Errorf("orphan RD %s references deleted RG %q (Bug 174)",
				rd.Name, rd.ResourceGroupName)

			continue
		}

		if err != nil {
			t.Errorf("lookup RG %s for RD %s: %v",
				rd.ResourceGroupName, rd.Name, err)
		}
	}
}

// TestBug174NodeDeleteHappyPath pins the no-race case: a node
// with no referencing Resources deletes cleanly with a 200 +
// maskInfo envelope. Guards against the rollback helper
// accidentally refusing on an empty-reference happy path.
func TestBug174NodeDeleteHappyPath(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "happy174", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/happy174")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	_, err := st.Nodes().Get(ctx, "happy174")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Node not deleted: %v", err)
	}
}

// TestBug174RGDeleteHappyPath pins the no-race case: an RG with
// no referencing RDs deletes cleanly with a 200 + maskInfo
// envelope.
func TestBug174RGDeleteHappyPath(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "happy174rg"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-groups/happy174rg")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	_, err := st.ResourceGroups().Get(ctx, "happy174rg")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("RG not deleted: %v", err)
	}
}
