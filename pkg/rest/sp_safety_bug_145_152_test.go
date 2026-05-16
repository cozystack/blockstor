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
	"strings"
	"sync"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 145 + Bug 152 — storage-pool delete safety vs. concurrent
// resource create.
//
// Bug 145 (P1) — TOCTOU race. `linstor r c -s <pool>` ran
// `refuseResourceCreateOnUnknownPool` (the Bug 118 gate, commit
// 054c144f5) THEN `Resources().Create`, with no atomicity between
// the existence check and the Resource write. A concurrent
// `linstor sp d <pool>` could slip between the two steps and
// delete the pool, leaving a phantom Resource CRD whose
// `Props["StorPoolName"]` pointed at a non-existent SP — the same
// dangling-reference symptom Bug 118 set out to fix.
//
// Bug 152 (P3) — `sp d` silent success on in-use pool. The v4
// report observed `linstor sp d <node> <pool>` succeeding even
// when live Resource CRDs still referenced the pool. The handler
// dropped the SP CRD, the Resource replicas became orphaned
// (their `Props["StorPoolName"]` now pointed at nothing), and the
// satellite reconciler had no way to resolve the storage provider
// for new volumes on those replicas.
//
// Fix shape:
//
//   - Bug 152 closes the silent-delete with a referencingResources
//     pre-check + 409 + FAIL_IN_USE envelope listing the referencing
//     replicas. Mirrors upstream LINSTOR's
//     `CtrlStorPoolApiCallHandler` refusal pattern. The check runs
//     BEFORE the store-level Delete so a refused call leaves the SP
//     CRD in place.
//
//   - Bug 152 fixes Bug 145 *transitively*: the SP delete handler
//     now refuses any pool that has a Resource referencing it. The
//     TOCTOU window collapses — either the racing r-c persists
//     first, in which case the next sp-d sees the reference and
//     refuses; or the sp-d wins, in which case the racing r-c hits
//     the Bug 118 SP-existence gate and refuses. Either ordering
//     surfaces a clean error envelope; neither leaves an orphan
//     CRD behind.
//
//   - `?force=true` is the escape hatch (precedent: Bug 92 node
//     delete, Bug 111 single-node, W13 VD shrink) — operators who
//     genuinely want to drop a referenced pool (e.g. reclaiming a
//     dead node's pool whose replicas are already marked TOMBSTONE)
//     can pass the knob and bypass the refusal.

// TestBug152StoragePoolDeleteRefusedWhenInUse pins the headline
// case from the v4 report: `sp d <node> <pool>` with a live
// Resource CRD on `(node, pool)` MUST refuse with 409 +
// FAIL_IN_USE envelope listing the referencing replica. The SP
// CRD MUST still exist after the refused call — a regression
// that dropped the pool while pretending to refuse would leave
// the cluster in a half-deleted state (CRD gone, on-disk VG
// still there, replica's StoragePool reference dangling).
func TestBug152StoragePoolDeleteRefusedWhenInUse(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "p152",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/LvmVg": "vg1", "StorDriver/ThinPool": "thin"},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "poke152"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "poke152",
		NodeName: "n1",
		Volumes: []apiv1.Volume{
			{VolumeNumber: 0, StoragePool: "p152"},
		},
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/storage-pools/p152")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 409 (Bug 152: in-use SP delete must refuse). Body: %s",
			resp.StatusCode, got)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("envelope: got empty, want one entry")
	}

	if rc[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit set", rc[0].RetCode)
	}

	if rc[0].RetCode&0xFFFF != apiCallRcFailInUse {
		t.Errorf("ret_code sub-code: got %#x, want FAIL_IN_USE (%d)",
			rc[0].RetCode&0xFFFF, apiCallRcFailInUse)
	}

	if !strings.Contains(rc[0].Message, "p152") || !strings.Contains(rc[0].Message, "n1") {
		t.Errorf("message: got %q, want pool 'p152' and node 'n1'", rc[0].Message)
	}

	if !strings.Contains(rc[0].Details, "poke152") {
		t.Errorf("details: got %q, want referencing replica 'poke152'", rc[0].Details)
	}

	// SP CRD MUST still exist after the refused call.
	if _, err := st.StoragePools().Get(ctx, "n1", "p152"); err != nil {
		t.Errorf("pool removed despite 409 refusal: %v", err)
	}
}

// TestBug152StoragePoolDeleteAllowedWhenEmpty is the happy-path
// counterpart: a pool with no referencing Resource replicas MUST
// delete cleanly with 200. Without this pin a regression that
// over-refused (e.g. matched on RD name rather than per-replica
// (node, pool)) would block every legitimate pool drop.
func TestBug152StoragePoolDeleteAllowedWhenEmpty(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "p152-empty",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/LvmVg": "vg1", "StorDriver/ThinPool": "thin"},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/storage-pools/p152-empty")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (no resources → happy delete). Body: %s",
			resp.StatusCode, got)
	}

	if _, err := st.StoragePools().Get(ctx, "n1", "p152-empty"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("pool still present after empty delete: err=%v", err)
	}
}

// TestBug152StoragePoolDeleteForceTrueBypasses pins the `?force=true`
// escape hatch (precedent: Bug 92 node delete, Bug 111 single-node
// evacuate, W13 VD shrink). Operators reclaiming a pool on a dead
// node whose replicas are already TOMBSTONE need a way past the
// refusal — without an escape hatch the only path forward would
// be to drop every referencing Resource CRD by hand first, which
// races the satellite reconciler.
//
// With ?force=true the delete MUST succeed (200) even when
// Resource references exist; the referencing replicas are left
// as-is for the operator to clean up out-of-band.
func TestBug152StoragePoolDeleteForceTrueBypasses(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "p152-force",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/LvmVg": "vg1", "StorDriver/ThinPool": "thin"},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "poke152-force"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "poke152-force",
		NodeName: "n1",
		Volumes: []apiv1.Volume{
			{VolumeNumber: 0, StoragePool: "p152-force"},
		},
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/storage-pools/p152-force?force=true")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (?force=true must bypass refusal). Body: %s",
			resp.StatusCode, got)
	}

	if _, err := st.StoragePools().Get(ctx, "n1", "p152-force"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("pool still present after ?force=true delete: err=%v", err)
	}
}

// TestBug145ResourceCreateRefusesAfterSPDeletion is the focused
// race reproducer from the v4 report. After the SP has been
// deleted (regardless of which goroutine raced to delete it), a
// subsequent `r c -s <pool>` MUST refuse with 4xx — the Bug 118
// gate must hold across all orderings, including the case where
// an `sp d` interleaved between the gate's existence check and
// the Resource store write.
//
// The most reliable in-memory reproduction: delete the SP first,
// then attempt r c with `StorPoolName` pointing at the now-absent
// pool. This is equivalent to the worst-case ordering of the race
// (sp-d wins, r-c retries with stale view), and pins that the gate
// doesn't somehow grandfather in a "pool used to exist" path.
func TestBug145ResourceCreateRefusesAfterSPDeletion(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "p145",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/LvmVg": "vg1", "StorDriver/ThinPool": "thin"},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "poke145"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Simulate the sp-d-wins ordering directly: drop the SP at the
	// store layer (no in-flight r c persisted yet, so the Bug 152
	// refusal doesn't fire) and verify the subsequent r c lands on
	// the Bug 118 gate.
	if err := st.StoragePools().Delete(ctx, "n1", "p145"); err != nil {
		t.Fatalf("pre-delete SP: %v", err)
	}

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: "n1",
			Props:    map[string]string{"StorPoolName": "p145"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/poke145/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 4xx (Bug 145: r c on deleted SP must refuse). Body: %s",
			resp.StatusCode, got)
	}

	// Phantom Resource CRD must NOT have been persisted.
	if _, err := st.Resources().Get(ctx, "poke145", "n1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Resource poke145.n1 persisted despite refused create: err=%v", err)
	}
}

// TestBug145ResourceCreateIsAtomic is the broad race exerciser.
// Fires N parallel (sp d, r c) pairs against fresh (node, pool)
// keys and asserts that every persisted Resource has a
// corresponding live SP CRD — i.e. zero orphans. Both orderings
// are acceptable outcomes per the design memo:
//
//	A) r c wins → SP refuses delete (Bug 152 refusal); SP and
//	   Resource both live.
//	B) sp d wins → r c sees missing SP and refuses (Bug 118
//	   gate); SP deleted, no Resource persisted.
//
// What's NOT acceptable: a Resource persisted on a node whose SP
// row no longer exists. That orphan is the v4 report's
// 2/3-reproduction symptom.
func TestBug145ResourceCreateIsAtomic(t *testing.T) {
	t.Parallel()

	const pairs = 50

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// Seed per-pair (RD, SP) so each goroutine pair races against
	// its own keys. Independent keys keep the test stable under
	// `-race` and isolate the assertion to "no orphan per pair".
	for i := range pairs {
		rdName := fmt.Sprintf("rd145-%d", i)
		poolName := fmt.Sprintf("p145-%d", i)

		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
			t.Fatalf("seed RD %s: %v", rdName, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        "n1",
			StoragePoolName: poolName,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			Props:           map[string]string{"StorDriver/LvmVg": "vg1", "StorDriver/ThinPool": "thin"},
		}); err != nil {
			t.Fatalf("seed SP %s: %v", poolName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	var wg sync.WaitGroup

	wg.Add(pairs * 2)

	for i := range pairs {
		rdName := fmt.Sprintf("rd145-%d", i)
		poolName := fmt.Sprintf("p145-%d", i)

		// Goroutine A: r c — try to persist a Resource pinned to the
		// pool. Either succeeds (Bug 152 refuses the racing sp d) or
		// 4xx's on the Bug 118 gate (sp d won the race).
		go func() {
			defer wg.Done()

			body, _ := json.Marshal(apiv1.ResourceCreate{
				Resource: apiv1.Resource{
					NodeName: "n1",
					Props:    map[string]string{"StorPoolName": poolName},
				},
			})

			resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
			_ = resp.Body.Close()
		}()

		// Goroutine B: sp d — try to drop the pool. Either succeeds
		// (no racing Resource persisted yet) or 409's on the Bug 152
		// refusal (r c won the race).
		go func() {
			defer wg.Done()

			resp := httpDelete(t, base+"/v1/nodes/n1/storage-pools/"+poolName)
			_ = resp.Body.Close()
		}()
	}

	wg.Wait()

	// Walk every persisted Resource on n1 — each MUST have a live
	// SP row matching its Volumes[0].StoragePool or its
	// Props["StorPoolName"]. An orphan is the bug.
	resources, err := st.Resources().List(ctx)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	for _, res := range resources {
		if res.NodeName != "n1" {
			continue
		}

		pool := res.Props["StorPoolName"]
		if pool == "" {
			continue // Diskless / autoplace path — no pool pinned.
		}

		_, err := st.StoragePools().Get(ctx, res.NodeName, pool)
		if errors.Is(err, store.ErrNotFound) {
			t.Errorf("orphan Resource %s/%s references deleted SP %q (Bug 145)",
				res.Name, res.NodeName, pool)

			continue
		}

		if err != nil {
			t.Errorf("lookup SP %s/%s for resource %s: %v",
				res.NodeName, pool, res.Name, err)
		}
	}
}
