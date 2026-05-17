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
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 263 (P1) — stand-caught on dev-kvaps. After
//
//	linstor r c worker-1 test2 -s zfs-thin
//	linstor r c worker-3 test2 -s zfs-thin
//
// worker-1 and worker-3 each had Status.DRBDNodeID allocated and the
// backing zvols were created, but `linstor r l` showed both Resources
// in Unknown state with `connections: Connecting` between them.
// Satellite worker-1 logs were stuck on
// `waiting for peer Status.DRBDNodeID allocation` for the freshly
// added worker-3 replica — the existing satellite never re-rendered
// its .res to include worker-3 as a peer and never ran
// `drbdadm adjust` to add it.
//
// Root cause: the REST `handleResourceCreate` path persisted the new
// Resource CRD but never bumped `blockstor.io/peer-changed` on the
// surviving siblings. Bug 67's delete-side bump was the only existing
// caller of `bumpPeerChangedOnSiblings`. Without a wake-up event on
// the surviving replicas, their satellite reconcilers relied on the
// c-r sibling watch firing on the new Resource — which DOES fire on
// the Spec create, but lands BEFORE the controller-side allocator has
// stamped Status.DRBDNodeID on the new replica, so
// `waitForControllerAllocation` requeues the survivor at 2s.
// The follow-up watch event for the Status patch is per-subresource
// and can be filtered or coalesced by controller-runtime under load,
// leaving the survivor stuck in the wait-for-allocation backoff.
//
// The annotation bump is the deterministic backstop: stamp it AFTER
// the create + Status-allocation both commit, so the survivor's
// reconcile sees both a fresh annotation event AND the post-allocation
// peer Status. The annotation value is monotonic — bumps from the
// CREATE path advance the same RFC3339Nano stamp Bug 67's delete-path
// emits, so the satellite watch can't short-circuit on
// resourceVersion gates.
func TestBug263ResourceCreateBumpsPeerChangedOnSurvivors(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "test2"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"worker-1", "worker-2", "worker-3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "zfs-thin",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	// One pre-existing diskful replica on worker-1 — the survivor.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "worker-1",
		Props:    map[string]string{"StorPoolName": "zfs-thin"},
	}); err != nil {
		t.Fatalf("seed worker-1 replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	before := time.Now().UTC()

	// `linstor r c worker-3 test2 -s zfs-thin`. Bare ResourceCreate
	// envelope with explicit StorPoolName so the create lands on the
	// fresh-create branch (not the takeover promote-path).
	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: "worker-3",
			Props:    map[string]string{"StorPoolName": "zfs-thin"},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("status: got %d, want 2xx", resp.StatusCode)
	}

	// Survivor worker-1 must carry a fresh, parseable peer-changed
	// stamp newer than the call-floor. The newly-created worker-3
	// replica must NOT carry the stamp — it doesn't need a wake-up
	// (its own reconciler picks the local Resource up via the For
	// watch).
	w1, err := st.Resources().Get(ctx, rdName, "worker-1")
	if err != nil {
		t.Fatalf("get survivor worker-1: %v", err)
	}

	raw, ok := w1.Annotations[apiv1.PeerChangedAnnotation]
	if !ok || raw == "" {
		t.Fatalf("survivor worker-1 missing %s annotation after peer create; annotations=%v",
			apiv1.PeerChangedAnnotation, w1.Annotations)
	}

	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("survivor worker-1 stamp %q is not RFC3339Nano: %v", raw, err)
	}

	if ts.Before(before) {
		t.Errorf("survivor worker-1 stamp %v predates the CREATE call floor %v", ts, before)
	}

	w3, err := st.Resources().Get(ctx, rdName, "worker-3")
	if err != nil {
		t.Fatalf("get newly-created worker-3: %v", err)
	}

	if v, ok := w3.Annotations[apiv1.PeerChangedAnnotation]; ok {
		t.Errorf("newly-created worker-3 must NOT carry %s (own reconciler picks it up via For); got %q",
			apiv1.PeerChangedAnnotation, v)
	}
}

// TestBug263TakeoverPromotionBumpsSiblings pins the takeover path:
// `linstor r c <node> <rd>` against an existing TIE_BREAKER witness
// (Bug 260 promote path) must ALSO bump the surviving diskful peers.
// The promote path mutates an existing Resource (drops TIE_BREAKER,
// stamps StorPoolName) — from the kernel's perspective this is a
// brand-new diskful peer joining, identical to a fresh Resource
// create, so the survivor satellites need the same wake-up.
func TestBug263TakeoverPromotionBumpsSiblings(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-takeover-263"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"worker-1", "worker-2", "worker-3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	// Two diskful replicas + one TIE_BREAKER witness on worker-3.
	for _, n := range []string{"worker-1", "worker-2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     rdName,
			NodeName: n,
			Props:    map[string]string{"StorPoolName": "pool"},
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "worker-3",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed TIE_BREAKER: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	before := time.Now().UTC()

	// Bare promote-takeover request (Bug 260 shape).
	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "worker-3"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("status: got %d, want 2xx (takeover path)", resp.StatusCode)
	}

	// Both surviving diskful peers must be stamped.
	for _, n := range []string{"worker-1", "worker-2"} {
		got, err := st.Resources().Get(ctx, rdName, n)
		if err != nil {
			t.Fatalf("get survivor %s: %v", n, err)
		}

		raw, ok := got.Annotations[apiv1.PeerChangedAnnotation]
		if !ok || raw == "" {
			t.Fatalf("survivor %s missing %s annotation after takeover; annotations=%v",
				n, apiv1.PeerChangedAnnotation, got.Annotations)
		}

		ts, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			t.Fatalf("survivor %s stamp %q is not RFC3339Nano: %v", n, raw, err)
		}

		if ts.Before(before) {
			t.Errorf("survivor %s stamp %v predates the takeover call floor %v", n, ts, before)
		}
	}
}
