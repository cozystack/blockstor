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
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestActivateDeactivateAPICallRcEnvelope pins the wire shape of the
// activate/deactivate handlers (Bug 45). golinstor's response parser
// calls json.Unmarshal against `[]ApiCallRc` unconditionally and fails
// with "Unable to parse REST json data: Expecting value" if the body
// is empty — which is what the old `w.WriteHeader(200)` path produced.
// The Python CLI further dereferences `replies[0].ret_code`, so the
// array must be non-empty too.
func TestActivateDeactivateAPICallRcEnvelope(t *testing.T) {
	for _, action := range []string{"deactivate", "activate"} {
		t.Run(action, func(t *testing.T) {
			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx,
				&apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
				t.Fatalf("seed RD: %v", err)
			}

			if err := st.Resources().Create(ctx,
				&apiv1.Resource{Name: "pvc-1", NodeName: "n1"}); err != nil {
				t.Fatalf("seed Resource: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPost(t,
				base+"/v1/resource-definitions/pvc-1/resources/n1/"+action, nil)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status: got %d, want 200", resp.StatusCode)
			}

			var rc []apiv1.APICallRc

			err := json.NewDecoder(resp.Body).Decode(&rc)
			if err != nil {
				t.Fatalf("decode envelope: %v", err)
			}

			if len(rc) == 0 {
				t.Fatalf("empty envelope — golinstor and Python CLI both crash here")
			}

			if rc[0].RetCode < 0 {
				t.Errorf("ret_code = %d, want >=0 (MASK_INFO success marker)", rc[0].RetCode)
			}

			if rc[0].Message == "" {
				t.Errorf("empty message — operator log will be unreadable")
			}
		})
	}
}

// TestAdjustAllOnExistingRD: posting to /v1/resource-definitions/{rd}/adjust
// returns 200 — the controller's job is to mark the RD for resync; the
// per-replica work happens out-of-band via the satellite reconciler.
func TestAdjustAllOnExistingRD(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/adjust", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
}

// TestAdjustAllUnknownRD: 404 on missing RD.
func TestAdjustAllUnknownRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/ghost/adjust", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestAdjustResource: POST /resources/{node}/adjust nudges one replica.
func TestAdjustResource(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: "n1"}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n1/adjust", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
}

// TestAdjustResourceUnknown: 404 if the per-replica Resource is missing.
func TestAdjustResourceUnknown(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n1/adjust", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceDeactivate sets the INACTIVE flag on the named replica.
// Idempotent: a second deactivate doesn't append a duplicate flag.
// Activate clears it.
func TestResourceDeactivate(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: "n1"}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	for range 2 {
		resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n1/deactivate", nil)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("deactivate status: got %d, want 200", resp.StatusCode)
		}
	}

	got, err := st.Resources().Get(ctx, "pvc-1", "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	count := 0

	for _, f := range got.Flags {
		if f == "INACTIVE" {
			count++
		}
	}

	if count != 1 {
		t.Errorf("INACTIVE flag count: got %d, want 1 (idempotent set); flags=%v", count, got.Flags)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n1/activate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("activate status: got %d, want 200", resp.StatusCode)
	}

	got, _ = st.Resources().Get(ctx, "pvc-1", "n1")
	for _, f := range got.Flags {
		if f == "INACTIVE" {
			t.Errorf("INACTIVE flag still present after activate: %v", got.Flags)
		}
	}
}

// TestResourceActivateUnknown: 404 when the Resource doesn't exist.
func TestResourceActivateUnknown(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/ghost/resources/n9/activate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// resourceWithPort builds an apiv1.Resource whose layer object carries
// a single TCPPort entry. The in-memory store doesn't have a separate
// Status.DRBDPort field — the port surfaces verbatim through
// LayerObject.Drbd.TCPPorts (the k8s store projects Status.DRBDPort
// onto that slice), so seeding it this way is the InMemory analogue
// of "Status.DRBDPort is allocated to <port>" and is exactly what
// ClearDRBDPort drops on this store.
func resourceWithPort(rd, node string, port int32) *apiv1.Resource {
	return &apiv1.Resource{
		Name:     rd,
		NodeName: node,
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Drbd: &apiv1.DrbdResourceLayer{
				TCPPorts: []int32{port},
			},
		},
	}
}

// tcpPortsOf is the read-side mirror of resourceWithPort: it pulls
// the per-replica TCPPorts slice out of a wire Resource without
// re-asserting the nil-chain at every callsite.
func tcpPortsOf(res apiv1.Resource) []int32 {
	if res.LayerObject == nil || res.LayerObject.Drbd == nil {
		return nil
	}

	return res.LayerObject.Drbd.TCPPorts
}

// TestResourceActivatePreservesPort pins the default deact + act
// behaviour: bare activate must NOT reshuffle the TCP port. The
// documented operator workflow (piraeus-operator node-maintenance)
// relies on the resource coming back at the same DRBD addr:port so
// in-flight peer reconnects don't churn through a fresh handshake.
// The port-collision fix preserves this invariant by gating the
// reallocation on an explicit `?reallocate-port=true` query
// parameter (regression guard for the issue tracked as item 46).
func TestResourceActivatePreservesPort(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, resourceWithPort("pvc-1", "n1", 7042)); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n1/activate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("activate status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, "pvc-1", "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	ports := tcpPortsOf(got)
	if len(ports) != 1 || ports[0] != 7042 {
		t.Errorf("TCPPorts: got %v, want [7042] (bare activate must preserve port)", ports)
	}
}

// TestResourceDeactivateActivateSnapshotShipTargetPreservesState pins
// scenario 4.W20 (cross-listed with wave1 5.19 and Bug 45): a DRBD-
// layered resource that serves as a snapshot-ship destination becomes
// **permanent-deactivated** after `r deactivate` — the kernel resource
// is brought down (INACTIVE flag set), but every piece of allocated
// state survives: layer stack, TCP port, props (the wire-level surface
// of the per-replica node-id), storage-pool routing. `r activate`
// re-enables by clearing the INACTIVE flag while leaving the same
// state in place; in particular, bare activate does NOT reshuffle the
// port (regression guard for the snapshot-ship "no replication, but
// state preserved" contract documented in UG9 §"Shipping a snapshot
// in the same cluster"). Idempotency is also pinned: a second
// deactivate / activate is a no-op.
//
// This is the unit-side companion to day2-resource-deactivate.md.
// Upstream LINSTOR returns the same INACTIVE flag with a WARNING
// envelope about the operation being permanent for DRBD-layered
// resources; the envelope shape is pinned by
// TestActivateDeactivateAPICallRcEnvelope, so this test focuses on
// the store-side state-preservation invariant.
func TestResourceDeactivateActivateSnapshotShipTargetPreservesState(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	rd := &apiv1.ResourceDefinition{
		Name:       "ship-target",
		LayerStack: apiv1.DefaultLayerStack(),
	}
	if err := st.ResourceDefinitions().Create(ctx, rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const (
		port      = int32(7042)
		nodeIDKey = "DrbdOptions/NodeId"
		nodeIDVal = "3"
		poolKey   = "StorPoolName"
		poolVal   = "pool_ssd"
	)

	seed := resourceWithPort("ship-target", "n1", port)
	seed.Props = map[string]string{
		nodeIDKey: nodeIDVal,
		poolKey:   poolVal,
	}

	if err := st.Resources().Create(ctx, seed); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Deactivate twice — the second call must be a no-op, not a
	// duplicate INACTIVE flag (idempotency contract from the existing
	// TestResourceDeactivate).
	for range 2 {
		resp := httpPost(t,
			base+"/v1/resource-definitions/ship-target/resources/n1/deactivate", nil)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("deactivate status: got %d, want 200", resp.StatusCode)
		}
	}

	got, err := st.Resources().Get(ctx, "ship-target", "n1")
	if err != nil {
		t.Fatalf("get after deactivate: %v", err)
	}

	if c := countFlag(got.Flags, "INACTIVE"); c != 1 {
		t.Errorf("INACTIVE count after 2x deactivate: got %d, want 1 (idempotent); flags=%v",
			c, got.Flags)
	}

	if ports := tcpPortsOf(got); len(ports) != 1 || ports[0] != port {
		t.Errorf("TCPPorts after deactivate: got %v, want [%d] (port must survive — snapshot-ship reuses it on activate)",
			ports, port)
	}

	if v := got.Props[nodeIDKey]; v != nodeIDVal {
		t.Errorf("%s after deactivate: got %q, want %q (node-id must survive — DRBD peers expect a stable id)",
			nodeIDKey, v, nodeIDVal)
	}

	if v := got.Props[poolKey]; v != poolVal {
		t.Errorf("%s after deactivate: got %q, want %q (storage-pool routing must survive)",
			poolKey, v, poolVal)
	}

	rdGot, err := st.ResourceDefinitions().Get(ctx, "ship-target")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if !containsString(rdGot.LayerStack, apiv1.LayerKindDRBD) {
		t.Errorf("LayerStack after deactivate: got %v, want DRBD present (permanent-deactivated keeps the layer composition)",
			rdGot.LayerStack)
	}

	// Now re-activate; do it twice to pin the idempotent clear.
	for range 2 {
		resp := httpPost(t,
			base+"/v1/resource-definitions/ship-target/resources/n1/activate", nil)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("activate status: got %d, want 200", resp.StatusCode)
		}
	}

	got, err = st.Resources().Get(ctx, "ship-target", "n1")
	if err != nil {
		t.Fatalf("get after activate: %v", err)
	}

	if c := countFlag(got.Flags, "INACTIVE"); c != 0 {
		t.Errorf("INACTIVE after activate: got %d occurrences, want 0; flags=%v",
			c, got.Flags)
	}

	if ports := tcpPortsOf(got); len(ports) != 1 || ports[0] != port {
		t.Errorf("TCPPorts after activate: got %v, want [%d] (bare activate must NOT reshuffle — same listen addr on re-up)",
			ports, port)
	}

	if v := got.Props[nodeIDKey]; v != nodeIDVal {
		t.Errorf("%s after activate: got %q, want %q (node-id round-trips through deact+act)",
			nodeIDKey, v, nodeIDVal)
	}
}

// countFlag returns the number of occurrences of target in flags. The
// duplicate-flag invariant is the wire-level expression of LINSTOR's
// idempotency contract for activate / deactivate — a second deactivate
// must NOT append a second INACTIVE entry.
func countFlag(flags []string, target string) int {
	n := 0

	for _, f := range flags {
		if f == target {
			n++
		}
	}

	return n
}

// containsString is the test-local equivalent of slices.Contains, kept
// here so the test doesn't pull in a fresh stdlib import just for one
// membership check.
func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}

	return false
}

// TestResourceActivateReallocatePortClears pins the port-collision
// recovery path (issue 46): `?reallocate-port=true` drops the
// persisted port allocation so the controller's allocator
// (resource_controller.allocateDRBDFields) gates on
// `Status.DRBDPort == nil` and re-runs to pick a fresh free port
// on the next reconcile.
//
// The in-memory store has no Status.DRBDPort field — clearing
// LayerObject.Drbd.TCPPorts is the wire-level equivalent the k8s
// store materialises from Status.DRBDPort. Asserting the slice is
// empty is the same invariant the controller's allocator gates on
// once the merge-patch lands.
func TestResourceActivateReallocatePortClears(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, resourceWithPort("pvc-1", "n1", 7042)); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t,
		base+"/v1/resource-definitions/pvc-1/resources/n1/activate?reallocate-port=true",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("activate status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, "pvc-1", "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ports := tcpPortsOf(got); len(ports) != 0 {
		t.Errorf("TCPPorts: got %v, want [] (reallocate-port=true must clear)", ports)
	}
}
