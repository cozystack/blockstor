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
	"fmt"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 186 (P2) — `vd d` doesn't refuse on Resources still
// referencing the volume.
//
// Upstream LINSTOR's CtrlVlmDfnDeleteApiCallHandler walks the
// referencing Resources (via Volumes whose VolumeNumber matches the
// dropped VD) and aborts with FAIL_IN_USE | MASK_ERROR if any
// replica still carries the volume. Blockstor's pre-fix handler
// blindly called `Store.VolumeDefinitions().Delete` and pruned the
// Volumes off each Resource afterwards (Bug 139 surfacing patch).
// Net: the VD spec was dropped, the satellite-observed Volume rows
// went with it, but the Resource CRDs themselves stayed alive on
// nodes that were arguably mid-flight using the dropped volume —
// no operator-visible signal that the delete was unsafe.
//
// Fix shape mirrors Bug 92 / Bug 174 envelope contract: 409 +
// FAIL_IN_USE | MASK_ERROR, Message names the parent RD and VlmNr,
// Cause lists the referencing Resources (sorted by NodeName so the
// surfaced text is deterministic), Correction points at the
// remedial commands. `?force=true` (and the body's `force` field
// for completeness with Bug 92 / W13) bypasses the refusal so the
// operator can drop the spec out from under a stuck satellite.

// TestBug186VDDeleteRefusedWhenResourceReferences pins the wire
// shape: when at least one Resource still carries the
// (VolumeNumber=vn) Volume row, DELETE returns 409 +
// FAIL_IN_USE | MASK_ERROR with the referencing Resources surfaced
// in the envelope's Cause line.
func TestBug186VDDeleteRefusedWhenResourceReferences(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName  = "pvc-bug186-refuse"
		nodeA   = "node-a"
		nodeB   = "node-b"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// Seed two Resources, each carrying a Status.Volumes row for the
	// VD being dropped — both should surface in the refusal Cause.
	for _, node := range []string{nodeA, nodeB} {
		err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     rdName,
			NodeName: node,
			Volumes: []apiv1.Volume{
				{VolumeNumber: volNum, DevicePath: "/dev/fake/" + rdName + "_00000"},
			},
		})
		if err != nil {
			t.Fatalf("seed Resource %s/%s: %v", rdName, node, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d",
			base, rdName, volNum))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("DELETE status: got %d, want 409", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty on refusal; want at least one entry")
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit set (apiCallRcError=%#x)",
			rcs[0].RetCode, apiCallRcError)
	}

	if rcs[0].RetCode&0xFFFF != apiCallRcFailInUse {
		t.Errorf("ret_code sub-code: got %d, want FAIL_IN_USE (%d)",
			rcs[0].RetCode&0xFFFF, apiCallRcFailInUse)
	}

	if rcs[0].RetCode&maskInfo != 0 {
		t.Errorf("ret_code: got %#x, MASK_INFO bit set on a conflict envelope", rcs[0].RetCode)
	}

	// Cause / Details / Message MUST name both referencing nodes so
	// the operator knows which Resources to drop first.
	hay := rcs[0].Message + "\n" + rcs[0].Cause + "\n" + rcs[0].Details + "\n" + rcs[0].Correc
	for _, node := range []string{nodeA, nodeB} {
		if !strings.Contains(hay, node) {
			t.Errorf("envelope omits referencing node %q; envelope=%+v", node, rcs[0])
		}
	}

	// Verify the VD spec survived the refusal — a stripped spec on a
	// rejected DELETE is a worse failure mode than the bug itself.
	if _, err := st.VolumeDefinitions().Get(ctx, rdName, volNum); err != nil {
		t.Errorf("VD %s/%d unexpectedly gone after refused DELETE: %v",
			rdName, volNum, err)
	}

	// Resources MUST keep their Volume rows after the refusal.
	for _, node := range []string{nodeA, nodeB} {
		got, err := st.Resources().Get(ctx, rdName, node)
		if err != nil {
			t.Fatalf("re-get Resource %s/%s: %v", rdName, node, err)
		}

		if len(got.Volumes) != 1 || got.Volumes[0].VolumeNumber != volNum {
			t.Errorf("Resource %s/%s lost its Volume row after refused DELETE: %+v",
				rdName, node, got.Volumes)
		}
	}
}

// TestBug186VDDeleteAllowedWhenNoReferences pins the happy path:
// when no Resource carries the volume row, DELETE succeeds with
// 200 + MASK_INFO (the pre-Bug-186 behaviour).
func TestBug186VDDeleteAllowedWhenNoReferences(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName  = "pvc-bug186-ok"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d",
			base, rdName, volNum))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", resp.StatusCode)
	}

	// VD must actually be gone after the success.
	if _, err := st.VolumeDefinitions().Get(ctx, rdName, volNum); err == nil {
		t.Errorf("VD %s/%d still present after successful DELETE", rdName, volNum)
	}
}

// TestBug186VDDeleteForceTrueBypasses pins the escape hatch:
// `?force=true` bypasses the FAIL_IN_USE refusal even when
// referencing Resources exist. Mirrors Bug 92 (node delete force),
// Bug 152 (sp delete force), Bug 174 (rg/node delete force) and the
// VD-shrink W13 force semantics.
func TestBug186VDDeleteForceTrueBypasses(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName  = "pvc-bug186-force"
		nodeA   = "node-a"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: nodeA,
		Volumes: []apiv1.Volume{
			{VolumeNumber: volNum, DevicePath: "/dev/fake/" + rdName + "_00000"},
		},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d?force=true",
			base, rdName, volNum))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE?force=true status: got %d, want 200", resp.StatusCode)
	}

	// VD spec is gone…
	if _, err := st.VolumeDefinitions().Get(ctx, rdName, volNum); err == nil {
		t.Errorf("VD %s/%d still present after force DELETE", rdName, volNum)
	}

	// …and the pre-existing Bug 139 prune ran on the bypass path,
	// so the Resource no longer carries the dropped Volume row.
	got, err := st.Resources().Get(ctx, rdName, nodeA)
	if err != nil {
		t.Fatalf("re-get Resource %s/%s: %v", rdName, nodeA, err)
	}

	if len(got.Volumes) != 0 {
		t.Errorf("Resource %s/%s still carries Volume rows after force DELETE: %+v",
			rdName, nodeA, got.Volumes)
	}
}
