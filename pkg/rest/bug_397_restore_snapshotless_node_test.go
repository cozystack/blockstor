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

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestSnapshotRestoreBug397RejectsSnapshotlessNode is the Bug 397 (P0
// DATA INTEGRITY) input-validation regression. An explicit `--node-name`
// restore onto a node that does NOT hold the snapshot must be REJECTED at
// the API edge — never silently stamp a diskful Resource there. Such a
// replica would fall back to a blank CreateVolume on the satellite and,
// taking the skip-init-sync day0 seed, latch UpToDate while EMPTY: an
// empty replica presenting as a good copy, promotable on failover.
//
// Snapshot lives on {n1, n2}; the operator asks to restore onto {n1, n3}.
// n3 is snapshot-less → the whole request is refused, and NO Resource (and
// no target RD) is created (reject before any Store mutation).
func TestSnapshotRestoreBug397RejectsSnapshotlessNode(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-src", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "zpool"},
	}); err != nil {
		t.Fatalf("seed source resource: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1", "n2"}, // snapshot present ONLY here
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// n3 does NOT hold the snapshot — must be rejected.
	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-restored",
		FromSnapshot: "snap-1",
		Nodes:        []string{"n1", "n3"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400 (restore onto snapshot-less node must be rejected)", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode APICallRc envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope: got %d entries, want 1", len(rcs))
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code %d must carry MASK_ERROR", rcs[0].RetCode)
	}

	if rcs[0].RetCode&apiCallRcFailNotFoundNode == 0 {
		t.Errorf("ret_code %d must carry FAIL_NOT_FOUND_NODE sub-code (%d)",
			rcs[0].RetCode, apiCallRcFailNotFoundNode)
	}

	if !contains(rcs[0].Message, "n3") {
		t.Errorf("message %q must name the offending node n3", rcs[0].Message)
	}

	// CRITICAL: no orphan state. The target RD must NOT have been created,
	// and no Resource may have been stamped on the bad node.
	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-restored"); err == nil {
		t.Errorf("target RD pvc-restored must NOT exist after a rejected restore (orphan state)")
	}

	res, err := st.Resources().ListByDefinition(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list restored Resources: %v", err)
	}

	if len(res) != 0 {
		t.Errorf("no Resource may be stamped after a rejected restore, got %d", len(res))
	}
}

// TestSnapshotRestoreBug397AcceptsSnapshotNodes is the positive control:
// restoring onto nodes that DO hold the snapshot proceeds normally and
// stamps one Resource per node. Guards against the Bug 397 input gate
// over-rejecting the legitimate restore path.
func TestSnapshotRestoreBug397AcceptsSnapshotNodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-src", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "zpool"},
	}); err != nil {
		t.Fatalf("seed source resource: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Subset of snap.Nodes — every requested node holds the snapshot.
	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-restored",
		FromSnapshot: "snap-1",
		Nodes:        []string{"n1", "n2"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (restore onto snapshot nodes must succeed)", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list restored Resources: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("Resource CRDs stamped: got %d, want 2", len(got))
	}
}

// TestSnapshotRestoreBug397NoNodesUnaffected verifies the auto-place path
// (no explicit --node-name) is untouched by the Bug 397 input gate: the
// gate only constrains the explicit-node branch, leaving the auto-place
// branch's own constrainAutoplaceToSnapshotNodes constraint to do its job.
func TestSnapshotRestoreBug397NoNodesUnaffected(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// No Nodes/NodeNames at all — auto-place branch, must reach 201.
	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-restored",
		FromSnapshot: "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (no-node restore must not be rejected by the explicit-node gate)", resp.StatusCode)
	}
}
