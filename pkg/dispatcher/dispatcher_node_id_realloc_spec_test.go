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

package dispatcher_test

import (
	"slices"
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// TestBuildDesiredDropsDeleteFlaggedPeers pins spec §4.2 phase 1:
// a peer Resource flagged DELETE (or DRBD_DELETE) must drop out of
// the dispatcher's rendered peer list so the next satellite
// reconcile's `computeRemovedPeers` diff sees it as removed and
// fires `drbdadm del-peer` + `drbdmeta forget-peer` against the
// kernel slot.
//
// Why this matters: without the drop, the survivors' satellites
// would keep the flagged peer in their `.res` and never run the
// forget-peer cleanup; the eventual physical reap of the peer's
// row would then be invisible to surviving DRBD-9 (the kernel slot
// stays alive bound to the old peer's bitmap), and a subsequent
// autoplace re-using the same `node-id` would wedge in Connecting.
func TestBuildDesiredDropsDeleteFlaggedPeers(t *testing.T) {
	t.Parallel()

	rdName := "pvc-spec-§4.2"

	for _, tc := range []struct {
		name       string
		flag       string
		wantInPeer bool
	}{
		{name: "DELETE drops peer", flag: "DELETE", wantInPeer: false},
		{name: "DRBD_DELETE drops peer", flag: "DRBD_DELETE", wantInPeer: false},
		{name: "no flag keeps peer", flag: "", wantInPeer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rd := &blockstoriov1alpha1.ResourceDefinition{
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
						{VolumeNumber: 0, SizeKib: 1024},
					},
				},
			}

			target := mkResource("w1", rdName, 0, nil)
			peer := mkResource("w2", rdName, 1, nil)

			if tc.flag != "" {
				peer.Spec.Flags = []string{tc.flag}
			}

			got := dispatcher.BuildDesired(target, []blockstoriov1alpha1.Resource{*peer}, nil, nil, rd, nil)
			if got == nil {
				t.Fatalf("BuildDesired returned nil")
			}

			present := false

			for _, name := range got.GetPeerNames() {
				if name == "w2" {
					present = true

					break
				}
			}

			if present != tc.wantInPeer {
				t.Fatalf("peer w2 in rendered peer list: got=%v, want=%v (flag=%q)",
					present, tc.wantInPeer, tc.flag)
			}

			// drbdOpts should similarly carry / omit the peer.<name>.*
			// keys in lockstep with the peer-name set.
			_, hasPort := got.DrbdOptions["peer.w2.port"]
			_, hasID := got.DrbdOptions["peer.w2.node-id"]
			_, hasAddr := got.DrbdOptions["peer.w2.address"]

			if (hasPort != tc.wantInPeer) || (hasID != tc.wantInPeer) || (hasAddr != tc.wantInPeer) {
				t.Errorf("drbdOpts peer.w2.* keys: port=%v id=%v addr=%v, want all=%v",
					hasPort, hasID, hasAddr, tc.wantInPeer)
			}
		})
	}
}

// TestBuildDesiredKeepsLocalEvenWithDeleteFlag pins the inverse: a
// LOCAL replica flagged DELETE is still rendered as the local
// host's `on <node> { ... }` block — the local satellite needs to
// observe its own flag so it can tear down its own kernel slot
// gracefully. Only the peer set is filtered by the spec §4.2
// "visible to surviving peers in flagged-for-deletion state" rule.
//
// (The satellite's own teardown is driven by the K8s
// DeletionTimestamp finalizer chain, not by the dispatcher — but
// the Spec.Flags=DELETE intermediate state must still produce a
// renderable `.res` so the local satellite can adjust the local
// kernel slot without panicking on a missing local block.)
func TestBuildDesiredKeepsLocalEvenWithDeleteFlag(t *testing.T) {
	t.Parallel()

	rdName := "pvc-spec-§4.2-local"
	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	target := mkResource("w1", rdName, 0, []string{"DELETE"})
	peer := mkResource("w2", rdName, 1, nil)

	got := dispatcher.BuildDesired(target, []blockstoriov1alpha1.Resource{*peer}, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	if got.GetNodeName() != "w1" {
		t.Errorf("local NodeName: got=%q want=%q", got.GetNodeName(), "w1")
	}

	// And the DELETE flag must propagate onto the DesiredResource
	// so downstream wire paths see it (the satellite's existing
	// handleDelete path triggers on DeletionTimestamp on the K8s
	// side; this Flags bit just helps logging / observability).
	if !slices.Contains(got.GetFlags(), "DELETE") {
		t.Errorf("local DesiredResource flags missing DELETE: got=%v", got.GetFlags())
	}
}

// mkResource is the local helper to keep test bodies compact.
// node-id is wired via Status.DRBDNodeID (the dispatcher reads it
// off Status as the source of truth, per Phase 8.1).
func mkResource(node, rdName string, nodeID int32, flags []string) *blockstoriov1alpha1.Resource {
	id := nodeID

	return &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               node,
			Flags:                  flags,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: &id,
		},
	}
}
