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

package dispatcher_test

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// TestU216AddPeerRegeneratesExistingReplicaMesh pins the upstream
// LINSTOR user report U216: autoplace-EXTENDING an existing resource
// produced a StandAlone replica because the EXISTING replicas' .res was
// never regenerated to include the newly-added peer — DRBD on the
// surviving siblings had no `on <newnode>` block / no connection path to
// the freshly-created replica, so the new replica's handshake found no
// peer and wedged in StandAlone.
//
// blockstor regenerates the DesiredResource for EVERY replica each
// reconcile from the live RD's full replica set (dispatcher.BuildDesired
// derives the per-peer drbdOpts from the complete `peers` slice). So when
// a 3rd replica joins a 2-replica resource, the next reconcile of each
// EXISTING replica must carry the new peer in DesiredResource.Peers AND
// the per-peer drbdOpts block — which the satellite renders into a fresh
// `on <newnode>` block + a full 3-way connection mesh in that replica's
// .res. The new replica can then reach Established/UpToDate and never
// StandAlone.
//
// This test pins the dispatcher half (the .res render half is pinned by
// pkg/drbd TestBuildEmitsConnectionMesh + pkg/satellite render tests; the
// kernel-truth half is the L6 cell u216-add-peer-mesh.sh / the L7 replay
// add-replica.yaml). Without the dispatcher including the new peer in
// EVERY existing replica's DesiredResource, no render path could ever
// emit the regenerated mesh — this is the root-cause guard.
func TestU216AddPeerRegeneratesExistingReplicaMesh(t *testing.T) {
	t.Parallel()

	const rdName = "pvc-u216"

	id := func(v int32) *int32 { return &v }

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	// The established 2-replica resource: n1 (id 0) + n2 (id 1).
	n1 := newDiskfulResource(rdName, "n1", id(0))
	n2 := newDiskfulResource(rdName, "n2", id(1))

	// The EXTENSION: a 3rd diskful replica joins on n3 with id 2.
	n3 := newDiskfulResource(rdName, "n3", id(2))

	all := []blockstoriov1alpha1.Resource{*n1, *n2, *n3}

	// Reconcile the EXISTING replica n1 against the now-3-member replica
	// set. n1's DesiredResource MUST list BOTH n2 and the newly-added n3
	// (the U216 root cause: the new peer missing from an existing
	// replica's desired peer set → no `on n3` block → StandAlone).
	got := dispatcher.BuildDesired(n1, peersExcept(all, "n1"), nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	peerNames := make([]string, 0, len(got.GetPeers()))
	for _, p := range got.GetPeers() {
		peerNames = append(peerNames, p.Name)
	}
	slices.Sort(peerNames)

	if !slices.Equal(peerNames, []string{"n2", "n3"}) {
		t.Fatalf("existing replica n1's DesiredResource.Peers = %v, want [n2 n3] "+
			"(the newly-added peer n3 MUST be regenerated into the existing "+
			"replica's peer set — U216: missing → StandAlone)", peerNames)
	}

	// The per-peer drbdOpts block for the new peer n3 must be fully
	// populated (node-id / address / port) so the satellite renders a
	// complete `on n3 { ... }` block + a connection path to it. A peer
	// in Peers but WITHOUT its drbdOpts block would render a malformed
	// .res (empty address → drbd dials 0.0.0.0 → never connects).
	for _, key := range []string{"peer.n3.node-id", "peer.n3.address", "peer.n3.port"} {
		if got.DrbdOptions[key] == "" {
			t.Errorf("drbdOpts missing %q for newly-added peer n3 — existing "+
				"replica's mesh not fully regenerated (drbdOpts=%v)", key, got.DrbdOptions)
		}
	}

	// Symmetry: the other existing replica n2 must ALSO carry n3 in its
	// peer set. Both surviving diskful peers need the new `on n3` block,
	// not just one — a partial mesh still wedges the new replica.
	got2 := dispatcher.BuildDesired(n2, peersExcept(all, "n2"), nil, nil, rd, nil)
	if got2 == nil {
		t.Fatalf("BuildDesired(n2) returned nil")
	}

	if !desiredHasPeer(got2, "n3") {
		t.Errorf("existing replica n2's DesiredResource is missing peer n3 — "+
			"every existing replica must regenerate the new peer (got %v)", got2.GetPeers())
	}

	if !desiredHasPeer(got2, "n1") {
		t.Errorf("existing replica n2's DesiredResource is missing peer n1 — "+
			"the original mesh must be preserved alongside the new peer (got %v)", got2.GetPeers())
	}
}

// peersExcept returns every replica in `all` whose NodeName differs from
// `self` — the dispatcher contract (BuildDesired takes target + the rest
// of the replica set as peers).
func peersExcept(all []blockstoriov1alpha1.Resource, self string) []blockstoriov1alpha1.Resource {
	out := make([]blockstoriov1alpha1.Resource, 0, len(all))
	for i := range all {
		if all[i].Spec.NodeName == self {
			continue
		}
		out = append(out, all[i])
	}
	return out
}

// desiredHasPeer reports whether a DesiredResource lists a peer by node
// name.
func desiredHasPeer(dr *intent.DesiredResource, name string) bool {
	for _, p := range dr.GetPeers() {
		if p.Name == name {
			return true
		}
	}
	return false
}
