// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// The draining replica of an earlier definition under the same name reported
// UpToDate at that name's day0 generation, so it passed the data gate and its
// generation was copied into the new replicas as their seed. The seed then
// stood in for the one the new volume needed, and its replicas came up
// Inconsistent. A peer on its way out seeds nothing.
func TestPickSeedFromPeersSkipsALeavingPeer(t *testing.T) {
	t.Parallel()

	const rdName = "pvc-reused"

	day0 := drbd.Day0GIFor(rdName, 0)

	upToDate := func(name string) blockstoriov1alpha1.Resource {
		r := blockstoriov1alpha1.Resource{}
		r.Name = name
		r.Spec.ResourceDefinitionName = rdName
		r.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
			{VolumeNumber: 0, DiskState: string(drbd.DiskStateUpToDate), CurrentGI: day0},
		}

		return r
	}

	now := metav1.Now()

	held := upToDate(rdName + ".node-down")
	held.DeletionTimestamp = &now
	held.Finalizers = []string{"blockstor.org/satellite-resource"}

	flagged := upToDate(rdName + ".node-down")
	flagged.Spec.Flags = []string{apiv1.ResourceFlagDelete}

	for name, leaving := range map[string]blockstoriov1alpha1.Resource{"finalizer-held": held, "flagged": flagged} {
		if got := pickSeedFromPeers([]blockstoriov1alpha1.Resource{leaving}, rdName+".node-a", 0); got != "" {
			t.Errorf("%s: seed = %q, want none from a leaving peer", name, got)
		}
	}

	if got := pickSeedFromPeers([]blockstoriov1alpha1.Resource{upToDate(rdName + ".node-b")}, rdName+".node-a", 0); got != day0 {
		t.Errorf("a peer that stays: seed = %q, want %q", got, day0)
	}
}
