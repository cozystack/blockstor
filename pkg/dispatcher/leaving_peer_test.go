// SPDX-License-Identifier: Apache-2.0

package dispatcher

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// A definition created over the draining replica of an earlier one under the
// same name saw that replica as a peer holding node id 0. It won the initial
// Primary election, which it never runs, so neither new replica seeded and
// the volume stayed Inconsistent. The election is among the replicas that
// stay, whether the leaving one is held by its finalizer or flagged DELETE.
func TestLowestDiskfulIDSkipsALeavingPeer(t *testing.T) {
	t.Parallel()

	replica := func(node string, id int32) blockstoriov1alpha1.Resource {
		r := blockstoriov1alpha1.Resource{}
		r.Name = "pvc-" + node
		r.Spec.NodeName = node
		r.Spec.DRBDNodeID = &id

		return r
	}

	now := metav1.Now()

	held := replica("node-down", 0)
	held.DeletionTimestamp = &now
	held.Finalizers = []string{"blockstor.org/satellite-resource"}

	flagged := replica("node-down", 0)
	flagged.Spec.Flags = []string{apiv1.ResourceFlagDelete}

	for name, leaving := range map[string]blockstoriov1alpha1.Resource{"finalizer-held": held, "flagged": flagged} {
		target := replica("node-a", 1)
		peers := []blockstoriov1alpha1.Resource{leaving, replica("node-b", 2)}

		if got := lowestDiskfulID(&target, peers); got != 1 {
			t.Errorf("%s: lowest diskful id = %d, want 1 (the leaving peer's 0 does not count)", name, got)
		}
	}

	target := replica("node-a", 1)
	if got := lowestDiskfulID(&target, []blockstoriov1alpha1.Resource{replica("node-down", 0)}); got != 0 {
		t.Errorf("a peer that stays: lowest diskful id = %d, want 0", got)
	}
}
