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

package controllers

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Replicas of one definition may carry two spellings of it: one written
// before the store stored CR names keeps Pvc-Mix and Node-2, a new one
// stores pvc-mix and node-1. The satellite reads them as one definition:
// an edit of it reaches the local replica, the peer list holds both, and
// the authoritative flags and the node-id check find the target among them.
func TestOldAndNewRowsOfADefinitionAreOneDefinition(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	local := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-mix", NodeName: "node-1"},
	}
	old := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-2"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Mix", NodeName: "Node-2"},
	}
	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"}}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(local, old, rd).Build()
	r := &ResourceReconciler{Client: cli, Config: Config{NodeName: "node-1"}}
	ctx := context.Background()

	if reqs := r.enqueueResourcesForRD(ctx, rd); len(reqs) != 1 || reqs[0].Name != local.Name {
		t.Errorf("an edit of pvc-mix enqueued %v, want %s", reqs, local.Name)
	}

	if reqs := r.enqueueLocalSiblings(ctx, old); len(reqs) != 1 || reqs[0].Name != local.Name {
		t.Errorf("a change of the Pvc-Mix row enqueued %v, want the local %s", reqs, local.Name)
	}

	peers, err := r.listPeerResources(ctx, local)
	if err != nil || len(peers) != 2 {
		t.Fatalf("peers of pvc-mix = %d, %v; want both rows", len(peers), err)
	}

	target := old.DeepCopy()
	target.Spec.ResourceDefinitionName, target.Spec.NodeName = "pvc-mix", "node-2"
	target.Spec.Flags = nil

	fresh := []blockstoriov1alpha1.Resource{*local, *old}
	fresh[1].Spec.Flags = []string{apiv1.ResourceFlagDiskless}

	adoptAuthoritativeFlags(target, fresh)

	if len(target.Spec.Flags) != 1 {
		t.Errorf("the flags of the Node-2 row did not reach the node-2 target: %v", target.Spec.Flags)
	}

	if missing := peersMissingNodeID(target, []blockstoriov1alpha1.Resource{*local, *old}); len(missing) != 1 || missing[0] != "node-1" {
		t.Errorf("peersMissingNodeID = %v, want only the other node", missing)
	}
}

// The local replica itself may be the row written in the registered
// spelling; an edit of the definition still reaches it.
func TestAnEditReachesALocalReplicaInTheRegisteredSpelling(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	local := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Mix", NodeName: "Node-1"},
	}
	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"}}

	r := &ResourceReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(local, rd).Build(),
		Config: Config{NodeName: "node-1"},
	}

	if reqs := r.enqueueResourcesForRD(context.Background(), rd); len(reqs) != 1 || reqs[0].Name != local.Name {
		t.Errorf("an edit of pvc-mix enqueued %v, want %s", reqs, local.Name)
	}
}

// An adopted snapshot's entries are backfilled under the snapshot's node
// list, Node-1, and the satellite started as node-1 reads and updates that
// entry rather than adding a second one for itself.
func TestSatelliteReadsAnEntryWrittenUnderTheRegisteredSpelling(t *testing.T) {
	t.Parallel()

	entries := []blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "Node-1", Ready: true, CreateTimestamp: 1}}

	if !perNodeStatusReady(entries, "node-1") {
		t.Error("perNodeStatusReady missed the Node-1 entry for node-1")
	}

	if out := upsertPerNodeStatus(entries, "node-1", 2); len(out) != 1 {
		t.Errorf("upsertPerNodeStatus added a second entry: %v", out)
	}
}

// A replica written before the store stored CR names keeps Pvc-Mix in spec;
// the satellite still finds its definition and goes on to apply it.
func TestSatelliteAppliesAnOlderRowOfAMixedCaseDefinition(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	old := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-1", Finalizers: []string{SatelliteResourceFinalizer}},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Mix", NodeName: "Node-1"},
	}
	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"}}

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(old, rd).Build()
	cli := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*blockstoriov1alpha1.ResourceList); ok {
				return errPastTheDefinition
			}

			return c.List(ctx, list, opts...)
		},
	})

	r := &ResourceReconciler{Client: cli, Config: Config{NodeName: "node-1"}}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: old.Name}})
	if !errors.Is(err, errPastTheDefinition) {
		t.Fatalf("reconcile = %v, want it past the definition Get into the peer read", err)
	}
}

// Case variants of a name outside RFC 1123 hash to two CR names and are two
// definitions, the way the store keeps them: My_Res and my_res do not share
// peers, flags or sibling events.
func TestCaseVariantsOutsideRFC1123AreTwoDefinitions(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	mine := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "mine"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "My_Res", NodeName: "node-1"},
	}
	otherLocal := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "other-local"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "my_res", NodeName: "node-1", Flags: []string{apiv1.ResourceFlagDiskless}},
	}
	otherRemote := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "other-remote"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "my_res", NodeName: "node-2"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mine, otherLocal, otherRemote).Build()
	r := &ResourceReconciler{Client: cli, Config: Config{NodeName: "node-1"}}
	ctx := context.Background()

	peers, err := r.listPeerResources(ctx, mine)
	if err != nil || len(peers) != 1 || peers[0].Name != mine.Name {
		t.Fatalf("peers of My_Res = %v, %v; want only its own row", names(peers), err)
	}

	target := mine.DeepCopy()
	adoptAuthoritativeFlags(target, []blockstoriov1alpha1.Resource{*otherLocal})

	if len(target.Spec.Flags) != 0 {
		t.Errorf("My_Res took the flags of the my_res replica on the same node: %v", target.Spec.Flags)
	}

	if reqs := r.enqueueLocalSiblings(ctx, otherRemote); len(reqs) != 1 || reqs[0].Name != otherLocal.Name {
		t.Errorf("a change of a my_res row enqueued %v, want only the local my_res replica", reqs)
	}
}

func names(list []blockstoriov1alpha1.Resource) []string {
	out := make([]string, 0, len(list))
	for i := range list {
		out = append(out, list[i].Name)
	}

	return out
}

// A replica written before the store stored CR names keeps Pvc-Mix in spec;
// its delete still reads the volume numbers off the definition pvc-mix.
func TestADeleteReadsTheVolumesOfTheDefinitionOfAnOlderRow(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Mix", NodeName: "node-1"},
	}
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0}, {VolumeNumber: 1}},
		},
	}

	r := &ResourceReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(res, rd).Build(),
		Config: Config{NodeName: "node-1"},
	}

	nums, err := r.lookupVolumeNumbers(context.Background(), res)
	if err != nil || len(nums) != 2 {
		t.Errorf("lookupVolumeNumbers = %v, %v; want the two volumes of pvc-mix", nums, err)
	}
}
