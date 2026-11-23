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

package k8s_test

import (
	"context"
	"slices"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/finalizers"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// `node lost` removes a gone node's replicas, pools and snapshot holds
// without its satellite, which will never run again: the satellite
// finalizers are released, and the replica is marked first so a satellite
// that is in fact running leaves its storage alone. Other nodes' holds stay.
func TestLostNodeReleasesWhatOnlyItsSatelliteWouldHave(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	ctx := t.Context()
	cli := fixture.client

	const hold = "example.org/test-hold"

	objs := []client.Object{
		&crdv1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-lost.node-lost", Finalizers: []string{finalizers.SatelliteResource, hold}},
			Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-lost", NodeName: "node-lost"},
		},
		&crdv1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-gone.node-lost", Finalizers: []string{finalizers.SatelliteResource}},
			Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-gone", NodeName: "node-lost"},
		},
		&crdv1alpha1.StoragePool{
			ObjectMeta: metav1.ObjectMeta{Name: "pool1.node-lost", Finalizers: []string{finalizers.SatelliteStoragePool}},
			Spec:       crdv1alpha1.StoragePoolSpec{NodeName: "node-lost", PoolName: "pool1", ProviderKind: "ZFS"},
		},
		&crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-lost.both", Finalizers: []string{
				finalizers.SnapshotNode("node-lost"), finalizers.SnapshotNode("node-b"),
			}},
			Spec: crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-lost", SnapshotName: "both", Nodes: []string{"node-lost", "node-b"}},
		},
		&crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-lost.shared", Finalizers: []string{
				finalizers.SharedSatelliteSnapshot, finalizers.SnapshotNode("node-b"),
			}},
			Spec: crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-lost", SnapshotName: "shared", Nodes: []string{"node-lost", "node-b"}},
		},
		&crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-lost.unmigrated", Finalizers: []string{finalizers.SharedSatelliteSnapshot}},
			Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-lost", SnapshotName: "unmigrated", Nodes: []string{"node-lost", "node-c"}},
		},
		// node-a still runs an older satellite and holds no own finalizer
		// yet, so the shared one stays; it must not wait on node-lost.
		&crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-lost.upgrading", Finalizers: []string{
				finalizers.SharedSatelliteSnapshot, finalizers.SnapshotNode("node-b"),
			}},
			Spec: crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-lost", SnapshotName: "upgrading", Nodes: []string{"node-a", "node-b", "node-lost"}},
		},
		&crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-lost.only", Finalizers: []string{finalizers.SnapshotNode("node-lost")}},
			Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-lost", SnapshotName: "only", Nodes: []string{"node-lost"}},
		},
	}

	for _, obj := range objs {
		if err := cli.Create(ctx, obj); err != nil {
			t.Fatalf("create %s: %v", obj.GetName(), err)
		}
	}

	var upgrading crdv1alpha1.Snapshot
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-lost.upgrading"}, &upgrading); err != nil {
		t.Fatalf("get pvc-lost.upgrading: %v", err)
	}

	upgrading.Status.NodeStatus = []crdv1alpha1.SnapshotPerNodeStatus{
		{NodeName: "node-a", Ready: true}, {NodeName: "node-b", Ready: true}, {NodeName: "node-lost", Ready: true},
	}
	if err := cli.Status().Update(ctx, &upgrading); err != nil {
		t.Fatalf("stamp node status: %v", err)
	}

	// Runs before wipeAll: the objects left holding finalizers would
	// otherwise outlive the test and show up in the next one.
	t.Cleanup(func() {
		for _, obj := range objs {
			if err := cli.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
				continue
			}

			obj.SetFinalizers(nil)
			_ = cli.Update(context.Background(), obj)
		}
	})

	if err := store.CascadeOrphansForLostNode(ctx, k8s.New(cli), "node-lost"); err != nil {
		t.Fatalf("CascadeOrphansForLostNode: %v", err)
	}

	var held crdv1alpha1.Resource
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-lost.node-lost"}, &held); err != nil {
		t.Fatalf("get the held replica: %v", err)
	}

	if held.DeletionTimestamp.IsZero() || held.Annotations[finalizers.StorageAbandoned] == "" ||
		slices.Contains(held.Finalizers, finalizers.SatelliteResource) {
		t.Errorf("held replica: deleting=%v annotations=%v finalizers=%v; want deleting, marked abandoned, satellite finalizer released",
			!held.DeletionTimestamp.IsZero(), held.Annotations, held.Finalizers)
	}

	gone := map[string]client.Object{
		"pvc-gone.node-lost": &crdv1alpha1.Resource{},
		"pool1.node-lost":    &crdv1alpha1.StoragePool{},
	}

	for name, obj := range gone {
		err := cli.Get(ctx, client.ObjectKey{Name: name}, obj)
		if !apierrors.IsNotFound(err) {
			t.Errorf("%s still there: err=%v finalizers=%v", name, err, obj.GetFinalizers())
		}
	}

	type snapWant struct {
		finalizers, nodes []string
	}

	want := map[string]snapWant{
		"pvc-lost.both":       {[]string{finalizers.SnapshotNode("node-b")}, []string{"node-b"}},
		"pvc-lost.shared":     {[]string{finalizers.SnapshotNode("node-b")}, []string{"node-b"}},
		"pvc-lost.unmigrated": {[]string{finalizers.SharedSatelliteSnapshot}, []string{"node-c"}},
		"pvc-lost.upgrading": {
			[]string{finalizers.SharedSatelliteSnapshot, finalizers.SnapshotNode("node-b")},
			[]string{"node-a", "node-b"},
		},
	}

	for name, w := range want {
		var snap crdv1alpha1.Snapshot
		if err := cli.Get(ctx, client.ObjectKey{Name: name}, &snap); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}

		if !slices.Equal(snap.Finalizers, w.finalizers) || !slices.Equal(snap.Spec.Nodes, w.nodes) {
			t.Errorf("%s finalizers %v nodes %v, want %v and %v", name, snap.Finalizers, snap.Spec.Nodes, w.finalizers, w.nodes)
		}
	}

	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-lost.upgrading"}, &upgrading); err != nil {
		t.Fatalf("get pvc-lost.upgrading: %v", err)
	}

	if slices.ContainsFunc(upgrading.Status.NodeStatus, func(st crdv1alpha1.SnapshotPerNodeStatus) bool { return st.NodeName == "node-lost" }) {
		t.Errorf("pvc-lost.upgrading still reports node-lost: %+v", upgrading.Status.NodeStatus)
	}

	// Once node-a stamps its own, every listed node holds one and the shared
	// finalizer can go: the lost node no longer blocks that.
	if !finalizers.OthersHoldTheirOwn(upgrading.Spec.Nodes, append(upgrading.Finalizers, finalizers.SnapshotNode("node-a")), "node-a") {
		t.Errorf("pvc-lost.upgrading: the shared finalizer still waits on a node that is gone (nodes %v)", upgrading.Spec.Nodes)
	}

	var only crdv1alpha1.Snapshot
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-lost.only"}, &only); !apierrors.IsNotFound(err) {
		t.Errorf("pvc-lost.only, whose only copy was on the lost node, still there: err=%v finalizers=%v", err, only.Finalizers)
	}
}
