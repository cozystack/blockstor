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
	}

	for _, obj := range objs {
		if err := cli.Create(ctx, obj); err != nil {
			t.Fatalf("create %s: %v", obj.GetName(), err)
		}
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

	want := map[string][]string{
		"pvc-lost.both":       {finalizers.SnapshotNode("node-b")},
		"pvc-lost.shared":     {finalizers.SnapshotNode("node-b")},
		"pvc-lost.unmigrated": {finalizers.SharedSatelliteSnapshot},
	}

	for name, finals := range want {
		var snap crdv1alpha1.Snapshot
		if err := cli.Get(ctx, client.ObjectKey{Name: name}, &snap); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}

		if !slices.Equal(snap.Finalizers, finals) {
			t.Errorf("%s finalizers %v, want %v", name, snap.Finalizers, finals)
		}
	}
}
