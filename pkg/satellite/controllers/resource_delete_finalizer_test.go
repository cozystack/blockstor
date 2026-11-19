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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/finalizers"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// The satellite finalizer is the only thing that keeps a deleted Resource's
// volume from being forgotten: once it goes, nothing deletes the volume. A
// satellite that has not registered the replica's pool yet must therefore
// keep the finalizer and retry, not release it over a volume still on disk.
func TestHandleDeleteKeepsFinalizerWhilePoolIsUnregistered(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-delete"
		rdName = "pvc-del"
	)

	baseCli, _, provider, _ := newDeleteRaceFixture(t)

	reconciler := &ResourceReconciler{
		Client: baseCli,
		Config: Config{
			NodeName:  node,
			Apply:     satellite.NewReconciler(satellite.ReconcilerConfig{NodeName: node}),
			Exec:      storage.NewFakeExec(),
			APIReader: baseCli,
		},
	}

	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: rdName + "." + node},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !result.Requeue && result.RequeueAfter == 0 { //nolint:staticcheck // SA1019: handleDelete answers with Requeue
		t.Errorf("Reconcile did not requeue a delete it could not finish")
	}

	if got := provider.deletesSnapshot(); len(got) != 0 {
		t.Errorf("DeleteVolume ran %d times through an unregistered pool", len(got))
	}

	var after blockstoriov1alpha1.Resource

	err = baseCli.Get(t.Context(), client.ObjectKey{Name: rdName + "." + node}, &after)
	if err != nil {
		t.Fatalf("Resource vanished while its pool was unregistered: %v", err)
	}

	if !slices.Contains(after.Finalizers, SatelliteResourceFinalizer) {
		t.Errorf("finalizer released before the volume was deleted: %v", after.Finalizers)
	}
}

// Every source of volume numbers counts: a volume the RD no longer lists but
// the replica's own record still names is deleted too, instead of being left
// on disk with nothing to delete it later.
func TestLookupVolumeNumbersJoinsEverySource(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-join"
		rdName = "pvc-join"
	)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0}},
		},
	}
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:        rdName + "." + node,
			Annotations: map[string]string{blockstoriov1alpha1.ResourceAnnotationVolumeNumbers: "0,1"},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{NodeName: node, ResourceDefinitionName: rdName},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 2}},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(newToggleDiskTestScheme(t)).WithObjects(rd, res).Build()

	got, err := (&ResourceReconciler{Client: cli, Config: Config{NodeName: node}}).lookupVolumeNumbers(t.Context(), res)
	if err != nil {
		t.Fatalf("lookupVolumeNumbers: %v", err)
	}

	if want := []int32{0, 1, 2}; !slices.Equal(got, want) {
		t.Errorf("lookupVolumeNumbers = %v, want %v", got, want)
	}
}

// A replica `node lost` abandoned keeps its storage even when its satellite
// is in fact still running: DRBD is torn down, the volumes stay, as LINSTOR
// leaves a lost node's storage.
func TestHandleDeleteOfAnAbandonedReplicaKeepsItsStorage(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-delete"
		rdName = "pvc-del"
	)

	baseCli, rec, provider, _ := newDeleteRaceFixture(t)

	var res blockstoriov1alpha1.Resource
	if err := baseCli.Get(t.Context(), client.ObjectKey{Name: rdName + "." + node}, &res); err != nil {
		t.Fatalf("get: %v", err)
	}

	res.Annotations = map[string]string{finalizers.StorageAbandoned: "node-lost"}
	if err := baseCli.Update(t.Context(), &res); err != nil {
		t.Fatalf("mark abandoned: %v", err)
	}

	reconciler := &ResourceReconciler{Client: baseCli, Config: Config{
		NodeName: node, Apply: rec, Exec: storage.NewFakeExec(), APIReader: baseCli,
	}}

	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: rdName + "." + node}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := provider.deletesSnapshot(); len(got) != 0 {
		t.Errorf("DeleteVolume ran %d times on an abandoned replica", len(got))
	}

	var after blockstoriov1alpha1.Resource

	err = baseCli.Get(t.Context(), client.ObjectKey{Name: rdName + "." + node}, &after)
	if err == nil && slices.Contains(after.Finalizers, SatelliteResourceFinalizer) {
		t.Errorf("finalizer kept on an abandoned replica: %v", after.Finalizers)
	}
}
