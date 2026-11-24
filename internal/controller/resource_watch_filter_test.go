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

package controller_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// watchedResource is a fully allocated, running replica as the
// controller's cache holds it on an idle cluster.
func watchedResource() *blockstoriov1alpha1.Resource {
	nodeID := int32(1)
	port := int32(7000)

	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pvc-a.n1",
			ResourceVersion: "100",
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "blockstor-satellite-volume-status", Operation: metav1.ManagedFieldsOperationApply, Subresource: "status"},
				{Manager: "blockstor-satellite", Operation: metav1.ManagedFieldsOperationApply, Subresource: "status"},
			},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-a",
			NodeName:               "n1",
			DRBDNodeID:             &nodeID,
			DRBDPort:               &port,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: &nodeID,
			DRBDPort:   &port,
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DevicePath: "/dev/drbd1000", DiskState: "UpToDate", CurrentGI: "AAAA"},
			},
		},
	}
}

// TestResourceWatchDropsBookkeepingOnlyWrites pins the write the test
// cluster produced several times a second while idle: a status apply
// that changed no value, only the resource version and the order and
// times of the managed-fields entries. Every such write used to wake
// the controller for the replica and each of its siblings, and a new
// Resource queued behind them for its DRBD identities.
func TestResourceWatchDropsBookkeepingOnlyWrites(t *testing.T) {
	t.Parallel()

	oldRes := watchedResource()
	newRes := oldRes.DeepCopy()
	newRes.ResourceVersion = "101"
	newRes.ManagedFields = []metav1.ManagedFieldsEntry{
		{Manager: "blockstor-satellite", Operation: metav1.ManagedFieldsOperationApply, Subresource: "status", Time: &metav1.Time{Time: time.Unix(2, 0)}},
		{Manager: "blockstor-satellite-volume-status", Operation: metav1.ManagedFieldsOperationApply, Subresource: "status", Time: &metav1.Time{Time: time.Unix(1, 0)}},
	}

	if controllerpkg.ResourceUpdateCarriesChange(oldRes, newRes) {
		t.Fatal("an update that moved only resourceVersion and managedFields reached the controller")
	}
}

// TestResourceWatchPassesRealChanges pins that the filter drops only
// bookkeeping: every change the reconciler can act on still passes,
// and so does the informer resync, which repeats one revision.
func TestResourceWatchPassesRealChanges(t *testing.T) {
	t.Parallel()

	cases := map[string]func(*blockstoriov1alpha1.Resource){
		"resync of the same revision": func(r *blockstoriov1alpha1.Resource) {},
		"spec": func(r *blockstoriov1alpha1.Resource) {
			r.Spec.Flags = []string{"DISKLESS"}
		},
		"status disk state": func(r *blockstoriov1alpha1.Resource) {
			r.Status.Volumes[0].DiskState = "Inconsistent"
		},
		"status in use": func(r *blockstoriov1alpha1.Resource) {
			r.Status.InUse = true
		},
		"annotation": func(r *blockstoriov1alpha1.Resource) {
			r.Annotations = map[string]string{"blockstor.org/volume-numbers": "0"}
		},
		"finalizer": func(r *blockstoriov1alpha1.Resource) {
			r.Finalizers = []string{"blockstor.org/satellite-resource"}
		},
		"deletion": func(r *blockstoriov1alpha1.Resource) {
			r.DeletionTimestamp = &metav1.Time{Time: time.Unix(3, 0)}
		},
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			oldRes := watchedResource()
			newRes := oldRes.DeepCopy()

			if name != "resync of the same revision" {
				newRes.ResourceVersion = "101"
			}

			mutate(newRes)

			if !controllerpkg.ResourceUpdateCarriesChange(oldRes, newRes) {
				t.Fatalf("%s: update was dropped", name)
			}
		})
	}
}
