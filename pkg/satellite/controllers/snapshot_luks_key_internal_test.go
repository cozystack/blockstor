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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

func luksSnapshotRig(t *testing.T, recorded []blockstoriov1alpha1.SnapshotLUKSKey, replicaKey *blockstoriov1alpha1.LUKSKey) (*SnapshotReconciler, *blockstoriov1alpha1.Snapshot) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-s.snap1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-s", SnapshotName: "snap1", Nodes: []string{"n1"}, TakeSnapshot: true,
			VolumeDefinitions: []blockstoriov1alpha1.SnapshotVolumeRef{{VolumeNumber: 0}},
			LUKSKeys:          recorded,
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-s.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-s", NodeName: "n1",
			Volumes: []blockstoriov1alpha1.ResourceVolumeSpec{{VolumeNumber: 0, LUKSKey: replicaKey}},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(snap, res).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).Build()

	return &SnapshotReconciler{Client: cli, Config: Config{NodeName: "n1"}}, snap
}

func testSnapshotKey() *blockstoriov1alpha1.LUKSKey {
	return &blockstoriov1alpha1.LUKSKey{KeyID: "c7324c99567678f7", Wrapped: []byte{1}, Origin: blockstoriov1alpha1.LUKSKeyOriginGenerated}
}

// A replica formatted after the Snapshot CR was created carries a password
// the snapshot did not record: its bytes there could never be restored, so
// this node's take fails instead of running.
func TestTakeRefusesAnUnrecordedLUKSKey(t *testing.T) {
	t.Parallel()

	r, snap := luksSnapshotRig(t, nil, testSnapshotKey())

	_, err := r.handleTakeSnapshotPhase(t.Context(), snap)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	var got blockstoriov1alpha1.Snapshot

	err = r.Get(t.Context(), client.ObjectKeyFromObject(snap), &got)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	failed := false

	for _, entry := range got.Status.NodeStatus {
		if entry.NodeName == "n1" && entry.Failed {
			failed = true
		}
	}

	if !failed {
		t.Fatalf("node status %+v, want n1 failed", got.Status.NodeStatus)
	}
}

// Recorded under the node's CR name in any case, or a volume without a
// key, is no refusal.
func TestUnrecordedLUKSKey(t *testing.T) {
	t.Parallel()

	recorded := []blockstoriov1alpha1.SnapshotLUKSKey{{NodeName: "N1", VolumeNumber: 0, LUKSKey: *testSnapshotKey()}}

	for name, tc := range map[string]struct {
		recorded []blockstoriov1alpha1.SnapshotLUKSKey
		key      *blockstoriov1alpha1.LUKSKey
		missing  bool
	}{
		"recorded":      {recorded: recorded, key: testSnapshotKey()},
		"no key":        {key: nil},
		"other node":    {recorded: []blockstoriov1alpha1.SnapshotLUKSKey{{NodeName: "n2", VolumeNumber: 0, LUKSKey: *testSnapshotKey()}}, key: testSnapshotKey(), missing: true},
		"other volume":  {recorded: []blockstoriov1alpha1.SnapshotLUKSKey{{NodeName: "n1", VolumeNumber: 1, LUKSKey: *testSnapshotKey()}}, key: testSnapshotKey(), missing: true},
		"none recorded": {key: testSnapshotKey(), missing: true},
	} {
		r, snap := luksSnapshotRig(t, tc.recorded, tc.key)

		got, err := r.unrecordedLUKSKey(t.Context(), snap, []int32{0})
		if err != nil || (got != "") != tc.missing {
			t.Errorf("%s: %q, %v; want missing %v", name, got, err, tc.missing)
		}
	}
}
