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
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// stampedPeerSlots reconciles one replica of a definition with the given
// count to steady state and returns the count stamped on the replica.
func stampedPeerSlots(t *testing.T, definition, preset *int32) *int32 {
	t.Helper()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "slots-rd"
		resourceName = rdName + ".n1"
	)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			PeerSlots:         definition,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 1024}},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n1",
			PeerSlots:              preset,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(rd, res).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	reconcileToSteadyState(t, ctx, rec, resourceName)

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resourceName}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Spec.DRBDNodeID == nil {
		t.Fatalf("node id not allocated")
	}

	return got.Spec.PeerSlots
}

// A replica gets its definition's peer slots, stamped with its node id,
// as LINSTOR copies the definition's count onto each replica it creates.
func TestReplicaIsStampedWithItsDefinitionPeerSlots(t *testing.T) {
	t.Parallel()

	seven := int32(7)
	if got := stampedPeerSlots(t, &seven, nil); got == nil || *got != 7 {
		t.Errorf("got %v, want 7", got)
	}
}

// A replica of a definition created before blockstor recorded a count was
// created with 15, and is stamped with 15 so a count set on the definition
// later does not change how its metadata is read.
func TestReplicaOfADefinitionWithoutACountIsStampedFifteen(t *testing.T) {
	t.Parallel()

	if got := stampedPeerSlots(t, nil, nil); got == nil || *got != 15 {
		t.Errorf("got %v, want 15", got)
	}
}

// A replica's count is settable once: a definition whose count changed
// does not change the count the replica's metadata was created with.
func TestReplicaPeerSlotsAreNotRestamped(t *testing.T) {
	t.Parallel()

	seven, twelve := int32(7), int32(12)
	if got := stampedPeerSlots(t, &twelve, &seven); got == nil || *got != 7 {
		t.Errorf("got %v, want the replica's own 7", got)
	}
}
