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
	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestAdoptedSubWindowPortMinorPreserved pins the migration/adoption
// contract: resources adopted from an existing LINSTOR cluster keep
// their original LINSTOR-assigned port/minor verbatim, even though
// those values sit BELOW blockstor's allocation window (TCP
// 20000-20999, minors 20000-65535). The allocator must NOT clamp,
// reject, or re-allocate an explicit sub-window value, and a fresh
// sibling on the SAME node must still allocate from the high window
// (proving the low value isn't treated as "the default base").
//
// LINSTOR assigns e.g. port 7050 and minor 1042 — both below the
// blockstor window. After the allocator runs, the adopted replica must
// still carry exactly 7050 / 1042.
func TestAdoptedSubWindowPortMinorPreserved(t *testing.T) {
	t.Parallel()

	const (
		adoptedPort  int32 = 7050
		adoptedMinor int32 = 1042
	)

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	// Adopted RD: explicit sub-window minor on its volume-0 definition.
	rdName := "pvc-adopted"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024, DRBDMinor: int32Ptr(adoptedMinor)},
			},
		},
	}
	if err := cli.Create(ctx, rd); err != nil {
		t.Fatalf("create adopted rd: %v", err)
	}

	// Adopted replica on w1: explicit sub-window Spec.DRBDPort + node-id.
	adopted := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + ".w1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "w1",
			DRBDPort:               int32Ptr(adoptedPort),
			DRBDNodeID:             int32Ptr(0),
		},
	}
	if err := cli.Create(ctx, adopted); err != nil {
		t.Fatalf("create adopted resource: %v", err)
	}

	// A freshly-created sibling RD also lands on w1 (same node) — its
	// allocation must come from the high window and must NOT collide
	// with the adopted low port.
	freshRDName := "pvc-fresh"

	freshRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: freshRDName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}
	if err := cli.Create(ctx, freshRD); err != nil {
		t.Fatalf("create fresh rd: %v", err)
	}

	create(ctx, t, cli, freshRDName, "w1")

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rdName)
	allocate(ctx, t, rec, cli, freshRDName)

	// The adopted replica's explicit sub-window port survives untouched.
	gotAdopted := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName + ".w1"}, gotAdopted); err != nil {
		t.Fatalf("get adopted resource: %v", err)
	}

	if gotAdopted.Spec.DRBDPort == nil || *gotAdopted.Spec.DRBDPort != adoptedPort {
		t.Errorf("adopted Spec.DRBDPort=%v, want %d preserved verbatim (no clamp/re-alloc)",
			gotAdopted.Spec.DRBDPort, adoptedPort)
	}

	// The adopted RD's explicit sub-window minor survives untouched.
	gotRD := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, gotRD); err != nil {
		t.Fatalf("get adopted rd: %v", err)
	}

	if len(gotRD.Spec.VolumeDefinitions) == 0 ||
		gotRD.Spec.VolumeDefinitions[0].DRBDMinor == nil ||
		*gotRD.Spec.VolumeDefinitions[0].DRBDMinor != adoptedMinor {
		t.Errorf("adopted volume-0 minor=%v, want %d preserved verbatim (no clamp/re-alloc)",
			minorOf(gotRD), adoptedMinor)
	}

	// The fresh sibling on the same node allocates from the high window
	// and never lands on the adopted low values.
	gotFresh := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: freshRDName + ".w1"}, gotFresh); err != nil {
		t.Fatalf("get fresh resource: %v", err)
	}

	if gotFresh.Spec.DRBDPort == nil {
		t.Fatalf("fresh Spec.DRBDPort not allocated")
	}

	freshPort := *gotFresh.Spec.DRBDPort
	if freshPort < drbd.DefaultPortMin || freshPort > drbd.DefaultPortMax {
		t.Errorf("fresh port %d outside default window [%d,%d]",
			freshPort, drbd.DefaultPortMin, drbd.DefaultPortMax)
	}

	if freshPort == adoptedPort {
		t.Errorf("fresh port %d collided with adopted sub-window port on same node", freshPort)
	}

	gotFreshRD := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: freshRDName}, gotFreshRD); err != nil {
		t.Fatalf("get fresh rd: %v", err)
	}

	freshMinor := minorOf(gotFreshRD)
	if freshMinor < drbd.DefaultMinorMin || freshMinor > drbd.DefaultMinorMax {
		t.Errorf("fresh minor %d outside default window [%d,%d]",
			freshMinor, drbd.DefaultMinorMin, drbd.DefaultMinorMax)
	}

	if freshMinor == adoptedMinor {
		t.Errorf("fresh minor %d collided with adopted sub-window minor", freshMinor)
	}
}

// minorOf returns the volume-0 minor of an RD, or -1 when unset.
func minorOf(rd *blockstoriov1alpha1.ResourceDefinition) int32 {
	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].VolumeNumber == 0 &&
			rd.Spec.VolumeDefinitions[i].DRBDMinor != nil {
			return *rd.Spec.VolumeDefinitions[i].DRBDMinor
		}
	}

	return -1
}
