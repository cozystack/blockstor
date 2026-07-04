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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Bug 433: the per-volume DRBDMinor (RD.Spec.VolumeDefinitions[].DRBDMinor)
// is the /dev/drbd<N> device identity. Per the CRD contract
// (api/v1alpha1/resourcedefinition_types.go) "a non-nil value is
// authoritative and is NEVER overwritten … the store-side
// VolumeDefinitions carry-across preserves the value through a REST
// modify". DRBDMinor has NO counterpart on the wire shape
// apiv1.VolumeDefinition (which carries only VolumeNumber/SizeKib/Props/
// Flags), so a VD-scoped modify that round-trips the entry through
// wireToCRDVD — volumeDefinitions.Update and PatchVolumeDefinitionSpec —
// silently zeroes the minor.
//
// This is the same wire-rebuild-drops-operator-only-field class as the
// carry-across family (Bug 206 RD.VolumeDefinitions, Bug 208 Node ranges,
// Bug 209 RD.Encryption). Only the VD-scoped element rebuild dropped it:
// the RD-scoped path preserves the whole VolumeDefinitions slice
// wholesale, so the minor rode along for free.
//
// Fix: both VD-scoped write paths route their write-back through
// wireToCRDVDPreserving, which carries DRBDMinor across the rebuild.
//
// These tests FAIL on the pre-fix tree (the minor comes back nil) and
// PASS with the carry-across in place.

const (
	bug433Minor  int32 = 20000
	bug433OneGiB int64 = 1 * 1024 * 1024
	bug433TwoGiB int64 = 2 * 1024 * 1024
)

// bug433SeedStore builds a k8s store fronting a fake client that holds a
// single RD with one volume carrying a non-nil DRBDMinor (the device
// identity we assert survives every routine modify).
func bug433SeedStore(t *testing.T, rdName string) (*k8s.Store, client.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	minor := bug433Minor
	seed := crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: crdv1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []crdv1alpha1.ResourceDefinitionVolume{
				{
					VolumeNumber: 0,
					SizeKib:      bug433OneGiB,
					DRBDMinor:    &minor,
				},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&seed).
		WithStatusSubresource(&crdv1alpha1.ResourceDefinition{}).
		Build()

	return k8s.New(cli), cli
}

// bug433DRBDMinor reads the durable DRBDMinor pointer for (rd, vol0)
// straight off the RD CRD (nil = wiped).
func bug433DRBDMinor(t *testing.T, cli client.Client, rdName string) *int32 {
	t.Helper()

	var got crdv1alpha1.ResourceDefinition
	if err := cli.Get(context.Background(), client.ObjectKey{Name: rdName}, &got); err != nil {
		t.Fatalf("get RD %q: %v", rdName, err)
	}

	for i := range got.Spec.VolumeDefinitions {
		if got.Spec.VolumeDefinitions[i].VolumeNumber == 0 {
			return got.Spec.VolumeDefinitions[i].DRBDMinor
		}
	}

	t.Fatalf("RD %q: volume 0 vanished", rdName)

	return nil
}

// assertMinorPreserved fails unless (rd, vol0) still carries the seeded
// device identity.
func bug433AssertMinorPreserved(t *testing.T, cli client.Client, rdName, via string) {
	t.Helper()

	m := bug433DRBDMinor(t, cli, rdName)
	if m == nil {
		t.Fatalf("%s WIPED the volume's DRBDMinor to nil (Bug 433). The per-volume DRBDMinor is "+
			"the /dev/drbd<N> device identity; the CRD contract says a non-nil minor is NEVER "+
			"overwritten and is preserved across a REST modify. wireToCRDVD drops it — the "+
			"VD-scoped write must route through wireToCRDVDPreserving.", via)
	}

	if *m != bug433Minor {
		t.Fatalf("%s CHANGED the volume's DRBDMinor: got %d want %d — the device identity of a "+
			"live volume must not move on a routine modify (Bug 433).", via, *m, bug433Minor)
	}
}

// TestBug433_PatchVDSpecResizePreservesDRBDMinor — a legal in-bounds
// `vd set-size` grow must not touch the volume's DRBD device minor.
func TestBug433_PatchVDSpecResizePreservesDRBDMinor(t *testing.T) {
	t.Parallel()

	const rd = "bug433-resize"

	s, cli := bug433SeedStore(t, rd)

	err := s.VolumeDefinitions().PatchVolumeDefinitionSpec(context.Background(), rd, 0,
		func(vd *apiv1.VolumeDefinition) error {
			vd.SizeKib = bug433TwoGiB

			return nil
		})
	if err != nil {
		t.Fatalf("patch (resize): %v", err)
	}

	bug433AssertMinorPreserved(t, cli, rd, "`vd set-size` grow (PatchVolumeDefinitionSpec)")
}

// TestBug433_PatchVDSpecPropModifyPreservesDRBDMinor — a pure
// `vd set-property` edit (no size change) must not touch the device
// identity.
func TestBug433_PatchVDSpecPropModifyPreservesDRBDMinor(t *testing.T) {
	t.Parallel()

	const rd = "bug433-props"

	s, cli := bug433SeedStore(t, rd)

	err := s.VolumeDefinitions().PatchVolumeDefinitionSpec(context.Background(), rd, 0,
		func(vd *apiv1.VolumeDefinition) error {
			if vd.Props == nil {
				vd.Props = map[string]string{}
			}
			vd.Props["Aux/bug433-probe"] = "x"

			return nil
		})
	if err != nil {
		t.Fatalf("patch (props): %v", err)
	}

	bug433AssertMinorPreserved(t, cli, rd, "`vd set-property` (PatchVolumeDefinitionSpec)")
}

// TestBug433_UpdatePreservesDRBDMinor — the sibling wholesale
// volumeDefinitions.Update path has the identical drop; a REST modify
// routed through it must likewise keep the device identity.
func TestBug433_UpdatePreservesDRBDMinor(t *testing.T) {
	t.Parallel()

	const rd = "bug433-update"

	s, cli := bug433SeedStore(t, rd)

	// The wire shape carries no DRBDMinor, so an operator Update supplies
	// only VolumeNumber/SizeKib/Props/Flags — the store must carry the
	// device identity across from the live CRD entry.
	err := s.VolumeDefinitions().Update(context.Background(), rd, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      bug433TwoGiB,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	bug433AssertMinorPreserved(t, cli, rd, "volumeDefinitions.Update")
}
