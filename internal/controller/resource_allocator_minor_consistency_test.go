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

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// preSeedRDMinor creates an RD that already carries the given minor on
// its single VolumeDefinition (the authoritative cluster-wide minor
// location in the identity-to-spec model). Used to populate the
// cluster-wide taken-set so a fresh RD's allocator must skip these.
func preSeedRDMinor(ctx context.Context, t *testing.T, cli client.Client, rdName string, minor int32) {
	t.Helper()

	m := minor
	rd := &blockstoriov1alpha1.ResourceDefinition{}
	rd.Name = rdName
	rd.Spec.VolumeDefinitions = []blockstoriov1alpha1.ResourceDefinitionVolume{
		{VolumeNumber: 0, SizeKib: 1024, DRBDMinor: &m},
	}

	if err := cli.Create(ctx, rd); err != nil {
		t.Fatalf("preSeedRDMinor create %s: %v", rdName, err)
	}
}

// Bug 268 (CRITICAL, data-correctness): the DRBD minor is the
// /dev/drbd<N> device identity, identical on every node hosting a
// volume. In the identity-to-spec model it lives PER VOLUME on
// RD.Spec.VolumeDefinitions[].DRBDMinor — one value the satellite's
// .res renderer writes into every `on <node>` block. The mirror onto
// each Resource.Status.DRBDMinor (volume 0) is therefore identical
// across peers by construction.
//
// This test pins that property AND the cluster-wide uniqueness: a
// fresh RD must pick a minor not already held by any other RD's
// VolumeDefinitions, so two RDs never collide on /dev/drbdN.
func TestBug268DRBDMinorSameAcrossPeersOfOneRD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	// Pre-seed sibling RDs holding minors 1000-1002 cluster-wide. The
	// fresh RD's allocator must skip all of them.
	preSeedRDMinor(ctx, t, cli, "sibling-a", 1000)
	preSeedRDMinor(ctx, t, cli, "sibling-b", 1001)
	preSeedRDMinor(ctx, t, cli, "sibling-c", 1002)

	rd := "pvc-bug268-minor-coherence"

	rdObj := &blockstoriov1alpha1.ResourceDefinition{}
	rdObj.Name = rd
	rdObj.Spec.VolumeDefinitions = []blockstoriov1alpha1.ResourceDefinitionVolume{
		{VolumeNumber: 0, SizeKib: 1024},
	}
	if err := cli.Create(ctx, rdObj); err != nil {
		t.Fatalf("create rd: %v", err)
	}

	for _, node := range []string{"w1", "w2"} {
		create(ctx, t, cli, rd, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rd)

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	var (
		seen     *int32
		seenNode string
	)

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rd {
			continue
		}

		got := list.Items[i].Status.DRBDMinor
		if got == nil {
			t.Fatalf("%s: minor mirror not populated", list.Items[i].Name)
		}

		if *got >= 1000 && *got <= 1002 {
			t.Errorf("%s minor %d collides with a sibling RD's minor (1000-1002)",
				list.Items[i].Name, *got)
		}

		if seen == nil {
			seen = got
			seenNode = list.Items[i].Spec.NodeName

			continue
		}

		if *got != *seen {
			t.Errorf("minor diverges across peers of RD %q: %s=%d vs %s=%d "+
				"(the per-volume minor must be identical on every node)",
				rd, seenNode, *seen, list.Items[i].Spec.NodeName, *got)
		}
	}
}

// TestBug268DRBDMinorRecordedOnRDSpec asserts the chosen minor is
// persisted on the parent RD's Spec.VolumeDefinitions — the
// authoritative, restore-safe location — and mirrored onto each
// Resource's Status.DRBDMinor for backward-compat readers.
func TestBug268DRBDMinorRecordedOnRDSpec(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	rdName := "pvc-bug268-rd-persist"

	rdObj := &blockstoriov1alpha1.ResourceDefinition{}
	rdObj.Name = rdName
	rdObj.Spec.VolumeDefinitions = []blockstoriov1alpha1.ResourceDefinitionVolume{
		{VolumeNumber: 0, SizeKib: 1024},
	}

	if err := cli.Create(ctx, rdObj); err != nil {
		t.Fatalf("create rd: %v", err)
	}

	for _, node := range []string{"w1", "w2"} {
		create(ctx, t, cli, rdName, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rdName)

	gotRD := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, gotRD); err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if len(gotRD.Spec.VolumeDefinitions) == 0 || gotRD.Spec.VolumeDefinitions[0].DRBDMinor == nil {
		t.Fatalf("RD.Spec.VolumeDefinitions[0].DRBDMinor not stamped after allocation; " +
			"the allocator must persist the per-volume minor on the parent RD Spec")
	}

	wantMinor := *gotRD.Spec.VolumeDefinitions[0].DRBDMinor

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		got := list.Items[i].Status.DRBDMinor
		if got == nil || *got != wantMinor {
			t.Errorf("replica %q Status.DRBDMinor mirror=%v, want %d (must mirror RD's vol-0 minor)",
				list.Items[i].Name, got, wantMinor)
		}
	}
}
