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

package controller

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// BUG-048 (P1, the silent VD drop). The ResourceReconciler's
// patchRDVolumeMinors writes the RD's per-volume DRBD minors with a JSON
// merge-patch. JSON merge-patch (RFC 7386) REPLACES the
// spec.volumeDefinitions array wholesale, so the write stamps the WHOLE
// list from whatever snapshot the reconciler read at the top of
// ensureRDVolumeMinors. If a concurrent number-less `linstor vd c`
// appended a new volume AFTER that read but BEFORE this patch, the
// wholesale replace silently drops the appended volume — and both `vd c`
// return success because the clobber is an async reconciler write, not
// their own create.
//
// The original code carried a comment claiming "optimistic concurrency"
// but never set metadata.resourceVersion, so the patch could NEVER 409
// and the appended volume vanished with no error anywhere — the
// operator-visible "vdCount short by one, both vd c rc0, zero
// conflict/retry in the logs" failure observed on the stand.
//
// The fix embeds metadata.resourceVersion in the merge-patch body so the
// apiserver rejects the stale wholesale replace with Conflict. These
// tests pin both halves of the contract:
//
//   - a stale-snapshot patch racing an appended volume MUST be rejected
//     with Conflict (and therefore NOT drop the appended volume), and
//   - a non-racing patch (fresh snapshot) MUST still succeed and stamp
//     the minors.

func bug048Scheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

func bug048RDVolNumbers(rd *blockstoriov1alpha1.ResourceDefinition) []int32 {
	out := make([]int32, 0, len(rd.Spec.VolumeDefinitions))
	for i := range rd.Spec.VolumeDefinitions {
		out = append(out, rd.Spec.VolumeDefinitions[i].VolumeNumber)
	}

	return out
}

// TestBug048MinorPatchRejectsStaleVolumeDefinitionClobber is the core
// regression. We:
//  1. seed an RD with [vol-0], read it into the reconciler's snapshot,
//  2. simulate a concurrent `vd c` appending vol-1 to the LIVE RD
//     (bumping its resourceVersion), then
//  3. call patchRDVolumeMinors with the STALE [vol-0] snapshot.
//
// Pre-fix (blind merge-patch, no resourceVersion) the patch silently
// succeeds and the live RD is clobbered back to [vol-0] — vol-1 is
// dropped. Post-fix the patch carries the snapshot's resourceVersion,
// the apiserver returns Conflict, and vol-1 survives.
func TestBug048MinorPatchRejectsStaleVolumeDefinitionClobber(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := bug048Scheme(t)

	const rdName = "drop-rd"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(rd).
		Build()

	rec := &ResourceReconciler{Client: cli, Scheme: scheme, APIReader: cli}

	// (1) reconciler snapshot of the RD (carries the stale resourceVersion).
	snap := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, snap); err != nil {
		t.Fatalf("get RD snapshot: %v", err)
	}
	// Reconciler "allocates" a minor for vol-0 on its snapshot.
	m0 := int32(1000)
	snap.Spec.VolumeDefinitions[0].DRBDMinor = &m0

	// (2) concurrent `vd c` appends vol-1 to the LIVE RD, bumping its RV.
	live := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, live); err != nil {
		t.Fatalf("get live RD: %v", err)
	}

	live.Spec.VolumeDefinitions = append(live.Spec.VolumeDefinitions,
		blockstoriov1alpha1.ResourceDefinitionVolume{VolumeNumber: 1, SizeKib: 1024})
	if err := cli.Update(ctx, live); err != nil {
		t.Fatalf("append vol-1 (simulated vd c): %v", err)
	}

	// (3) reconciler patches minors from its STALE [vol-0] snapshot.
	err := rec.patchRDVolumeMinors(ctx, snap)
	if !apierrors.IsConflict(err) {
		t.Fatalf("patchRDVolumeMinors with stale snapshot returned err=%v, want Conflict; "+
			"a non-Conflict means the wholesale volumeDefinitions replace was NOT optimistic-locked "+
			"and would silently drop the concurrently-appended vol-1 (BUG-048)", err)
	}

	// The appended volume MUST survive the rejected clobber.
	got := &blockstoriov1alpha1.ResourceDefinition{}
	if gerr := cli.Get(ctx, client.ObjectKey{Name: rdName}, got); gerr != nil {
		t.Fatalf("get RD after rejected patch: %v", gerr)
	}

	nums := bug048RDVolNumbers(got)
	if len(nums) != 2 || nums[0] != 0 || nums[1] != 1 {
		t.Fatalf("RD volumeDefinitions after rejected clobber = %v, want [0 1] "+
			"(vol-1 was silently dropped — the BUG-048 lost update)", nums)
	}
}

// TestBug048MinorPatchSucceedsOnFreshSnapshot proves the optimistic-lock
// does not break the happy path: a patch built from a CURRENT snapshot
// (no racing writer) must still commit and stamp the minors.
func TestBug048MinorPatchSucceedsOnFreshSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := bug048Scheme(t)

	const rdName = "fresh-rd"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
				{VolumeNumber: 1, SizeKib: 1024},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(rd).
		Build()

	rec := &ResourceReconciler{Client: cli, Scheme: scheme, APIReader: cli}

	snap := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, snap); err != nil {
		t.Fatalf("get RD snapshot: %v", err)
	}

	m0, m1 := int32(1000), int32(1001)
	snap.Spec.VolumeDefinitions[0].DRBDMinor = &m0
	snap.Spec.VolumeDefinitions[1].DRBDMinor = &m1

	if err := rec.patchRDVolumeMinors(ctx, snap); err != nil {
		t.Fatalf("patchRDVolumeMinors on fresh snapshot: %v", err)
	}

	got := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, got); err != nil {
		t.Fatalf("get RD after patch: %v", err)
	}

	if nums := bug048RDVolNumbers(got); len(nums) != 2 || nums[0] != 0 || nums[1] != 1 {
		t.Fatalf("RD volumeDefinitions after fresh patch = %v, want [0 1]", nums)
	}

	for i := range got.Spec.VolumeDefinitions {
		if got.Spec.VolumeDefinitions[i].DRBDMinor == nil {
			t.Fatalf("vol-%d minor not stamped after fresh patch",
				got.Spec.VolumeDefinitions[i].VolumeNumber)
		}
	}
}
