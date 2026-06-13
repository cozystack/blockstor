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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 393 (release-gate, durability/availability) — the auto-tiebreaker
// witness reaper `removeWitnesses` raced the placer's redundancy
// backfill. The reaper's old Get-then-Delete was non-atomic: it re-read
// the witness flags, but the Delete carried no version precondition, so
// a promotion landing in the gap between the read and the Delete was
// silently deleted BY NAME. Net effect: after an inactive-replica
// return / node event, `r c --auto-place` promoted the witness to a
// diskful backfill, the reaper then clobbered that very replica, and
// redundancy was never restored (fewer diskful than place_count) — the
// inactive-return-backfills-redundancy replay timed out at
// active_diskful_count >= 2.
//
// The fix routes the reap through store.ResourceStore.DeleteIfTieBreaker,
// which carries the flag re-check and the delete across as ONE
// optimistic-concurrency operation. This test pins it by injecting the
// concurrent promotion at the exact race point — between the reaper's
// observation of the witness and the delete commit — and asserting the
// promoted replica SURVIVES and redundancy holds.

// promoteRacingStore decorates a Store and, on the FIRST
// DeleteIfTieBreaker call, promotes the targeted witness to diskful
// BEFORE delegating to the inner store — modelling the placer's backfill
// landing in the reaper's race window. The inner store's conditional
// delete (re-check + version/lock guard) must then refuse the reap.
type promoteRacingStore struct {
	store.Store

	promoted *bool
}

func (s *promoteRacingStore) Resources() store.ResourceStore {
	return &promoteRacingResources{ResourceStore: s.Store.Resources(), promoted: s.promoted}
}

type promoteRacingResources struct {
	store.ResourceStore

	promoted *bool
}

func (r *promoteRacingResources) DeleteIfTieBreaker(ctx context.Context, rdName, node string) (bool, error) {
	if !*r.promoted {
		*r.promoted = true

		// The placer's redundancy backfill (corner-D2b promoteWitness):
		// strip DISKLESS + TIE_BREAKER and stamp a storage pool, in place
		// on the same (rd, node) key — exactly what pkg/placer does, and
		// what bumps the backing object's version on the k8s store.
		err := r.PatchResourceSpec(ctx, rdName, node, func(live *apiv1.Resource) error {
			keep, _ := apiv1.PromoteWitnessFlags(live.Flags, true)
			live.Flags = keep

			if live.Props == nil {
				live.Props = map[string]string{}
			}

			live.Props["StorPoolName"] = "pool"

			return nil
		})
		if err != nil {
			return false, err
		}
	}

	return r.ResourceStore.DeleteIfTieBreaker(ctx, rdName, node)
}

// TestBug393BackfillSurvivesWitnessReap is the core regression: with the
// witness already promoted to a diskful backfill the instant the reaper
// fires its delete, the promoted replica must survive and the RD must
// still carry two diskful replicas (redundancy restored), with no
// surviving TIE_BREAKER.
func TestBug393BackfillSurvivesWitnessReap(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	inner := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := inner.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	// Two diskful replicas → the witness invariant is NOT wanted, so the
	// reconciler's witness pass will try to reap the witness on n3.
	for _, n := range []string{"n1", "n2"} {
		if err := inner.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug393", NodeName: n,
			Props: map[string]string{"StorPoolName": "pool"},
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	// The witness on n3 — the placer is concurrently promoting it to a
	// diskful backfill replica (the inactive-return redundancy refill).
	if err := inner.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug393", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed witness: %v", err)
	}

	promoted := false
	st := &promoteRacingStore{Store: inner, promoted: &promoted}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug393"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// The witness list as snapshotted at the top of ensureTiebreaker —
	// n3 still looks like a witness here. The reaper must NOT clobber the
	// row once it has been promoted under it.
	witnesses := []apiv1.Resource{
		{
			Name: "pvc-bug393", NodeName: "n3",
			Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		},
	}

	if err := rec.RemoveWitnesses(ctx, "pvc-bug393", witnesses); err != nil {
		t.Fatalf("RemoveWitnesses: %v", err)
	}

	if !promoted {
		t.Fatalf("test bug: the racing promotion never fired")
	}

	// The promoted backfill replica on n3 MUST survive.
	got, err := inner.Resources().Get(ctx, "pvc-bug393", "n3")
	if err != nil {
		t.Fatalf("Bug 393 regression: promoted backfill on n3 was clobbered by the reaper: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("n3 still carries TIE_BREAKER after promotion: %v", got.Flags)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("n3 still DISKLESS after diskful promotion: %v", got.Flags)
	}

	// Redundancy holds: all three nodes carry a diskful replica, none a
	// witness. place_count-2's worth of redundancy is restored, not lost.
	all, err := inner.Resources().ListByDefinition(ctx, "pvc-bug393")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	diskful := 0

	for i := range all {
		if slices.Contains(all[i].Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("surviving witness on %s: %v", all[i].NodeName, all[i].Flags)
		}

		if !slices.Contains(all[i].Flags, apiv1.ResourceFlagDiskless) {
			diskful++
		}
	}

	if diskful < 2 {
		t.Errorf("redundancy not restored: got %d diskful replicas, want >= 2", diskful)
	}
}

// TestBug393WitnessStillReapedWhenGenuineOrphan guards against
// over-correction: a witness that is NOT racing any promotion (a genuine
// orphan, the legitimate reap path) must STILL be reaped. The fix must
// only skip the delete when identity actually changed under it.
func TestBug393WitnessStillReapedWhenGenuineOrphan(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug393-orphan", NodeName: n,
			Props: map[string]string{"StorPoolName": "pool"},
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug393-orphan", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed orphan witness: %v", err)
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{Store: st}

	witnesses := []apiv1.Resource{
		{
			Name: "pvc-bug393-orphan", NodeName: "n3",
			Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		},
	}

	if err := rec.RemoveWitnesses(ctx, "pvc-bug393-orphan", witnesses); err != nil {
		t.Fatalf("RemoveWitnesses: %v", err)
	}

	// The genuine orphan witness must be gone.
	if _, err := st.Resources().Get(ctx, "pvc-bug393-orphan", "n3"); err == nil {
		t.Errorf("genuine orphan witness on n3 survived reap — over-correction")
	}

	// The diskful pair is untouched.
	for _, n := range []string{"n1", "n2"} {
		if _, err := st.Resources().Get(ctx, "pvc-bug393-orphan", n); err != nil {
			t.Errorf("diskful %s vanished: %v", n, err)
		}
	}
}
