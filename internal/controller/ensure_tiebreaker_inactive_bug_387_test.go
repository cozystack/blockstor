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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug387InactiveReplicaNotCountedAsVotingDiskful pins the Bug 387
// fix: an INACTIVE replica (`drbdadm down`) is NOT a voting peer in the
// DRBD quorum, so the auto-tiebreaker policy must not count it as a
// diskful replica.
//
// Repro (operator-level): an RD with 2 active diskful + 1 INACTIVE
// diskful. The operator runs `linstor r d <one-of-the-active-diskful>`,
// leaving 1 active diskful + 1 INACTIVE diskful. Before the fix the
// reconciler saw "2 diskful, 0 non-witness diskless, even parity" and
// spuriously grew a TIE_BREAKER witness (1 active diskful + 1 witness =
// 2 votes, no majority protection) — diverging from upstream LINSTOR,
// which simply deletes the replica with no witness conversion.
//
// Post-fit invariant: the INACTIVE replica is dropped from the voting
// set, leaving exactly 1 voting diskful → no witness, quorum "off".
func TestBug387InactiveReplicaNotCountedAsVotingDiskful(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	// Three satellites; worker-1 holds the INACTIVE replica, worker-3
	// the surviving active diskful, worker-2 is the now-free node the
	// buggy reconciler would have picked as the spurious witness.
	for _, n := range []string{"worker-1", "worker-2", "worker-3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	// worker-1: INACTIVE diskful (operator-deactivated, not voting).
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "test1", NodeName: "worker-1",
		Flags: []string{apiv1.ResourceFlagInactive},
	}); err != nil {
		t.Fatalf("seed inactive replica: %v", err)
	}

	// worker-3: surviving active diskful (the only voting peer).
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "test1", NodeName: "worker-3",
	}); err != nil {
		t.Fatalf("seed active replica: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "test1"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	// No witness must have been auto-added on any node — the topology
	// has only 1 voting diskful, so a TIE_BREAKER would be a useless
	// 2-voter quorum with no majority.
	replicas, err := st.Resources().ListByDefinition(ctx, "test1")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if len(replicas) != 2 {
		t.Fatalf("replica count: got %d, want 2 (inactive + active, no witness); replicas=%v",
			len(replicas), replicas)
	}

	for i := range replicas {
		for _, f := range replicas[i].Flags {
			if f == apiv1.ResourceFlagTieBreaker {
				t.Errorf("spurious TIE_BREAKER witness created on %s; an INACTIVE replica must not be counted as a voting diskful",
					replicas[i].NodeName)
			}
		}
	}

	// worker-2 (the free node) must stay empty — the buggy reconciler
	// converted the deleted diskful's freed node into a witness.
	if _, err := st.Resources().Get(ctx, "test1", "worker-2"); err == nil {
		t.Errorf("unexpected witness on worker-2 — the deleted diskful must not be converted into a tiebreaker")
	}

	// quorum prop must be "off": 1 voting diskful can never reach
	// majority, so the controller writes "off" to match DRBD reality.
	final := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "test1"}, final); err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	if final.Spec.Props["DrbdOptions/Resource/quorum"] != "off" {
		t.Errorf("quorum prop: got %q, want off (1 voting diskful + 1 inactive)",
			final.Spec.Props["DrbdOptions/Resource/quorum"])
	}
}

// TestBug387TwoActiveDiskfulStillGetWitness is the positive control for
// the Bug 387 fix: dropping INACTIVE replicas from the voting set must
// NOT regress the canonical auto-witness invariant. An RD with 2 active
// diskful (no INACTIVE flag) plus a third INACTIVE replica must still
// grow a witness — the two active peers are a genuine even-parity
// quorum that needs the third voter.
func TestBug387TwoActiveDiskfulStillGetWitness(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"worker-1", "worker-2", "worker-3", "worker-4"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	// Two active diskful peers — a real even-parity quorum.
	for _, n := range []string{"worker-2", "worker-3"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "test2", NodeName: n,
		}); err != nil {
			t.Fatalf("seed active replica %s: %v", n, err)
		}
	}

	// An INACTIVE replica that must be ignored by the policy.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "test2", NodeName: "worker-1",
		Flags: []string{apiv1.ResourceFlagInactive},
	}); err != nil {
		t.Fatalf("seed inactive replica: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "test2"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	// Witness landed on worker-4 (the only free, non-diskful,
	// non-inactive node — worker-1 is excluded because it hosts a
	// replica row).
	got, err := st.Resources().Get(ctx, "test2", "worker-4")
	if err != nil {
		t.Fatalf("witness not created on worker-4: %v", err)
	}

	hasTB := false

	for _, f := range got.Flags {
		if f == apiv1.ResourceFlagTieBreaker {
			hasTB = true

			break
		}
	}

	if !hasTB {
		t.Errorf("witness must carry TIE_BREAKER flag; got %v", got.Flags)
	}

	final := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "test2"}, final); err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	if final.Spec.Props["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop: got %q, want majority (2 active diskful + witness)",
			final.Spec.Props["DrbdOptions/Resource/quorum"])
	}
}
