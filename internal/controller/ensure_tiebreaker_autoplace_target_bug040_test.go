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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// BUG-040 (release-gate, stand dev sweep @73895806c) — the
// auto-tiebreaker witness landed on the one node the operator had
// explicitly drained with `linstor node set-property <node>
// AutoplaceTarget false` (corner F4 maintenance-drain workflow).
//
// Stand repro (cli-matrix/n-autoplace-target-excludes):
//
//	$ linstor n sp dev-worker-1 AutoplaceTarget false
//	$ linstor rd c ccf-aptgt-1 && linstor vd c ccf-aptgt-1 256M
//	$ linstor r c --auto-place=2 -s lvm-thin ccf-aptgt-1
//	→ diskful on dev-worker-2 + dev-worker-3 (placer honours the prop),
//	  but ensureTiebreaker then stamped a [DISKLESS TIE_BREAKER] row on
//	  dev-worker-1 — controller log 2026-06-13T00:45:16Z, reconcileID
//	  6874c482: "ensureTiebreaker … willCreate=true".
//
// pickTiebreakerNodeForRD only consulted the EVICTED/LOST flag set;
// the AutoplaceTarget prop — which the placer has honoured since
// corner F4 (#102) — was never read, so the witness (an automatic
// placement) violated the operator's drain. Upstream LINSTOR routes
// tiebreaker selection through the same autoplacer that honours
// AutoplaceTarget, so upstream never pins a witness to a drained node:
// with no other spare the witness is simply not created and quorum
// falls back to off (isQuorumFeasible: 2 diskful + 0 diskless → off).

// TestBug040WitnessNeverLandsOnAutoplaceExcludedNode pins the create
// branch: 2 diskful + the only spare node carrying
// AutoplaceTarget=false. The witness MUST NOT be created anywhere and
// the quorum prop MUST resolve to off — not the phantom-witness
// majority the pre-fix applyWitnessDecision computed.
func TestBug040WitnessNeverLandsOnAutoplaceExcludedNode(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n3", Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{apiv1.PropAutoplaceTarget: "false"},
	}); err != nil {
		t.Fatalf("seed n3 (AutoplaceTarget=false): %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug040", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug040"},
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

	all, err := st.Resources().ListByDefinition(ctx, "pvc-bug040")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// Invariant 1: nothing landed on the drained node.
	if _, err := st.Resources().Get(ctx, "pvc-bug040", "n3"); err == nil {
		t.Errorf("BUG-040: a replica landed on the AutoplaceTarget=false node n3; entries=%v", all)
	}

	// Invariant 2: exactly the two diskful replicas — no witness was
	// created anywhere (the drained node was the only spare).
	diskful, diskless, witness := classifyReplicas(all)
	if diskful != 2 || diskless != 0 || witness != 0 {
		t.Errorf("BUG-040: composition diskful=%d diskless=%d witness=%d, want 2/0/0; entries=%v",
			diskful, diskless, witness, all)
	}

	// Invariant 3: with only 2 voters the quorum prop must be off —
	// the pre-fix code appended a phantom witness to the quorum
	// computation and stamped majority on a 2-voter RD. EnsureTiebreaker
	// mutates the in-memory rd it was handed (setQuorum's optimistic
	// loop), so assert on that.
	if q := rd.Spec.Props["DrbdOptions/Resource/quorum"]; q != "off" {
		t.Errorf("BUG-040: quorum prop got %q, want off (2 diskful, no witness possible)", q)
	}
}

// TestBug040WitnessLandsWhenAutoplaceTargetNotFalse pins the fail-open
// parse: true / garbage / unset values keep the spare node eligible,
// so the witness lands there exactly as before the fix.
func TestBug040WitnessLandsWhenAutoplaceTargetNotFalse(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		props map[string]string
	}{
		{name: "explicit-true", props: map[string]string{apiv1.PropAutoplaceTarget: "true"}},
		{name: "garbage-value", props: map[string]string{apiv1.PropAutoplaceTarget: "nope"}},
		{name: "unset", props: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := newScheme(t)
			st := store.NewInMemory()
			ctx := context.Background()

			for _, n := range []string{"n1", "n2"} {
				if err := st.Nodes().Create(ctx, &apiv1.Node{
					Name: n, Type: apiv1.NodeTypeSatellite,
				}); err != nil {
					t.Fatalf("seed node %s: %v", n, err)
				}
			}

			if err := st.Nodes().Create(ctx, &apiv1.Node{
				Name: "n3", Type: apiv1.NodeTypeSatellite, Props: tc.props,
			}); err != nil {
				t.Fatalf("seed n3: %v", err)
			}

			for _, n := range []string{"n1", "n2"} {
				if err := st.Resources().Create(ctx, &apiv1.Resource{
					Name: "pvc-bug040b", NodeName: n,
				}); err != nil {
					t.Fatalf("seed diskful %s: %v", n, err)
				}
			}

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug040b"},
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

			witnessRow, err := st.Resources().Get(ctx, "pvc-bug040b", "n3")
			if err != nil {
				t.Fatalf("witness must land on the eligible spare n3 (props=%v): %v", tc.props, err)
			}

			diskful, _, witness := classifyReplicas([]apiv1.Resource{witnessRow})
			if diskful != 0 || witness != 1 {
				t.Errorf("n3 row must be a TIE_BREAKER witness; flags=%v", witnessRow.Flags)
			}
		})
	}
}

// TestBug040ExistingWitnessSurvivesAutoplaceTargetFlip pins the
// existing-replica half of the AutoplaceTarget contract: the prop
// excludes the node from NEW placements only — a witness that already
// lives on the node stays put when the operator later stamps
// AutoplaceTarget=false (same "no migration" semantic the placer
// documents for diskful replicas, and what upstream's drain workflow
// promises).
func TestBug040ExistingWitnessSurvivesAutoplaceTargetFlip(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	// The witness host: drained AFTER the witness landed.
	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n3", Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{apiv1.PropAutoplaceTarget: "false"},
	}); err != nil {
		t.Fatalf("seed n3: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug040c", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug040c", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed existing witness on n3: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug040c"},
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

	witnessRow, err := st.Resources().Get(ctx, "pvc-bug040c", "n3")
	if err != nil {
		t.Fatalf("BUG-040: existing witness on n3 must survive the AutoplaceTarget flip: %v", err)
	}

	_, _, witness := classifyReplicas([]apiv1.Resource{witnessRow})
	if witness != 1 {
		t.Errorf("n3 row must still be a TIE_BREAKER witness; flags=%v", witnessRow.Flags)
	}

	// Quorum stays majority: 2 diskful + the surviving witness.
	if q := rd.Spec.Props["DrbdOptions/Resource/quorum"]; q != "majority" {
		t.Errorf("quorum prop got %q, want majority (witness survived)", q)
	}
}
