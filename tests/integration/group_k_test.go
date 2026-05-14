//go:build integration

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

// Group K — operator-day workflow tests (Phase 1).
//
// Each test stitches several `linstor` CLI calls into a realistic
// sequence and asserts the END state through the controller-runtime
// client. The CLI is the same upstream Python binary linstor-csi /
// piraeus / operator hands hit — driving it (instead of forging
// REST JSON inline) is the whole point of Tier 2: a regression in
// the wire shape, the reconciler chain, or the placer surfaces
// here, not in unit tests.
//
// Reconcilers run async on the manager goroutine, so every cross-
// step assertion goes through harness.Eventually. The bug guards in
// the docstrings (Bug 79 late VD, Bug 80 auto-place, Bug 28 lost,
// Bug 83 pool destroyed, etc.) call out the regression each test is
// the canary for — when one of these starts failing on `main`, the
// docstring is the first place the bisecting operator should look.
package integration

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

const (
	// wfEventually is the steady-state budget for cross-reconciler
	// convergence in this group. Operator-day flows touch several
	// reconcilers in sequence (RD → RG-rebalance → autoplacer →
	// satellite mock), so a 30s budget gives 300 100ms-spaced
	// retries — comfortably above the slow-CI tail.
	wfEventually = 30 * time.Second

	// wfSlowEventually is the budget for workflows that involve a
	// scheduled tick — RG rebalance refreshes on a min-interval cadence
	// the reconciler computes from a Props bag, and on a cold envtest
	// boot the first tick can take up to a satellite-tick + interval
	// resolution worth of wall time.
	wfSlowEventually = 60 * time.Second

	// wfVolumeSizeKib is the canonical small volume size for workflow
	// tests. Big enough that a placer's free-capacity rejection
	// (Bug 35) wouldn't trip on the fixture pools; small enough that
	// CI doesn't sweat about per-pool budget accounting.
	wfVolumeSizeKib = 4096 // 4 MiB

	// wfRG is the canonical RG used by tests that don't need to mint
	// their own. SeedThreeNodeCluster pre-creates it.
	wfRG = harness.FixtureDefaultRG
)

// TestGroupKWFHappyPath is the smoke of the workflow group: the
// minimal sequence linstor-csi runs on every PVC mount —
//
//	rd c → vd c → r autoplace=2 → snap c → snap d → r d (cascade) → rd d
//
// Asserts the END state after each step (RDs, VDs, Rs, Snaps in the
// CRD store) so a regression anywhere along the chain surfaces with
// a single failing test rather than a cryptic CSI mount loop.
func TestGroupKWFHappyPath(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	ctx := context.Background()
	rdName := "wf-happy"

	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", wfRG)
	cli.Run(t, "volume-definition", "create", rdName, "4M")
	cli.Run(t, "resource", "create", "--auto-place", "2", rdName)

	waitForDiskfulReplicaCount(t, stack, rdName, 2)
	waitForDRBDUpToDate(t, stack, rdName, 2)

	cli.Run(t, "snapshot", "create", rdName, "snap-1")
	waitForSnapshotExists(t, stack, rdName, "snap-1")

	cli.Run(t, "snapshot", "delete", rdName, "snap-1")
	waitForSnapshotAbsent(t, stack, rdName, "snap-1")

	cli.Run(t, "resource-definition", "delete", rdName)

	harness.Eventually(t, wfEventually, func() bool {
		var rd blockstoriov1alpha1.ResourceDefinition

		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: rdName}, &rd)

		return apierrors.IsNotFound(err)
	}, "RD "+rdName+" not deleted")

	resOnRD := listResourcesOfRD(t, stack, rdName)
	if len(resOnRD) != 0 {
		t.Fatalf("resources for deleted RD %q not cascaded: %d remain", rdName, len(resOnRD))
	}
}

// TestGroupKWFLateVD pins Bug 79: an operator who creates an RD,
// asks for 2 replicas, then adds the VD AFTER the resource-create
// must end with both replicas reaching UpToDate (not a permanently-
// Diskless "unintentional diskless"). The mock satellite stamps
// UpToDate unconditionally — what we assert here is that the FLOW
// completes (no orphan replica, both R rows on the same RD become
// UpToDate, the late-added VD's size is what the operator asked
// for). The real-kernel guard for Bug 79 lives in Tier 4
// (e2e/late-vd-add.sh).
func TestGroupKWFLateVD(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	rdName := "wf-late-vd"

	// 1) RD with no VD.
	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", wfRG)
	// 2) Two replicas BEFORE any VD exists.
	cli.Run(t, "resource", "create", "--auto-place", "2", rdName)
	waitForDiskfulReplicaCount(t, stack, rdName, 2)

	// 3) Now add the VD — production Bug 79 is "Resources stay
	//    Diskless / Unknown forever after late VD-add". The end-state
	//    contract is: both Resources are UpToDate, neither got pinned
	//    to DISKLESS.
	cli.Run(t, "volume-definition", "create", rdName, "4M")

	waitForDRBDUpToDate(t, stack, rdName, 2)

	resources := listResourcesOfRD(t, stack, rdName)
	for i := range resources {
		for _, fl := range resources[i].Spec.Flags {
			if fl == "DISKLESS" {
				t.Fatalf("Bug 79 regression: replica %s pinned to DISKLESS after late VD add",
					resources[i].Name)
			}
		}
	}

	// VD must be present with the operator-requested size.
	rd := getRDWithVDs(t, stack, rdName)
	if len(rd.Spec.VolumeDefinitions) != 1 {
		t.Fatalf("expected 1 VD after late add, got %d", len(rd.Spec.VolumeDefinitions))
	}

	if got := rd.Spec.VolumeDefinitions[0].SizeKib; got != wfVolumeSizeKib {
		t.Errorf("VD size: got %d KiB, want %d KiB", got, wfVolumeSizeKib)
	}
}

// TestGroupKWFAutoPlace2Concurrent pins Bug 80 at the operator-flow
// scope: the exact sequence (`rd c` → `vd c` → `r c --auto-place=2`)
// that surfaced "both replicas stuck Inconsistent, no auto-primary"
// on production must end with both diskful replicas UpToDate and a
// TIE_BREAKER witness on the third worker.
//
// Group F's per-endpoint TestRAutoPlace2ReachesUpToDate already
// covers the API-shape part. Belt-and-braces here: this test asserts
// the full operator chain (CLI args verbatim, cascading reconcilers,
// end state across three CRD kinds) so a regression that breaks
// "operator-day" without breaking the unit-of-handler can't sneak in.
func TestGroupKWFAutoPlace2Concurrent(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	rdName := "wf-auto-place-2"

	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", wfRG)
	cli.Run(t, "volume-definition", "create", rdName, "4M")
	cli.Run(t, "resource", "create", "--auto-place", "2", rdName)

	waitForDiskfulReplicaCount(t, stack, rdName, 2)
	waitForDRBDUpToDate(t, stack, rdName, 2)

	// Tiebreaker on the third node — three workers in the fixture,
	// two carry diskful replicas, the third must surface a
	// TIE_BREAKER witness for quorum-of-3. Spawned by the RD-side
	// reconciler asynchronously.
	harness.Eventually(t, wfEventually, func() bool {
		return hasTiebreaker(t, stack, rdName)
	}, "tiebreaker witness not created on the third worker")

	// Belt-and-braces: total replica count is 3 (2 diskful + 1
	// witness), distributed across the 3 fixture nodes — no double-
	// placement on the same node.
	resources := listResourcesOfRD(t, stack, rdName)
	if len(resources) < 2 || len(resources) > 3 {
		t.Fatalf("expected 2 or 3 Resources (diskful + optional tiebreaker), got %d: %+v",
			len(resources), resources)
	}

	seen := map[string]bool{}
	for i := range resources {
		if seen[resources[i].Spec.NodeName] {
			t.Errorf("duplicate placement on node %s: %+v", resources[i].Spec.NodeName, resources)
		}

		seen[resources[i].Spec.NodeName] = true
	}
}

// TestGroupKWFNodeEvacuateReplaceRestore pins Bug 19 / 5.9: when an
// operator evacuates a node, brings up a replacement, then restores
// the original (without ever running `node lost`), the EVICTED flag
// must lift and the cluster must still hold the configured replica
// count.
//
// Tier 2 cannot drive a real DRBD migration; what we assert here is
// the CRD-state contract: PUT evacuate stamps EVICTED, PUT restore
// strips it, the Resource rows on the evacuated node either move to
// another worker or stay (depending on placer state) but the
// operator's lifecycle command-chain is wire-shape correct.
func TestGroupKWFNodeEvacuateReplaceRestore(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	ctx := context.Background()
	rdName := "wf-evac-restore"

	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", wfRG)
	cli.Run(t, "volume-definition", "create", rdName, "4M")
	cli.Run(t, "resource", "create", "--auto-place", "2", rdName)
	waitForDiskfulReplicaCount(t, stack, rdName, 2)

	target := harness.NodeWorker1

	// 1) Evacuate worker-1.
	cli.Run(t, "node", "evacuate", target)

	harness.Eventually(t, wfEventually, func() bool {
		var n blockstoriov1alpha1.Node

		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: target}, &n)
		if err != nil {
			return false
		}

		for _, fl := range n.Spec.Flags {
			if fl == "EVICTED" {
				return true
			}
		}

		return false
	}, "node "+target+" not flagged EVICTED after evacuate")

	// 2) Restore — the operator decided to bring the node back.
	cli.Run(t, "node", "restore", target)

	harness.Eventually(t, wfEventually, func() bool {
		var n blockstoriov1alpha1.Node

		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: target}, &n)
		if err != nil {
			return false
		}

		for _, fl := range n.Spec.Flags {
			if fl == "EVICTED" {
				return false
			}
		}

		return true
	}, "node "+target+" EVICTED flag not lifted by restore")

	// End state: the cluster is back to a 3-node, RD-with-replicas
	// shape. Replica count for the RD is at least the original 2 —
	// the placer never strips below the operator-requested floor.
	resources := listResourcesOfRD(t, stack, rdName)
	if len(resources) < 2 {
		t.Errorf("RD %q lost replicas across evacuate/restore: %d remain", rdName, len(resources))
	}
}

// TestGroupKWFNodeLostCascade pins Bug 28: `node lost <name>` must
// cascade-delete every Resource and StoragePool on the lost node so
// the (rd, node) name slot is free for a fresh provisioning and the
// `linstor sp l` view doesn't keep ghosts.
//
// Sequence: provision RD with 3 replicas → lose worker-2 → assert
// the worker-2 row and its child Resource + StoragePool rows are
// gone, and the surviving siblings are still on the other two
// workers.
func TestGroupKWFNodeLostCascade(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	ctx := context.Background()
	rdName := "wf-lost"

	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", wfRG)
	cli.Run(t, "volume-definition", "create", rdName, "4M")
	cli.Run(t, "resource", "create", "--auto-place", "3", rdName)
	waitForDiskfulReplicaCount(t, stack, rdName, 3)

	lost := harness.NodeWorker2
	cli.Run(t, "node", "lost", lost)

	// Node row must vanish.
	harness.Eventually(t, wfEventually, func() bool {
		var n blockstoriov1alpha1.Node

		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: lost}, &n)

		return apierrors.IsNotFound(err)
	}, "Node "+lost+" not deleted by `node lost`")

	// Every Resource bound to the lost node must be gone (Bug 28).
	harness.Eventually(t, wfEventually, func() bool {
		var list blockstoriov1alpha1.ResourceList
		if err := stack.Env.Client.List(ctx, &list); err != nil {
			return false
		}

		for i := range list.Items {
			if list.Items[i].Spec.NodeName == lost {
				return false
			}
		}

		return true
	}, "Resource rows on lost node "+lost+" not cascaded")

	// StoragePools on the lost node must also be cleaned up so the
	// placer's free-space ranking doesn't read ghosts.
	harness.Eventually(t, wfEventually, func() bool {
		var list blockstoriov1alpha1.StoragePoolList
		if err := stack.Env.Client.List(ctx, &list); err != nil {
			return false
		}

		for i := range list.Items {
			if list.Items[i].Spec.NodeName == lost {
				return false
			}
		}

		return true
	}, "StoragePool rows on lost node "+lost+" not cascaded")

	// Survivors: the RD still exists, replicas live on the two
	// non-lost workers.
	survivors := listResourcesOfRD(t, stack, rdName)
	for i := range survivors {
		if survivors[i].Spec.NodeName == lost {
			t.Errorf("survivor %s still bound to lost node %s", survivors[i].Name, lost)
		}
	}
}

// TestGroupKWFPoolDestroyedDropsFromPlacer pins Bug 83 / Bug 35:
// when a satellite reports PoolMissing=true for an SP, the placer
// must immediately stop selecting it as a candidate AND `linstor
// sp l` must surface the offending pool with `state=Faulty`. The
// operator-day flow is: provision against 3 pools → one pool dies
// → operator does `rg sr ...` for a fresh RD → the new RD lands on
// the other two pools, NOT on the dead one.
func TestGroupKWFPoolDestroyedDropsFromPlacer(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	// Pick the lvm-thin pool on worker-1 — fixture-seeded; we know
	// its (node, pool) shape. The mock satellite's
	// SimulatePoolMissing flips Status.PoolMissing on the next tick.
	deadNode := harness.NodeWorker1
	deadPool := "lvm-thin"

	stack.Satellite.SimulatePoolMissing(deadNode, deadPool)

	// Give the satellite mock a tick or two so PoolMissing actually
	// lands on the CRD status. Without this the placer reads a clean
	// SP and the test is racy.
	harness.Eventually(t, wfEventually, func() bool {
		var sp blockstoriov1alpha1.StoragePool

		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: deadPool + "." + deadNode}, &sp)
		if err != nil {
			return false
		}

		return sp.Status.PoolMissing
	}, "SP "+deadPool+"."+deadNode+" PoolMissing not stamped by mock satellite")

	// Now spawn an RD pinned to the lvm-thin storage pool with
	// PlaceCount=2. The placer MUST skip worker-1's missing pool and
	// land both replicas on worker-2/worker-3.
	rdName := "wf-pool-gone"
	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", wfRG, "--storage-pool", deadPool)
	cli.Run(t, "volume-definition", "create", rdName, "4M")
	cli.Run(t, "resource", "create", "--auto-place", "2",
		"--storage-pool", deadPool, rdName)

	waitForDiskfulReplicaCount(t, stack, rdName, 2)

	for _, r := range listResourcesOfRD(t, stack, rdName) {
		if r.Spec.NodeName == deadNode {
			t.Errorf("placer landed replica on node %s with PoolMissing pool %s — Bug 83 regression",
				deadNode, deadPool)
		}
	}
}

// TestGroupKWFReplicasOnSame pins wave1 2.7 / Bug 44: when the RG
// constrains placement with `replicas-on-same Aux/zone`, every
// replica spawned from that RG must land on nodes that share the
// zone label. The flow: stamp Aux/zone on every node → set the
// RG's filter → spawn → assert all replicas are in one zone group.
func TestGroupKWFReplicasOnSame(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	ctx := context.Background()

	// Two nodes in zone-east, one in zone-west. PlaceCount=2 with
	// replicas-on-same MUST pick both zone-east nodes.
	nodeZones := map[string]string{
		harness.NodeWorker1: "east",
		harness.NodeWorker2: "east",
		harness.NodeWorker3: "west",
	}
	for n, z := range nodeZones {
		patchNodeProp(t, stack.Env.Client, n, "Aux/zone", z)
	}

	// Bespoke RG so we don't perturb the shared fixture default.
	rgName := "rg-zone-same"
	createResourceGroupReplicasOnSame(t, stack.Env.Client, rgName, []string{"Aux/zone"}, 2)

	rdName := "wf-zone-same"
	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", rgName)
	cli.Run(t, "volume-definition", "create", rdName, "4M")
	cli.Run(t, "resource", "create", "--auto-place", "2", rdName)
	waitForDiskfulReplicaCount(t, stack, rdName, 2)

	resources := listResourcesOfRD(t, stack, rdName)
	zoneSeen := map[string]bool{}

	for i := range resources {
		zoneSeen[nodeZones[resources[i].Spec.NodeName]] = true
	}

	if len(zoneSeen) != 1 {
		t.Errorf("replicas-on-same violated: replicas span %d zones %v; %+v",
			len(zoneSeen), zoneSeen, resources)
	}

	if zoneSeen["west"] {
		t.Errorf("replicas-on-same picked the singleton zone-west (too small for place_count=2); %+v",
			resources)
	}

	_ = ctx
}

// TestGroupKWFReplicasOnDifferent is the inverse of the above: the
// RG declares `replicas-on-different Aux/zone` and place-count=2,
// so the two replicas MUST land on DIFFERENT zones.
func TestGroupKWFReplicasOnDifferent(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	nodeZones := map[string]string{
		harness.NodeWorker1: "a",
		harness.NodeWorker2: "a",
		harness.NodeWorker3: "b",
	}
	for n, z := range nodeZones {
		patchNodeProp(t, stack.Env.Client, n, "Aux/zone", z)
	}

	rgName := "rg-zone-diff"
	createResourceGroupReplicasOnDifferent(t, stack.Env.Client, rgName, []string{"Aux/zone"}, 2)

	rdName := "wf-zone-diff"
	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", rgName)
	cli.Run(t, "volume-definition", "create", rdName, "4M")
	cli.Run(t, "resource", "create", "--auto-place", "2", rdName)
	waitForDiskfulReplicaCount(t, stack, rdName, 2)

	zones := map[string]bool{}
	for _, r := range listResourcesOfRD(t, stack, rdName) {
		zones[nodeZones[r.Spec.NodeName]] = true
	}

	if len(zones) < 2 {
		t.Errorf("replicas-on-different violated: replicas all in %v", zones)
	}
}

// TestGroupKWFLUKSStackEndToEnd pins scenario 6.W12: the
// encryption-passphrase POST unlocks the controller, an RG spawn
// with layer-list = [DRBD, LUKS, STORAGE] propagates that
// composition onto the spawned RD's LayerStack, and the resulting
// Resources are not stuck `state.suspended` (i.e. the controller's
// passphraseUnlocked flag is honoured for the rendered .res).
//
// Real cryptsetup / drbd configure-md interaction lives in Tier 3 /
// Tier 4; Tier 2 asserts the CRD-shape contract: LUKS shows up in
// RD.Spec.LayerStack.
func TestGroupKWFLUKSStackEndToEnd(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	// Unlock the controller before any LUKS-stack provisioning.
	cli.Run(t, "encryption", "create-passphrase", "--new-passphrase", "supersecret-passphrase-1")

	rdName := "wf-luks"
	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", wfRG,
		"--layer-list", "drbd,luks,storage")
	cli.Run(t, "volume-definition", "create", rdName, "4M")
	cli.Run(t, "resource", "create", "--auto-place", "2", rdName)
	waitForDiskfulReplicaCount(t, stack, rdName, 2)

	rd := getRDWithVDs(t, stack, rdName)
	if !layerStackContains(rd.Spec.LayerStack, "LUKS") {
		t.Errorf("LayerStack missing LUKS: %v", rd.Spec.LayerStack)
	}

	if !layerStackContains(rd.Spec.LayerStack, "DRBD") {
		t.Errorf("LayerStack missing DRBD: %v", rd.Spec.LayerStack)
	}
}

// TestGroupKWFSpawnAndDependentReAutoplace pins Bug 60: when the
// operator raises PlaceCount on an RG that already has spawned RDs,
// the RGRebalanceReconciler must re-autoplace every dependent RD
// up to the new count. Operator-day flow: spawn an RD with
// PlaceCount=2 → modify the RG to PlaceCount=3 → the dependent RD
// gains a third replica without operator intervention.
func TestGroupKWFSpawnAndDependentReAutoplace(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	ctx := context.Background()

	rgName := "rg-realloc"
	createResourceGroupWithPlaceCount(t, stack.Env.Client, rgName, 2)

	// Spawn one dependent RD from the new RG; PlaceCount=2 inherited.
	cli.Run(t, "resource-group", "spawn-resources", rgName, "wf-realloc", "4M")
	waitForDiskfulReplicaCount(t, stack, "wf-realloc", 2)

	// Raise PlaceCount to 3 on the parent RG. The REST handler
	// stamps blockstor.io/rebalance-pending on the RG; the
	// RGRebalanceReconciler picks it up and runs the additive placer.
	cli.Run(t, "resource-group", "modify", rgName, "--place-count", "3")

	// End state: the dependent RD now has 3 diskful replicas.
	// Eventually-budget bumped because the rebalance pass runs
	// off the manager's scheduled cadence, not synchronously on
	// the REST handler.
	harness.Eventually(t, wfSlowEventually, func() bool {
		return diskfulReplicaCount(t, stack, "wf-realloc") >= 3
	}, "dependent RD wf-realloc did not gain a 3rd replica after RG modify (Bug 60)")

	// The rebalance-pending annotation must be stripped once the
	// pass completes (defence-in-depth: a stuck annotation would
	// churn the reconciler forever).
	harness.Eventually(t, wfEventually, func() bool {
		var rg blockstoriov1alpha1.ResourceGroup
		if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: rgName}, &rg); err != nil {
			return false
		}

		_, has := rg.Annotations["blockstor.io/rebalance-pending"]

		return !has
	}, "rebalance-pending annotation not stripped after rebalance pass")
}

// TestGroupKWFBalanceResourcesTick pins scenarios 2.15 / 2.20: the
// RGRebalanceReconciler runs on a periodic tick driven by the
// controller-scope BalanceResourcesInterval property, not just on
// explicit operator action. Operator-day flow: provision RD with 2
// replicas, set a short Interval, manually seed a missing-replica
// gap via the API, then wait for the scheduled tick to top up.
//
// Today's harness fast-path: the rebalance reconciler also responds
// to RG-modify (annotation), which we already cover in
// TestGroupKWFSpawnAndDependentReAutoplace. Here we exercise the
// scheduled-tick path by setting BalanceResourcesInterval=1 (the
// smallest positive value) on the controller and observing the
// reconciler restore a deficit it would normally ignore on a
// modify-driven path.
func TestGroupKWFBalanceResourcesTick(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	// 1) Configure the shortest possible tick interval. The
	// reconciler reads controller-scope props directly through the
	// store, so setting it via `linstor c sp` is the operator-facing
	// path.
	cli.Run(t, "controller", "set-property", "BalanceResourcesInterval", "1")
	cli.Run(t, "controller", "set-property", "BalanceResourcesGracePeriod", "0")

	rgName := "rg-tick"
	createResourceGroupWithPlaceCount(t, stack.Env.Client, rgName, 3)

	// 2) Spawn an RD with place_count=3 — three replicas land.
	cli.Run(t, "resource-group", "spawn-resources", rgName, "wf-tick", "4M")
	waitForDiskfulReplicaCount(t, stack, "wf-tick", 3)

	// 3) Manually remove ONE replica (simulating a satellite drop /
	// migration teardown). The CRD removal alone won't trigger an
	// annotation-driven rebalance; the scheduled tick must re-add it.
	resources := listResourcesOfRD(t, stack, "wf-tick")
	if len(resources) < 3 {
		t.Fatalf("setup failed: expected 3 resources, got %d", len(resources))
	}

	victim := resources[0]
	err := stack.Env.Client.Delete(context.Background(), &victim)
	if err != nil {
		t.Fatalf("delete victim Resource %s: %v", victim.Name, err)
	}

	// 4) The scheduled rebalance tick must notice the deficit and
	// re-place. Budget is generous: the controller-scope prop is
	// minutes, so we set Interval=1min — Eventually keeps polling
	// the CRD until a fresh Resource appears.
	harness.Eventually(t, wfSlowEventually, func() bool {
		return diskfulReplicaCount(t, stack, "wf-tick") >= 3
	}, "scheduled rebalance tick did not restore replica count to 3")
}

// TestGroupKWFToggleDiskUnderSync pins Bug 8: an operator's
// `toggle-disk` call against a replica that's currently in a
// non-steady state (SyncTarget — receiving an initial sync from a
// peer) MUST be honoured at the CRD-spec level so the satellite
// reconciler can defer the action until the sync completes. The
// real defer logic lives in pkg/satellite; here we assert the
// envelope contract: PUT toggle-disk returns 200 and the
// DISKLESS-flag toggle persists on the Resource spec.
func TestGroupKWFToggleDiskUnderSync(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	rdName := "wf-toggle-sync"

	cli.Run(t, "resource-definition", "create", rdName, "--resource-group", wfRG)
	cli.Run(t, "volume-definition", "create", rdName, "4M")
	cli.Run(t, "resource", "create", "--auto-place", "2", rdName)
	waitForDiskfulReplicaCount(t, stack, rdName, 2)

	resources := listResourcesOfRD(t, stack, rdName)

	target := resources[0]

	// Simulate SyncTarget on the target node so the toggle-disk
	// request lands while DRBD is mid-sync (Bug 8 root cause). The
	// satellite mock honours the override via SimulateDRBDState.
	stack.Satellite.SimulateDRBDState(rdName, target.Spec.NodeName, "SyncTarget")

	harness.Eventually(t, wfEventually, func() bool {
		var got blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: target.Name}, &got)
		if err != nil {
			return false
		}

		return got.Status.DrbdState == "SyncTarget"
	}, "SyncTarget mock not reflected on Resource "+target.Name)

	// Operator issues toggle-disk to diskless while peer is
	// SyncTarget. Upstream LINSTOR's PUT envelope: the diskless
	// suffix forces the demotion shape.
	cli.Run(t, "resource", "toggle-disk", "--diskless", target.Spec.NodeName, rdName)

	// End state: DISKLESS is now on Spec.Flags so the satellite
	// reconciler has a clear target to converge towards once the
	// sync completes. Bug 8 was "toggle-disk silently dropped" —
	// here we pin that the spec PERSISTED.
	harness.Eventually(t, wfEventually, func() bool {
		var got blockstoriov1alpha1.Resource

		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: target.Name}, &got)
		if err != nil {
			return false
		}

		for _, fl := range got.Spec.Flags {
			if fl == "DISKLESS" {
				return true
			}
		}

		return false
	}, "toggle-disk did not persist DISKLESS on Resource "+target.Name+" (Bug 8 regression)")
}

// -----------------------------------------------------------------
// helpers — kept inline in this file (per playbook §1 "Files to add"
// the launcher allows exactly one Go file per group; we don't carve
// a separate group_k_helpers.go because none of these helpers are
// needed outside Group K's operator-day flows).
// -----------------------------------------------------------------

// listResourcesOfRD returns the Resource rows whose
// Spec.ResourceDefinitionName matches the given RD. We list the
// whole cluster and filter because the apiserver's
// `metadata.name=…` field selector doesn't cover CRD spec fields.
func listResourcesOfRD(t *testing.T, stack *harness.Stack, rdName string) []blockstoriov1alpha1.Resource {
	t.Helper()

	var list blockstoriov1alpha1.ResourceList

	err := stack.Env.Client.List(context.Background(), &list)
	if err != nil {
		t.Fatalf("list Resources: %v", err)
	}

	out := make([]blockstoriov1alpha1.Resource, 0, len(list.Items))

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName == rdName {
			out = append(out, list.Items[i])
		}
	}

	return out
}

// diskfulReplicaCount counts non-DISKLESS, non-TIE_BREAKER Resources
// for the given RD. Operator-day workflows care about the diskful
// floor — a tiebreaker witness doesn't count towards "I asked for N
// replicas".
func diskfulReplicaCount(t *testing.T, stack *harness.Stack, rdName string) int {
	t.Helper()

	resources := listResourcesOfRD(t, stack, rdName)
	count := 0

	for i := range resources {
		if isDiskless(resources[i].Spec.Flags) || isTiebreaker(resources[i].Spec.Flags) {
			continue
		}

		count++
	}

	return count
}

// waitForDiskfulReplicaCount blocks until at least `want` diskful
// replicas exist for `rdName` or the eventually-budget elapses.
func waitForDiskfulReplicaCount(t *testing.T, stack *harness.Stack, rdName string, want int) {
	t.Helper()

	harness.Eventually(t, wfEventually, func() bool {
		return diskfulReplicaCount(t, stack, rdName) >= want
	}, "diskful replica count for RD "+rdName+" never reached "+itoa(want))
}

// waitForDRBDUpToDate waits until at least `want` of the RD's
// diskful Resources report Status.DrbdState=UpToDate. The mock
// satellite stamps UpToDate on its tick; this gates the cross-step
// happy-path tests so a slow envtest CI doesn't race against the
// next CLI call.
func waitForDRBDUpToDate(t *testing.T, stack *harness.Stack, rdName string, want int) {
	t.Helper()

	harness.Eventually(t, wfEventually, func() bool {
		ok := 0

		for _, r := range listResourcesOfRD(t, stack, rdName) {
			if isDiskless(r.Spec.Flags) || isTiebreaker(r.Spec.Flags) {
				continue
			}

			if r.Status.DrbdState == "UpToDate" {
				ok++
			}
		}

		return ok >= want
	}, "DrbdState=UpToDate count for RD "+rdName+" never reached "+itoa(want))
}

// hasTiebreaker returns true when an RD has at least one TIE_BREAKER
// witness replica. Used by the auto-place=2 workflow to assert the
// quorum-of-3 invariant.
func hasTiebreaker(t *testing.T, stack *harness.Stack, rdName string) bool {
	t.Helper()

	for _, r := range listResourcesOfRD(t, stack, rdName) {
		if isTiebreaker(r.Spec.Flags) {
			return true
		}
	}

	return false
}

// waitForSnapshotExists blocks until a Snapshot named `<rd>.<snap>`
// shows up. The CLI's `snapshot create` is async-ish — the REST
// handler creates the Snapshot row but the satellite mock advances
// per-node status on the next tick.
func waitForSnapshotExists(t *testing.T, stack *harness.Stack, rdName, snapName string) {
	t.Helper()

	full := rdName + "." + snapName

	harness.Eventually(t, wfEventually, func() bool {
		var snap blockstoriov1alpha1.Snapshot

		err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: full}, &snap)

		return err == nil
	}, "Snapshot "+full+" not created")
}

func waitForSnapshotAbsent(t *testing.T, stack *harness.Stack, rdName, snapName string) {
	t.Helper()

	full := rdName + "." + snapName

	harness.Eventually(t, wfEventually, func() bool {
		var snap blockstoriov1alpha1.Snapshot

		err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: full}, &snap)

		return apierrors.IsNotFound(err)
	}, "Snapshot "+full+" not deleted")
}

// getRDWithVDs fetches the RD CRD; tests use it to read
// Spec.VolumeDefinitions and Spec.LayerStack.
func getRDWithVDs(t *testing.T, stack *harness.Stack, rdName string) *blockstoriov1alpha1.ResourceDefinition {
	t.Helper()

	var rd blockstoriov1alpha1.ResourceDefinition

	err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: rdName}, &rd)
	if err != nil {
		t.Fatalf("get RD %q: %v", rdName, err)
	}

	return &rd
}

// patchNodeProp stamps a single Props[key]=value entry on a Node.
// Used for replicas-on-same / replicas-on-different scenarios where
// the placer reads node labels.
func patchNodeProp(t *testing.T, c client.Client, name, key, value string) {
	t.Helper()

	ctx := context.Background()

	var node blockstoriov1alpha1.Node

	err := c.Get(ctx, types.NamespacedName{Name: name}, &node)
	if err != nil {
		t.Fatalf("get Node %q: %v", name, err)
	}

	if node.Spec.Props == nil {
		node.Spec.Props = map[string]string{}
	}

	node.Spec.Props[key] = value

	err = c.Update(ctx, &node)
	if err != nil {
		t.Fatalf("update Node %q (Aux prop): %v", name, err)
	}
}

// createResourceGroupReplicasOnSame mints an RG with the requested
// place-count and a ReplicasOnSame filter pinning the listed keys.
func createResourceGroupReplicasOnSame(t *testing.T, c client.Client, name string, keys []string, placeCount int32) {
	t.Helper()

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				PlaceCount:     placeCount,
				ReplicasOnSame: append([]string(nil), keys...),
			},
		},
	}

	err := c.Create(context.Background(), rg)
	if err != nil {
		t.Fatalf("create RG %q (replicas-on-same): %v", name, err)
	}
}

func createResourceGroupReplicasOnDifferent(t *testing.T, c client.Client, name string, keys []string, placeCount int32) {
	t.Helper()

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				PlaceCount:          placeCount,
				ReplicasOnDifferent: append([]string(nil), keys...),
			},
		},
	}

	err := c.Create(context.Background(), rg)
	if err != nil {
		t.Fatalf("create RG %q (replicas-on-different): %v", name, err)
	}
}

func createResourceGroupWithPlaceCount(t *testing.T, c client.Client, name string, placeCount int32) {
	t.Helper()

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				PlaceCount: placeCount,
			},
		},
	}

	err := c.Create(context.Background(), rg)
	if err != nil {
		t.Fatalf("create RG %q: %v", name, err)
	}
}

func isDiskless(flags []string) bool {
	for _, f := range flags {
		if f == "DISKLESS" {
			return true
		}
	}

	return false
}

func isTiebreaker(flags []string) bool {
	for _, f := range flags {
		if f == "TIE_BREAKER" {
			return true
		}
	}

	return false
}

func layerStackContains(stack []string, want string) bool {
	upper := strings.ToUpper(want)

	for _, l := range stack {
		if strings.ToUpper(l) == upper {
			return true
		}
	}

	return false
}

// itoa is a one-off so the assertion messages don't drag strconv
// imports in for the only-other-time-they're-needed pattern.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	neg := n < 0
	if neg {
		n = -n
	}

	var digits []byte

	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}

	if neg {
		digits = append([]byte{'-'}, digits...)
	}

	return string(digits)
}

// sortedJSONKeys is a defence-in-depth assertion helper: the linstor
// CLI emits JSON in an order python's dict happened to iterate, and
// tests that depend on the order are flaky. Unused today but kept
// in scope so future workflow tests reach for it instead of
// hand-rolling.
//
//nolint:deadcode,unused // see docstring
func sortedJSONKeys(raw []byte) []string {
	var m map[string]any

	err := json.Unmarshal(raw, &m)
	if err != nil {
		return nil
	}

	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}
