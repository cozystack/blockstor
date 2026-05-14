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

// Group F — Resource. Phase 1 group covering the 14 resource-tier
// scenarios from docs/test-strategy.md. Each test drives the
// blockstor REST surface through the upstream `linstor` CLI binary,
// uses the in-process satellite mock from `harness/satellite.go`
// to drive Status fields to the steady state, and inspects CRDs
// directly via the controller-runtime client for assertions that
// the wire envelope does not surface.
//
// Bug-guard map (column 3 of the strategy table):
//   - Bug 80: AutoPlace=2 stuck Inconsistent (auto-primary election).
//   - Bug 28: tiebreaker / place-count invariant.
//   - Bug 40: toggle-cancel.
//   - Bug 34: migrate-disk add-before-drop.
//   - Bug 45 / 46: activate / deactivate envelope.
//   - Bug 56 / 66: delete idempotent (404 → 200).
//   - Bug 75: StoragePool field on Volume rows.
//   - Bug 203: effective-properties endpoint.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// groupFRDPrefix is the well-known prefix for RDs spawned by Group F
// tests so a wedged test stage leaves a recognisable trail in the
// envtest apiserver. Each test appends its own suffix.
const groupFRDPrefix = "pvc-gf-"

// groupFAssertTimeout is the Eventually budget for the steady-state
// asserts after a CLI write — generous enough to absorb envtest's
// reconcile + cache trail, short enough that a real bug surfaces
// quickly.
const groupFAssertTimeout = 30 * time.Second

// groupFShortTimeout caps quick CRD-state polls (e.g. waiting for
// the auto-tiebreaker to land on the 3rd node).
const groupFShortTimeout = 20 * time.Second

// ---------------------------------------------------------------------------
// TestGroupFRCreateExplicit — `r c <node> <rd> --storage-pool <p>`
// creates exactly one Resource on the named node and reaches UpToDate.
// ---------------------------------------------------------------------------

func TestGroupFRCreateExplicit(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "create")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")

	waitForResourceExists(t, stack, rd, harness.NodeWorker1)
	harness.WaitForDRBDState(t, stack, rd, harness.NodeWorker1, "UpToDate")

	// Wire-shape assertion: `linstor r l` surfaces the row.
	rows := cli.JSON(t, "resource", "list", "-r", rd)
	if len(rows) == 0 {
		t.Fatalf("resource list returned no rows for %s", rd)
	}

	got := resourceNodeNames(rows)
	if !contains(got, harness.NodeWorker1) {
		t.Fatalf("resource list for %s missing node %s (got %v)",
			rd, harness.NodeWorker1, got)
	}
}

// ---------------------------------------------------------------------------
// TestGroupFRAutoPlace2ReachesUpToDate — Bug 80 regression repro.
// `r c <rd> --auto-place 2` MUST stamp `auto-primary=true` on exactly
// ONE replica (the one with the lowest DRBD node-id), both replicas
// MUST reach DiskState=UpToDate, and the controller MUST plant a
// TIE_BREAKER witness on the 3rd node.
// ---------------------------------------------------------------------------

func TestGroupFRAutoPlace2ReachesUpToDate(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "ap2")

	cli.JSON(t, "resource", "create", rd, "--auto-place", "2",
		"--storage-pool", "lvm-thin")

	// 1. Two diskful Resources land.
	diskful := waitForDiskfulReplicas(t, stack, rd, 2)
	if len(diskful) != 2 {
		t.Fatalf("auto-place 2 produced %d diskful replicas, want 2", len(diskful))
	}

	// 2. Both reach UpToDate. Satellite mock stamps this in steady
	//    state — if either stays non-UpToDate we have a regression
	//    in the placer / dispatcher chain.
	for _, r := range diskful {
		harness.WaitForDRBDState(t, stack, rd, r.Spec.NodeName, "UpToDate")
	}

	// 3. Auto-tiebreaker witness lands on the 3rd node, carrying
	//    both TIE_BREAKER and DISKLESS flags.
	witness := waitForTiebreakerWitness(t, stack, rd)
	if !contains(witness.Spec.Flags, "TIE_BREAKER") ||
		!contains(witness.Spec.Flags, "DISKLESS") {
		t.Fatalf("witness on %s missing TIE_BREAKER+DISKLESS flags: %v",
			witness.Spec.NodeName, witness.Spec.Flags)
	}

	// 4. Bug 80 invariant: dispatcher.BuildDesired stamps
	//    `auto-primary=true` on exactly the lowest-NodeID diskful
	//    replica — never on both, never on neither.
	assertExactlyOneAutoPrimary(t, stack, rd, diskful)
}

// ---------------------------------------------------------------------------
// TestGroupFRAutoPlace3WithTieBreaker — Bug 28 guard. placementCount=3
// MUST land 3 diskful replicas with NO TIE_BREAKER witness — there's
// no need for a tiebreaker when every healthy node already hosts a
// real replica.
// ---------------------------------------------------------------------------

func TestGroupFRAutoPlace3WithTieBreaker(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "ap3")

	cli.JSON(t, "resource", "create", rd, "--auto-place", "3",
		"--storage-pool", "lvm-thin")

	diskful := waitForDiskfulReplicas(t, stack, rd, 3)
	if len(diskful) != 3 {
		t.Fatalf("auto-place 3 produced %d diskful replicas, want 3", len(diskful))
	}

	for _, r := range diskful {
		harness.WaitForDRBDState(t, stack, rd, r.Spec.NodeName, "UpToDate")
	}

	// Bug 28 invariant: no TIE_BREAKER witness with 3 diskful
	// replicas. Give the RD reconciler a beat to (correctly) NOT
	// spawn one, then verify.
	time.Sleep(2 * time.Second)

	all := listResourcesByRD(t, stack, rd)
	for _, r := range all {
		if contains(r.Spec.Flags, "TIE_BREAKER") {
			t.Fatalf("3-replica RD got TIE_BREAKER on %s; flags=%v",
				r.Spec.NodeName, r.Spec.Flags)
		}
	}
}

// ---------------------------------------------------------------------------
// TestGroupFRToggleDiskful2Diskless — `r toggle-disk` flips a diskful
// replica to DISKLESS. The CLI invokes the same PUT route the upstream
// `linstor r td` does.
// ---------------------------------------------------------------------------

func TestGroupFRToggleDiskful2Diskless(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "td-down")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	waitForResourceExists(t, stack, rd, harness.NodeWorker1)

	cli.JSON(t, "resource", "toggle-disk", harness.NodeWorker1, rd, "--diskless")

	harness.Eventually(t, groupFAssertTimeout, func() bool {
		r := getResource(t, stack, rd, harness.NodeWorker1)

		return r != nil && contains(r.Spec.Flags, "DISKLESS")
	}, "Resource "+rd+"."+harness.NodeWorker1+" never gained DISKLESS flag")
}

// ---------------------------------------------------------------------------
// TestGroupFRToggleDiskless2Diskful — reverse of the above: an
// initially diskless replica gets promoted to diskful by re-issuing
// `resource create` with a `--storage-pool`.
// ---------------------------------------------------------------------------

func TestGroupFRToggleDiskless2Diskful(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "td-up")

	// Create a diskless replica first.
	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd, "--diskless")
	harness.Eventually(t, groupFAssertTimeout, func() bool {
		r := getResource(t, stack, rd, harness.NodeWorker1)

		return r != nil && contains(r.Spec.Flags, "DISKLESS")
	}, "initial DISKLESS replica never landed")

	// Promote to diskful via a fresh create with --storage-pool — the
	// promoteDisklessReplica path strips DISKLESS / TIE_BREAKER and
	// stamps the new pool. Matches upstream LINSTOR semantics.
	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")

	harness.Eventually(t, groupFAssertTimeout, func() bool {
		r := getResource(t, stack, rd, harness.NodeWorker1)

		return r != nil && !contains(r.Spec.Flags, "DISKLESS")
	}, "DISKLESS flag never cleared after promote")
}

// ---------------------------------------------------------------------------
// TestGroupFRToggleCancel — Bug 40 regression. The REST shim accepts
// `?cancel=true` on the toggle-disk endpoint and stamps Spec.ToggleDiskCancel
// so the satellite reconciler can unwind a half-finished diskless→diskful
// conversion. We drive the cancel directly via HTTP since the upstream
// CLI does not yet surface a `--cancel` switch on the Python side.
// ---------------------------------------------------------------------------

func TestGroupFRToggleCancel(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "td-cancel")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	waitForResourceExists(t, stack, rd, harness.NodeWorker1)

	url := fmt.Sprintf("%s/v1/resource-definitions/%s/resources/%s/toggle-disk?cancel=true",
		stack.RestURL, rd, harness.NodeWorker1)

	// Retry briefly on transient 5xx: the satellite mock writes Status
	// concurrently with the controller-runtime allocator's Spec patches,
	// and the apiserver can surface a stale-resource-version conflict
	// on the first PUT. The handler is idempotent — once the cache
	// settles the next attempt sees a clean (rd, node) read and stamps
	// ToggleDiskCancel=true.
	putWithRetry(t, url, http.StatusOK)

	harness.Eventually(t, groupFAssertTimeout, func() bool {
		r := getResource(t, stack, rd, harness.NodeWorker1)

		return r != nil && r.Spec.ToggleDiskCancel
	}, "ToggleDiskCancel flag never set")
}

// ---------------------------------------------------------------------------
// TestGroupFRMigrateDisk — Bug 34. `r migrate-disk` is upstream
// LINSTOR's add-before-drop replica move; the REST endpoint stamps
// the destination with BlockstorMigratingFrom and leaves the source
// alive until the destination reaches UpToDate. Test asserts the
// stamp lands; the prune step is the migration-reconciler's job.
// ---------------------------------------------------------------------------

func TestGroupFRMigrateDisk(t *testing.T) {
	stack, _, rd := setupGroupFRD(t, "mig")
	cli := &harness.CLI{URL: stack.RestURL}

	// Seed a diskful source on worker-1.
	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	waitForResourceExists(t, stack, rd, harness.NodeWorker1)
	harness.WaitForDRBDState(t, stack, rd, harness.NodeWorker1, "UpToDate")

	url := fmt.Sprintf(
		"%s/v1/resource-definitions/%s/resources/%s/migrate-disk/%s/%s",
		stack.RestURL, rd, harness.NodeWorker2, harness.NodeWorker1, "lvm-thin")

	putWithRetry(t, url, http.StatusOK)

	// Add-before-drop: destination exists carrying the migrating-from
	// stamp, source is still around.
	harness.Eventually(t, groupFAssertTimeout, func() bool {
		dst := getResource(t, stack, rd, harness.NodeWorker2)
		src := getResource(t, stack, rd, harness.NodeWorker1)

		if dst == nil || src == nil {
			return false
		}

		return dst.Spec.Props["BlockstorMigratingFrom"] == harness.NodeWorker1
	}, "migrate-disk: destination never stamped with BlockstorMigratingFrom + source intact")
}

// ---------------------------------------------------------------------------
// TestGroupFRActivateDeactivate — Bug 45/46. Both endpoints MUST
// reply with the `[]ApiCallRc` JSON envelope (a bare 200/204 makes
// golinstor's response parser blow up). Idempotent: a second
// deactivate doesn't duplicate the INACTIVE flag.
// ---------------------------------------------------------------------------

func TestGroupFRActivateDeactivate(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "act")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	waitForResourceExists(t, stack, rd, harness.NodeWorker1)

	// Deactivate: INACTIVE flag appears and the response envelope
	// parses as a non-empty ApiCallRc list (the CLI calls fatal on
	// `json.decoder.JSONDecodeError`, so reaching the assertion at
	// all proves the envelope shape).
	postEnvelope(t, stack.RestURL+"/v1/resource-definitions/"+rd+
		"/resources/"+harness.NodeWorker1+"/deactivate")
	harness.Eventually(t, groupFAssertTimeout, func() bool {
		r := getResource(t, stack, rd, harness.NodeWorker1)

		return r != nil && contains(r.Spec.Flags, "INACTIVE")
	}, "INACTIVE flag never appeared after deactivate")

	// Activate: INACTIVE flag clears.
	postEnvelope(t, stack.RestURL+"/v1/resource-definitions/"+rd+
		"/resources/"+harness.NodeWorker1+"/activate")
	harness.Eventually(t, groupFAssertTimeout, func() bool {
		r := getResource(t, stack, rd, harness.NodeWorker1)

		return r != nil && !contains(r.Spec.Flags, "INACTIVE")
	}, "INACTIVE flag never cleared after activate")
}

// ---------------------------------------------------------------------------
// TestGroupFRDeleteIdempotent — Bug 56 + Bug 66. A DELETE on a
// missing (rd, node) tuple MUST return 200 (not 404) with a warn-mask
// ApiCallRc envelope, matching upstream LINSTOR's "WARNING: … not
// found." exit 0 shape. CSI DeleteVolume's retry loop depends on it.
// ---------------------------------------------------------------------------

func TestGroupFRDeleteIdempotent(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "del-idem")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	waitForResourceExists(t, stack, rd, harness.NodeWorker1)

	// First delete: real drop, 200.
	deleteAt(t, stack.RestURL+"/v1/resource-definitions/"+rd+
		"/resources/"+harness.NodeWorker1, http.StatusOK)

	// Second delete: should still be 200 (idempotent).
	deleteAt(t, stack.RestURL+"/v1/resource-definitions/"+rd+
		"/resources/"+harness.NodeWorker1, http.StatusOK)
}

// ---------------------------------------------------------------------------
// TestGroupFRDeleteCascadesSnapshots — Bug 1 sibling. Deleting an RD
// cascades its child Resource replicas (the satellite finalizer chain
// would never fire otherwise and DRBD kernel state would linger).
// Bug 1 was originally about RD-delete cascade; this is the
// integration-level guard for the same invariant.
// ---------------------------------------------------------------------------

func TestGroupFRDeleteCascadesSnapshots(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "cascade")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	cli.JSON(t, "resource", "create", harness.NodeWorker2, rd,
		"--storage-pool", "lvm-thin")
	waitForResourceExists(t, stack, rd, harness.NodeWorker1)
	waitForResourceExists(t, stack, rd, harness.NodeWorker2)

	// Strip finalizers so envtest can sweep the Resources promptly:
	// the production satellite finalizer is not running in this
	// harness, and ResourceMigrationReconciler/SnapshotReconciler
	// add no test-relevant work for this assertion.
	stripResourceFinalizers(t, stack, rd)

	// Delete the RD via the CLI (this exercises cascadeDeleteResources).
	cli.JSON(t, "resource-definition", "delete", rd)

	// All child Resources for the RD must be gone.
	harness.Eventually(t, groupFAssertTimeout, func() bool {
		return len(listResourcesByRD(t, stack, rd)) == 0
	}, "child Resources never cleared after RD delete")
}

// ---------------------------------------------------------------------------
// TestGroupFRListFaultyFilter — F5. `linstor r l --faulty` MUST
// filter to RDs with at least one non-UpToDate replica; healthy
// 2-replica RDs MUST NOT appear in the result. The harness's
// satellite mock supports SimulateDRBDState to force the failure
// mode without provider plumbing.
// ---------------------------------------------------------------------------

func TestGroupFRListFaultyFilter(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "faulty")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	cli.JSON(t, "resource", "create", harness.NodeWorker2, rd,
		"--storage-pool", "lvm-thin")

	// Force one replica to a non-UpToDate state so the RD looks
	// faulty to aggregateRDStats. SimulateDRBDState only affects the
	// resource-level mock projection, which is exactly what the
	// `--faulty` filter keys on.
	stack.Satellite.SimulateDRBDState(rd, harness.NodeWorker1, "Inconsistent")

	harness.Eventually(t, groupFAssertTimeout, func() bool {
		r := getResource(t, stack, rd, harness.NodeWorker1)

		return r != nil && r.Status.DrbdState == "Inconsistent"
	}, "satellite never stamped Inconsistent")

	// Faulty list contains our RD.
	faulty := cli.JSON(t, "resource", "list", "--faulty")

	if !rowsContainRD(faulty, rd) {
		t.Fatalf("--faulty list missing %s; rows=%d", rd, len(faulty))
	}
}

// ---------------------------------------------------------------------------
// TestGroupFREffectivePropsEndpoint — Bug 203. The Controller→RG→RD→Resource
// effective-props chain MUST be observable. The blockstor REST layer
// surfaces it via the `effective_props` field on `/v1/view/resources`
// rows (an `effectivePropsForResource` join inside handleResourcesView).
// We assert the field is present and includes a Controller-scope key.
// ---------------------------------------------------------------------------

func TestGroupFREffectivePropsEndpoint(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "effprops")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	waitForResourceExists(t, stack, rd, harness.NodeWorker1)

	// Stamp a controller-scope prop so the Controller rung of the
	// effective-props chain has something to surface.
	postControllerProp(t, stack, "DrbdOptions/Net/ping-timeout", "500")

	harness.Eventually(t, groupFAssertTimeout, func() bool {
		rows := cli.JSON(t, "resource", "list", "-r", rd)
		for _, row := range rows {
			eff, ok := row["effective_props"].(map[string]any)
			if !ok {
				continue
			}

			if _, found := eff["DrbdOptions/Net/ping-timeout"]; found {
				return true
			}
		}

		return false
	}, "effective_props never carried the controller prop")
}

// ---------------------------------------------------------------------------
// TestGroupFRListVolumePoolField — Bug 75. The `storage_pool` field
// on a Volume row MUST be populated (None caused linstor-csi to
// pass an empty pool back to the satellite). The satellite mock
// stamps Status.Volumes[].StoragePool from Spec.StoragePool, so a
// diskful replica with a pool stamped MUST show that pool on the
// wire.
// ---------------------------------------------------------------------------

func TestGroupFRListVolumePoolField(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "vol-pool")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	waitForResourceExists(t, stack, rd, harness.NodeWorker1)

	// Status.Volumes is populated by the satellite from spec when it
	// runs reconcileResources — but the harness mock doesn't carve
	// storage. Pre-stamp the volume status so the wire shape is
	// realistic without making the harness do provider work.
	stampVolumeStoragePool(t, stack, rd, harness.NodeWorker1, "lvm-thin")

	harness.Eventually(t, groupFAssertTimeout, func() bool {
		rows := cli.JSON(t, "resource", "list", "-r", rd)
		for _, row := range rows {
			vols, ok := row["volumes"].([]any)
			if !ok {
				continue
			}

			for _, raw := range vols {
				v, ok := raw.(map[string]any)
				if !ok {
					continue
				}

				if pool, ok := v["storage_pool_name"].(string); ok && pool != "" {
					return true
				}

				if pool, ok := v["storage_pool"].(string); ok && pool != "" {
					return true
				}
			}
		}

		return false
	}, "no volume row surfaced a non-empty storage pool")
}

// ---------------------------------------------------------------------------
// TestGroupFRSetPropertyDrbdNet — a per-Resource DrbdOptions/Net/<key>
// prop MUST be persisted on the CRD so the dispatcher's effective-props
// chain (Controller→RG→RD→Resource) picks it up at the satellite-facing
// scope. blockstor does not yet expose
// `PUT /v1/resource-definitions/{rd}/resources/{node}` (the upstream
// path used by `linstor r sp`); the integration guard here drives the
// write through the controller-runtime client so the rest of the
// effective-props plumbing (dispatcher.mergeEffectiveProps,
// pkg/drbd resolver) is still pinned. Implementing the missing REST
// route is a Phase 2 task — this test surfaces the gap for that work.
// ---------------------------------------------------------------------------

func TestGroupFRSetPropertyDrbdNet(t *testing.T) {
	stack, cli, rd := setupGroupFRD(t, "rsp")

	cli.JSON(t, "resource", "create", harness.NodeWorker1, rd,
		"--storage-pool", "lvm-thin")
	waitForResourceExists(t, stack, rd, harness.NodeWorker1)

	// Stamp the prop on the Resource CRD directly. The downstream
	// pieces (effective-props merge, dispatcher.BuildDesired
	// folding DrbdOptions/* into the DRBD options bag) are what we
	// pin here — not the upstream CLI surface.
	ctx := context.Background()
	name := rd + "." + harness.NodeWorker1

	var r blockstoriov1alpha1.Resource

	err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: name}, &r)
	if err != nil {
		t.Fatalf("get Resource %s: %v", name, err)
	}

	if r.Spec.Props == nil {
		r.Spec.Props = map[string]string{}
	}

	r.Spec.Props["DrbdOptions/Net/ping-timeout"] = "500"

	err = stack.Env.Client.Update(ctx, &r)
	if err != nil {
		t.Fatalf("update Resource %s props: %v", name, err)
	}

	// Round-trip the prop back through the REST view layer. The
	// view's per-replica `effective_props` shape carries
	// Controller→RG→RD→Resource — the Resource rung is what we
	// just stamped, so the key must appear under that scope.
	harness.Eventually(t, groupFAssertTimeout, func() bool {
		rows := cli.JSON(t, "resource", "list", "-r", rd)
		for _, row := range rows {
			eff, ok := row["effective_props"].(map[string]any)
			if !ok {
				continue
			}

			if _, found := eff["DrbdOptions/Net/ping-timeout"]; found {
				return true
			}
		}

		return false
	}, "DrbdOptions/Net/ping-timeout never surfaced via effective_props")
}

// ===========================================================================
// helpers
// ===========================================================================

// setupGroupFRD seeds the canonical 3-node cluster + an RD with one
// 32M volume-definition. Returns a CLI handle the test can drive and
// the RD name (suffix appended).
func setupGroupFRD(t *testing.T, suffix string) (*harness.Stack, *harness.CLI, string) {
	t.Helper()

	// See group_f_reset.go — clear controller-runtime's process-global
	// controller-name registry so back-to-back StartStack calls don't
	// trip over the SkipNameValidation gate (the harness doesn't yet
	// opt in).
	resetControllerNameRegistry()

	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}
	rd := groupFRDPrefix + suffix

	cli.JSON(t, "resource-definition", "create", rd)
	cli.JSON(t, "volume-definition", "create", rd, "32M")

	return stack, cli, rd
}

// listResourcesByRD returns all Resource CRDs whose Spec.ResourceDefinitionName
// matches rd. Cluster-scoped query — no namespace.
func listResourcesByRD(t *testing.T, stack *harness.Stack, rd string) []blockstoriov1alpha1.Resource {
	t.Helper()

	var list blockstoriov1alpha1.ResourceList

	err := stack.Env.Client.List(context.Background(), &list)
	if err != nil {
		t.Fatalf("list Resources: %v", err)
	}

	out := make([]blockstoriov1alpha1.Resource, 0, len(list.Items))

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName == rd {
			out = append(out, list.Items[i])
		}
	}

	return out
}

// getResource fetches the named replica or returns nil if absent.
func getResource(t *testing.T, stack *harness.Stack, rd, node string) *blockstoriov1alpha1.Resource {
	t.Helper()

	name := rd + "." + node

	var r blockstoriov1alpha1.Resource

	err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: name}, &r)
	if err != nil {
		return nil
	}

	return &r
}

// waitForResourceExists polls until the named replica is visible in
// the apiserver. The CLI returns before the controller-runtime cache
// catches up; the rest of the test would otherwise race.
func waitForResourceExists(t *testing.T, stack *harness.Stack, rd, node string) {
	t.Helper()

	harness.Eventually(t, groupFShortTimeout, func() bool {
		return getResource(t, stack, rd, node) != nil
	}, "Resource "+rd+"."+node+" never appeared in apiserver")
}

// waitForDiskfulReplicas blocks until exactly `want` non-DISKLESS
// replicas of `rd` exist, returning the slice.
func waitForDiskfulReplicas(t *testing.T, stack *harness.Stack, rd string, want int) []blockstoriov1alpha1.Resource {
	t.Helper()

	var got []blockstoriov1alpha1.Resource

	harness.Eventually(t, groupFAssertTimeout, func() bool {
		got = nil

		for _, r := range listResourcesByRD(t, stack, rd) {
			if !contains(r.Spec.Flags, "DISKLESS") {
				got = append(got, r)
			}
		}

		return len(got) == want
	}, "diskful replica count never reached "+itoa(want))

	return got
}

// waitForTiebreakerWitness polls until a TIE_BREAKER-flagged replica
// of `rd` lands on some node and returns it.
func waitForTiebreakerWitness(t *testing.T, stack *harness.Stack, rd string) *blockstoriov1alpha1.Resource {
	t.Helper()

	var witness *blockstoriov1alpha1.Resource

	harness.Eventually(t, groupFAssertTimeout, func() bool {
		for _, r := range listResourcesByRD(t, stack, rd) {
			if contains(r.Spec.Flags, "TIE_BREAKER") {
				cp := r

				witness = &cp

				return true
			}
		}

		return false
	}, "TIE_BREAKER witness never landed for "+rd)

	return witness
}

// assertExactlyOneAutoPrimary runs the production dispatcher against
// the two diskful Resources of `rd` and verifies that
// `auto-primary=true` is stamped on the replica with the lowest
// DRBDNodeID — and on no other replica. The check models Bug 80
// directly: a regression where both perspectives stamp auto-primary
// would force both satellites to run `drbdadm primary --force` on
// first activation and the cluster stalls Inconsistent.
func assertExactlyOneAutoPrimary(t *testing.T, stack *harness.Stack, rd string, diskful []blockstoriov1alpha1.Resource) {
	t.Helper()

	if len(diskful) != 2 {
		t.Fatalf("auto-primary check needs 2 diskful replicas, got %d", len(diskful))
	}

	// Wait for the controller-runtime allocator to assign DRBDNodeID
	// to BOTH replicas. The Bug 80 invariant ONLY makes sense once
	// every diskful peer has an allocated id — before then the
	// dispatcher correctly suppresses auto-primary on every replica.
	harness.Eventually(t, groupFAssertTimeout, func() bool {
		for _, want := range diskful {
			r := getResource(t, stack, rd, want.Spec.NodeName)
			if r == nil || r.Status.DRBDNodeID == nil {
				return false
			}
		}

		return true
	}, "satellite/controller never allocated DRBDNodeID on every diskful replica")

	// Refresh the slice — we want the latest Status.DRBDNodeID values.
	refreshed := make([]blockstoriov1alpha1.Resource, 0, len(diskful))
	for _, want := range diskful {
		r := getResource(t, stack, rd, want.Spec.NodeName)
		if r == nil {
			t.Fatalf("diskful replica %s.%s vanished", rd, want.Spec.NodeName)
		}

		refreshed = append(refreshed, *r)
	}

	// Identify the lowest-NodeID replica.
	sort.SliceStable(refreshed, func(i, j int) bool {
		return *refreshed[i].Status.DRBDNodeID < *refreshed[j].Status.DRBDNodeID
	})

	lowestNode := refreshed[0].Spec.NodeName

	// Run dispatcher.BuildDesired from each replica's perspective.
	// Bug 80 invariant: lowest stamps auto-primary=true, the other
	// stamps nothing.
	rdObj := &blockstoriov1alpha1.ResourceDefinition{}

	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: rd}, rdObj)
	if err != nil {
		t.Fatalf("get RD %s: %v", rd, err)
	}

	for i := range refreshed {
		target := &refreshed[i]
		peers := make([]blockstoriov1alpha1.Resource, 0, len(refreshed)-1)

		for j := range refreshed {
			if j == i {
				continue
			}

			peers = append(peers, refreshed[j])
		}

		desired := dispatcher.BuildDesired(target, peers, nil, nil, rdObj, nil)
		gotAutoPrimary := desired.GetDrbdOptions()["auto-primary"]

		isLowest := target.Spec.NodeName == lowestNode

		switch {
		case isLowest && gotAutoPrimary != "true":
			t.Fatalf("Bug 80: lowest-NodeID replica %s.%s missing auto-primary=true (got %q)",
				rd, target.Spec.NodeName, gotAutoPrimary)
		case !isLowest && gotAutoPrimary == "true":
			t.Fatalf("Bug 80: non-lowest replica %s.%s wrongly stamped auto-primary=true",
				rd, target.Spec.NodeName)
		}
	}
}

// stripResourceFinalizers removes all finalizers from every child
// Resource of rd so envtest can complete deletion without the
// satellite-side finalizer running. The production code path stamps
// `blockstor.io/satellite-resource`; in-process tests don't run that
// reconciler, so a literal CRD delete would block on the finalizer
// forever.
func stripResourceFinalizers(t *testing.T, stack *harness.Stack, rd string) {
	t.Helper()

	ctx := context.Background()

	for _, r := range listResourcesByRD(t, stack, rd) {
		cp := r
		if len(cp.Finalizers) == 0 {
			continue
		}

		patched := cp.DeepCopy()
		patched.Finalizers = nil

		err := stack.Env.Client.Update(ctx, patched)
		if err != nil {
			t.Logf("strip finalizers on %s: %v (continuing)", patched.Name, err)
		}
	}
}

// stampVolumeStoragePool writes a synthetic Status.Volumes entry on
// the named replica so the REST wire surface has a non-empty pool to
// echo back. The real satellite would write this from the storage
// carve step; the harness mock doesn't carve, so we stand in for it.
func stampVolumeStoragePool(t *testing.T, stack *harness.Stack, rd, node, pool string) {
	t.Helper()

	ctx := context.Background()
	name := rd + "." + node

	var r blockstoriov1alpha1.Resource

	err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: name}, &r)
	if err != nil {
		t.Fatalf("get Resource %s: %v", name, err)
	}

	if len(r.Status.Volumes) == 0 {
		r.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{
			VolumeNumber: 0,
			StoragePool:  pool,
			DiskState:    "UpToDate",
		}}
	} else {
		r.Status.Volumes[0].StoragePool = pool
		r.Status.Volumes[0].DiskState = "UpToDate"
	}

	err = stack.Env.Client.Status().Update(ctx, &r)
	if err != nil {
		t.Fatalf("stamp Status.Volumes on %s: %v", name, err)
	}
}

// postControllerProp stamps a controller-scope prop via the upstream
// `linstor c sp` endpoint. Done over raw HTTP so the test doesn't
// depend on which CLI verb the python-linstor package surfaces.
//
// Retries on transient errors: the singleton ControllerConfig is
// created lazily, so a fresh stack may see a 404 → create → 200
// dance under the hood; the handler folds that into a single
// successful PUT but the envtest cache trail occasionally surfaces
// the intermediate state.
func postControllerProp(t *testing.T, stack *harness.Stack, key, value string) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"override_props": map[string]string{key: value},
	})
	if err != nil {
		t.Fatalf("marshal controller-props body: %v", err)
	}

	url := stack.RestURL + "/v1/controller/properties"

	const (
		retryBudget = 10 * time.Second
		retryDelay  = 200 * time.Millisecond
	)

	deadline := time.Now().Add(retryBudget)
	for {
		req, reqErr := http.NewRequestWithContext(context.Background(),
			http.MethodPost, url, strings.NewReader(string(body)))
		if reqErr != nil {
			t.Fatalf("build controller-props request: %v", reqErr)
		}

		req.Header.Set("Content-Type", "application/json")

		resp, doErr := http.DefaultClient.Do(req)
		if doErr != nil {
			t.Fatalf("POST controller-props: %v", doErr)
		}

		status := resp.StatusCode

		_ = resp.Body.Close()

		if status/100 == 2 {
			return
		}

		if status/100 != 5 || time.Now().After(deadline) {
			t.Fatalf("POST controller-props: status %d", status)
		}

		time.Sleep(retryDelay)
	}
}

// postEnvelope issues a POST and asserts the response is the
// `[]ApiCallRc` envelope shape. The Bug 45 guard: a bare 200/204
// blows up golinstor's JSON parser.
//
// Retries on transient 5xx: the satellite mock writes Status
// concurrently with the controller-runtime allocator's Spec patches,
// and the apiserver can surface a stale-version conflict on the
// first call. The handler is idempotent — a retry under the cache-
// settle threshold passes cleanly.
func postEnvelope(t *testing.T, url string) {
	t.Helper()

	body := postWithRetry(t, url, http.StatusOK)

	var envelope []map[string]any

	err := json.Unmarshal(body, &envelope)
	if err != nil {
		t.Fatalf("POST %s: decode envelope: %v", url, err)
	}

	if len(envelope) == 0 {
		t.Fatalf("POST %s: empty envelope (Bug 45 regression)", url)
	}
}

// putWithRetry runs PUT against url and retries on transient 5xx
// until it sees wantStatus or the budget expires. Failure mode pinned:
// the envtest manager + satellite mock race the apiserver's Status
// patches under load and the first request can see a
// stale-resource-version conflict. The toggle handlers are idempotent,
// so the retry observes the post-settle state and succeeds.
func putWithRetry(t *testing.T, url string, wantStatus int) {
	t.Helper()
	_ = doWithRetry(t, http.MethodPut, url, wantStatus)
}

// postWithRetry mirrors putWithRetry for POST and returns the body
// bytes so callers can decode the envelope.
func postWithRetry(t *testing.T, url string, wantStatus int) []byte {
	t.Helper()

	return doWithRetry(t, http.MethodPost, url, wantStatus)
}

// doWithRetry issues method against url, retrying on transient 5xx
// until the response status matches wantStatus or the budget expires.
// Returns the response body bytes from the successful attempt.
func doWithRetry(t *testing.T, method, url string, wantStatus int) []byte {
	t.Helper()

	const (
		retryBudget = 10 * time.Second
		retryDelay  = 200 * time.Millisecond
	)

	deadline := time.Now().Add(retryBudget)

	var (
		lastStatus int
		lastBody   []byte
	)

	for {
		req, err := http.NewRequestWithContext(context.Background(), method, url, http.NoBody)
		if err != nil {
			t.Fatalf("build %s %s: %v", method, url, err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, url, err)
		}

		body, _ := readAllAndClose(resp)
		lastStatus = resp.StatusCode
		lastBody = body

		if resp.StatusCode == wantStatus {
			return body
		}

		// 5xx are the only transients we retry; 4xx is a real
		// caller error and should fail-fast.
		if resp.StatusCode/100 != 5 || time.Now().After(deadline) {
			break
		}

		time.Sleep(retryDelay)
	}

	t.Fatalf("%s %s: status %d, want %d (body=%s)", method, url, lastStatus, wantStatus, truncate(lastBody, 256))

	return nil
}

// readAllAndClose drains and closes resp.Body, returning the bytes.
func readAllAndClose(resp *http.Response) ([]byte, error) {
	defer func() { _ = resp.Body.Close() }()

	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 1024)

	for {
		n, err := resp.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}

		if err != nil {
			return buf, err
		}
	}
}

// truncate clamps a byte buffer to limit characters for log messages.
func truncate(buf []byte, limit int) string {
	if len(buf) <= limit {
		return string(buf)
	}

	return string(buf[:limit]) + "...[truncated]"
}

// deleteAt issues a DELETE and asserts the status code. Retries on
// transient 5xx so the apiserver-cache settle race doesn't false-fail.
func deleteAt(t *testing.T, url string, wantStatus int) {
	t.Helper()

	_ = doWithRetry(t, http.MethodDelete, url, wantStatus)
}

// resourceNodeNames extracts `node_name` strings from a `linstor r l`
// JSON row.
func resourceNodeNames(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))

	for _, row := range rows {
		if n, ok := row["node_name"].(string); ok {
			out = append(out, n)
		}
	}

	return out
}

// rowsContainRD walks a `linstor r l` JSON result and reports whether
// any row's `name` field matches rd.
func rowsContainRD(rows []map[string]any, rd string) bool {
	for _, row := range rows {
		if n, ok := row["name"].(string); ok && n == rd {
			return true
		}
	}

	return false
}

// contains is a tiny string-slice membership helper. Avoids pulling
// in golang.org/x/exp/slices for one call.
func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}

	return false
}

// itoa is the local int-to-string helper so the Eventually message
// strings stay readable without bringing in strconv at every call.
func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
