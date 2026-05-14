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

package integration

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// Group E pins LINSTOR VolumeDefinition (VD) contracts surfaced by
// the upstream `linstor` CLI. One top-level test per row of the
// docs/test-strategy.md Group E table:
//
//	TestGroupEVDCreateListDelete         — basic CRUD round-trip
//	TestGroupEVDModifyGrowSize           — Bug 36 size-modify merge
//	TestGroupEVDModifyMergeProps         — Bug 36/37 props merge
//	TestGroupEVDLateAddTriggersReconcile — **Bug 79** late-VD regression guard
//	TestGroupEVDCreateWithoutRD          — wire-shape: 404 on missing RD
//	TestGroupEVDSetProperty              — `linstor vd sp <rd> <vn> k v`
//	TestGroupEVDFreeSpaceFromBackingPool — Bug 35 placer capacity gate
//
// Each test boots a fresh harness.StartStack so failures isolate
// cleanly. The Phase 0+ harness sets controller-runtime's
// SkipNameValidation, so multiple StartStack calls in one `go test`
// binary register their reconcilers without colliding in the
// process-global controller-name set.

const (
	// vdEventuallyBudget caps the wait for VDs/Resources to settle
	// after a CLI mutation. Mirrors the kubebuilder Eventually
	// default — short enough that a hung reconciler is caught
	// quickly, long enough that envtest's apiserver retry loop
	// (~3-5s in slow CI) clears before we give up.
	vdEventuallyBudget = 20 * time.Second

	// vdReconcileSettleWindow is how long we let the satellite mock
	// project Status onto freshly-created Resources before asserting
	// that the project is stable (no late DISKLESS stamp). 1s is
	// roughly 5 satellite ticks (200ms each); enough to surface a
	// late mutation, short enough to keep the test under 30s.
	vdReconcileSettleWindow = time.Second
)

// TestGroupEVDCreateListDelete pins the basic VD CRUD wire shape:
// `linstor vd c <rd> <size>` lands a VD, `linstor vd l` enumerates
// it, `linstor vd d <rd> <vn>` removes it. Regression guard for the
// envelope shape — a Python-CLI traceback would fail Run() via the
// shared client-compat patterns.
func TestGroupEVDCreateListDelete(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const rdName = "vd-crud"

	cli.Run(t, "resource-definition", "create", rdName)

	cli.Run(t, "volume-definition", "create", rdName, "10M")

	// `vd l` returns a list of RDs each carrying inline
	// volume_definitions; flatten and assert one VD on our RD.
	rows := cli.JSON(t, "volume-definition", "list")

	found := vdSizeKibForRD(rows, rdName)
	if found == 0 {
		t.Fatalf("VD on %q not found in `vd l`: %+v", rdName, rows)
	}

	// 10 MiB ~= 10240 KiB. The CLI rounds up to the nearest KiB;
	// any non-zero size on our RD is enough — exact byte-shape is
	// pinned by pkg/rest unit tests.
	if found < 10000 {
		t.Errorf("VD size_kib too small: got %d, want >= 10240 (10 MiB)", found)
	}

	cli.Run(t, "volume-definition", "delete", rdName, "0")

	rows = cli.JSON(t, "volume-definition", "list")
	if got := vdSizeKibForRD(rows, rdName); got != 0 {
		t.Errorf("VD still present after delete: %+v", rows)
	}
}

// TestGroupEVDModifyGrowSize pins Bug 36: `linstor vd set-size`
// growing 10M → 20M must update the stored SizeKib AND preserve
// existing Props. The csi-resizer's ControllerExpandVolume hot
// path is exactly this — a regression that zeroed Props on a
// size-only PUT (the pre-Bug-36 wholesale-decode behaviour) would
// silently drop DrbdOptions/Net/protocol and similar settings.
func TestGroupEVDModifyGrowSize(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const rdName = "vd-grow"

	cli.Run(t, "resource-definition", "create", rdName)
	cli.Run(t, "volume-definition", "create", rdName, "10M")
	cli.Run(t, "volume-definition", "set-property", rdName, "0", "keep-me", "stay")

	// Grow 10M → 20M.
	cli.Run(t, "volume-definition", "set-size", rdName, "0", "20M")

	rows := cli.JSON(t, "volume-definition", "list")

	size := vdSizeKibForRD(rows, rdName)
	if size < 20000 {
		t.Errorf("VD size after grow: got %d KiB, want >= 20480 (20 MiB)", size)
	}

	// Props merge: keep-me must survive the size-only modify (Bug
	// 36 — the pre-fix wholesale Decode + Update collapsed Props
	// to nil whenever the body omitted the key).
	props := vdPropsForRD(rows, rdName)
	if got := props["keep-me"]; got != "stay" {
		t.Errorf("Bug 36 regression: pre-existing prop dropped on size modify; got props=%v", props)
	}
}

// TestGroupEVDModifyMergeProps pins Bug 36 / 37: a props-only
// modify (`linstor vd set-property`) must NOT zero the stored
// SizeKib. The pre-fix wholesale Decode(&VolumeDefinition)
// collapsed size_kib to 0 whenever the body omitted it; the next
// legitimate grow then no-op'd because UsableKib > 0 >= new
// SizeKib.
func TestGroupEVDModifyMergeProps(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const rdName = "vd-props"

	cli.Run(t, "resource-definition", "create", rdName)
	cli.Run(t, "volume-definition", "create", rdName, "10M")

	// Pre-stamp two props so the merge has something to preserve.
	cli.Run(t, "volume-definition", "set-property", rdName, "0", "old-key", "old-value")
	cli.Run(t, "volume-definition", "set-property", rdName, "0", "another-key", "another")

	// Props-only modify: add a new key. Size must be preserved.
	cli.Run(t, "volume-definition", "set-property", rdName, "0", "new-key", "new-value")

	rows := cli.JSON(t, "volume-definition", "list")

	size := vdSizeKibForRD(rows, rdName)
	if size < 10000 {
		t.Errorf("Bug 36/37 regression: size dropped to %d on props-only modify (want >= 10240)", size)
	}

	props := vdPropsForRD(rows, rdName)

	for _, want := range []struct{ key, val string }{
		{"old-key", "old-value"},
		{"another-key", "another"},
		{"new-key", "new-value"},
	} {
		if got := props[want.key]; got != want.val {
			t.Errorf("Bug 37 regression: prop %q = %q, want %q (full props=%v)",
				want.key, got, want.val, props)
		}
	}
}

// TestGroupEVDLateAddTriggersReconcile is the load-bearing test of
// Group E: the regression guard for **Bug 79**. Production sequence
// (operator hit this 2026-05-14):
//
//  1. `linstor rd c <name>` — RD created, no VolumeDefinitions yet.
//  2. `linstor r c <node1> <name> --storage-pool <sp>` — Resource 1.
//  3. `linstor r c <node2> <name> --storage-pool <sp>` — Resource 2.
//
// Without the Bug 79 fix in pkg/satellite/reconciler.go::applyDRBD,
// the satellite would have written the .md-created marker on the
// empty-volume first pass, pinning firstActivation=false forever; a
// late `linstor vd c <name> <size>` (step 4) would then come up
// with no DRBD metadata and the kernel reports disk:Diskless while
// Spec.Flags lacks DISKLESS — "Unintentional Diskless" in `linstor
// r l`.
//
// What this Tier 2 test guards:
//
//   - Steps 1-3 do not mutate Spec.Flags of the two original
//     Resources to add DISKLESS (the visible CRD-level evidence of
//     Bug 79 — a controller / reconciler regression that flipped
//     flags during a no-VD apply pass).
//   - Step 4 (the late VD add) results in both Resources reaching
//     Status.DrbdState = "UpToDate", NOT "Diskless".
//   - After step 4, Spec.Flags STILL lacks DISKLESS on either of
//     the two diskful replicas.
//
// The kernel-level (DRBD-metadata-present) half of Bug 79 lives in
// Tier 4 (real DRBD on Talos VMs); Tier 2 covers the contract /
// reconciler shape — which is where the regression would actually
// re-land if the applyDRBD fix were reverted.
func TestGroupEVDLateAddTriggersReconcile(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const (
		rdName = "vd-late"
		node1  = harness.NodeWorker1
		node2  = harness.NodeWorker2
		pool   = "lvm-thin"
	)

	// Step 1: RD with no VolumeDefinitions.
	cli.Run(t, "resource-definition", "create", rdName)

	// Steps 2-3: two diskful Resources before any VD exists. The
	// `--storage-pool=<pool>` form (equals-sign) keeps argparse from
	// consuming the trailing positional args (node + rd) into the
	// option's nargs='+' list.
	cli.Run(t, "resource", "create", "--storage-pool="+pool, node1, rdName)
	cli.Run(t, "resource", "create", "--storage-pool="+pool, node2, rdName)

	// Give the satellite mock several ticks to project state. We
	// intentionally allow a settle window — the bug pattern was a
	// LATE mutation (the controller stamping DISKLESS on a no-VD
	// reconcile pass). A snapshot taken immediately would miss
	// that.
	time.Sleep(vdReconcileSettleWindow)

	// Pre-VD invariant: neither original Resource has DISKLESS on
	// Spec.Flags. The Bug 79 reconciler regression would have
	// stamped DISKLESS during the empty-volume reconcile pass —
	// this assertion catches that exact flavour of regression
	// even before the VD is added.
	for _, node := range []string{node1, node2} {
		flags := mustGetResourceFlags(t, stack, rdName, node)
		if slices.Contains(flags, "DISKLESS") {
			t.Fatalf("Bug 79 regression: Resource %s.%s has DISKLESS on Spec.Flags "+
				"BEFORE late-VD add (flags=%v); the reconciler should leave "+
				"Spec.Flags alone when the RD has no VolumeDefinitions",
				rdName, node, flags)
		}
	}

	// Step 4: the late-VD add. Production trigger for Bug 79's
	// surfacing — the moment the operator finally creates the
	// volume, the satellite must run create-md against the now-
	// present backing storage and bring DRBD up disk:UpToDate, NOT
	// disk:Diskless.
	cli.Run(t, "volume-definition", "create", rdName, "10M")

	// Both Resources must reach Status.DrbdState == "UpToDate"
	// after the late-VD add. The mock satellite stamps UpToDate on
	// the next tick; this Eventually keeps the test deterministic
	// without a hard sleep.
	for _, node := range []string{node1, node2} {
		harness.WaitForDRBDState(t, stack, rdName, node, "UpToDate")
	}

	// Critical Bug 79 invariant: after the late VD lands, the two
	// original diskful Resources must NOT carry DISKLESS on either
	// Spec.Flags OR Status.DrbdState. A "200 OK" wire result with
	// a Diskless-tagged replica is the exact production failure
	// mode.
	for _, node := range []string{node1, node2} {
		flags := mustGetResourceFlags(t, stack, rdName, node)
		if slices.Contains(flags, "DISKLESS") {
			t.Errorf("Bug 79 regression: Resource %s.%s ends up DISKLESS in "+
				"Spec.Flags after late-VD add (flags=%v); diskful replicas "+
				"created BEFORE the VD must remain diskful, never silently "+
				"demoted to DISKLESS by the late-VD reconcile pass",
				rdName, node, flags)
		}

		state := mustGetResourceDrbdState(t, stack, rdName, node)
		if state == "Diskless" {
			t.Errorf("Bug 79 regression: Resource %s.%s Status.DrbdState=Diskless "+
				"after late-VD add (want UpToDate); the late VD must trigger "+
				"create-md on the now-present backing storage, not leave the "+
				"kernel reporting Unintentional Diskless",
				rdName, node)
		}
	}
}

// TestGroupEVDCreateWithoutRD pins the wire-shape contract:
// `linstor vd c <missing-rd> <size>` returns a clean error envelope
// (negative ret_code with MASK_ERROR bit), NOT a Python traceback.
// The upstream CLI prints the JSON envelope to stdout and exits 0 in
// --machine-readable mode regardless of envelope-level error masks
// — parse the envelope and assert the error shape directly via the
// harness CLI.JSON helper.
func TestGroupEVDCreateWithoutRD(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	rows := cli.JSON(t, "volume-definition", "create", "ghost-rd", "10M")

	if len(rows) == 0 {
		t.Fatalf("vd-create on missing rd: empty envelope; want one ApiCallRc with error mask")
	}

	// retCode is a float64 after JSON unmarshal — MASK_ERROR
	// (0x4000000000000000, i.e. the high bit) renders as a large
	// negative float. Any negative ret_code is an error in
	// LINSTOR's wire shape (see pkg/api/v1.maskError); pin that.
	retCode, _ := rows[0]["ret_code"].(float64)
	if retCode >= 0 {
		t.Errorf("vd-create on missing rd: ret_code = %v, want negative (MASK_ERROR bit)", retCode)
	}

	// Message must carry an operator-actionable marker. Different
	// codepaths render slightly different text; accept any of the
	// canonical shapes so a stable refactor of the error string
	// doesn't break the regression guard.
	msg, _ := rows[0]["message"].(string)
	lower := strings.ToLower(msg)

	for _, ok := range []string{"not found", "does not exist", "ghost-rd"} {
		if strings.Contains(lower, ok) {
			return
		}
	}

	t.Errorf("vd-create on missing rd: message %q lacks any 'not found' marker", msg)
}

// TestGroupEVDSetProperty pins `linstor vd set-property <rd> <vn>
// <key> <value>` → the CRD's VD.Props map is updated. Driven by
// linstor-csi's CreateVolume property hand-off (StoragePoolName,
// VolumeBlockSize, etc.).
func TestGroupEVDSetProperty(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	const (
		rdName = "vd-sp"
		key    = "DrbdOptions/Net/protocol"
		val    = "C"
	)

	cli.Run(t, "resource-definition", "create", rdName)
	cli.Run(t, "volume-definition", "create", rdName, "10M")
	cli.Run(t, "volume-definition", "set-property", rdName, "0", key, val)

	rows := cli.JSON(t, "volume-definition", "list")

	props := vdPropsForRD(rows, rdName)
	if got := props[key]; got != val {
		t.Errorf("VD prop %q: got %q, want %q (full props=%v)", key, got, val, props)
	}
}

// TestGroupEVDFreeSpaceFromBackingPool pins Bug 35: when every
// candidate StoragePool reports a FreeCapacity floor below the
// requested VD size, autoplace must fail-fast with a capacity-
// shortfall envelope — NOT a 200 OK that the satellite then has to
// surface as an opaque downstream failure.
//
// We simulate the over-subscribed cluster by stamping a tiny
// FreeCapacity + TotalCapacity onto every fixture pool, then asking
// the placer to land a VD orders of magnitude larger. The placer's
// Bug-35 floor must reject every pool and the REST handler must
// surface a 409 / 4xx with operator-actionable text.
func TestGroupEVDFreeSpaceFromBackingPool(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	cli := &harness.CLI{URL: stack.RestURL}

	// Stamp tiny capacity onto every fixture pool. desiredPoolStatus
	// (harness/satellite.go) preserves a non-zero TotalCapacity, so
	// once we stamp 128 KiB the satellite mock will NOT reset to the
	// 10-GiB default on its next tick.
	const tinyKib int64 = 128 // 128 KiB

	stampTinyPoolCapacity(t, stack, tinyKib)

	const rdName = "vd-shortfall"

	cli.Run(t, "resource-definition", "create", rdName)

	// 1 GiB VD against pools that have 128 KiB free — well above
	// the placer's hard floor. The VD itself lands (no capacity
	// check on the VD POST), but the subsequent autoplace must
	// fail.
	cli.Run(t, "volume-definition", "create", rdName, "1G")

	// Autoplace request: ask for 2 replicas. The placer's Bug-35
	// floor walks every pool, sees FreeCapacity = 128 KiB <
	// requiredKib = 1 GiB, and surfaces CapacityShortfallError ->
	// REST envelope with an error ret_code and an actionable
	// message naming the shortfall.
	//
	// Like vd-create-without-rd, the Python CLI exits 0 in
	// --machine-readable mode but stamps the error mask on the
	// returned envelope. Parse via cli.JSON and inspect ret_code +
	// message directly.
	rows := cli.JSON(t, "resource", "create", "--auto-place", "2", rdName)
	if len(rows) == 0 {
		t.Fatalf("Bug 35 regression: autoplace envelope empty (want error with shortfall)")
	}

	retCode, _ := rows[0]["ret_code"].(float64)
	if retCode >= 0 {
		t.Errorf("Bug 35 regression: autoplace ret_code = %v, want negative (MASK_ERROR)",
			retCode)
	}

	// CapacityShortfallError renders as either the placer's own
	// "not enough free capacity" prefix OR upstream LINSTOR's
	// "Not enough available nodes" wrapper, with the placer's
	// numeric breakdown ("Capacity shortfall: required N KiB,
	// max free M KiB") attached. Accept any of the canonical
	// shapes so a stable refactor of the wrapping text doesn't
	// break the regression guard — but require at least the
	// "capacity" marker so we know the placer's gate actually
	// fired (not an unrelated topology rejection).
	combined := strings.ToLower(fmt.Sprintf("%v", rows[0]))

	if !strings.Contains(combined, "capacity") {
		t.Errorf("Bug 35 regression: autoplace envelope lacks 'capacity' marker; got %v",
			rows[0])

		return
	}

	for _, marker := range []string{
		"not enough free capacity",
		"capacity shortfall",
		"free capacity",
		"available nodes",
	} {
		if strings.Contains(combined, marker) {
			return
		}
	}

	t.Errorf("Bug 35 regression: autoplace did NOT surface a capacity-shortfall "+
		"message; got envelope %v", rows[0])
}

// ---------- helpers (file-local; nothing leaks to other groups) ----------

// vdSizeKibForRD walks the `linstor vd l` response (an array of RDs
// each carrying inline volume_definitions) and returns the size_kib
// of volume 0 on rdName. Returns 0 when not found.
func vdSizeKibForRD(rows []map[string]any, rdName string) int64 {
	for _, row := range rows {
		if name, _ := row["name"].(string); name != rdName {
			continue
		}

		vds, _ := row["volume_definitions"].([]any)
		for _, raw := range vds {
			vd, _ := raw.(map[string]any)
			// JSON unmarshal of int64 → float64; defensive.
			if size, ok := vd["size_kib"].(float64); ok {
				return int64(size)
			}
		}
	}

	return 0
}

// vdPropsForRD extracts the props map of volume 0 on rdName.
// Returns an empty (non-nil) map when not found so callers can do
// `props[key]` without a nil-check.
func vdPropsForRD(rows []map[string]any, rdName string) map[string]string {
	out := map[string]string{}

	for _, row := range rows {
		if name, _ := row["name"].(string); name != rdName {
			continue
		}

		vds, _ := row["volume_definitions"].([]any)
		for _, raw := range vds {
			vd, _ := raw.(map[string]any)

			props, _ := vd["props"].(map[string]any)
			for k, v := range props {
				if s, ok := v.(string); ok {
					out[k] = s
				}
			}

			return out
		}
	}

	return out
}

// mustGetResourceFlags reads Resource.<rd>.<node>.spec.flags via the
// envtest typed client. Fails the test if the Resource is missing.
func mustGetResourceFlags(t *testing.T, stack *harness.Stack, rd, node string) []string {
	t.Helper()

	var r blockstoriov1alpha1.Resource

	name := rd + "." + node

	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: name}, &r)
	if err != nil {
		t.Fatalf("Get Resource %s: %v", name, err)
	}

	return r.Spec.Flags
}

// mustGetResourceDrbdState reads Resource.Status.DrbdState. Empty
// string means "not yet projected by the satellite". The Eventually
// in WaitForDRBDState gates the meaningful read; this helper is for
// the post-Eventually invariant check.
func mustGetResourceDrbdState(t *testing.T, stack *harness.Stack, rd, node string) string {
	t.Helper()

	var r blockstoriov1alpha1.Resource

	name := rd + "." + node

	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: name}, &r)
	if err != nil {
		t.Fatalf("Get Resource %s: %v", name, err)
	}

	return r.Status.DrbdState
}

// stampTinyPoolCapacity writes a tiny FreeCapacity + TotalCapacity
// onto every fixture StoragePool's Status subresource. The
// satellite mock's desiredPoolStatus preserves a non-zero
// TotalCapacity (see harness/satellite.go), so once we stamp this,
// the mock will not reset to the 10 GiB default on the next tick.
//
// We retry the Status().Update on conflict because the satellite
// mock may be writing to the same Status subresource concurrently.
func stampTinyPoolCapacity(t *testing.T, stack *harness.Stack, tinyKib int64) {
	t.Helper()

	ctx := context.Background()

	for _, node := range harness.FixtureNodes() {
		for _, prov := range harness.FixtureProviders() {
			poolName := prov.PoolName + "." + node
			stampOnePool(t, ctx, stack, poolName, tinyKib)
		}
	}

	// Wait for the satellite mock to observe our stamp (it may
	// have raced ahead with a 10-GiB default project). The mock is
	// idempotent on equal-status: once we win the write,
	// subsequent ticks no-op. Poll until the cluster-wide view
	// reflects the tiny cap.
	harness.Eventually(t, vdEventuallyBudget, func() bool {
		var pools blockstoriov1alpha1.StoragePoolList

		err := stack.Env.Client.List(ctx, &pools)
		if err != nil {
			return false
		}

		for i := range pools.Items {
			if pools.Items[i].Status.FreeCapacity != tinyKib {
				return false
			}
		}

		return true
	}, fmt.Sprintf("every pool's Status.FreeCapacity == %d", tinyKib))
}

// stampOnePool retries Status().Update on conflict. Pulled out of
// stampTinyPoolCapacity so the test stays under the funlen budget
// even when the conflict retry loop grows.
func stampOnePool(
	t *testing.T,
	ctx context.Context,
	stack *harness.Stack,
	poolName string,
	tinyKib int64,
) {
	t.Helper()

	const maxAttempts = 10

	for attempt := 0; attempt < maxAttempts; attempt++ {
		var pool blockstoriov1alpha1.StoragePool

		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: poolName}, &pool)
		if err != nil {
			t.Fatalf("get StoragePool %s: %v", poolName, err)
		}

		pool.Status.FreeCapacity = tinyKib
		pool.Status.TotalCapacity = tinyKib

		err = stack.Env.Client.Status().Update(ctx, &pool)
		if err == nil {
			return
		}

		if !apierrors.IsConflict(err) {
			t.Fatalf("status update StoragePool %s: %v", poolName, err)
		}
		// Conflict: re-read and try again.
	}

	t.Fatalf("status update StoragePool %s: gave up after %d conflict retries",
		poolName, maxAttempts)
}

