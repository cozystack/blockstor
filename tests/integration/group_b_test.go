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

// Phase 1, Group B — Storage Pool. Each subtest below pins one row of
// the `docs/test-strategy.md` Group B table. See that file's column 3
// for the bug-guard each subtest is locking down.
//
// Why one parent `TestGroupB` function with subtests instead of nine
// independent top-level Test*: controller-runtime's manager refuses to
// register two controllers with the same name in the same process,
// and the harness wires every reconciler under its canonical
// production name (e.g. "node"). Booting two managers from a single
// `go test` invocation therefore fails the second one with
// "controller with name node already exists" — see the harness's
// buildIntegrationManager which doesn't set Options.SkipNameValidation.
// Until the harness exposes a knob (Phase 0 follow-up), Group B
// shares a single Stack across its 9 subtests; the `^TestGroupB`
// regex in the DoD command still picks them all up via subtest names.
//
// All subtests share one stack but use disjoint pools / RDs / devices
// so they remain logically independent — re-running with -run '^.*X$'
// on any one of them works because the only shared mutation is
// SimulatePoolMissing, which is keyed on (worker-1, lvm-thin) — a
// pool no other subtest mutates.
//
// The tests deliberately drive blockstor through the upstream `linstor`
// CLI binary (via harness.CLI) so the python-linstor parser is the
// final assertion on the wire shape — the same parser that has crashed
// us before with bare 405s, xml.etree fallbacks, etc.
package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// TestGroupB is the umbrella Phase 1 Group B suite. Boots one Stack
// + seeds the canonical 3-node × 3-pool fixture, then runs every
// subtest sequentially against it. See package comment for why the
// nine tests share a single Stack.
func TestGroupB(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	t.Run("SPListAfterFixtures", func(t *testing.T) {
		testSPListAfterFixtures(t, stack)
	})
	t.Run("SPCreatePerProvider", func(t *testing.T) {
		testSPCreatePerProvider(t, stack)
	})
	t.Run("SPDeleteEmpty", func(t *testing.T) {
		testSPDeleteEmpty(t, stack)
	})
	t.Run("SPDeleteRefusesIfInUse", func(t *testing.T) {
		testSPDeleteRefusesIfInUse(t, stack)
	})
	t.Run("SPCapacityFlow", func(t *testing.T) {
		testSPCapacityFlow(t, stack)
	})
	t.Run("SPSetProperty", func(t *testing.T) {
		testSPSetProperty(t, stack)
	})
	t.Run("PhysicalStorageList", func(t *testing.T) {
		testPhysicalStorageList(t, stack)
	})
	t.Run("PhysicalStorageCreateDevicePool", func(t *testing.T) {
		testPhysicalStorageCreateDevicePool(t, stack)
	})
	t.Run("PhysicalStorageCDPPropsPerKind", func(t *testing.T) {
		testPhysicalStorageCDPPropsPerKind(t, stack)
	})

	// SPPoolMissingReportsFaulty mutates the satellite mock's
	// SimulatePoolMissing state on (worker-1, lvm-thin). Kept last
	// so the Faulty state doesn't leak into earlier subtests that
	// read `sp list` and expect every pool to be Ok.
	t.Run("SPPoolMissingReportsFaulty", func(t *testing.T) {
		testSPPoolMissingReportsFaulty(t, stack)
	})
}

// findPool walks the rows returned by `linstor sp list` and returns
// the entry matching (node, pool). Returns nil when no row matches.
func findPool(rows []map[string]any, node, pool string) map[string]any {
	for _, row := range rows {
		if row["node_name"] == node && row["storage_pool_name"] == pool {
			return row
		}
	}

	return nil
}

// testSPListAfterFixtures pins the basic Group B fixture: the
// SeedThreeNodeCluster helper plants 9 pools (3 providers × 3 nodes);
// `linstor sp list` MUST surface all of them. A regression that
// silently dropped pools at the wire boundary (e.g. a per-node label
// filter the harness fixture didn't carry) would show up here.
func testSPListAfterFixtures(t *testing.T, stack *harness.Stack) {
	t.Helper()

	cli := &harness.CLI{URL: stack.RestURL}

	rows := cli.JSON(t, "storage-pool", "list")

	got := 0

	for _, name := range harness.FixtureNodes() {
		for _, prov := range harness.FixtureProviders() {
			if findPool(rows, name, prov.PoolName) != nil {
				got++
			}
		}
	}

	if got != 9 {
		t.Fatalf("fixture pools visible in `sp list`: got %d, want 9 (rows=%d)",
			got, len(rows))
	}
}

// testSPCreatePerProvider is the Bug 63 / 73 guard. The python
// linstor-client's `sp create lvmthin/zfsthin/file/filethin/diskless`
// sub-commands feed the kind through several normalisations
// (lowercase, compressed `lvmthin` / `zfsthin` / `filethin`) before
// they hit our REST POST. The CRD MUST land with the canonical
// uppercase `ProviderKind` so the satellite's NewProviderFromKind
// switch (and every consumer that reads the CRD enum) matches.
func testSPCreatePerProvider(t *testing.T, stack *harness.Stack) {
	t.Helper()

	cli := &harness.CLI{URL: stack.RestURL}

	cases := []struct {
		args         []string // linstor sp c <subcommand> [node, name, drv-pool]
		poolName     string
		wantProvider string // canonical CRD value
	}{
		{
			args:         []string{"storage-pool", "create", "lvmthin", "worker-1", "g-lvm-thin", "vg1/thin1"},
			poolName:     "g-lvm-thin",
			wantProvider: "LVM_THIN",
		},
		{
			args:         []string{"storage-pool", "create", "zfsthin", "worker-1", "g-zfs-thin", "zpool/thin"},
			poolName:     "g-zfs-thin",
			wantProvider: "ZFS_THIN",
		},
		{
			args:         []string{"storage-pool", "create", "file", "worker-1", "g-file", "/srv/g-file"},
			poolName:     "g-file",
			wantProvider: "FILE",
		},
		{
			args:         []string{"storage-pool", "create", "filethin", "worker-1", "g-file-thin", "/srv/g-file-thin"},
			poolName:     "g-file-thin",
			wantProvider: "FILE_THIN",
		},
		{
			args:         []string{"storage-pool", "create", "diskless", "worker-1", "g-diskless"},
			poolName:     "g-diskless",
			wantProvider: "DISKLESS",
		},
	}

	for _, tc := range cases {
		cli.Run(t, tc.args...)

		var pool blockstoriov1alpha1.StoragePool

		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: tc.poolName + ".worker-1"}, &pool)
		if err != nil {
			t.Fatalf("Get StoragePool %q: %v", tc.poolName, err)
		}

		if pool.Spec.ProviderKind != tc.wantProvider {
			t.Errorf("Bug 63/73: pool %q: ProviderKind = %q, want %q",
				tc.poolName, pool.Spec.ProviderKind, tc.wantProvider)
		}
	}

	// Cross-check `sp list` echoes the same canonical token on the
	// wire — the CLI parser keys on this string for filter / display.
	rows := cli.JSON(t, "storage-pool", "list")
	for _, tc := range cases {
		row := findPool(rows, "worker-1", tc.poolName)
		if row == nil {
			t.Errorf("pool %q missing from sp list", tc.poolName)

			continue
		}

		got, _ := row["provider_kind"].(string)
		if got != tc.wantProvider {
			t.Errorf("Bug 63/73: sp list pool %q: provider_kind = %q, want %q",
				tc.poolName, got, tc.wantProvider)
		}
	}
}

// testSPDeleteEmpty pins the basic delete contract: `sp d` on a pool
// with no resources MUST return a 200 envelope (CLI exits 0) and the
// CRD must be removed from the apiserver. Picks the (worker-2, file)
// fixture pool — sized so no other subtest depends on it.
func testSPDeleteEmpty(t *testing.T, stack *harness.Stack) {
	t.Helper()

	cli := &harness.CLI{URL: stack.RestURL}

	cli.Run(t, "storage-pool", "delete", "worker-2", "file")

	var pool blockstoriov1alpha1.StoragePool

	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: "file.worker-2"}, &pool)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("StoragePool file.worker-2 still present after delete: err=%v", err)
	}
}

// testSPDeleteRefusesIfInUse pins Bug 52 / scenario 6.W06: an `sp d`
// on a pool that has a Resource replica referencing it MUST refuse
// with 409 + FAIL_IN_USE rather than cascading the drop into the
// live replica. The pool's CRD stays in place. Uses a per-subtest
// (worker-2, lvm-thin) target so it doesn't interfere with other
// subtests that read the fixture.
func testSPDeleteRefusesIfInUse(t *testing.T, stack *harness.Stack) {
	t.Helper()

	ctx := context.Background()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-using-lvm"},
	}
	if err := stack.Env.Client.Create(ctx, rd); err != nil {
		t.Fatalf("create RD: %v", err)
	}

	// Resource on (worker-2, lvm-thin). The wire-shape referencingResources
	// walker reads Volumes from CRD Status — Spec carries the placement,
	// Status records the observed per-volume binding.
	r := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-using-lvm.worker-2"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "rd-using-lvm",
			NodeName:               "worker-2",
			StoragePool:            "lvm-thin",
		},
	}
	if err := stack.Env.Client.Create(ctx, r); err != nil {
		t.Fatalf("create Resource: %v", err)
	}

	// Plant Status.Volumes via a retry loop — the in-process
	// satellite mock concurrently writes Status.DrbdState on the
	// same Resource (200ms tick), so a naive Get+Update races and
	// drops one of the two writes. Retry on the apiserver's
	// optimistic-lock conflict until both fields land.
	const setStatusBudget = 5 * time.Second

	harness.Eventually(t, setStatusBudget, func() bool {
		var rGot blockstoriov1alpha1.Resource

		if err := stack.Env.Client.Get(ctx,
			types.NamespacedName{Name: "rd-using-lvm.worker-2"}, &rGot); err != nil {
			return false
		}

		// Idempotent: if a previous attempt already set the
		// volume, leave it and return success.
		for i := range rGot.Status.Volumes {
			if rGot.Status.Volumes[i].VolumeNumber == 0 &&
				rGot.Status.Volumes[i].StoragePool == "lvm-thin" {
				return true
			}
		}

		rGot.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
			{VolumeNumber: 0, StoragePool: "lvm-thin"},
		}

		return stack.Env.Client.Status().Update(ctx, &rGot) == nil
	}, "seed Resource Status.Volumes (satellite race)")

	// Drive the delete via HTTP so we can read the 409 status code
	// directly — the CLI converts it to a stderr "Error" but exits
	// non-zero, which the harness CLI.Run would t.Fatal on. The
	// underlying contract this test pins is the REST envelope.
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		stack.RestURL+"/v1/nodes/worker-2/storage-pools/lvm-thin", http.NoBody)
	if err != nil {
		t.Fatalf("build DELETE request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("Bug 52: DELETE status = %d, want 409 Conflict", resp.StatusCode)
	}

	var body []map[string]any

	if derr := json.NewDecoder(resp.Body).Decode(&body); derr != nil {
		t.Fatalf("decode envelope: %v", derr)
	}

	if len(body) == 0 {
		t.Fatalf("Bug 52: empty ApiCallRc envelope on FAIL_IN_USE refusal")
	}

	msg, _ := body[0]["message"].(string)
	if !strings.Contains(msg, "lvm-thin") || !strings.Contains(strings.ToLower(msg), "still using") {
		t.Errorf("Bug 52: refusal message = %q, want it to name the pool + 'still using'", msg)
	}

	// CRD must survive the refused delete.
	var pool blockstoriov1alpha1.StoragePool

	gErr := stack.Env.Client.Get(ctx,
		types.NamespacedName{Name: "lvm-thin.worker-2"}, &pool)
	if gErr != nil {
		t.Errorf("Bug 52: StoragePool removed despite FAIL_IN_USE refusal: %v", gErr)
	}
}

// testSPPoolMissingReportsFaulty pins Bugs 83 + 74. When the satellite
// stamps Status.PoolMissing=true, `linstor sp list` MUST render the
// pool's `state` as "Faulty" rather than the default "Ok" — without
// it operators have no signal that the backing pool was destroyed
// out-of-band (`zpool destroy`, `vgremove`).
//
// The Bug 83 plan also calls for a populated `reports[]` field; the
// current wire shape leaves it omitted on `sp list` (the field is
// declared in apiv1.StoragePool but never written by the k8s store).
// When that lands the assertion below tightens; for now we pin the
// state transition, which is the operator-visible Bug 83 symptom.
func testSPPoolMissingReportsFaulty(t *testing.T, stack *harness.Stack) {
	t.Helper()

	cli := &harness.CLI{URL: stack.RestURL}

	stack.Satellite.SimulatePoolMissing("worker-1", "lvm-thin")

	const waitFaulty = 10 * time.Second

	harness.Eventually(t, waitFaulty, func() bool {
		rows := cli.JSON(t, "storage-pool", "list")
		row := findPool(rows, "worker-1", "lvm-thin")
		if row == nil {
			return false
		}

		state, _ := row["state"].(string)

		return state == "Faulty"
	}, "Bug 83: pool state did not flip to Faulty after PoolMissing=true")
}

// testSPCapacityFlow pins the basic satellite → REST capacity flow.
// The satellite mock stamps a default 10 GiB capacity on every
// fixture pool; the wire view MUST surface it.
func testSPCapacityFlow(t *testing.T, stack *harness.Stack) {
	t.Helper()

	cli := &harness.CLI{URL: stack.RestURL}

	const waitForCapacity = 10 * time.Second

	harness.Eventually(t, waitForCapacity, func() bool {
		rows := cli.JSON(t, "storage-pool", "list")
		row := findPool(rows, "worker-2", "zfs-thin")
		if row == nil {
			return false
		}

		free, _ := row["free_capacity"].(float64)
		total, _ := row["total_capacity"].(float64)

		return free > 0 && total > 0
	}, "satellite-stamped capacity never surfaced on sp list")
}

// testSPSetProperty pins the basic `sp set-property` contract: the
// CLI invocation must reach the controller and the property must
// land in the CRD's Spec.Props. Bug 85 — wires
// `PUT /v1/nodes/{node}/storage-pools/{pool}` (the python CLI's
// `storage_pool_modify` endpoint). Before the fix the route was
// unwired and the CLI exited non-zero with HTTP-Status(405) that
// tripped the python xml.etree fallback. The test drives the
// production path end-to-end via the upstream `linstor` CLI binary
// so the python parser is the final assertion on the wire shape.
func testSPSetProperty(t *testing.T, stack *harness.Stack) {
	t.Helper()

	cli := &harness.CLI{URL: stack.RestURL}

	// Exercise the production path: linstor sp set-property → PUT
	// /v1/nodes/worker-3/storage-pools/lvm-thin with
	// `{override_props: {PrefNic: default}}`. cli.Run aborts the
	// test on any python traceback / xml.etree fallback (the
	// Bug-59-class checks live there).
	cli.Run(t, "storage-pool", "set-property", "worker-3", "lvm-thin",
		"PrefNic", "default")

	// Round-trip via the CRD: the new prop must land in
	// Spec.Props on the canonical `<pool>.<node>` CRD.
	var pool blockstoriov1alpha1.StoragePool

	gErr := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: "lvm-thin.worker-3"}, &pool)
	if gErr != nil {
		t.Fatalf("Get StoragePool: %v", gErr)
	}

	if pool.Spec.Props["PrefNic"] != "default" {
		t.Errorf("Spec.Props[PrefNic] = %q, want %q after sp set-property",
			pool.Spec.Props["PrefNic"], "default")
	}
}

// testPhysicalStorageList pins Bugs 51 / 70 / 72: `ps list` on a
// cluster with zero free PhysicalDevice CRDs MUST return a non-error
// envelope rather than crashing the python parser, and on a cluster
// with a free device it MUST surface that device (Bug 51's "ps l
// returned nothing because the satellite-side discovery loop didn't
// publish"). The zvol / DRBD filtering (Bug 70 / 72) is exercised at
// the satellite-side unit layer; here we keep the wire-shape pin.
func testPhysicalStorageList(t *testing.T, stack *harness.Stack) {
	t.Helper()

	cli := &harness.CLI{URL: stack.RestURL}

	// Zero devices: the python CLI must parse the envelope cleanly.
	// `JSON` already aborts the test on traceback / xml parser
	// failure, so reaching this point is the Bug 51 guard for the
	// "empty cluster" path.
	empty := cli.JSON(t, "physical-storage", "list")
	if empty == nil {
		t.Fatalf("Bug 51: ps list returned a nil slice on zero-device cluster")
	}

	// Now seed an Available PhysicalDevice on worker-2 and assert
	// it surfaces. Status fields carry the wire view, so we Create
	// the empty Spec then patch Status separately (the CRD is
	// declared with subresource=Status).
	const (
		devName   = "worker-2.test-sdb"
		devPath   = "/dev/disk/by-id/scsi-test-sdb"
		devSizeBy = int64(2147483648) // 2 GiB
	)

	dev := &blockstoriov1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name: devName,
			Labels: map[string]string{
				blockstoriov1alpha1.PhysicalDeviceLabelNode: "worker-2",
			},
		},
	}

	ctx := context.Background()
	if err := stack.Env.Client.Create(ctx, dev); err != nil {
		t.Fatalf("create PhysicalDevice: %v", err)
	}

	var got blockstoriov1alpha1.PhysicalDevice

	if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: devName}, &got); err != nil {
		t.Fatalf("get PhysicalDevice: %v", err)
	}

	got.Status = blockstoriov1alpha1.PhysicalDeviceStatus{
		NodeName:   "worker-2",
		StableID:   "scsi-test-sdb",
		DevicePath: devPath,
		SizeBytes:  devSizeBy,
		Phase:      blockstoriov1alpha1.PhysicalDevicePhaseAvailable,
	}
	if err := stack.Env.Client.Status().Update(ctx, &got); err != nil {
		t.Fatalf("status update: %v", err)
	}

	const waitPSL = 10 * time.Second

	harness.Eventually(t, waitPSL, func() bool {
		// `ps list` returns buckets keyed by (size, rotational)
		// with a per-node device map; the JSON helper flattens
		// the outer wrapper. We just search for our DevicePath
		// anywhere in the envelope.
		raw := cli.Run(t, "physical-storage", "list")

		return strings.Contains(string(raw), devPath)
	}, "Bug 51: seeded PhysicalDevice never surfaced on ps list")
}

// testPhysicalStorageCreateDevicePool pins Bug 68 / 73: a
// `linstor ps cdp zfs --pool-name X /dev/Y` invocation MUST reach
// the apiserver, the canonical provider-kind normalisation MUST run
// (Bug 73 — lowercase `zfs` → `ZFS`), and a matching free
// PhysicalDevice MUST flip to `Spec.AttachTo` so the satellite picks
// it up. End-to-end-up-to-SP semantics: the StoragePool CRD also
// lands so the next satellite tick has somewhere to register the
// provider.
func testPhysicalStorageCreateDevicePool(t *testing.T, stack *harness.Stack) {
	t.Helper()

	const (
		devName = "worker-3.cdp-sdc"
		devPath = "/dev/disk/by-id/scsi-cdp-sdc"
		spName  = "cdp-zfs"
	)

	ctx := context.Background()

	dev := &blockstoriov1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name: devName,
			Labels: map[string]string{
				blockstoriov1alpha1.PhysicalDeviceLabelNode: "worker-3",
			},
		},
	}
	if err := stack.Env.Client.Create(ctx, dev); err != nil {
		t.Fatalf("create PhysicalDevice: %v", err)
	}

	var got blockstoriov1alpha1.PhysicalDevice
	if err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: devName}, &got); err != nil {
		t.Fatalf("get PhysicalDevice: %v", err)
	}

	got.Status = blockstoriov1alpha1.PhysicalDeviceStatus{
		NodeName:   "worker-3",
		StableID:   "scsi-cdp-sdc",
		DevicePath: devPath,
		SizeBytes:  4294967296, // 4 GiB
		Phase:      blockstoriov1alpha1.PhysicalDevicePhaseAvailable,
	}
	if err := stack.Env.Client.Status().Update(ctx, &got); err != nil {
		t.Fatalf("status update: %v", err)
	}

	cli := &harness.CLI{URL: stack.RestURL}

	// The Bug 73 provider-kind normalisation runs server-side:
	// the lowercase `zfs` token here must land canonical `ZFS`.
	// `--storage-pool` carries the target StoragePool name;
	// `--pool-name` is the zpool name on disk.
	cli.Run(t,
		"physical-storage", "create-device-pool",
		"zfs", "worker-3", devPath,
		"--pool-name", "myzpool",
		"--storage-pool", spName,
	)

	// 1. PhysicalDevice flipped to AttachTo with canonical
	//    ProviderKind=ZFS.
	const waitAttach = 10 * time.Second

	harness.Eventually(t, waitAttach, func() bool {
		var pd blockstoriov1alpha1.PhysicalDevice

		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: devName}, &pd)
		if err != nil {
			return false
		}

		if pd.Spec.AttachTo == nil {
			return false
		}

		return pd.Spec.AttachTo.ProviderKind == "ZFS" &&
			pd.Spec.AttachTo.StoragePoolName == spName
	}, "Bug 68/73: PhysicalDevice never flipped to AttachTo on cdp request")

	// 2. StoragePool CRD created end-to-end (Bug 68 — without the
	//    SP create the satellite sits in PoolMissing forever).
	harness.Eventually(t, waitAttach, func() bool {
		var pool blockstoriov1alpha1.StoragePool

		err := stack.Env.Client.Get(ctx,
			types.NamespacedName{Name: spName + ".worker-3"}, &pool)
		if err != nil {
			return false
		}

		return pool.Spec.ProviderKind == "ZFS"
	}, "Bug 68: StoragePool CRD never appeared after cdp request")

	// 3. Bug 88: the auto-created StoragePool's Spec.Props must
	//    carry the kind-specific `StorDriver/ZPool` key with the
	//    operator-supplied `--pool-name` value. Without this, the
	//    satellite's `newZFS` factory rejects every reconcile with
	//    "ZFS provider requires StorDriver/ZPool in props" and the
	//    pool's Status.{FreeCapacity, TotalCapacity,
	//    SupportsSnapshots} stay zero — the live-stand failure mode
	//    the bug reports.
	var pool blockstoriov1alpha1.StoragePool
	if err := stack.Env.Client.Get(ctx,
		types.NamespacedName{Name: spName + ".worker-3"}, &pool); err != nil {
		t.Fatalf("Bug 88: get StoragePool: %v", err)
	}

	if got := pool.Spec.Props["StorDriver/ZPool"]; got != "myzpool" {
		t.Errorf("Bug 88: Spec.Props[StorDriver/ZPool]: got %q, want %q (props=%+v) — satellite would log `ZFS attach requires ZPoolName` every reconcile",
			got, "myzpool", pool.Spec.Props)
	}
}

// testPhysicalStorageCDPPropsPerKind pins Bug 88 across all six
// provider kinds blockstor supports. The operator-typed `--pool-name`
// MUST land as the kind-specific `StorDriver/*` prop on the
// auto-created StoragePool CRD so the satellite's
// `NewProviderFromKind` can instantiate the backend on its next
// reconcile.
//
// Drives the REST handler directly (POST /v1/physical-storage/<node>)
// to cover provider kinds the python-linstor CLI binary may reject
// at parse time (e.g. FILE_THIN) — the wire contract is the source
// of truth, the CLI is just one client of it.
func testPhysicalStorageCDPPropsPerKind(t *testing.T, stack *harness.Stack) {
	t.Helper()

	cases := []struct {
		name         string
		providerKind string
		poolName     string
		spName       string
		devSuffix    string
		wantProps    map[string]string
	}{
		{
			name:         "lvm",
			providerKind: "LVM",
			poolName:     "vg-thick",
			spName:       "cdp-lvm",
			devSuffix:    "lvm",
			wantProps:    map[string]string{"StorDriver/LvmVg": "vg-thick"},
		},
		{
			name:         "lvm-thin",
			providerKind: "LVM_THIN",
			poolName:     "vg-lt/thin-lt",
			spName:       "cdp-lvmthin",
			devSuffix:    "lvmthin",
			wantProps: map[string]string{
				"StorDriver/LvmVg":    "vg-lt",
				"StorDriver/ThinPool": "thin-lt",
			},
		},
		{
			name:         "zfs-thin",
			providerKind: "ZFS_THIN",
			poolName:     "tank-thin",
			spName:       "cdp-zfsthin",
			devSuffix:    "zfsthin",
			wantProps:    map[string]string{"StorDriver/ZPoolThin": "tank-thin"},
		},
		{
			name:         "file",
			providerKind: "FILE",
			poolName:     "/var/lib/blockstor/cdp-file",
			spName:       "cdp-file",
			devSuffix:    "file",
			wantProps:    map[string]string{"StorDriver/FileDir": "/var/lib/blockstor/cdp-file"},
		},
		{
			name:         "file-thin",
			providerKind: "FILE_THIN",
			poolName:     "/var/lib/blockstor/cdp-filethin",
			spName:       "cdp-filethin",
			devSuffix:    "filethin",
			wantProps:    map[string]string{"StorDriver/FileDir": "/var/lib/blockstor/cdp-filethin"},
		},
	}

	ctx := context.Background()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			devName := "worker-3.cdp-" + tc.devSuffix
			devPath := "/dev/disk/by-id/scsi-cdp-" + tc.devSuffix

			dev := &blockstoriov1alpha1.PhysicalDevice{
				ObjectMeta: metav1.ObjectMeta{
					Name: devName,
					Labels: map[string]string{
						blockstoriov1alpha1.PhysicalDeviceLabelNode: "worker-3",
					},
				},
			}
			if err := stack.Env.Client.Create(ctx, dev); err != nil {
				t.Fatalf("create PhysicalDevice: %v", err)
			}

			var got blockstoriov1alpha1.PhysicalDevice
			if err := stack.Env.Client.Get(ctx,
				types.NamespacedName{Name: devName}, &got); err != nil {
				t.Fatalf("get PhysicalDevice: %v", err)
			}

			got.Status = blockstoriov1alpha1.PhysicalDeviceStatus{
				NodeName:   "worker-3",
				StableID:   "scsi-cdp-" + tc.devSuffix,
				DevicePath: devPath,
				SizeBytes:  4294967296,
				Phase:      blockstoriov1alpha1.PhysicalDevicePhaseAvailable,
			}
			if err := stack.Env.Client.Status().Update(ctx, &got); err != nil {
				t.Fatalf("status update: %v", err)
			}

			// POST directly through the REST API — the Bug 88
			// regression is in the handler, not the CLI; mirror
			// the JSON shape python-linstor emits for
			// `--pool-name X --storage-pool Y`.
			body := `{
				"provider_kind": "` + tc.providerKind + `",
				"pool_name": "` + tc.poolName + `",
				"device_paths": ["` + devPath + `"],
				"with_storage_pool": {"name": "` + tc.spName + `"}
			}`

			resp, err := http.Post(stack.RestURL+"/v1/physical-storage/worker-3",
				"application/json", strings.NewReader(body))
			if err != nil {
				t.Fatalf("POST cdp: %v", err)
			}
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("status: got %d, want 202", resp.StatusCode)
			}

			const waitProps = 10 * time.Second

			poolKey := types.NamespacedName{Name: tc.spName + ".worker-3"}

			harness.Eventually(t, waitProps, func() bool {
				var pool blockstoriov1alpha1.StoragePool
				if err := stack.Env.Client.Get(ctx, poolKey, &pool); err != nil {
					return false
				}

				return pool.Spec.ProviderKind == tc.providerKind
			}, "Bug 88: StoragePool CRD never appeared for "+tc.name)

			var pool blockstoriov1alpha1.StoragePool
			if err := stack.Env.Client.Get(ctx, poolKey, &pool); err != nil {
				t.Fatalf("Bug 88: get StoragePool %s: %v", poolKey.Name, err)
			}

			for key, want := range tc.wantProps {
				if got := pool.Spec.Props[key]; got != want {
					t.Errorf("Bug 88 %s: Spec.Props[%q]: got %q, want %q (props=%+v)",
						tc.name, key, got, want, pool.Spec.Props)
				}
			}
		})
	}
}
