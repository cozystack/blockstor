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

package rest

import (
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestToggleDiskPromotesDiskless: PUT toggle-disk on a DISKLESS
// replica drops the DISKLESS flag — the satellite picks the rest
// of the work up from the auto-diskful path on its next reconcile.
func TestToggleDiskPromotesDiskless(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/resources/n1/toggle-disk", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-1", "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag still present after toggle-disk: %v", got.Flags)
	}
}

// TestToggleDiskWithExplicitPool stamps the storage pool name on
// the typed Spec.Props["StorPoolName"] when promoting via the
// `/storage-pool/{pool}` path. Pins the upstream-LINSTOR-shaped
// URL and verifies the controller's auto-diskful path won't have
// to pick a pool.
func TestToggleDiskWithExplicitPool(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-pool"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-pool",
		NodeName: "n2",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-pool/resources/n2/toggle-disk/storage-pool/zfs-thin", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-pool", "n2")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["StorPoolName"] != "zfs-thin" {
		t.Errorf("Props[StorPoolName]: got %q, want zfs-thin", got.Props["StorPoolName"])
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag still present: %v", got.Flags)
	}
}

// TestToggleDiskAddDiskScenario4W22 pins scenario 4.W22 (wave2-04
// lifecycle): `linstor r td <node> <rd> --storage-pool <pool>` promotes
// a DISKLESS replica to diskful with the named pool. The REST handler
// MUST:
//
//  1. Stamp the typed storage-pool name (Props["StorPoolName"], which
//     pkg/store/k8s wireToCRDResourceSpec lifts into Spec.StoragePool
//     for the satellite reconciler's storage-carve path).
//  2. Drop the DISKLESS flag so the satellite's runApply path takes
//     the diskful branch (applyStorageIfDiskful provisions the LV/ZVOL
//     + applyDRBD runs drbdadm adjust, which attaches and syncs from
//     peers through Inconsistent -> SyncTarget -> UpToDate).
//  3. NOT stamp the cancel intent — Spec.ToggleDiskCancel must remain
//     false on a forward add-disk operation. Flipping it here would
//     race the reconciler's add-disk path with handleToggleDiskCancel
//     and unwind the very allocation the operator asked for. Cancel
//     is only ever set via `?cancel=true` (Bug 40).
//
// Cross-listed with wave1 4.9 (inverse direction). UG9 §"Toggling a
// resource between diskful and diskless" (lines 3608-3629).
func TestToggleDiskAddDiskScenario4W22(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-4w22"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-4w22",
		NodeName: "worker-3",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-4w22/resources/worker-3/toggle-disk/storage-pool/pool_ssd",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-4w22", "worker-3")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// (1) Typed storage pool stamped.
	if got.Props["StorPoolName"] != "pool_ssd" {
		t.Errorf("Props[StorPoolName]: got %q, want pool_ssd", got.Props["StorPoolName"])
	}

	// (2) DISKLESS dropped so the satellite takes the diskful Apply branch.
	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag still present after add-disk: %v", got.Flags)
	}

	// (3) Cancel intent stays clear — only `?cancel=true` flips it
	// (Bug 40). A forward add-disk that also stamped cancel would race
	// the reconciler's unwind path with its own allocation.
	if got.ToggleDiskCancel {
		t.Errorf("ToggleDiskCancel unexpectedly true after add-disk: %+v", got)
	}
}

// TestToggleDiskDemotesDiskful: PUT on a diskful replica adds the
// DISKLESS flag — the satellite tears down the LV on its next
// reconcile via the existing detach hook.
func TestToggleDiskDemotesDiskful(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-d"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-d",
		NodeName: "n3",
		// no DISKLESS flag — currently diskful
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-d/resources/n3/toggle-disk", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-d", "n3")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag missing after toggle-disk: %v", got.Flags)
	}
}

// TestToggleDiskForceDisklessOnDiskful pins scenario 4.W23 (P0):
// `linstor r td <node> <rd> --diskless` on a currently-diskful
// replica POSTs to the explicit `/toggle-disk/diskless` suffix.
// REST adds the DISKLESS flag on Spec; the satellite reconciler
// picks the demote up from there — drbdadm detach on the local
// volume frees the backing LV/dataset, the DRBD peer survives as
// a connection-mesh-only (diskless) replica that still serves I/O
// through its UpToDate peers. The historical StorPoolName stays
// stamped so a follow-on `r td` (without `--diskless`) can promote
// the same replica back to diskful without re-passing the pool.
func TestToggleDiskForceDisklessOnDiskful(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-w23"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-w23",
		NodeName: "n-demote",
		// No DISKLESS flag — currently diskful with a backing LV.
		Props: map[string]string{"StorPoolName": "zfs-thin"},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-w23/resources/n-demote/toggle-disk/diskless", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-w23", "n-demote")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// REST flag flip: DISKLESS must be present after the demote. The
	// satellite reconciler reads this on the next pass and routes
	// applyStorageIfDiskful's diskless branch (skip storage) plus
	// drbdadm detach on the (now-removed) local backing device.
	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag missing after `r td --diskless`: %v", got.Flags)
	}

	// Pool intact: demote keeps the historical pool stamped so a
	// later toggle-back to diskful re-uses the same backing pool
	// without the operator having to re-pass it on the next
	// `r td <node> <rd> --storage-pool <pool>` call.
	if got.Props["StorPoolName"] != "zfs-thin" {
		t.Errorf("Props[StorPoolName] dropped on demote: got %q, want zfs-thin",
			got.Props["StorPoolName"])
	}
}

// TestToggleDiskForceDisklessIdempotent pins idempotency of the
// `/toggle-disk/diskless` suffix: re-issuing the same call against
// an already-diskless replica is a no-op flag-wise (DISKLESS stays
// set, no duplicate entry) and still returns 200. Matches upstream
// LINSTOR's behaviour: `linstor r td --diskless` against a diskless
// replica reports success without churning storage state.
func TestToggleDiskForceDisklessIdempotent(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-w23i"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-w23i",
		NodeName: "n-already",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-w23i/resources/n-already/toggle-disk/diskless", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-w23i", "n-already")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag missing after idempotent re-demote: %v", got.Flags)
	}

	// applyFlagMutation must not double-stamp the flag: a duplicate
	// would surface in `linstor r l` as "DISKLESS,DISKLESS" and trip
	// downstream callers that slice the Flags list by exact match.
	var disklessCount int

	for _, f := range got.Flags {
		if f == apiv1.ResourceFlagDiskless {
			disklessCount++
		}
	}

	if disklessCount != 1 {
		t.Errorf("DISKLESS flag count: got %d, want 1 (no duplicate); flags=%v",
			disklessCount, got.Flags)
	}
}

// TestToggleDiskForceDisklessUnknownReplica: explicit `--diskless`
// suffix against a missing (rd, node) pair surfaces 404, not a
// silent create. Pins the suffix handler shares the same
// not-found semantics as the un-suffixed toggle-disk endpoint.
func TestToggleDiskForceDisklessUnknownReplica(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/ghost/resources/n9/toggle-disk/diskless", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestToggleDiskUnknownReplica: 404 on a missing (rd, node) pair.
func TestToggleDiskUnknownReplica(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/ghost/resources/n9/toggle-disk", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestMigrateDiskBasicFlow: src has a diskful replica, dst has no
// replica yet. PUT migrate-disk under Option B (strict
// add-before-drop) should:
//   - create dst diskful with StorPoolName stamped
//   - stamp BlockstorMigratingFrom=<src-node> on dst
//   - LEAVE src in place — the ResourceMigrationReconciler will
//     prune it asynchronously once dst's Status.Volumes report
//     UpToDate
//
// Pins the redundancy invariant: at no point during this REST call
// does the diskful count drop below the original. The src lives
// until the destination is observed durable on the wire.
func TestMigrateDiskBasicFlow(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-mig"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-mig",
		NodeName: "src-node",
		// No DISKLESS flag — diskful.
	}); err != nil {
		t.Fatalf("seed src Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-mig/resources/dst-node/migrate-disk/src-node/zfs-thin",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	// dst exists, diskful, pool stamped, migrating-from prop set.
	got, err := st.Resources().Get(t.Context(), "pvc-mig", "dst-node")
	if err != nil {
		t.Fatalf("Get dst: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("dst DISKLESS flag still set: %v", got.Flags)
	}

	if got.Props["StorPoolName"] != "zfs-thin" {
		t.Errorf("dst StorPoolName: got %q, want zfs-thin", got.Props["StorPoolName"])
	}

	if got.Props[MigratingFromProp] != "src-node" {
		t.Errorf("dst %s: got %q, want src-node",
			MigratingFromProp, got.Props[MigratingFromProp])
	}

	// src MUST still be present — Option B defers the prune to the
	// reconciler. Deleting it here would re-introduce the redundancy
	// regression Option B exists to fix.
	srcRes, err := st.Resources().Get(t.Context(), "pvc-mig", "src-node")
	if err != nil {
		t.Fatalf("src Resource pruned by REST handler (Option A regression): %v", err)
	}

	if slices.Contains(srcRes.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("src unexpectedly flagged DISKLESS by migrate handler: %v", srcRes.Flags)
	}
}

// TestMigrateDiskWithExistingDiskless: dst already declared diskless
// (the typical two-step upstream flow: `linstor r c <dst> <rd>
// --drbd-diskless` then `linstor r td -s <pool> --migrate-from
// <src>`). Migrate flips dst diskful, stamps migrating-from, and
// leaves src untouched (the reconciler prunes it once dst is
// UpToDate — strict add-before-drop, Option B).
func TestMigrateDiskWithExistingDiskless(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-mig2"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-mig2", NodeName: "src-node",
	}); err != nil {
		t.Fatalf("seed src Resource: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-mig2", NodeName: "dst-node",
		Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed dst Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-mig2/resources/dst-node/migrate-disk/src-node/zfs-thin",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-mig2", "dst-node")
	if err != nil {
		t.Fatalf("Get dst: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("dst DISKLESS not cleared: %v", got.Flags)
	}

	if got.Props["StorPoolName"] != "zfs-thin" {
		t.Errorf("dst StorPoolName: got %q, want zfs-thin", got.Props["StorPoolName"])
	}

	if got.Props[MigratingFromProp] != "src-node" {
		t.Errorf("dst %s: got %q, want src-node",
			MigratingFromProp, got.Props[MigratingFromProp])
	}

	// src MUST live until the reconciler observes dst UpToDate.
	if _, err := st.Resources().Get(t.Context(), "pvc-mig2", "src-node"); err != nil {
		t.Errorf("src Resource pruned synchronously by REST handler: %v", err)
	}
}

// TestMigrateDiskRefusesPrimaryInUse: an active Primary replica
// can't be migrated implicitly — UG9 requires the operator to
// demote the consumer first. 409 Conflict per writeError.
func TestMigrateDiskRefusesPrimaryInUse(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-busy"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	primary := true

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-busy",
		NodeName: "src-node",
		State:    apiv1.ResourceState{InUse: &primary},
	}); err != nil {
		t.Fatalf("seed src Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-busy/resources/dst-node/migrate-disk/src-node/zfs-thin",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
	}

	// src must be untouched.
	if _, err := st.Resources().Get(t.Context(), "pvc-busy", "src-node"); err != nil {
		t.Errorf("src removed despite 409: %v", err)
	}
}

// TestMigrateDiskAddBeforeDropOrdering pins the Bug 34 Option B
// ordering contract for scenario 4.W21 / wave1 4.10: the REST
// migrate-disk handler must add the destination diskful replica
// BEFORE anything happens to the source. The diskful replica count
// must monotonically rise across the REST call (N → N+1) and only
// the asynchronous ResourceMigrationReconciler — running after dst
// reaches UpToDate on the wire — is allowed to drop it back to N by
// pruning src.
//
// Concretely the test seeds a 3-diskful-replica RD, calls PUT
// migrate-disk with a fourth node as the destination, and asserts:
//
//  1. The handler returns 200 (Option B is an async-pending success,
//     not 202).
//  2. ListByDefinition immediately afterwards returns FOUR diskful
//     Resources for the RD — the three originals plus dst — proving
//     the add happened before any drop.
//  3. The src Resource is still byte-for-byte diskful: no DISKLESS
//     flag, no ToggleDiskCancel, no MigratingFrom prop on it (the
//     prop belongs on dst, not src).
//  4. The handler stamped MigratingFromProp on dst BEFORE clearing
//     DISKLESS on src — verified indirectly: src has no spec mutation
//     at all, while dst carries both the pool stamp and the prop.
//
// The test then simulates the reconciler completing (delete src,
// strip prop on dst) and asserts the final count is back to three
// diskful — i.e. the full lifecycle is N → N+1 → N, never N → N-1
// → N as the Option A regression would produce.
func TestMigrateDiskAddBeforeDropOrdering(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-order"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, node := range []string{"alpha", "bravo", "charlie"} {
		if err := st.Resources().Create(t.Context(), &apiv1.Resource{
			Name:     "pvc-order",
			NodeName: node,
			// No DISKLESS flag — diskful.
		}); err != nil {
			t.Fatalf("seed %s: %v", node, err)
		}
	}

	// Pre-condition: exactly 3 diskful replicas, 0 diskless.
	pre, err := st.Resources().ListByDefinition(t.Context(), "pvc-order")
	if err != nil {
		t.Fatalf("pre ListByDefinition: %v", err)
	}

	if diskfulCount(pre) != 3 {
		t.Fatalf("pre: diskful count got %d, want 3 (%v)", diskfulCount(pre), pre)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Migrate alpha → delta. Under Option B the handler must
	// raise the diskful count to 4 (add dst) and leave alpha
	// alone (deferred drop). Failure mode under Option A: handler
	// deletes alpha synchronously, count goes 3 → 2 → 3 with a
	// transient window of 2 visible to any concurrent ListResources.
	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-order/resources/delta/migrate-disk/alpha/zfs-thin",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Post-condition immediately after REST returns: 4 diskful
	// replicas exist for the RD. This is the add-before-drop
	// pin — any handler that pruned src synchronously would land
	// here with 3 (and only later swing back to 4 once a separate
	// reconcile re-stamped dst, which is the broken Option A flow).
	post, err := st.Resources().ListByDefinition(t.Context(), "pvc-order")
	if err != nil {
		t.Fatalf("post ListByDefinition: %v", err)
	}

	if got := diskfulCount(post); got != 4 {
		t.Errorf("post-REST diskful count: got %d, want 4 (add-before-drop violated): %v",
			got, post)
	}

	// src (alpha) must be present and untouched.
	alpha, err := st.Resources().Get(t.Context(), "pvc-order", "alpha")
	if err != nil {
		t.Fatalf("src alpha pruned by REST handler (Option A regression): %v", err)
	}

	if slices.Contains(alpha.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("src alpha flipped DISKLESS at REST layer: %v", alpha.Flags)
	}

	if alpha.Props[MigratingFromProp] != "" {
		t.Errorf("src alpha carries MigratingFrom prop (belongs on dst): %q",
			alpha.Props[MigratingFromProp])
	}

	if alpha.ToggleDiskCancel {
		t.Errorf("src alpha unexpectedly carries ToggleDiskCancel")
	}

	// dst (delta) must be the new diskful with both the pool stamp
	// and the migrating-from trigger set in the same REST round-trip.
	// The reconciler only fires when BOTH conditions are observed
	// together; a half-stamped dst would deadlock the migration.
	delta, err := st.Resources().Get(t.Context(), "pvc-order", "delta")
	if err != nil {
		t.Fatalf("dst delta missing after REST: %v", err)
	}

	if slices.Contains(delta.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("dst delta still DISKLESS: %v", delta.Flags)
	}

	if delta.Props["StorPoolName"] != "zfs-thin" {
		t.Errorf("dst delta StorPoolName: got %q, want zfs-thin",
			delta.Props["StorPoolName"])
	}

	if delta.Props[MigratingFromProp] != "alpha" {
		t.Errorf("dst delta MigratingFrom: got %q, want alpha",
			delta.Props[MigratingFromProp])
	}

	// Simulate the ResourceMigrationReconciler completing the second
	// half of Option B: it observes dst UpToDate, deletes src, strips
	// the prop on dst. The final state must be exactly three diskful
	// replicas (alpha pruned, bravo/charlie/delta diskful) and the
	// migrating-from prop cleared on dst — proving the full N → N+1
	// → N lifecycle and that no other state mutation is needed past
	// what the REST handler stamped.
	if err := st.Resources().Delete(t.Context(), "pvc-order", "alpha"); err != nil {
		t.Fatalf("simulate reconciler prune of src: %v", err)
	}

	delta.Props = map[string]string{"StorPoolName": "zfs-thin"} // prop stripped by reconciler.
	if err := st.Resources().Update(t.Context(), &delta); err != nil {
		t.Fatalf("simulate reconciler clear of prop: %v", err)
	}

	final, err := st.Resources().ListByDefinition(t.Context(), "pvc-order")
	if err != nil {
		t.Fatalf("final ListByDefinition: %v", err)
	}

	if got := diskfulCount(final); got != 3 {
		t.Errorf("final diskful count: got %d, want 3 (%v)", got, final)
	}

	for _, res := range final {
		if res.Props[MigratingFromProp] != "" {
			t.Errorf("MigratingFrom prop leaked on %s after reconciler clear: %q",
				res.NodeName, res.Props[MigratingFromProp])
		}
	}
}

// diskfulCount returns the number of diskful replicas in the slice —
// i.e. replicas without the DISKLESS flag set. Used by the
// add-before-drop ordering test to assert the redundancy invariant
// across each phase of an Option B migration.
func diskfulCount(rs []apiv1.Resource) int {
	n := 0

	for i := range rs {
		if !slices.Contains(rs[i].Flags, apiv1.ResourceFlagDiskless) {
			n++
		}
	}

	return n
}

// TestToggleDiskCancelQuerySetsSpecField pins the Bug 40 cancel
// surface: PUT toggle-disk?cancel=true on a diskful (or in-flight
// converting) replica MUST set Spec.ToggleDiskCancel=true on the
// stored Resource without flipping the DISKLESS flag. The flag flip
// is the reconciler's job once it has torn down storage + DRBD —
// REST only writes the intent.
func TestToggleDiskCancelQuerySetsSpecField(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-c"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-c",
		NodeName: "n-cancel",
		// no DISKLESS flag — Resource is mid-conversion or already
		// diskful; either way, REST handler stamps the cancel
		// intent without touching Flags.
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-c/resources/n-cancel/toggle-disk?cancel=true", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-c", "n-cancel")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !got.ToggleDiskCancel {
		t.Errorf("ToggleDiskCancel false after cancel=true: got %+v", got)
	}

	// Crucial: REST must NOT flip DISKLESS itself — the reconciler
	// is the one that re-stamps DISKLESS after the rollback runs.
	// A pre-emptive flag flip would make consumers see the Resource
	// as connection-mesh-only before the storage + drbdadm cleanup
	// finished, racing the satellite's tear-down path.
	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("cancel handler unexpectedly flipped DISKLESS: %v", got.Flags)
	}
}

// TestMigrateDiskUnknownRD: missing ResourceDefinition surfaces as
// 404, not as a follow-on 500 from a later lookup.
func TestMigrateDiskUnknownRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/ghost-rd/resources/dst/migrate-disk/src/zfs-thin",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}
