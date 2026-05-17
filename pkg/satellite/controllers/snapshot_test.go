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

package controllers_test

import (
	"context"
	"slices"
	"testing"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// newSnapshotScheme is the runtime.Scheme used by Snapshot
// reconciler tests — corev1 + the blockstor v1alpha1 group.
func newSnapshotScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("blockstor: %v", err)
	}

	return s
}

// seedThinResource registers an LVM-thin provider on the
// satellite reconciler and primes its resource→pool map by
// running one Apply pass. The follow-up SnapshotReconciler
// `handleDelete` then routes `DeleteSnapshot` to the right
// provider via the recorded mapping.
func seedThinResource(t *testing.T, fx *storage.FakeExec, resourceName, pool string) *satellite.Reconciler {
	t.Helper()

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: thin},
	})

	_, err := rec.Apply(context.Background(), []*intent.DesiredResource{
		{
			Name: resourceName, NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: pool},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply (seed): %v", err)
	}

	return rec
}

// TestSnapshotReconcileAddsFinalizer pins the Bug 64 fix: the
// first observation of a Snapshot scoped to this satellite MUST
// stamp `SatelliteSnapshotFinalizer` before `CreateSnapshot`
// runs. Without the finalizer, kube-apiserver removes the CRD
// on `kubectl delete` before the satellite sees the
// DeletionTimestamp event, and the on-disk ZFS / LVM-thin
// snapshot survives as an orphan.
func TestSnapshotReconcileAddsFinalizer(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
	})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     storage.NewFakeExec(),
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if result.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0 after stamping finalizer, got %+v", result)
	}

	var got blockstoriov1alpha1.Snapshot
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Finalizers, controllers.SatelliteSnapshotFinalizer) {
		t.Errorf("SatelliteSnapshotFinalizer missing after Reconcile: %v", got.Finalizers)
	}
}

// TestSnapshotReconcileDrainsOnDelete pins the second half of
// the Bug 64 lifecycle: a Snapshot with a DeletionTimestamp +
// our finalizer must drive `lvremove` on the provider before
// the finalizer is stripped. A regression would either skip
// `DeleteSnapshot` (orphan LV) or strip the finalizer before
// the on-disk teardown succeeded (apiserver finalises the CRD
// while the LV lingers).
func TestSnapshotReconcileDrainsOnDelete(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)
	now := metav1.Now()

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-1.snap-1",
			Finalizers:        []string{controllers.SatelliteSnapshotFinalizer},
			DeletionTimestamp: &now,
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// Apply seed: `lvs` for pvc-1_00000 returns empty so
	// CreateVolume runs (lvcreate --thin …).
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Bug 212: DeleteSnapshot pre-checks LV existence. Stub the
	// snapshot LV as present so lvremove still fires.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})

	rec := seedThinResource(t, fx, "pvc-1", "thin1")

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// lvremove against the snapshot LV MUST have fired.
	wantCmd := "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_snap-1_00000"
	if !slices.Contains(fx.CommandLines(), wantCmd) {
		t.Errorf("DeleteSnapshot did not invoke %q on the provider; got %v",
			wantCmd, fx.CommandLines())
	}

	// Finalizer MUST be stripped so the apiserver can finalise.
	var got blockstoriov1alpha1.Snapshot
	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err == nil && slices.Contains(got.Finalizers, controllers.SatelliteSnapshotFinalizer) {
		t.Errorf("SatelliteSnapshotFinalizer still present after successful drain: %v",
			got.Finalizers)
	}
}

// TestSnapshotReconcileMarksFailedOnTerminalError pins the F18
// cli-parity fix: when Apply.CreateSnapshot returns a terminal
// error (Terminal=true — e.g. parent volume not found, unknown
// resource, provider returned ErrTerminal), the SnapshotReconciler
// MUST stamp Status.Flags=["FAILED"] on the CRD before returning
// and MUST NOT requeue. The wire shape's crdToWireSnapshot
// surfaces this as `flags: ["FAILED"]`, which the Python CLI
// maps to State="Failed" in `linstor s l`.
//
// A regression that left Status.Flags empty would leave the CLI
// in State="Incomplete" forever, hiding the failure from the
// operator and from CSI's CreateSnapshot success-polling loop.
//
// Setup: no providers registered for the snapshot's RD ⇒
// providerForResource returns "unknown resource", which the
// reconciler classifies as Terminal=true.
func TestSnapshotReconcileMarksFailedOnTerminalError(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-missing",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	// Empty providers map ⇒ Apply.CreateSnapshot's
	// providerForResource lookup fails ⇒ Terminal=true.
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
	})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     storage.NewFakeExec(),
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Terminal failures MUST NOT requeue — they're dead-letter.
	if result.Requeue || result.RequeueAfter > 0 {
		t.Errorf("terminal failure should NOT requeue; got %+v", result)
	}

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) {
		t.Errorf("Status.Flags missing FAILED after terminal error: %+v",
			got.Status.Flags)
	}
}

// TestSnapshotReconcileKeepsIncompleteOnTransientError pins the
// flip side: when Apply.CreateSnapshot returns a transient
// failure (Terminal=false — lvm temporary lock, exec wrapper
// noise, busy dataset), the reconciler MUST requeue without
// stamping Status.Flags. The CRD stays in the "Incomplete" wire
// state and the controller-runtime rate limiter handles
// back-off.
//
// A regression that prematurely stamped FAILED on a transient
// failure would dead-letter a snapshot that the next pass
// would have completed successfully, and force the operator
// to delete + recreate it for a recoverable hiccup.
//
// Setup: provider's lvcreate exits non-zero (the exec wrapper
// returns a plain error that does NOT wrap ErrTerminal /
// ErrNotFound) ⇒ Terminal=false.
func TestSnapshotReconcileKeepsIncompleteOnTransientError(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// Seed: lvs returns empty so CreateVolume runs.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Snapshot path: lvcreate -s fails with a transient-looking
	// error. The error is NOT wrapped in ErrTerminal/ErrNotFound,
	// so the reconciler classifies it as transient.
	fx.Expect("lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000",
		storage.FakeResponse{Err: errLvmTemporaryLock})

	rec := seedThinResource(t, fx, "pvc-1", "thin1")

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Transient failures MUST requeue so the next pass retries.
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("transient failure should requeue; got %+v", result)
	}

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) {
		t.Errorf("Status.Flags should not contain FAILED on transient error; got %+v",
			got.Status.Flags)
	}
}

// errLvmTemporaryLock simulates a transient lvm condition the
// kernel resolves on its own (e.g. a busy vg lock that lvcreate
// retries on its next invocation). NOT wrapped in
// storage.ErrTerminal / storage.ErrNotFound on purpose — the
// reconciler reads that absence as "transient, retry".
var errLvmTemporaryLock = errors.New("lvm: Locking type 1 initialisation failed")

// TestSnapshotReconcileNoOpOnUnrelatedNode pins the
// node-membership filter: a Snapshot whose `Spec.Nodes` does
// NOT contain our NodeName must short-circuit Reconcile with
// no provider calls, no finalizer stamping, and no Update
// against the apiserver. The predicate at SetupWithManager is
// the watch-layer filter; this defensive check covers the
// case where the watch cache is mid-resync and a stray event
// for someone else's Snapshot reaches us.
func TestSnapshotReconcileNoOpOnUnrelatedNode(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n2", "n3"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
	})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if result.RequeueAfter != 0 {
		t.Errorf("unrelated-node Snapshot should NOT requeue; got %+v", result)
	}

	if len(fx.CommandLines()) != 0 {
		t.Errorf("unrelated-node Snapshot triggered provider calls: %v", fx.CommandLines())
	}

	var got blockstoriov1alpha1.Snapshot
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if slices.Contains(got.Finalizers, controllers.SatelliteSnapshotFinalizer) {
		t.Errorf("finalizer stamped on a Snapshot for another node: %v", got.Finalizers)
	}
}

// TestSnapshotDeleteScenarioW02PerReplicaBackendTeardown pins
// scenario 8.W02 (wave2-08-snapshots.md): `snapshot delete <rd> <snap>`
// MUST remove the snapshot across all diskful replicas + their backing
// storage. Concretely, each per-node satellite's Reconcile pass MUST
// fire the provider's backend teardown (lvremove for LVM-thin / zfs
// destroy for ZFS) on its own node before stripping the finalizer.
//
// The single-node path is covered by TestSnapshotReconcileDrainsOnDelete;
// this test exercises the multi-replica half — the same Snapshot CRD
// (Spec.Nodes lists more than one peer) must drive the local
// DeleteSnapshot dispatch independently on each node. A regression
// that scoped the teardown to a single node would let the surviving
// replicas leak orphan thin-pool LVs / ZFS datasets — the exact
// failure mode Bug 64's finalizer was added to prevent.
//
// Each iteration uses a fresh fake client so the per-node assertion
// stays isolated from the shared-CRD update race that two reconcilers
// would otherwise have over one client (the controller-runtime
// production flow has each satellite running against the real
// apiserver, with optimistic-concurrency retries handling that race).
func TestSnapshotDeleteScenarioW02PerReplicaBackendTeardown(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	for _, node := range []string{"n1", "n2"} {
		now := metav1.Now()

		snap := &blockstoriov1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pvc-1.snap-1",
				Finalizers:        []string{controllers.SatelliteSnapshotFinalizer},
				DeletionTimestamp: &now,
			},
			Spec: blockstoriov1alpha1.SnapshotSpec{
				ResourceDefinitionName: "pvc-1",
				SnapshotName:           "snap-1",
				Nodes:                  []string{"n1", "n2"},
			},
		}

		cli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(snap).
			Build()

		fx := storage.NewFakeExec()
		// Apply seed for this node: lvs returns empty → CreateVolume
		// runs and records the pool mapping the snapshot teardown
		// will key off.
		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
			storage.FakeResponse{Stdout: []byte("")})
		// Bug 212: DeleteSnapshot pre-checks LV existence. Report the
		// snapshot LV as present so lvremove still fires on each node.
		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_snap-1_00000",
			storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})

		rec := seedThinResource(t, fx, "pvc-1", "thin1")

		reconciler := &controllers.SnapshotReconciler{
			Client: cli,
			Config: controllers.Config{
				NodeName: node,
				Apply:    rec,
				Exec:     fx,
			},
		}

		_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
		})
		if err != nil {
			t.Fatalf("Reconcile (%s): %v", node, err)
		}

		// Per-replica backend teardown MUST have fired on this node.
		wantCmd := "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_snap-1_00000"
		if !slices.Contains(fx.CommandLines(), wantCmd) {
			t.Errorf("node %s: scenario 8.W02 expects %q; got %v",
				node, wantCmd, fx.CommandLines())
		}

		// This node's pass MUST strip the finalizer once the
		// backend teardown succeeds. In the multi-replica production
		// flow each satellite stripes the single shared finalizer
		// optimistically — last write wins, which is safe because
		// the teardown is idempotent.
		var got blockstoriov1alpha1.Snapshot

		gErr := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
		if gErr == nil && slices.Contains(got.Finalizers, controllers.SatelliteSnapshotFinalizer) {
			t.Errorf("node %s: SatelliteSnapshotFinalizer leaked after teardown: %v",
				node, got.Finalizers)
		}
	}
}

// TestSnapshotDeleteScenarioW02IdempotentOnAbsentFinalizer pins the
// idempotent half of scenario 8.W02 (Bug 66 envelope analogue at the
// satellite layer): a Reconcile pass that observes a Snapshot with a
// DeletionTimestamp but no SatelliteSnapshotFinalizer (already
// stripped, or never stamped — e.g. Snapshot created before this
// satellite came up) MUST short-circuit without invoking the
// provider's DeleteSnapshot. A regression that called DeleteSnapshot
// on a finalizer-absent Snapshot would re-issue lvremove against an
// LV that's already gone (lvm: "Failed to find logical volume"), and
// — worse — risk racing a freshly-created snapshot of the same name
// if a re-create-after-delete cycle is in flight.
func TestSnapshotDeleteScenarioW02IdempotentOnAbsentFinalizer(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)
	now := metav1.Now()

	// DeletionTimestamp present, our SatelliteSnapshotFinalizer
	// absent — the post-strip terminal state from this satellite's
	// point of view (a peer satellite's finalizer or apiserver-default
	// keeps the CRD alive while we observe it). A stranger-finalizer
	// stand-in keeps the fake client happy (it refuses to create an
	// object with DeletionTimestamp and an empty finalizers list) while
	// preserving the contract under test: our reconciler observes a CRD
	// whose ours-finalizer is missing and MUST short-circuit.
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-1.snap-1",
			Finalizers:        []string{"someone-else.example.com/holding"},
			DeletionTimestamp: &now,
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// Deliberately no fx.Expect() for any lvremove command — if the
	// reconciler dispatches the provider's DeleteSnapshot here, the
	// FakeExec returns an unmatched-command error and the assertion
	// below fires with a clear message naming the regressed command.

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
	})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if result.RequeueAfter > 0 {
		t.Errorf("finalizer-absent delete pass should NOT requeue; got %+v", result)
	}

	// No provider commands at all — the short-circuit MUST happen
	// before Apply.DeleteSnapshot dispatches.
	if len(fx.CommandLines()) != 0 {
		t.Errorf("scenario 8.W02 idempotent: expected no backend commands; got %v",
			fx.CommandLines())
	}
}

// TestSnapshotReconcileStampsNodeStatusOnSuccess is the Bug 106
// regression guard: when Apply.CreateSnapshot returns Ok=true, the
// SnapshotReconciler MUST upsert our node's entry in
// Status.NodeStatus with Ready=true and a non-zero CreateTimestamp.
//
// Why this matters: the apiserver's `stampSnapshotSuccessful`
// derivation (commit 3c593c5f7) walks `Snapshots[].CreateTimestamp`
// to decide whether every diskful peer reported back, and the
// store-side projection in pkg/store/k8s/snapshots.go reads
// `crd.Status.NodeStatus[i].CreateTimestamp` to populate it. Before
// this stamp landed, no code path ever wrote to Status.NodeStatus
// on success, so the wire field stayed zero, the apiserver's
// success denominator never closed, and `linstor s l` rendered
// State=Incomplete forever on a perfectly-materialised snapshot.
//
// Setup mirrors TestSnapshotReconcileDrainsOnDelete's seed (one
// thin-pool provider on n1, RD `pvc-1` already applied) but with
// a fresh Snapshot whose `lvcreate --snapshot` succeeds —
// reproducing the production "happy-path snapshot on a 2-diskful
// + 1-tiebreaker topology hangs in Incomplete" trace.
func TestSnapshotReconcileStampsNodeStatusOnSuccess(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1", "n2"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// Seed: lvs returns empty so the seed CreateVolume pass runs.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Snapshot path: lvcreate --snapshot succeeds.
	fx.Expect("lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	rec := seedThinResource(t, fx, "pvc-1", "thin1")

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if result.RequeueAfter > 0 {
		t.Errorf("successful snapshot should NOT requeue; got %+v", result)
	}

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// FAILED must not leak onto the success path.
	if slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) {
		t.Errorf("Status.Flags carries FAILED on success: %+v", got.Status.Flags)
	}

	// Bug 106 wire-shape assertion: our node's NodeStatus entry MUST
	// exist with Ready=true and a non-zero CreateTimestamp so the
	// apiserver's success derivation can flip State to "Successful".
	if len(got.Status.NodeStatus) != 1 {
		t.Fatalf("Status.NodeStatus: got %d entries, want 1 (the n1 stamp). full: %+v",
			len(got.Status.NodeStatus), got.Status.NodeStatus)
	}

	entry := got.Status.NodeStatus[0]
	if entry.NodeName != "n1" {
		t.Errorf("Status.NodeStatus[0].NodeName: got %q, want n1", entry.NodeName)
	}

	if !entry.Ready {
		t.Errorf("Status.NodeStatus[0].Ready: got false, want true (Bug 106: success not visible to apiserver)")
	}

	if entry.CreateTimestamp == 0 {
		t.Errorf("Status.NodeStatus[0].CreateTimestamp: got 0; apiserver's `s l` will stay Incomplete")
	}
}

// TestSnapshotReconcileNodeStatusIdempotentRestamp pins the
// "already-stamped" short-circuit: re-running Reconcile on a
// Snapshot whose Status.NodeStatus already carries our entry
// with the same CreateTimestamp MUST NOT trigger another
// Status().Update — pointless ResourceVersion churn and a
// retry-budget burner on the satellite. Conditional on the
// satellite's CreateSnapshot still returning Ok=true on a
// second invocation (which it does — the provider's lvExists
// pre-check (Bug 216) folds an already-materialised snapshot LV
// into success without re-running `lvcreate --snapshot`).
func TestSnapshotReconcileNodeStatusIdempotentRestamp(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	// Pre-stamp Status.NodeStatus with a synthetic timestamp so we
	// can detect whether the second pass replaced it (it shouldn't:
	// the satellite reconciler picks `time.Now().Unix()` afresh on
	// every CreateSnapshot, so a re-stamp would change the value
	// and the assertion below would fire).
	const seedTimestamp int64 = 1714000000

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", Ready: true, CreateTimestamp: seedTimestamp},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// Seed lvs response + lvcreate --snapshot success.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Provider is idempotent — lvcreate on an existing LV returns
	// success without re-running, mimicking the production short-circuit.
	fx.Expect("lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	rec := seedThinResource(t, fx, "pvc-1", "thin1")

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.Status.NodeStatus) != 1 {
		t.Fatalf("Status.NodeStatus: got %d entries, want 1", len(got.Status.NodeStatus))
	}

	if got.Status.NodeStatus[0].CreateTimestamp != seedTimestamp {
		t.Errorf("Status.NodeStatus[0].CreateTimestamp: got %d, want %d (idempotent re-stamp must skip the update)",
			got.Status.NodeStatus[0].CreateTimestamp, seedTimestamp)
	}
}
