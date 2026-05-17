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

package controllers

import (
	"context"
	"encoding/json"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// flakyCreateProvider is a minimal storage.Provider whose CreateVolume
// fails the first N invocations and succeeds afterwards. Used to
// simulate the upstream-LINSTOR toggle-disk retry scenario: storage
// pool briefly unavailable, satellite retries, eventually converges.
type flakyCreateProvider struct {
	failsRemaining int32
	created        int32
}

func (f *flakyCreateProvider) Kind() string { return "FAKE" }

func (f *flakyCreateProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{TotalCapacityKib: 1024 * 1024, FreeCapacityKib: 1024 * 1024}, nil
}

func (f *flakyCreateProvider) CreateVolume(_ context.Context, _ storage.Volume) error {
	atomic.AddInt32(&f.created, 1)

	if atomic.LoadInt32(&f.failsRemaining) > 0 {
		atomic.AddInt32(&f.failsRemaining, -1)

		return errors.New("create-md: storage pool transiently unavailable")
	}

	return nil
}

func (f *flakyCreateProvider) DeleteVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (f *flakyCreateProvider) ResizeVolume(_ context.Context, _ storage.Volume) error { return nil }

func (f *flakyCreateProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{
		DevicePath: "/dev/fake/" + vol.ResourceName,
		UsableKib:  vol.SizeKib,
	}, nil
}

func (f *flakyCreateProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (f *flakyCreateProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (f *flakyCreateProvider) RestoreVolumeFromSnapshot(_ context.Context, _ storage.Volume, _ storage.Snapshot) error {
	return nil
}

// newToggleDiskTestScheme returns a scheme wired with the CRDs the
// reconciler walks during a Resource reconcile. Mirrors the
// StoragePool tests' shape.
func newToggleDiskTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("blockstor scheme: %v", err)
	}

	return s
}

// makeStorageOnlyRD seeds a ResourceDefinition that bypasses the DRBD
// allocation gate (LayerStack=["STORAGE"]) so the test can isolate the
// storage-carve failure path. The toggle-disk retry counter is
// orthogonal to the DRBD half — the upstream LINSTOR contract counts
// every per-resource Apply failure, regardless of which sub-layer
// surfaced the error.
func makeStorageOnlyRD(name string) *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 65536},
			},
		},
	}
}

// makeToggledResource returns a Resource that the satellite reconciler
// will treat as "mid-conversion to diskful" — DISKLESS flag absent,
// finalizer already stamped (so the test skips the finalizer-only
// short-circuit), spec.storagePool pointing at the test pool, and
// matching node-name.
func makeToggledResource(rdName, node, pool string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       rdName + "." + node,
			Finalizers: []string{SatelliteResourceFinalizer},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               node,
			StoragePool:            pool,
			// no DISKLESS flag — mid-conversion to diskful
		},
	}
}

// TestToggleDiskIncrementsRetriesOnFailure pins Bug 39: on a transient
// storage-carve failure during diskless→diskful conversion, the
// satellite reconciler MUST increment Status.ToggleDiskRetries by 1
// per failing Apply pass and reset to 0 once the conversion converges.
//
// Three reconcile passes drive the state machine:
//
//  1. Provider's CreateVolume fails → retries 0 → 1.
//  2. Provider's CreateVolume fails again → retries 1 → 2.
//  3. Provider's CreateVolume succeeds → retries 2 → 0.
//
// Without the fix, the counter stayed at 0 forever and operators had
// no signal that the conversion was looping — the exact symptom
// scenario 07-toggle-disk §7.6 reproduces.
func TestToggleDiskIncrementsRetriesOnFailure(t *testing.T) {
	t.Parallel()

	const (
		rdName = "pvc-toggle"
		node   = "n-toggle"
		pool   = "lvm-thin"
	)

	scheme := newToggleDiskTestScheme(t)

	rd := makeStorageOnlyRD(rdName)
	res := makeToggledResource(rdName, node, pool)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rd, res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	provider := &flakyCreateProvider{failsRemaining: 2}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: provider},
		NodeName:  node,
	})

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName: node,
			Apply:    rec,
			Exec:     storage.NewFakeExec(),
		},
	}

	// --- Pass 1: first CreateVolume fails → retries 1. ---

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: res.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pass 1: %v", err)
	}

	var afterPass1 blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: res.Name}, &afterPass1)
	if err != nil {
		t.Fatalf("Get after pass 1: %v", err)
	}

	if afterPass1.Status.ToggleDiskRetries != 1 {
		t.Errorf("Pass 1: ToggleDiskRetries = %d, want 1",
			afterPass1.Status.ToggleDiskRetries)
	}

	// --- Pass 2: second CreateVolume fails → retries 2. ---

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: res.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pass 2: %v", err)
	}

	var afterPass2 blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: res.Name}, &afterPass2)
	if err != nil {
		t.Fatalf("Get after pass 2: %v", err)
	}

	if afterPass2.Status.ToggleDiskRetries != 2 {
		t.Errorf("Pass 2: ToggleDiskRetries = %d, want 2",
			afterPass2.Status.ToggleDiskRetries)
	}

	// --- Pass 3: CreateVolume succeeds → retries reset to 0. ---

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: res.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pass 3: %v", err)
	}

	var afterPass3 blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: res.Name}, &afterPass3)
	if err != nil {
		t.Fatalf("Get after pass 3: %v", err)
	}

	if afterPass3.Status.ToggleDiskRetries != 0 {
		t.Errorf("Pass 3: ToggleDiskRetries = %d, want 0 (reset on success)",
			afterPass3.Status.ToggleDiskRetries)
	}

	// Sanity: the third pass actually called CreateVolume — without
	// this assertion a regression that mistakenly short-circuits the
	// Apply chain could still hit retries=0 by accident.
	if got := atomic.LoadInt32(&provider.created); got != 3 {
		t.Errorf("CreateVolume call count: got %d, want 3", got)
	}
}

// recordingDeleteProvider counts DeleteVolume calls so the cancel
// rollback test can pin that the satellite actually invoked the
// storage tear-down (rather than just flipping flags and walking
// away with a half-carved LV on disk).
type recordingDeleteProvider struct {
	flakyCreateProvider
	deleted int32
}

func (r *recordingDeleteProvider) DeleteVolume(_ context.Context, _ storage.Volume) error {
	atomic.AddInt32(&r.deleted, 1)

	return nil
}

// TestToggleDiskCancelUnwindsPartialState pins Bug 40: when the REST
// shim writes Spec.ToggleDiskCancel=true mid-conversion, the satellite
// reconciler MUST tear down the partially-carved storage (DeleteVolume
// + the drbdadm-down path inside DeleteResource), re-stamp the
// DISKLESS flag on Spec, clear ToggleDiskCancel, and reset
// Status.ToggleDiskRetries to 0.
//
// Setup: Resource has Status.Volumes populated (storage was carved on
// an earlier reconcile) and Status.ToggleDiskRetries=3 (the prior
// conversion attempt failed three times before the operator gave up
// and asked for a cancel). The DISKLESS flag is absent (Spec asks
// for diskful) and Spec.ToggleDiskCancel=true is the cancel intent
// the REST handler stamped.
//
// Expected end-state: Spec.Flags ∋ DISKLESS, Spec.ToggleDiskCancel=
// false, Status.ToggleDiskRetries=0, DeleteVolume called at least
// once (the rollback path).
func TestToggleDiskCancelUnwindsPartialState(t *testing.T) {
	t.Parallel()

	const (
		rdName = "pvc-cancel"
		node   = "n-cancel"
		pool   = "lvm-thin"
	)

	scheme := newToggleDiskTestScheme(t)

	rd := makeStorageOnlyRD(rdName)
	res := makeToggledResource(rdName, node, pool)
	res.Spec.ToggleDiskCancel = true
	// Simulate a Resource that was already mid-conversion: storage
	// was carved on a prior reconcile (DevicePath populated) and the
	// retry counter has been bumped to 3 by prior failures.
	res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DevicePath: "/dev/fake/" + rdName, AllocatedKib: 65536, DiskState: "Inconsistent"},
	}
	res.Status.ToggleDiskRetries = 3

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rd, res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	provider := &recordingDeleteProvider{}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: provider},
		NodeName:  node,
	})

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName: node,
			Apply:    rec,
			Exec:     storage.NewFakeExec(),
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: res.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile cancel: %v", err)
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: res.Name}, &after)
	if err != nil {
		t.Fatalf("Get after cancel: %v", err)
	}

	if !slices.Contains(after.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag NOT re-stamped on cancel: %v", after.Spec.Flags)
	}

	if after.Spec.ToggleDiskCancel {
		t.Errorf("ToggleDiskCancel still true after rollback: %+v", after.Spec)
	}

	if after.Status.ToggleDiskRetries != 0 {
		t.Errorf("ToggleDiskRetries NOT cleared on cancel: got %d, want 0",
			after.Status.ToggleDiskRetries)
	}

	if got := atomic.LoadInt32(&provider.deleted); got == 0 {
		t.Errorf("DeleteVolume not invoked during cancel rollback")
	}
}

// TestPatchToggleDiskRetriesIsFieldSurgical pins Bug 293 (P0,
// data-correctness): the satellite's `patchToggleDiskRetries`
// must NOT issue a wholesale Status subresource REPLACE — every
// Status field the local snapshot is missing would otherwise get
// wiped from the apiserver, including the controller-side
// allocator's `DRBDNodeID` / `DRBDPort` / `DRBDMinor`. Race window:
// the controller stamps the IDs via SSA, the c-r informer cache
// trails, the satellite reconcile fires on an Apply failure
// mid-conversion, reads the pre-allocation cached snapshot, and a
// wholesale `Status().Update` overwrites Status with the cached
// nil-IDs. Subsequent reconciles wedge on
// `waitForControllerAllocation` logging `nodeID:null port:null
// minor:null` indefinitely; surface symptom is
// `recovery-down-reverses.sh` timing out at 30s.
//
// Assertion: the helper must issue a JSON merge-patch whose body
// is scoped to `status.toggleDiskRetries` ONLY. We pin this via
// the controller-runtime fake client's SubResourcePatch
// interceptor: every Status-subresource patch the helper writes is
// captured, decoded, and inspected. The patch body MUST NOT carry
// keys for any other Status field. The fake client's own merge
// logic is too lenient to catch the bug end-to-end (it preserves
// fields the patch doesn't mention even when the writer would have
// issued a full Update), so we assert at the patch-body level
// instead — the apiserver-side wholesale-replace semantic is well
// known and surfaces exactly when the body carries fields beyond
// the one being mutated.
func TestPatchToggleDiskRetriesIsFieldSurgical(t *testing.T) {
	t.Parallel()

	const (
		rdName = "down-reverses"
		node   = "n-revive"
	)

	resName := rdName + "." + node

	scheme := newToggleDiskTestScheme(t)

	// Apiserver-side: Resource carries the controller-allocator's
	// DRBD-IDs. The fake client persists them; the assertion below
	// then independently checks the patch body the helper would
	// have sent to a real apiserver.
	nodeID := int32(1)
	port := int32(7000)
	minor := int32(1000)

	apiserverRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       resName,
			Finalizers: []string{SatelliteResourceFinalizer},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               node,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: &nodeID,
			DRBDPort:   &port,
			DRBDMinor:  &minor,
		},
	}

	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(apiserverRes).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	// Capture every Status subresource patch the helper issues so
	// the body shape can be asserted. We need the FULL JSON body —
	// patch.Type() must be JSON merge-patch, and the decoded body
	// must contain `status.toggleDiskRetries` and NOTHING ELSE
	// under `status`.
	type captured struct {
		patchType types.PatchType
		body      []byte
	}

	var patches []captured

	cli := interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string,
			obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			body, err := patch.Data(obj)
			if err != nil {
				return errors.Wrap(err, "extract patch body")
			}

			patches = append(patches, captured{
				patchType: patch.Type(),
				body:      body,
			})

			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
		// Bug 293 regression guard: any Status().Update (the pre-fix
		// wholesale-replace shape) is the bug. Fail fast so the test
		// surfaces a clear message instead of a roundabout assertion
		// failure later on patch-count.
		SubResourceUpdate: func(_ context.Context, _ client.Client, _ string,
			obj client.Object, _ ...client.SubResourceUpdateOption,
		) error {
			return errors.Newf("forbidden Status subresource Update on %s/%s "+
				"(Bug 293: pre-fix wholesale-replace would wipe DRBD-IDs); "+
				"writers must use a JSON merge-patch scoped to the mutated field",
				obj.GetNamespace(), obj.GetName())
		},
	})

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{NodeName: node},
	}

	// Caller's "stale cached snapshot": no DRBD-IDs visible. The
	// helper MUST NOT carry these nil-IDs into the patch body —
	// the JSON merge-patch must mention `toggleDiskRetries` only.
	staleCopy := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       resName,
			Finalizers: []string{SatelliteResourceFinalizer},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               node,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			ToggleDiskRetries: 0,
		},
	}

	err := reconciler.patchToggleDiskRetries(context.Background(), staleCopy, 1)
	if err != nil {
		t.Fatalf("patchToggleDiskRetries: %v", err)
	}

	if len(patches) != 1 {
		t.Fatalf("Status subresource patch count: got %d, want 1", len(patches))
	}

	got := patches[0]

	// Bug 293 invariant #1: the helper MUST use a JSON merge-patch
	// (NOT a Status().Update / Apply with the full object). Update
	// is the pre-fix shape that wipes adjacent Status fields.
	if got.patchType != types.MergePatchType {
		t.Errorf("patch type: got %q, want %q (JSON merge-patch)",
			got.patchType, types.MergePatchType)
	}

	// Bug 293 invariant #2: the body must mention `toggleDiskRetries`
	// ONLY under `status`. Any other key (drbdPort, drbdMinor,
	// drbdNodeId, volumes, conditions, …) implies the writer is
	// echoing the caller's stale snapshot, which on a real apiserver
	// would replace the wholesale Status subresource and wipe the
	// allocator's DRBD-IDs.
	var decoded struct {
		Status map[string]any `json:"status"`
	}

	err = json.Unmarshal(got.body, &decoded)
	if err != nil {
		t.Fatalf("decode patch body %q: %v", got.body, err)
	}

	if len(decoded.Status) != 1 {
		t.Errorf("patch body has %d Status keys, want 1; body=%s",
			len(decoded.Status), got.body)
	}

	if _, ok := decoded.Status["toggleDiskRetries"]; !ok {
		t.Errorf("patch body missing toggleDiskRetries; body=%s", got.body)
	}

	// Belt-and-suspenders: pin every controller-allocator key
	// individually — these are the exact fields the pre-fix bug
	// wiped.
	for _, banned := range []string{"drbdNodeId", "drbdPort", "drbdMinor"} {
		if _, ok := decoded.Status[banned]; ok {
			t.Errorf("patch body carries forbidden key %q (Bug 293 regression); body=%s",
				banned, got.body)
		}
	}

	// Caller's local copy must reflect the new retry value so an
	// in-reconcile follow-up read doesn't see a stale 0.
	if staleCopy.Status.ToggleDiskRetries != 1 {
		t.Errorf("local copy ToggleDiskRetries: got %d, want 1",
			staleCopy.Status.ToggleDiskRetries)
	}
}
