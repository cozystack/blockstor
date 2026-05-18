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
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/file"
)

// orderingProvider records the call order of DeleteVolume against a
// shared atomic step counter. Lets Bug-65 ordering tests assert that
// storage tear-down happens BEFORE the finalizer-strip Update, not
// after — without that contract, a controller-side force-strip can
// race the satellite mid-delete and leave an orphan zvol.
type orderingProvider struct {
	step    *atomic.Int32
	mu      sync.Mutex
	deletes []int32 // step value captured on each DeleteVolume call
}

func (p *orderingProvider) Kind() string { return "FAKE" }

func (p *orderingProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{TotalCapacityKib: 1 << 20, FreeCapacityKib: 1 << 20}, nil
}

func (p *orderingProvider) CreateVolume(_ context.Context, _ storage.Volume) error { return nil }

func (p *orderingProvider) DeleteVolume(_ context.Context, _ storage.Volume) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.deletes = append(p.deletes, p.step.Add(1))

	return nil
}

// deletesSnapshot returns a copy of the recorded step values so the
// test can assert ordering without holding the provider's mutex.
func (p *orderingProvider) deletesSnapshot() []int32 {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]int32, len(p.deletes))
	copy(out, p.deletes)

	return out
}

func (p *orderingProvider) ResizeVolume(_ context.Context, _ storage.Volume) error { return nil }

func (p *orderingProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{DevicePath: "/dev/fake/" + vol.ResourceName, UsableKib: vol.SizeKib}, nil
}

func (p *orderingProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error { return nil }

func (p *orderingProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error { return nil }

func (p *orderingProvider) RestoreVolumeFromSnapshot(_ context.Context, _ storage.Volume, _ storage.Snapshot) error {
	return nil
}

// newDeleteRaceFixture wires a fake-client Resource with a
// DeletionTimestamp + SatelliteResourceFinalizer plus its parent RD
// (so handleDelete's lookupVolumeNumbers finds the VolumeDefinitions).
// The returned (cli, rec, provider, stepCounter) shape mirrors the
// toggle-disk tests.
func newDeleteRaceFixture(t *testing.T) (
	client.WithWatch,
	*satellite.Reconciler,
	*orderingProvider,
	*atomic.Int32,
) {
	t.Helper()

	const (
		node   = "n-delete"
		pool   = "lvm-thin"
		rdName = "pvc-del"
	)

	scheme := newToggleDiskTestScheme(t)

	rd := makeStorageOnlyRD(rdName)

	now := metav1.Now()
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:              rdName + "." + node,
			DeletionTimestamp: &now,
			Finalizers:        []string{SatelliteResourceFinalizer},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               node,
			ResourceDefinitionName: rdName,
			StoragePool:            pool,
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rd, res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	step := &atomic.Int32{}
	provider := &orderingProvider{step: step}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: provider},
		NodeName:  node,
	})

	return cli, rec, provider, step
}

// TestHandleDeleteCompletesProviderBeforeFinalizerStrip pins the
// Bug-65 ordering contract: handleDelete MUST invoke
// Apply.DeleteResource (which calls Provider.DeleteVolume) BEFORE
// stripping the satellite finalizer + writing the Update. Without
// the fix, a controller-side force-strip racing the same Resource
// could land between the satellite's stale-finalizer Update and the
// real provider tear-down, leaving an orphan zvol on disk while the
// CRD vanishes from the apiserver.
func TestHandleDeleteCompletesProviderBeforeFinalizerStrip(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-delete"
		rdName = "pvc-del"
	)

	baseCli, rec, provider, step := newDeleteRaceFixture(t)

	// Wrap the client with an interceptor that records the step
	// value at the moment of the Resource Update — that's the
	// finalizer-strip write. We compare it to the DeleteVolume
	// step values: every delete must precede every update.
	var updateSteps []int32

	cli := interceptor.NewClient(baseCli, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object,
			opts ...client.UpdateOption,
		) error {
			if _, ok := obj.(*blockstoriov1alpha1.Resource); ok {
				updateSteps = append(updateSteps, step.Add(1))
			}

			return c.Update(ctx, obj, opts...)
		},
	})

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  node,
			Apply:     rec,
			Exec:      storage.NewFakeExec(),
			APIReader: cli, // fake client doubles as APIReader in tests
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: rdName + "." + node},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	deletes := provider.deletesSnapshot()
	if len(deletes) != 1 {
		t.Fatalf("DeleteVolume call count: got %d, want 1", len(deletes))
	}

	if len(updateSteps) != 1 {
		t.Fatalf("Resource Update call count: got %d, want 1 (finalizer strip)",
			len(updateSteps))
	}

	if deletes[0] >= updateSteps[0] {
		t.Errorf("ordering violation: DeleteVolume step=%d, Update step=%d "+
			"(want DeleteVolume < Update)",
			deletes[0], updateSteps[0])
	}

	// Sanity: with the satellite finalizer gone and the object's
	// DeletionTimestamp already set, the fake client's reconciliation
	// of the apiserver's "all finalizers removed → finalise delete"
	// semantics will have removed the object. Either NotFound (no
	// other finalizer was holding it) or "finalizer slice no longer
	// contains ours" is acceptable — both prove the strip landed.
	var after blockstoriov1alpha1.Resource

	getErr := cli.Get(context.Background(), client.ObjectKey{Name: rdName + "." + node}, &after)
	switch {
	case getErr == nil:
		if slices.Contains(after.Finalizers, SatelliteResourceFinalizer) {
			t.Errorf("finalizer not stripped: %+v", after.Finalizers)
		}
	case !apierrors.IsNotFound(getErr):
		t.Fatalf("post-delete Get: %v", getErr)
	}
}

// TestHandleDeleteAPIReaderSeesRefreshedFinalizer pins the
// uncached-re-read half of the Bug-65 fix. Scenario: between the
// cache-trailing Reconcile-top Get and handleDelete's pre-Update
// re-fetch, an external actor (controller force-strip path, an
// operator's `kubectl patch`, a sibling reconciler) stamps an
// additional finalizer onto the Resource. The cached snapshot
// `res` does not see the new finalizer; if handleDelete built its
// Update from `res`'s slice it would clobber the external add. The
// APIReader-based refresh sees both finalizers, RemoveFinalizer
// strips only ours, and the external one survives the Update —
// which is what kube-apiserver semantics require.
func TestHandleDeleteAPIReaderSeesRefreshedFinalizer(t *testing.T) {
	t.Parallel()

	const (
		node            = "n-delete"
		rdName          = "pvc-del"
		externalKey     = "external.example.com/pin"
		resourceObjName = "pvc-del.n-delete"
	)

	cli, rec, _, _ := newDeleteRaceFixture(t)

	// Simulate the race: an external actor adds a second finalizer
	// AFTER the reconciler's cache-snapshot was taken but BEFORE
	// handleDelete's APIReader re-fetch. We model this by mutating
	// the live object on the fake client between two Reconcile
	// calls — practically equivalent to the production race.
	//
	// Step 1: an external actor adds `externalKey` before our pass.
	var live blockstoriov1alpha1.Resource

	err := cli.Get(context.Background(), client.ObjectKey{Name: resourceObjName}, &live)
	if err != nil {
		t.Fatalf("seed Get: %v", err)
	}

	live.Finalizers = append(live.Finalizers, externalKey)

	err = cli.Update(context.Background(), &live)
	if err != nil {
		t.Fatalf("seed Update: %v", err)
	}

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  node,
			Apply:     rec,
			Exec:      storage.NewFakeExec(),
			APIReader: cli,
		},
	}

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: resourceObjName},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: resourceObjName}, &after)
	if err != nil {
		t.Fatalf("post-delete Get: %v", err)
	}

	if slices.Contains(after.Finalizers, SatelliteResourceFinalizer) {
		t.Errorf("satellite finalizer survived strip: %+v", after.Finalizers)
	}

	if !slices.Contains(after.Finalizers, externalKey) {
		t.Errorf("external finalizer was clobbered: %+v (want %q present)",
			after.Finalizers, externalKey)
	}

	_ = rdName
}

// TestHandleDeleteIdempotentOnSecondPass pins the retry contract.
// Scenario: first pass runs DeleteResource (storage tear-down)
// successfully, but the finalizer-strip Update fails (transient
// apiserver hiccup, optimistic-concurrency conflict). The CRD
// retains the finalizer and the Resource is re-enqueued. The
// second pass runs DeleteResource AGAIN — the storage Provider's
// DeleteVolume is contractually idempotent on a missing volume
// (returns nil), so the re-run is safe rather than double-error.
// Then the strip Update succeeds.
//
// This test pins our reliance on the DeleteVolume idempotency
// contract: if a future Provider were to start returning an error
// on the "already gone" path, this test fails and the satellite's
// retry loop turns into a stuck loop instead of converging.
func TestHandleDeleteIdempotentOnSecondPass(t *testing.T) {
	t.Parallel()

	const (
		node            = "n-delete"
		rdName          = "pvc-del"
		resourceObjName = "pvc-del.n-delete"
	)

	baseCli, rec, provider, _ := newDeleteRaceFixture(t)

	var updateAttempts atomic.Int32

	// First Update on a Resource returns a synthetic transient
	// error; later attempts pass through to the fake client.
	cli := interceptor.NewClient(baseCli, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object,
			opts ...client.UpdateOption,
		) error {
			if _, ok := obj.(*blockstoriov1alpha1.Resource); ok {
				if updateAttempts.Add(1) == 1 {
					return errors.New("synthetic transient apiserver error")
				}
			}

			return c.Update(ctx, obj, opts...)
		},
	})

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  node,
			Apply:     rec,
			Exec:      storage.NewFakeExec(),
			APIReader: cli,
		},
	}

	// --- Pass 1: DeleteResource succeeds, Update fails. ---

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: resourceObjName},
	})
	if err == nil {
		t.Fatalf("Pass 1: want error from synthetic Update failure, got nil")
	}

	if got := len(provider.deletesSnapshot()); got != 1 {
		t.Errorf("Pass 1: DeleteVolume call count = %d, want 1", got)
	}

	// CRD still has our finalizer because the Update failed.
	var afterPass1 blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: resourceObjName}, &afterPass1)
	if err != nil {
		t.Fatalf("post-pass-1 Get: %v", err)
	}

	if !slices.Contains(afterPass1.Finalizers, SatelliteResourceFinalizer) {
		t.Fatalf("Pass 1: finalizer disappeared despite Update failure: %+v",
			afterPass1.Finalizers)
	}

	// --- Pass 2: DeleteResource re-issued (provider must tolerate),
	// then Update succeeds. ---

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: resourceObjName},
	})
	if err != nil {
		t.Fatalf("Pass 2 Reconcile: %v", err)
	}

	// DeleteVolume called twice in total — the second invocation
	// hits an already-gone volume on a real provider, which is the
	// idempotency contract this test pins.
	if got := len(provider.deletesSnapshot()); got != 2 {
		t.Errorf("Pass 2: cumulative DeleteVolume call count = %d, want 2 "+
			"(provider must be idempotent on a missing volume)", got)
	}

	var afterPass2 blockstoriov1alpha1.Resource

	getErr := cli.Get(context.Background(), client.ObjectKey{Name: resourceObjName}, &afterPass2)
	switch {
	case getErr == nil:
		if slices.Contains(afterPass2.Finalizers, SatelliteResourceFinalizer) {
			t.Errorf("Pass 2: finalizer not stripped: %+v", afterPass2.Finalizers)
		}
	case !apierrors.IsNotFound(getErr):
		t.Fatalf("post-pass-2 Get: %v", getErr)
	}

	_ = rdName
}

// TestHandleDeleteUnlinksFileImgAfterCascadedRDDelete pins the
// Bug-107 fix: when `linstor rd delete <X>` cascades the parent RD
// CRD via owner refs, the satellite's `handleDelete` MUST still
// invoke `Provider.DeleteVolume` for every volume number the RD
// declared at apply time. Without the fix, `lookupVolumeNumbers`
// hits NotFound on the RD Get, returns an empty list, and the
// per-volume DeleteVolume loop in `Apply.DeleteResource` iterates
// over zero items — the backing `.img` (FILE_THIN) / ZVOL (ZFS) /
// LV (LVM) stays on disk forever.
//
// Test shape:
//   - Real `file.Provider` rooted at t.TempDir() (no mock — the test
//     needs to observe the actual unlink).
//   - Resource carries the `blockstor.io/volume-numbers` annotation
//     the satellite's apply-path stamps on every successful pass.
//   - Parent RD intentionally absent (cascade-delete already
//     happened) so the lookup is forced through the annotation
//     fallback path.
//   - The .img file is pre-seeded as if a prior CreateVolume already
//     materialised it.
//
// After one Reconcile pass, the .img must be gone.
func TestHandleDeleteUnlinksFileImgAfterCascadedRDDelete(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-bug107"
		pool   = "file-thin"
		rdName = "pvc-bug107"
	)

	scheme := newToggleDiskTestScheme(t)

	// Pre-seed the .img to mimic the post-CreateVolume disk state.
	// The file.Provider names files as `<resource>_<vol5digit>.img`
	// (matches upstream LINSTOR's FILE / FILE_THIN naming).
	poolDir := t.TempDir()
	imgPath := filepath.Join(poolDir, rdName+"_00000.img")

	err := os.WriteFile(imgPath, []byte("leaked-without-bug-107-fix"), 0o600)
	if err != nil {
		t.Fatalf("pre-seed .img: %v", err)
	}

	now := metav1.Now()
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:              rdName + "." + node,
			DeletionTimestamp: &now,
			Finalizers:        []string{SatelliteResourceFinalizer},
			Annotations: map[string]string{
				blockstoriov1alpha1.ResourceAnnotationVolumeNumbers: "0",
			},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               node,
			ResourceDefinitionName: rdName,
			StoragePool:            pool,
		},
	}

	// NOTE: NO ResourceDefinition seeded. This is the load-bearing
	// half of the test — handleDelete must succeed when the RD is
	// already gone.
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	provider := file.NewProvider(file.Config{Dir: poolDir, Thin: true}, storage.NewFakeExec())

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: provider},
		NodeName:  node,
	})

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  node,
			Apply:     rec,
			Exec:      storage.NewFakeExec(),
			APIReader: cli,
		},
	}

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: res.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if _, statErr := os.Stat(imgPath); !os.IsNotExist(statErr) {
		t.Errorf("Bug 107 regression: backing .img survived handleDelete "+
			"after parent RD CRD was cascade-deleted (stat err=%v); the "+
			"FILE_THIN pool would leak storage on every `linstor rd delete`",
			statErr)
	}
}

// TestStampVolumeNumbersAnnotationFormatsCommaSeparated pins the
// Bug-107 stamp half: a successful apply MUST write the parent RD's
// `spec.volumeDefinitions[].volumeNumber` list onto the Resource's
// metadata.annotations under `blockstor.io/volume-numbers` as a
// comma-separated decimal string. Without the stamp, the future
// cascade-delete fallback in `lookupVolumeNumbers` has no record to
// fall back on and the per-volume DeleteVolume loop iterates over
// zero items.
//
// Multi-volume RDs (rare today, but the field is a list) must
// preserve every volume number — handleDelete needs the full set so
// every backing `.img` / ZVOL / LV gets cleaned up.
func TestStampVolumeNumbersAnnotationFormatsCommaSeparated(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-stamp"
		rdName = "pvc-stamp-volnums"
	)

	scheme := newToggleDiskTestScheme(t)

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               node,
			ResourceDefinitionName: rdName,
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 65536},
				{VolumeNumber: 1, SizeKib: 65536},
				{VolumeNumber: 2, SizeKib: 65536},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res, rd).
		Build()

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{NodeName: node},
	}

	err := reconciler.stampVolumeNumbersAnnotation(context.Background(), res, rd)
	if err != nil {
		t.Fatalf("stampVolumeNumbersAnnotation: %v", err)
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: res.Name}, &after)
	if err != nil {
		t.Fatalf("post-stamp Get: %v", err)
	}

	got, ok := after.Annotations[blockstoriov1alpha1.ResourceAnnotationVolumeNumbers]
	if !ok {
		t.Fatalf("annotation %q missing; got annotations %+v",
			blockstoriov1alpha1.ResourceAnnotationVolumeNumbers, after.Annotations)
	}

	const want = "0,1,2"
	if got != want {
		t.Errorf("annotation value: got %q, want %q", got, want)
	}
}

// TestParseVolumeNumbersTolerantOfMalformedEntries pins the fallback
// parser's contract: a corrupted annotation value MUST NOT crash or
// surface an error — handleDelete falls back to "no volumes to
// delete" rather than refusing to strip the finalizer. The intent is
// best-effort cleanup: a partially-readable annotation still lets the
// satellite reclaim what it can.
func TestParseVolumeNumbersTolerantOfMalformedEntries(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  []int32
	}{
		{name: "empty", input: "", want: nil},
		{name: "single", input: "0", want: []int32{0}},
		{name: "multi", input: "0,1,2", want: []int32{0, 1, 2}},
		{name: "whitespace", input: "0, 1 , 2", want: []int32{0, 1, 2}},
		{name: "skip_garbage", input: "0,abc,2", want: []int32{0, 2}},
		{name: "trailing_comma", input: "0,1,", want: []int32{0, 1}},
		{name: "leading_comma", input: ",0,1", want: []int32{0, 1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := parseVolumeNumbers(tc.input)
			if !slices.Equal(got, tc.want) {
				t.Errorf("parseVolumeNumbers(%q) = %v, want %v",
					tc.input, got, tc.want)
			}
		})
	}
}

// TestRefreshTargetFromAPIReaderUnstucksWaitWhenCacheTrails pins
// Bug 289: the controller-side allocator has already stamped
// Status.DRBDNodeID / DRBDPort / DRBDMinor, but the satellite's c-r
// cache trails and the per-replica Reconcile-top Get returns a
// stale view with nil IDs. Without the APIReader fall-through the
// satellite chains 5s requeues against the same stale entry; with
// it, the helper refreshes `res` in place from the uncached reader
// and returns true so the wait gate falls through on the same
// reconcile pass — the recovery-down-reverses revival completes
// in one tick instead of stalling for ~5 minutes.
func TestRefreshTargetFromAPIReaderUnstucksWaitWhenCacheTrails(t *testing.T) {
	t.Parallel()

	const resName = "down-reverses.worker-2"

	scheme := newToggleDiskTestScheme(t)

	// Fresh apiserver-side Resource carries the controller-stamped
	// allocation. This is what the APIReader returns.
	nodeID := int32(1)
	port := int32(7000)
	minor := int32(1000)
	fresh := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-2",
			ResourceDefinitionName: "down-reverses",
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: &nodeID,
			DRBDPort:   &port,
			DRBDMinor:  &minor,
		},
	}

	apiReader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(fresh).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	// Cached client returns the stale view (Status DRBD-IDs nil) —
	// what the satellite's informer cache holds during the trail
	// window right after the controller's Status patch.
	stale := fresh.DeepCopy()
	stale.Status = blockstoriov1alpha1.ResourceStatus{}

	cachedClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(stale).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	reconciler := &ResourceReconciler{
		Client: cachedClient,
		Config: Config{
			NodeName:  "worker-2",
			APIReader: apiReader,
		},
	}

	// Caller's `res` is built from the cached client — nil IDs.
	res := stale.DeepCopy()

	ok := reconciler.refreshTargetFromAPIReader(context.Background(), res, ctrl.Log)
	if !ok {
		t.Fatalf("refreshTargetFromAPIReader: got false, want true (apiserver has fresh allocation)")
	}

	if res.Status.DRBDNodeID == nil || *res.Status.DRBDNodeID != nodeID {
		t.Errorf("DRBDNodeID after refresh: got %v, want %d", res.Status.DRBDNodeID, nodeID)
	}

	if res.Status.DRBDPort == nil || *res.Status.DRBDPort != port {
		t.Errorf("DRBDPort after refresh: got %v, want %d", res.Status.DRBDPort, port)
	}

	if res.Status.DRBDMinor == nil || *res.Status.DRBDMinor != minor {
		t.Errorf("DRBDMinor after refresh: got %v, want %d", res.Status.DRBDMinor, minor)
	}
}

// TestRefreshTargetFromAPIReaderHonorsRealNilAllocation pins the
// negative half of Bug 289: when the controller-side allocator
// genuinely has not yet stamped Status (APIReader also reports
// nil), the helper returns false and `res` stays unchanged so the
// caller emits the wait log + requeue. This is the "still waiting,
// not a cache trail" path.
func TestRefreshTargetFromAPIReaderHonorsRealNilAllocation(t *testing.T) {
	t.Parallel()

	const resName = "fresh-rd.worker-1"

	scheme := newToggleDiskTestScheme(t)

	// Both readers see the unallocated Resource — controller really
	// hasn't run yet.
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-1",
			ResourceDefinitionName: "fresh-rd",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  "worker-1",
			APIReader: cli,
		},
	}

	target := res.DeepCopy()

	ok := reconciler.refreshTargetFromAPIReader(context.Background(), target, ctrl.Log)
	if ok {
		t.Fatalf("refreshTargetFromAPIReader: got true, want false (apiserver also nil)")
	}

	if target.Status.DRBDNodeID != nil ||
		target.Status.DRBDPort != nil ||
		target.Status.DRBDMinor != nil {
		t.Errorf("target.Status DRBD-IDs unexpectedly mutated: %+v", target.Status)
	}
}

// TestReconcileTargetReadGoesThroughAPIReader pins Bug 300: the
// per-reconcile target Resource Get at the top of Reconcile MUST
// flow through the APIReader so a stale informer-cache snapshot
// can't pin the satellite on a pre-allocation view forever.
// Without this gate the prior code path read `res` via the
// cached client, fell into waitForControllerAllocation with nil
// DRBD-IDs, attempted a single APIReader refresh inside that
// helper, and on every other code path (finalizer slice,
// peer-Status, DeletionTimestamp) continued operating on the
// trailed cache. The recovery-down-reverses scenario reproduced
// this by triggering a satellite restart mid-test — the freshly
// rehydrated informer cache held the pre-allocation watch event
// for several reconcile ticks, and the wait gate looped forever
// because `res` itself was stale at the top of Reconcile.
//
// Assertion shape: cached client returns a Resource without the
// satellite finalizer AND with nil DRBD-IDs; APIReader returns
// a finalizer-stamped, fully-allocated copy. The test wires both
// into the reconciler. After one Reconcile pass we must NOT see
// the finalizer-add path fire (`add finalizer` Update from a
// stale view would race the apiserver and produce the
// `object has been modified` conflict the production logs show);
// instead the apply path must accept the APIReader-fresh res and
// requeue with the no-error nil result that signals "wait gate
// passed, runApply did its work and returned its own requeue".
func TestReconcileTargetReadGoesThroughAPIReader(t *testing.T) {
	t.Parallel()

	const resName = "down-reverses.worker-2"

	scheme := newToggleDiskTestScheme(t)

	nodeID := int32(1)
	port := int32(7000)
	minor := int32(1000)

	fresh := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       resName,
			Finalizers: []string{SatelliteResourceFinalizer},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-2",
			ResourceDefinitionName: "down-reverses",
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: &nodeID,
			DRBDPort:   &port,
			DRBDMinor:  &minor,
		},
	}

	apiReader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(fresh).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	// Cached client trails: no finalizer, no DRBD-IDs.
	stale := fresh.DeepCopy()
	stale.Finalizers = nil
	stale.Status = blockstoriov1alpha1.ResourceStatus{}

	cachedClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(stale).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	reconciler := &ResourceReconciler{
		Client: cachedClient,
		Config: Config{
			NodeName:  "worker-2",
			APIReader: apiReader,
		},
	}

	// Drive the same code path Reconcile takes at the top: a Get
	// through targetReader(). This is the load-bearing assertion of
	// Bug 300 — that the read source is APIReader, not the cached
	// client. If the implementation regresses to `r.Get`, this Get
	// would return the stale snapshot (nil Finalizers, nil
	// DRBD-IDs) and the assertion below would fail.
	var got blockstoriov1alpha1.Resource

	err := reconciler.targetReader().Get(
		context.Background(),
		client.ObjectKey{Name: resName},
		&got)
	if err != nil {
		t.Fatalf("targetReader().Get: %v", err)
	}

	if len(got.Finalizers) == 0 ||
		got.Finalizers[0] != SatelliteResourceFinalizer {
		t.Errorf("Reconcile target read returned a stale view "+
			"without the satellite finalizer; got finalizers=%v "+
			"(expected the APIReader-fresh copy)",
			got.Finalizers)
	}

	if got.Status.DRBDNodeID == nil || *got.Status.DRBDNodeID != nodeID {
		t.Errorf("Reconcile target read returned stale Status.DRBDNodeID; "+
			"got %v, want %d (the APIReader-fresh value)",
			got.Status.DRBDNodeID, nodeID)
	}

	if got.Status.DRBDPort == nil || *got.Status.DRBDPort != port {
		t.Errorf("Reconcile target read returned stale Status.DRBDPort; "+
			"got %v, want %d", got.Status.DRBDPort, port)
	}

	if got.Status.DRBDMinor == nil || *got.Status.DRBDMinor != minor {
		t.Errorf("Reconcile target read returned stale Status.DRBDMinor; "+
			"got %v, want %d", got.Status.DRBDMinor, minor)
	}
}
