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
	"sync"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/storage"
)

// fakeListerProvider is a minimal storage.Provider + storage.VolumeLister
// stub used to drive the sweeper without spinning up real ZFS/LVM.
//
// The harness records each DeleteVolume call so each test can pin the
// exact set of orphans the sweeper reaped, without relying on FakeExec
// command-line parsing (which would couple the test to the
// implementation choice of zfs-vs-lvm for the orphan path).
type fakeListerProvider struct {
	mu      sync.Mutex
	volumes []storage.VolumeRef
	deleted []storage.Volume
	listErr error
	delErr  error
}

func (f *fakeListerProvider) Kind() string {
	return "FAKE"
}

func (f *fakeListerProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{}, nil
}

func (f *fakeListerProvider) CreateVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (f *fakeListerProvider) DeleteVolume(_ context.Context, vol storage.Volume) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.delErr != nil {
		return f.delErr
	}

	f.deleted = append(f.deleted, vol)

	return nil
}

func (f *fakeListerProvider) ResizeVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (f *fakeListerProvider) VolumeStatus(_ context.Context, _ storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{}, nil
}

func (f *fakeListerProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (f *fakeListerProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (f *fakeListerProvider) RestoreVolumeFromSnapshot(_ context.Context, _ storage.Volume, _ storage.Snapshot) error {
	return nil
}

func (f *fakeListerProvider) ListVolumeNames(_ context.Context) ([]storage.VolumeRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.listErr != nil {
		return nil, f.listErr
	}

	return append([]storage.VolumeRef(nil), f.volumes...), nil
}

func (f *fakeListerProvider) snapshotDeleted() []storage.Volume {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]storage.Volume(nil), f.deleted...)
}

// storageSweeperFixture wires up the standard test rig: fake k8s
// client preloaded with a Node CRD + supplied Resources/RDs, a
// providers map carrying a single fake provider, and a constructed
// runnable ready for sweepOnce. Centralises the test boilerplate.
func storageSweeperFixture(
	t *testing.T,
	nodeName string,
	provider *fakeListerProvider,
	rds []*blockstoriov1alpha1.ResourceDefinition,
	resources []*blockstoriov1alpha1.Resource,
) *StorageOrphanSweeperRunnable {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	objs := make([]client.Object, 0, len(rds)+len(resources)+1)
	objs = append(objs, &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	})

	for _, rd := range rds {
		objs = append(objs, rd)
	}

	for _, r := range resources {
		objs = append(objs, r)
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()

	return &StorageOrphanSweeperRunnable{
		Client: cli,
		Providers: func() map[string]storage.Provider {
			return map[string]storage.Provider{"pool0": provider}
		},
		NodeName: nodeName,
	}
}

// TestStorageSweeperLeavesOwnedVolumeAlone pins the core invariant
// — when a volume on disk has a matching Resource CRD on this node,
// the sweeper MUST NOT call DeleteVolume. A regression here would
// destroy live PV data on every tick: the worst possible failure
// mode for this code.
func TestStorageSweeperLeavesOwnedVolumeAlone(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-aaa"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 65536},
			},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-aaa.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "n1",
			ResourceDefinitionName: "pvc-aaa",
		},
	}

	provider := &fakeListerProvider{
		volumes: []storage.VolumeRef{
			{ResourceName: "pvc-aaa", VolumeNumber: 0},
		},
	}

	sweeper := storageSweeperFixture(t, "n1", provider,
		[]*blockstoriov1alpha1.ResourceDefinition{rd},
		[]*blockstoriov1alpha1.Resource{res})

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if got := provider.snapshotDeleted(); len(got) != 0 {
		t.Errorf("sweeper destroyed owned volume: %+v", got)
	}
}

// TestStorageSweeperReapsOrphan pins the load-bearing case — Bug 43.
// A ZVOL exists on disk but its owning Resource CRD has been
// force-deleted; the sweeper MUST call DeleteVolume. Without this,
// every force-strip event leaks a ZVOL until manual cleanup.
func TestStorageSweeperReapsOrphan(t *testing.T) {
	t.Parallel()

	provider := &fakeListerProvider{
		volumes: []storage.VolumeRef{
			{ResourceName: "pvc-orphan", VolumeNumber: 0},
		},
	}

	sweeper := storageSweeperFixture(t, "n1", provider, nil, nil)

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	got := provider.snapshotDeleted()
	if len(got) != 1 {
		t.Fatalf("expected 1 DeleteVolume, got %d (%+v)", len(got), got)
	}

	if got[0].ResourceName != "pvc-orphan" {
		t.Errorf("DeleteVolume on wrong resource: %s", got[0].ResourceName)
	}

	if got[0].PoolName != "pool0" {
		t.Errorf("DeleteVolume with empty PoolName: %+v", got[0])
	}
}

// TestStorageSweeperLeavesForeignVolumesAlone pins the prefix
// allowlist — operator-created datasets that happen to share the
// pool but don't match `pvc-` / `bs-` MUST survive the sweep.
// Defence in depth against parser regressions.
func TestStorageSweeperLeavesForeignVolumesAlone(t *testing.T) {
	t.Parallel()

	provider := &fakeListerProvider{
		volumes: []storage.VolumeRef{
			{ResourceName: "operator-data", VolumeNumber: 0},
		},
	}

	sweeper := storageSweeperFixture(t, "n1", provider, nil, nil)

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if got := provider.snapshotDeleted(); len(got) != 0 {
		t.Errorf("sweeper destroyed non-blockstor volume: %+v", got)
	}
}

// TestStorageSweeperOnlyConsidersLocalResources pins the per-node
// scope: a Resource CRD that exists for the same RD but on a
// different node MUST NOT protect this node's on-disk volume from
// being reaped — local storage state is per-node.
func TestStorageSweeperOnlyConsidersLocalResources(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-xxx"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 65536},
			},
		},
	}

	// CRD says the RD lives on n2; from n1's PoV the local
	// on-disk volume is an orphan.
	foreign := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-xxx.n2"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "n2",
			ResourceDefinitionName: "pvc-xxx",
		},
	}

	provider := &fakeListerProvider{
		volumes: []storage.VolumeRef{
			{ResourceName: "pvc-xxx", VolumeNumber: 0},
		},
	}

	sweeper := storageSweeperFixture(t, "n1", provider,
		[]*blockstoriov1alpha1.ResourceDefinition{rd},
		[]*blockstoriov1alpha1.Resource{foreign})

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	got := provider.snapshotDeleted()
	if len(got) != 1 {
		t.Fatalf("expected 1 DeleteVolume (foreign-CRD orphan), got %d", len(got))
	}
}

// TestStorageSweeperRespectsRateLimit pins the bound on per-cycle
// destruction. Setting MaxDeletePerCycle=2 with 10 orphans MUST
// cap the reap at 2 and defer the rest to the next tick.
func TestStorageSweeperRespectsRateLimit(t *testing.T) {
	t.Parallel()

	vols := make([]storage.VolumeRef, 0, 10)
	for i := range 10 {
		vols = append(vols, storage.VolumeRef{
			ResourceName: "pvc-churn-" + strItoa(i),
			VolumeNumber: 0,
		})
	}

	provider := &fakeListerProvider{volumes: vols}

	sweeper := storageSweeperFixture(t, "n1", provider, nil, nil)
	sweeper.MaxDeletePerCycle = 2

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	got := provider.snapshotDeleted()
	if len(got) != 2 {
		t.Errorf("rate-limit: got %d DeleteVolume, want 2", len(got))
	}
}

// TestStorageSweeperSkipAnnotation pins the operator escape hatch:
// when the local Node CRD carries StorageSweeperSkipAnnotation,
// the sweeper MUST do nothing even with orphans present.
func TestStorageSweeperSkipAnnotation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "n1",
			Annotations: map[string]string{StorageSweeperSkipAnnotation: sweeperSkipValue},
		},
		Spec: blockstoriov1alpha1.NodeSpec{Type: "SATELLITE"},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		Build()

	provider := &fakeListerProvider{
		volumes: []storage.VolumeRef{
			{ResourceName: "pvc-orphan", VolumeNumber: 0},
		},
	}

	sweeper := &StorageOrphanSweeperRunnable{
		Client: cli,
		Providers: func() map[string]storage.Provider {
			return map[string]storage.Provider{"pool0": provider}
		},
		NodeName: "n1",
	}

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if got := provider.snapshotDeleted(); len(got) != 0 {
		t.Errorf("sweeper ignored skip annotation: %+v", got)
	}
}

// TestStorageSweeperMidDeleteRaceProtection pins the key bug-43
// invariant: when a Resource CRD still EXISTS with a non-zero
// DeletionTimestamp, the satellite's handleDelete is responsible
// for the storage cleanup — the sweeper MUST NOT race it. The
// sweep only reaps volumes whose CRD has fully vanished
// (force-strip aftermath).
func TestStorageSweeperMidDeleteRaceProtection(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mid"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 65536},
			},
		},
	}

	now := metav1.Now()
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-mid.n1",
			DeletionTimestamp: &now,
			Finalizers:        []string{SatelliteResourceFinalizer},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "n1",
			ResourceDefinitionName: "pvc-mid",
		},
	}

	provider := &fakeListerProvider{
		volumes: []storage.VolumeRef{
			{ResourceName: "pvc-mid", VolumeNumber: 0},
		},
	}

	sweeper := storageSweeperFixture(t, "n1", provider,
		[]*blockstoriov1alpha1.ResourceDefinition{rd},
		[]*blockstoriov1alpha1.Resource{res})

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if got := provider.snapshotDeleted(); len(got) != 0 {
		t.Errorf("sweeper raced mid-delete handleDelete: %+v", got)
	}
}

// TestStorageSweeperHandlesMissingRD pins the wildcard-owned path:
// a Resource CRD survives but its parent RD has been
// cascade-deleted out of order. The volume is still in flight to
// the handleDelete path; the sweeper MUST NOT race it.
func TestStorageSweeperHandlesMissingRD(t *testing.T) {
	t.Parallel()

	// No RD — only the Resource CRD.
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-noparent.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "n1",
			ResourceDefinitionName: "pvc-noparent",
		},
	}

	provider := &fakeListerProvider{
		volumes: []storage.VolumeRef{
			{ResourceName: "pvc-noparent", VolumeNumber: 0},
		},
	}

	sweeper := storageSweeperFixture(t, "n1", provider, nil,
		[]*blockstoriov1alpha1.Resource{res})

	err := sweeper.sweepOnce(t.Context(), logr.Discard())
	if err != nil {
		t.Fatalf("sweepOnce: %v", err)
	}

	if got := provider.snapshotDeleted(); len(got) != 0 {
		t.Errorf("sweeper raced missing-RD handleDelete: %+v", got)
	}
}

// strItoa is a tiny test-local int-to-string helper so the table
// rows in TestStorageSweeperRespectsRateLimit stay readable. Using
// strconv here would force an import only this one test needs.
func strItoa(n int) string {
	if n == 0 {
		return "0"
	}

	var neg bool
	if n < 0 {
		neg = true
		n = -n
	}

	var buf [20]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	if neg {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}
