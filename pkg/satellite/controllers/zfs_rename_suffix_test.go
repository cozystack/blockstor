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

package controllers

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// suffixProvider records the rename suffix each delete was given.
type suffixProvider struct {
	orderingProvider

	mu        sync.Mutex
	volumes   []string
	snapshots []string
}

func (p *suffixProvider) DeleteVolume(ctx context.Context, vol storage.Volume) error {
	return p.DeleteVolumeWithSuffix(ctx, vol, "")
}

func (p *suffixProvider) DeleteVolumeWithSuffix(_ context.Context, _ storage.Volume, suffix string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.volumes = append(p.volumes, suffix)

	return nil
}

func (p *suffixProvider) DeleteSnapshot(_ context.Context, snap storage.Snapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.snapshots = append(p.snapshots, snap.DeletedSuffix)

	return nil
}

const suffixNode = "n1"

func suffixFixture(t *testing.T, kind string, resProps map[string]string, snaps ...*blockstoriov1alpha1.Snapshot) (client.Client, *ResourceReconciler, *suffixProvider) {
	t.Helper()

	now := metav1.Now()
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-z." + suffixNode, DeletionTimestamp: &now, Finalizers: []string{SatelliteResourceFinalizer}},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName: suffixNode, ResourceDefinitionName: "pvc-z", StoragePool: "pool1", Props: resProps,
		},
	}
	pool := &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: suffixNode + ".pool1"},
		Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: suffixNode, PoolName: "pool1", ProviderKind: kind},
	}

	objs := make([]client.Object, 0, 3+len(snaps))
	objs = append(objs, res, pool, makeStorageOnlyRD("pvc-z"))

	for _, s := range snaps {
		objs = append(objs, s)
	}

	cli := fake.NewClientBuilder().WithScheme(newToggleDiskTestScheme(t)).WithObjects(objs...).Build()
	provider := &suffixProvider{orderingProvider: orderingProvider{step: &atomic.Int32{}}}

	r := &ResourceReconciler{Client: cli, Config: Config{
		NodeName:  suffixNode,
		Apply:     satellite.NewReconciler(satellite.ReconcilerConfig{Providers: map[string]storage.Provider{"pool1": provider}, NodeName: suffixNode}),
		Exec:      storage.NewFakeExec(),
		APIReader: cli,
	}}

	return cli, r, provider
}

func suffixSnap(name string, nodes []string, props map[string]string) *blockstoriov1alpha1.Snapshot {
	return &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-z." + name},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-z", SnapshotName: name, Nodes: nodes, Props: props,
		},
	}
}

func snapProps(t *testing.T, cli client.Client, name string) map[string]string {
	t.Helper()

	var snap blockstoriov1alpha1.Snapshot

	err := cli.Get(t.Context(), client.ObjectKey{Name: "pvc-z." + name}, &snap)
	if err != nil {
		t.Fatalf("Get snapshot %s: %v", name, err)
	}

	return snap.Spec.Props
}

// A ZFS replica's delete records its rename suffix on this node's Snapshots
// before the provider renames anything, and hands the same suffix to the
// provider, as LINSTOR's controller does before it tells the satellite.
func TestZFSReplicaDeleteRecordsTheRenameSuffix(t *testing.T) {
	t.Parallel()

	key := snapshotRenameSuffixProp(suffixNode)

	cli, r, provider := suffixFixture(t, "ZFS_THIN", nil,
		suffixSnap("mine", []string{"N1", "n2"}, nil),
		suffixSnap("other", []string{"n2"}, nil),
		suffixSnap("older", []string{suffixNode}, map[string]string{key: "2026-01-01T00-00-00-000"}),
	)

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-z." + suffixNode}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if len(provider.volumes) != 1 || provider.volumes[0] == "" {
		t.Fatalf("DeleteVolume suffixes %v, want one recorded suffix", provider.volumes)
	}

	suffix := provider.volumes[0]

	if got := snapProps(t, cli, "mine")[key]; got != suffix {
		t.Errorf("this node's snapshot carries %q, want the suffix the delete used %q", got, suffix)
	}

	if got := snapProps(t, cli, "other")[key]; got != "" {
		t.Errorf("another node's snapshot got a suffix %q", got)
	}

	if got := snapProps(t, cli, "older")[key]; got != "2026-01-01T00-00-00-000" {
		t.Errorf("a snapshot an earlier replica's delete renamed lost its suffix: %q", got)
	}
}

// A delete that is retried reuses the suffix it recorded on the Resource, so
// a crash between the record and the rename cannot split the two.
func TestZFSReplicaDeleteReusesTheRecordedSuffix(t *testing.T) {
	t.Parallel()

	const recorded = "2026-03-04T05-06-07-089"

	cli, r, provider := suffixFixture(t, "ZFS", map[string]string{ZFSRenameSuffixProp: recorded},
		suffixSnap("mine", []string{suffixNode}, nil))

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-z." + suffixNode}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if len(provider.volumes) != 1 || provider.volumes[0] != recorded {
		t.Errorf("DeleteVolume suffixes %v, want [%s]", provider.volumes, recorded)
	}

	if got := snapProps(t, cli, "mine")[snapshotRenameSuffixProp(suffixNode)]; got != recorded {
		t.Errorf("snapshot carries %q, want %q", got, recorded)
	}
}

// Only ZFS renames, so only a ZFS replica records a suffix.
func TestNonZFSReplicaDeleteRecordsNoSuffix(t *testing.T) {
	t.Parallel()

	cli, r, provider := suffixFixture(t, "LVM_THIN", nil, suffixSnap("mine", []string{suffixNode}, nil))

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-z." + suffixNode}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if len(provider.volumes) != 1 || provider.volumes[0] != "" {
		t.Errorf("DeleteVolume suffixes %v, want one empty", provider.volumes)
	}

	if got := snapProps(t, cli, "mine"); len(got) != 0 {
		t.Errorf("snapshot props %v, want none", got)
	}
}

// A snapshot delete hands the provider the suffix recorded for this node.
func TestSnapshotDeleteUsesTheRecordedSuffix(t *testing.T) {
	t.Parallel()

	const recorded = "2026-03-04T05-06-07-089"

	now := metav1.Now()
	snap := suffixSnap("mine", []string{suffixNode}, map[string]string{snapshotRenameSuffixProp(suffixNode): recorded})
	snap.DeletionTimestamp = &now
	snap.Finalizers = []string{snapshotNodeFinalizer(suffixNode)}

	cli, r, provider := suffixFixture(t, "ZFS", nil, snap)

	snapshots := &SnapshotReconciler{Client: cli, Config: r.Config}

	_, err := snapshots.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-z.mine"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if len(provider.snapshots) != 1 || provider.snapshots[0] != recorded {
		t.Errorf("DeleteSnapshot suffixes %v, want [%s]", provider.snapshots, recorded)
	}
}
