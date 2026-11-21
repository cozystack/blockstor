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

package controller_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/blocksize"
	"github.com/cozystack/blockstor/pkg/store"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
)

const (
	bsRD   = "pvc-bs"
	bsPool = "zfs-thin"
	bsNode = "n1"
)

// bsFixture is one definition with one volume, a 4096 pool on n1 and
// whatever else a case adds.
type bsFixture struct {
	rd      *blockstoriov1alpha1.ResourceDefinition
	objects []client.Object
}

func newBSFixture(vdProps map[string]string) *bsFixture {
	return &bsFixture{
		rd: &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: bsRD},
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				LayerStack: []string{"DRBD", "STORAGE"},
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
					{VolumeNumber: 0, SizeKib: 1 << 20, Props: vdProps},
				},
			},
		},
		objects: []client.Object{
			bsStoragePool(bsNode, "4096"),
			bsStoragePool("n2", "4096"),
		},
	}
}

func bsStoragePool(node, minIoSize string) *blockstoriov1alpha1.StoragePool {
	return &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: node + "." + bsPool},
		Spec: blockstoriov1alpha1.StoragePoolSpec{
			NodeName:     node,
			PoolName:     bsPool,
			ProviderKind: "ZFS_THIN",
			Props:        map[string]string{blocksize.PropPoolMinIoSize: minIoSize},
		},
	}
}

func bsReplica(node string, flags ...string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: bsRD + "." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: bsRD,
			NodeName:               node,
			StoragePool:            bsPool,
			Flags:                  flags,
		},
	}
}

// running marks a replica as one the controller already gave its DRBD
// identity, so the satellite may run it.
func running(res *blockstoriov1alpha1.Resource) *blockstoriov1alpha1.Resource {
	id := int32(1)
	port := int32(7000)
	res.Spec.DRBDNodeID = &id
	res.Spec.DRBDPort = &port
	res.Status.DRBDNodeID = &id
	res.Status.DRBDPort = &port

	return res
}

// reconcile builds the client, reconciles target a few times so the
// requeues after allocation run too, and returns the definition's
// volume 0 props.
func (f *bsFixture) reconcile(t *testing.T, target *blockstoriov1alpha1.Resource) map[string]string {
	t.Helper()

	ctx := context.Background()
	scheme := newScheme(t)

	objects := append([]client.Object{f.rd, target}, f.objects...)

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(objects...).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	for range 4 {
		_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: target.Name}})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}

	var rd blockstoriov1alpha1.ResourceDefinition

	err := cli.Get(ctx, client.ObjectKey{Name: bsRD}, &rd)
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	return rd.Spec.VolumeDefinitions[0].Props
}

// TestNewReplicaTakesPoolBlockSize pins the issue: the first replica of a
// new volume lands on a pool whose minimum I/O size is 4096, and the
// volume definition gets block-size 4096 before the replica is given its
// identity, as LINSTOR's resource create does. Without it the definition
// stayed bare and DRBD came up with 512-byte sectors on the zvol.
func TestNewReplicaTakesPoolBlockSize(t *testing.T) {
	t.Parallel()

	f := newBSFixture(nil)

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "4096" {
		t.Fatalf("block-size = %q, want 4096 (props %v)", got, props)
	}

	if blocksize.Frozen(props) {
		t.Fatalf("a replica nobody uses froze the size: %v", props)
	}
}

// TestNewReplicaOnPoolWithoutRecordedSize: a pool with no recorded size
// counts as 512, and LINSTOR still writes it down.
func TestNewReplicaOnPoolWithoutRecordedSize(t *testing.T) {
	t.Parallel()

	f := newBSFixture(nil)
	f.objects = []client.Object{bsStoragePool(bsNode, "")}

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "512" {
		t.Fatalf("block-size = %q, want 512", got)
	}
}

// TestNewDisklessReplicaCountsAsDefault: a diskless replica has no pool,
// so it counts as 512 like LINSTOR's diskless pool.
func TestNewDisklessReplicaCountsAsDefault(t *testing.T) {
	t.Parallel()

	f := newBSFixture(nil)

	props := f.reconcile(t, bsReplica(bsNode, "DISKLESS"))

	if got := props[blocksize.PropBlockSize]; got != "512" {
		t.Fatalf("block-size = %q, want 512", got)
	}
}

// TestSpecialLayerCountsAs4096: a stack with a layer other than DRBD and
// STORAGE counts as 4096 whatever the pool says.
func TestSpecialLayerCountsAs4096(t *testing.T) {
	t.Parallel()

	f := newBSFixture(nil)
	f.rd.Spec.LayerStack = []string{"DRBD", "LUKS", "STORAGE"}
	f.objects = []client.Object{bsStoragePool(bsNode, "512")}

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "4096" {
		t.Fatalf("block-size = %q, want 4096", got)
	}
}

// TestAutoBlockSizeOffLeavesDefinition: Linstor/Drbd/auto-block-size
// false on the resource group switches the rule off.
func TestAutoBlockSizeOffLeavesDefinition(t *testing.T) {
	t.Parallel()

	f := newBSFixture(nil)
	f.rd.Spec.ResourceGroupName = "rg-off"
	f.objects = append(f.objects, &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "rg-off"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			Props: map[string]string{blocksize.PropAuto: "false"},
		},
	})

	props := f.reconcile(t, bsReplica(bsNode))

	if _, ok := props[blocksize.PropBlockSize]; ok {
		t.Fatalf("auto-block-size false, yet block-size written: %v", props)
	}
}

// TestNewReplicaOfRunningVolumeWithoutSizeKeepsDefault: a volume created
// before the rule has replicas running at DRBD's default and no size on
// its definition. A new replica gets the default written down instead of
// the pool's 4096, so it matches its peers and the next attach of the
// old ones does not change the sector size under their data.
func TestNewReplicaOfRunningVolumeWithoutSizeKeepsDefault(t *testing.T) {
	t.Parallel()

	f := newBSFixture(nil)
	f.objects = append(f.objects, running(bsReplica("n2")))

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "512" {
		t.Fatalf("block-size = %q, want 512 for a volume already running at the default", got)
	}
}

// TestLargerPoolRaisesWhileNothingRuns: a replica on a larger pool raises
// the size while no other replica has been brought up and none is
// Primary.
func TestLargerPoolRaisesWhileNothingRuns(t *testing.T) {
	t.Parallel()

	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "512"})
	f.objects = append(f.objects, bsReplica("n2", "DISKLESS"))

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "4096" {
		t.Fatalf("block-size = %q, want 4096", got)
	}
}

// TestFrozenSizeIsNotRaised: a frozen size stays, as LINSTOR refuses the
// replica rather than change it.
func TestFrozenSizeIsNotRaised(t *testing.T) {
	t.Parallel()

	f := newBSFixture(map[string]string{
		blocksize.PropBlockSize: "512",
		blocksize.PropFreeze:    "True",
	})
	f.objects = append(f.objects, bsReplica("n2", "DISKLESS"))

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "512" {
		t.Fatalf("block-size = %q, a frozen 512 was changed", got)
	}
}

// TestSizeNotRaisedUnderRunningDiskfulPeer: raising under a diskful peer
// that already runs would need the DRBD restart LINSTOR does, so the size
// stays.
func TestSizeNotRaisedUnderRunningDiskfulPeer(t *testing.T) {
	t.Parallel()

	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "512"})
	f.objects = append(f.objects, running(bsReplica("n2")))

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "512" {
		t.Fatalf("block-size = %q, the size changed under a running diskful peer", got)
	}
}

// TestRestoredDefinitionKeepsSourceSize: a restored definition carries
// its source's size from the snapshot, and the pool does not override it.
func TestRestoredDefinitionKeepsSourceSize(t *testing.T) {
	t.Parallel()

	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "512"})
	f.rd.Spec.Props = map[string]string{store.RestoreFromSnapshotProp: "src:snap"}

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "512" {
		t.Fatalf("block-size = %q, the restored volume lost its source's size", got)
	}
}

// TestPrimaryReplicaFreezesSize pins the freeze: once a replica is in use
// the definition gets Linstor/Drbd/freeze-block-size True, as LINSTOR
// stamps on the first in-use report.
func TestPrimaryReplicaFreezesSize(t *testing.T) {
	t.Parallel()

	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096"})
	target := running(bsReplica(bsNode))
	target.Status.Role = "Primary"
	target.Status.InUse = true

	props := f.reconcile(t, target)

	if !blocksize.Frozen(props) {
		t.Fatalf("a Primary replica left the size unfrozen: %v", props)
	}

	if got := props[blocksize.PropBlockSize]; got != "4096" {
		t.Fatalf("block-size = %q, want 4096", got)
	}
}

// TestSecondaryReplicaDoesNotFreeze: nothing in use, nothing frozen.
func TestSecondaryReplicaDoesNotFreeze(t *testing.T) {
	t.Parallel()

	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096"})
	target := running(bsReplica(bsNode))
	target.Status.Role = "Secondary"

	props := f.reconcile(t, target)

	if blocksize.Frozen(props) {
		t.Fatalf("a Secondary replica froze the size: %v", props)
	}
}

// TestFirstDecisionCoversEveryWaitingReplica: two replicas created
// together, one on a 512 pool and one on a 4096 pool. Whichever reconciles
// first decides for both, as LINSTOR decides every replica of one
// autoplace before any comes up. Deciding for the first alone wrote 512,
// gave it its identity, and then refused the second one's raise because
// a diskful replica already ran.
func TestFirstDecisionCoversEveryWaitingReplica(t *testing.T) {
	t.Parallel()

	f := newBSFixture(nil)
	f.objects = []client.Object{
		bsStoragePool(bsNode, "512"),
		bsStoragePool("n2", "4096"),
		bsReplica("n2"),
	}

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "4096" {
		t.Fatalf("block-size = %q, want 4096 for the replica still waiting on the 4096 pool", got)
	}
}

// TestRunningPeerIsNotPending: a peer that already has its identity was
// decided already and does not raise the size for a new replica.
func TestRunningPeerIsNotPending(t *testing.T) {
	t.Parallel()

	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "512"})
	f.objects = []client.Object{
		bsStoragePool(bsNode, "512"),
		bsStoragePool("n2", "4096"),
		running(bsReplica("n2")),
	}

	props := f.reconcile(t, bsReplica(bsNode))

	if got := props[blocksize.PropBlockSize]; got != "512" {
		t.Fatalf("block-size = %q, want 512", got)
	}
}

var errFreezeRefused = errors.New("admission refused the freeze")

// TestRefusedFreezeStillApplies: a freeze the API server keeps refusing
// is retried, but the in-use replica's housekeeping still runs. Here it
// drops a volume its definition no longer has; returning before the
// apply left that undone for as long as the freeze failed.
func TestRefusedFreezeStillApplies(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	f := newBSFixture(map[string]string{blocksize.PropBlockSize: "4096"})

	target := running(bsReplica(bsNode))
	target.Status.Role = "Primary"
	target.Status.InUse = true
	target.Spec.Volumes = []blockstoriov1alpha1.ResourceVolumeSpec{{VolumeNumber: 0}, {VolumeNumber: 7}}

	base := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(append([]client.Object{f.rd, target}, f.objects...)...).
		Build()

	cli := interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if data, _ := patch.Data(obj); strings.Contains(string(data), blocksize.PropFreeze) {
				return errFreezeRefused
			}

			return c.Patch(ctx, obj, patch, opts...)
		},
	})

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	// The first passes give the replica the rest of its identity and
	// requeue; the freeze comes after.
	var err error

	for range 4 {
		_, err = rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: target.Name}})
	}

	if !errors.Is(err, errFreezeRefused) {
		t.Fatalf("Reconcile: %v, want the freeze error so it is retried", err)
	}

	var got blockstoriov1alpha1.Resource

	err = cli.Get(ctx, client.ObjectKey{Name: target.Name}, &got)
	if err != nil {
		t.Fatalf("get replica: %v", err)
	}

	for _, vol := range got.Spec.Volumes {
		if vol.VolumeNumber == 7 {
			t.Fatalf("the apply did not run while the freeze failed: volumes %v", got.Spec.Volumes)
		}
	}
}

// scopeCountingReader counts the Resource lists that reach it without a
// field selector, reads of every replica in the cluster, until stopped.
type scopeCountingReader struct {
	client.Reader

	mu       sync.Mutex
	unscoped int
	stopped  bool
}

func (r *scopeCountingReader) stop() {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
}

func (r *scopeCountingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*blockstoriov1alpha1.ResourceList); ok {
		var lo client.ListOptions
		lo.ApplyOptions(opts)

		r.mu.Lock()
		if lo.FieldSelector == nil && !r.stopped {
			r.unscoped++
		}
		r.mu.Unlock()
	}

	return r.Reader.List(ctx, list, opts...)
}

// TestBlockSizeReadsOnlyTheDefinitionsReplicas pins how the decision for
// a new replica reads its peers. Listing every Resource in the cluster
// past the cache, once per new replica, made a provisioning burst on a
// cluster with a few hundred replicas spend most of each allocation
// decoding replicas of other volumes, and the satellites build nothing
// before the allocation is done. The peer created a moment ago, which
// the cache does not show yet, must still count.
func TestBlockSizeReadsOnlyTheDefinitionsReplicas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	f := newBSFixture(nil)
	target := bsReplica(bsNode)
	fresh := bsReplica("n2")

	const others = 20

	objects := make([]client.Object, 0, others+4)
	objects = append(objects, f.rd, target, bsStoragePool(bsNode, "512"), bsStoragePool("n2", "4096"))

	for i := range others {
		objects = append(objects, &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pvc-other-%02d.n2", i)},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: fmt.Sprintf("pvc-other-%02d", i), NodeName: "n2"},
		})
	}

	byDefinition := func(obj client.Object) []string {
		res, ok := obj.(*blockstoriov1alpha1.Resource)
		if !ok {
			return nil
		}

		return []string{res.Spec.ResourceDefinitionName}
	}

	build := func(objs ...client.Object) client.WithWatch {
		return fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
			WithIndex(&blockstoriov1alpha1.Resource{}, storek8s.FieldResourceDefinitionName, byDefinition).
			WithObjects(objs...).
			Build()
	}

	live := build(append(slices.Clone(objects), fresh)...)
	cached := build(objects...)

	apiReader := &scopeCountingReader{Reader: live}
	cache := interceptor.NewClient(live, interceptor.Funcs{
		Get: func(ctx context.Context, _ client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return cached.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			return cached.List(ctx, list, opts...)
		},
		// The block size is the first thing written for a new replica;
		// what reads past the cache after it is the allocation's.
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*blockstoriov1alpha1.ResourceDefinition); ok {
				apiReader.stop()
			}

			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	rec := &controllerpkg.ResourceReconciler{Client: cache, Scheme: scheme, APIReader: apiReader}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: target.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var rd blockstoriov1alpha1.ResourceDefinition
	if err := live.Get(ctx, client.ObjectKey{Name: bsRD}, &rd); err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if got := rd.Spec.VolumeDefinitions[0].Props[blocksize.PropBlockSize]; got != "4096" {
		t.Errorf("block-size = %q, want 4096 for the peer created a moment ago on the 4096 pool", got)
	}

	if apiReader.unscoped != 0 {
		t.Errorf("the decision listed every Resource past the cache %d times, want 0", apiReader.unscoped)
	}
}
