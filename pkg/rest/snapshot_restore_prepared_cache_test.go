// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"maps"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// markerBehindCache serves cached reads of a definition without the restore
// marker, the way a controller-runtime cache does until the watch delivers
// the request's own patch. Uncached reads and writes reach the backend.
type markerBehindCache struct{ store.Store }

type markerBehindCacheRDs struct{ store.ResourceDefinitionStore }

func (m markerBehindCache) ResourceDefinitions() store.ResourceDefinitionStore {
	return markerBehindCacheRDs{m.Store.ResourceDefinitions()}
}

func (r markerBehindCacheRDs) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	rd, err := r.ResourceDefinitionStore.Get(ctx, name)
	rd.Props = maps.Clone(rd.Props)
	delete(rd.Props, store.RestoreFromSnapshotProp)

	return rd, err //nolint:wrapcheck // pass-through test double
}

// The prepared path stamps the marker through the API server and the same
// request then judged the target through the cache, which had not seen the
// stamp: linstor-csi's first resource restore was refused as somebody else's.
func TestSnapshotRestoreIntoAPreparedTargetDoesNotReadItsOwnMarkerFromTheCache(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-lag")
	seedRestoreSnapshot(t, backend, "src-lag", "snap-lag", []string{"node-a"})

	base, stop := startServerWithStore(t, markerBehindCache{backend})
	defer stop()

	csiPrepareRestoreTarget(t, base, "src-lag", "snap-lag", "pvc-lag")

	if code := restoreOnce(t, base, "src-lag", "snap-lag", map[string]any{
		"to_resource": "pvc-lag", "nodes": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Fatalf("first CSI resource restore with the marker not yet in the cache = %d, want 201", code)
	}

	replicas, err := backend.Resources().ListByDefinition(ctx, "pvc-lag")
	if err != nil || len(replicas) != 1 || replicas[0].NodeName != "node-a" {
		t.Errorf("replicas after the restore = %v (%v), want one on node-a", replicas, err)
	}
}

// replicaAppearsOnStamp places a live replica on the requested node just after
// the restore marker is patched onto the target: what a concurrent retry of the
// same restore does once it finds the marker this request wrote.
type replicaAppearsOnStamp struct {
	store.Store

	target, node string
}

type replicaAppearsOnStampRDs struct {
	store.ResourceDefinitionStore

	outer replicaAppearsOnStamp
}

func (r replicaAppearsOnStamp) ResourceDefinitions() store.ResourceDefinitionStore {
	return replicaAppearsOnStampRDs{ResourceDefinitionStore: r.Store.ResourceDefinitions(), outer: r}
}

func (r replicaAppearsOnStampRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	err := r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate)
	if err == nil && strings.EqualFold(name, r.outer.target) {
		_ = r.outer.Store.Resources().Create(ctx, &apiv1.Resource{Name: name, NodeName: r.outer.node})
	}

	return err //nolint:wrapcheck // pass-through test double
}

// A replica that appears once the marker is on is what a concurrent retry of
// the same restore places. Refusing over it and taking the marker back off
// left a definition another request had already answered for with no marker.
func TestSnapshotRestoreKeepsItsMarkerOverAReplicaARetryPlaced(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-gain")
	seedRestoreSnapshot(t, backend, "src-gain", "snap-gain", []string{"node-a"})

	plain, stopPlain := startServerWithStore(t, backend)
	csiPrepareRestoreTarget(t, plain, "src-gain", "snap-gain", "pvc-gain")
	stopPlain()

	base, stop := startServerWithStore(t, replicaAppearsOnStamp{Store: backend, target: "pvc-gain", node: "node-a"})
	defer stop()

	if code := restoreOnce(t, base, "src-gain", "snap-gain", map[string]any{
		"to_resource": "pvc-gain", "nodes": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Errorf("restore into a target a concurrent retry placed a replica on = %d, want 201", code)
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-gain")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if rd.Props[store.RestoreFromSnapshotProp] != restoreMarker("src-gain", "snap-gain") {
		t.Errorf("the restore marker = %q, want it kept", rd.Props[store.RestoreFromSnapshotProp])
	}

	if replicas, _ := backend.Resources().ListByDefinition(ctx, "pvc-gain"); len(replicas) != 1 {
		t.Errorf("replicas = %v, want the one on node-a", replicas)
	}
}

// holdAfterMarking holds the first request that stamps the restore marker on
// the target right after the stamp, until released: long enough for a
// concurrent retry of the same restore to run start to finish.
type holdAfterMarking struct {
	store.Store

	target  string
	first   *atomic.Bool
	stamped chan struct{}
	release chan struct{}
}

type holdAfterMarkingRDs struct {
	store.ResourceDefinitionStore

	outer holdAfterMarking
}

func (h holdAfterMarking) ResourceDefinitions() store.ResourceDefinitionStore {
	return holdAfterMarkingRDs{ResourceDefinitionStore: h.Store.ResourceDefinitions(), outer: h}
}

func (h holdAfterMarkingRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	err := h.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate)
	if err == nil && strings.EqualFold(name, h.outer.target) {
		// Only the first stamp is held. A sync.Once would hold every later
		// patch too, the concurrent retry's own among them, until release.
		if h.outer.first.CompareAndSwap(false, true) {
			close(h.outer.stamped)
			<-h.outer.release
		}
	}

	return err //nolint:wrapcheck // pass-through test double
}

// linstor-csi re-issues CreateVolume on a timeout. The retry found the marker
// the first request had just written, resumed, placed the replica and answered
// 201; the first request then took that replica for somebody else's, removed
// the marker and answered 409, leaving the definition CSI was told is restored
// with no marker.
func TestSnapshotRestoreSurvivesAConcurrentRetryOfTheSameRestore(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-twin")
	seedRestoreSnapshot(t, backend, "src-twin", "snap-twin", []string{"node-a"})

	plain, stopPlain := startServerWithStore(t, backend)
	csiPrepareRestoreTarget(t, plain, "src-twin", "snap-twin", "pvc-twin")
	stopPlain()

	held := holdAfterMarking{
		Store: backend, target: "pvc-twin", first: &atomic.Bool{},
		stamped: make(chan struct{}), release: make(chan struct{}),
	}

	base, stop := startServerWithStore(t, held)
	defer stop()

	body := map[string]any{"to_resource": "pvc-twin", "nodes": []string{"node-a"}}
	first := make(chan int, 1)

	go func() { first <- restoreOnce(t, base, "src-twin", "snap-twin", body) }()

	<-held.stamped

	if code := restoreOnce(t, base, "src-twin", "snap-twin", body); code != http.StatusCreated {
		t.Errorf("the concurrent retry = %d, want 201", code)
	}

	close(held.release)

	if code := <-first; code != http.StatusCreated {
		t.Errorf("the request that stamped the marker = %d, want 201", code)
	}

	rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-twin")
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if rd.Props[store.RestoreFromSnapshotProp] != restoreMarker("src-twin", "snap-twin") {
		t.Errorf("the restore marker = %q, want it kept", rd.Props[store.RestoreFromSnapshotProp])
	}

	if replicas, _ := backend.Resources().ListByDefinition(ctx, "pvc-twin"); len(replicas) != 1 {
		t.Errorf("replicas = %v, want exactly one on node-a", replicas)
	}
}
