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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Bug 399: after `vd d` removes a volume, drbdsetup emits
// `destroy device name:<rd> volume:<n>`. The observer's volCache was
// otherwise APPEND-ONLY (mergeVolumes only ever added entries — there
// was no destroy path), so the cache kept re-emitting a phantom
// Status.Volumes entry for the deleted volume on every subsequent tick.
// Combined with the controller/satellite purge removing it, this
// produced a permanent Status flap. These tests pin the eviction path.

// volSet collapses a Volumes slice to the set of VolumeNumbers it
// carries, for order-independent assertions.
func volSet(vols []volumeObservation) map[int32]bool {
	out := map[int32]bool{}
	for i := range vols {
		out[vols[i].VolumeNumber] = true
	}

	return out
}

// TestTranslateDeviceEventDestroyMarksRemoved pins that a `destroy
// device` frame translates into a single Removed volume observation
// keyed by the volume number — the signal mergeVolumes needs to evict
// the cache entry.
func TestTranslateDeviceEventDestroyMarksRemoved(t *testing.T) {
	t.Parallel()

	obs, ok := translateEvent(drbd.Event{
		Kind:   eventKindDevice,
		Action: eventActionDestroy,
		Fields: map[string]string{
			"name":   "test",
			"volume": "1",
		},
	})
	if !ok {
		t.Fatalf("destroy device frame was dropped, want translated")
	}

	if obs.ResourceName != "test" {
		t.Errorf("ResourceName = %q, want test", obs.ResourceName)
	}

	if len(obs.Volumes) != 1 {
		t.Fatalf("Volumes = %+v, want exactly one entry", obs.Volumes)
	}

	if obs.Volumes[0].VolumeNumber != 1 || !obs.Volumes[0].Removed {
		t.Errorf("Volumes[0] = %+v, want {VolumeNumber:1, Removed:true}", obs.Volumes[0])
	}
}

// TestMergeVolumesEvictsRemovedVolume is the core flap-stopper: a two-
// volume cache that receives a `destroy device` for volume 1 must drop
// it, and the re-emitted snapshot must carry only the surviving volume.
// A later device tick for the survivor must NOT resurrect volume 1.
func TestMergeVolumesEvictsRemovedVolume(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{}

	// Two volumes observed UpToDate (the 2-volume RD before `vd d 1`).
	o.mergeVolumes(&observation{
		ResourceName: "test",
		Volumes: []volumeObservation{
			{VolumeNumber: 0, DiskState: "UpToDate"},
		},
	})
	o.mergeVolumes(&observation{
		ResourceName: "test",
		Volumes: []volumeObservation{
			{VolumeNumber: 1, DiskState: "UpToDate"},
		},
	})

	// `destroy device volume:1` arrives after the vd-d cascade.
	destroy := &observation{
		ResourceName: "test",
		Volumes: []volumeObservation{
			{VolumeNumber: 1, Removed: true},
		},
	}
	o.mergeVolumes(destroy)

	// The destroy must count as a change (it mutated the cache) and the
	// re-emitted snapshot must carry only volume 0.
	if got := volSet(destroy.Volumes); got[1] || !got[0] {
		t.Errorf("snapshot after destroy = %v, want only volume 0", got)
	}

	// A subsequent statistics tick on the survivor must NOT bring
	// volume 1 back from the dead — that resurrection was the flap.
	tick := &observation{
		ResourceName: "test",
		Volumes: []volumeObservation{
			{VolumeNumber: 0, DiskState: "UpToDate"},
		},
	}
	o.mergeVolumes(tick)

	o.volMu.Lock()
	_, vol1Present := o.volCache["test"][1]
	cacheLen := len(o.volCache["test"])
	o.volMu.Unlock()

	if vol1Present {
		t.Errorf("volume 1 resurrected in cache after eviction — flap would persist")
	}

	if cacheLen != 1 {
		t.Errorf("volCache len = %d, want 1 (only volume 0)", cacheLen)
	}
}

// TestMergeVolumesDestroyUnknownVolumeNoop pins that a `destroy device`
// for a volume the cache never knew about is a no-op: it must NOT mark
// the observation as changed (which would spin an empty Status write)
// and must NOT add a phantom entry.
func TestMergeVolumesDestroyUnknownVolumeNoop(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{}

	o.mergeVolumes(&observation{
		ResourceName: "test",
		Volumes: []volumeObservation{
			{VolumeNumber: 0, DiskState: "UpToDate"},
		},
	})

	// Destroy for a volume that was never cached.
	ev := &observation{
		ResourceName: "test",
		Volumes: []volumeObservation{
			{VolumeNumber: 7, Removed: true},
		},
	}
	o.mergeVolumes(ev)

	// No change → mergeVolumes nils out ev.Volumes so the apply path
	// short-circuits (no spurious Status write).
	if ev.Volumes != nil {
		t.Errorf("destroy of unknown volume emitted a snapshot %+v, want no write", ev.Volumes)
	}

	o.volMu.Lock()
	_, phantom := o.volCache["test"][7]
	o.volMu.Unlock()

	if phantom {
		t.Errorf("destroy of unknown volume added a phantom cache entry")
	}
}

// Bug 399 (diskless/tiebreaker facet): the partial fix stopped the flap
// on diskful replicas (the `destroy device` eviction above), but a
// DISKLESS / tiebreaker replica still flapped. A diskless replica has no
// local backing disk to tear down, so drbd-9 never emits a
// `destroy device` frame for its removed volume — its `vN:Diskless`
// device frame simply stops arriving. With no destroy signal the
// append-only volCache kept the stale entry forever and the observer
// re-emitted a phantom Status.Volumes[n] on every resync tick. The fix:
// converge the cache to the RD's live volume set (the
// `blockstor.io/volume-numbers` annotation the reconciler stamps), which
// is destroy-event-independent. These tests pin that convergence.

// TestPruneVolumeCacheToDesiredEvictsDisklessOrphan is the diskless
// flap-stopper. The cache holds two Diskless volumes (the tiebreaker's
// 2-volume view before `vd d 1`). No `destroy device` ever arrives for
// volume 1 (the diskless replica has no disk to destroy). The prune
// against an RD whose annotation now lists only "0" must evict volume 1,
// re-emit a snapshot carrying only volume 0, and a subsequent tick must
// NOT resurrect volume 1 — that resurrection was the flap.
func TestPruneVolumeCacheToDesiredEvictsDisklessOrphan(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{NodeName: "worker-3"}

	// Diskless replica caches both volumes via `device disk:Diskless`
	// frames — exactly how a tiebreaker reports its volumes.
	o.mergeVolumes(&observation{
		ResourceName: "flap399",
		Volumes:      []volumeObservation{{VolumeNumber: 0, DiskState: "Diskless"}},
	})
	o.mergeVolumes(&observation{
		ResourceName: "flap399",
		Volumes:      []volumeObservation{{VolumeNumber: 1, DiskState: "Diskless"}},
	})

	// `vd d 1` removed volume 1 from the RD. The reconciler re-stamped
	// the annotation to "0". No `destroy device` frame for the diskless
	// replica — the only convergence signal is the desired set.
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: "flap399.worker-3",
			Annotations: map[string]string{
				blockstoriov1alpha1.ResourceAnnotationVolumeNumbers: "0",
			},
		},
	}

	ev := &observation{ResourceName: "flap399"}
	o.pruneVolumeCacheToDesired(ev, res)

	if got := volSet(ev.Volumes); got[1] || !got[0] {
		t.Errorf("snapshot after prune = %v, want only volume 0", got)
	}

	o.volMu.Lock()
	_, vol1Present := o.volCache["flap399"][1]
	cacheLen := len(o.volCache["flap399"])
	o.volMu.Unlock()

	if vol1Present {
		t.Errorf("volume 1 still cached after prune — diskless flap would persist")
	}

	if cacheLen != 1 {
		t.Errorf("volCache len = %d, want 1 (only volume 0)", cacheLen)
	}

	// A later Diskless device tick on the survivor must not re-add the
	// orphan, and a re-prune must be a no-op (converged → no write).
	o.mergeVolumes(&observation{
		ResourceName: "flap399",
		Volumes:      []volumeObservation{{VolumeNumber: 0, DiskState: "Diskless"}},
	})

	settled := &observation{ResourceName: "flap399"}
	o.pruneVolumeCacheToDesired(settled, res)

	if settled.Volumes != nil {
		t.Errorf("converged prune emitted a snapshot %+v, want no write", settled.Volumes)
	}
}

// TestPruneVolumeCacheToDesiredNoAnnotationNoop pins the safety guard:
// when the desired set is unknown (no / empty / unparseable
// `blockstor.io/volume-numbers` annotation) the prune must NOT touch the
// cache. Blanking a replica's volumes on a missing record would be far
// worse than a transient stale entry (early convergence before the first
// stamp lands).
func TestPruneVolumeCacheToDesiredNoAnnotationNoop(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{NodeName: "worker-3"}

	o.mergeVolumes(&observation{
		ResourceName: "flap399",
		Volumes:      []volumeObservation{{VolumeNumber: 0, DiskState: "Diskless"}},
	})
	o.mergeVolumes(&observation{
		ResourceName: "flap399",
		Volumes:      []volumeObservation{{VolumeNumber: 1, DiskState: "Diskless"}},
	})

	// Resource with no volume-numbers annotation at all.
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "flap399.worker-3"},
	}

	ev := &observation{ResourceName: "flap399"}
	o.pruneVolumeCacheToDesired(ev, res)

	if ev.Volumes != nil {
		t.Errorf("prune with unknown desired set emitted %+v, want no write", ev.Volumes)
	}

	o.volMu.Lock()
	cacheLen := len(o.volCache["flap399"])
	o.volMu.Unlock()

	if cacheLen != 2 {
		t.Errorf("volCache len = %d, want 2 — prune must not evict on unknown desired set", cacheLen)
	}
}

// TestPruneVolumeCacheToDesiredKeepsLateAdd guards the vd-c late-add
// (Bug 384): a volume the RD still declares must NEVER be pruned. The
// annotation reflects the live RD, so a freshly-added volume is already
// in the desired set; the prune only removes volumes the RD has dropped.
func TestPruneVolumeCacheToDesiredKeepsLateAdd(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{NodeName: "worker-3"}

	o.mergeVolumes(&observation{
		ResourceName: "flap399",
		Volumes:      []volumeObservation{{VolumeNumber: 0, DiskState: "Diskless"}},
	})

	// RD now declares volumes 0 AND 1 (a vd-c just landed). The cache
	// only has 0 so far; the prune must not blank it, and must not add 1
	// (the device frame for the new volume arrives separately).
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: "flap399.worker-3",
			Annotations: map[string]string{
				blockstoriov1alpha1.ResourceAnnotationVolumeNumbers: "0,1",
			},
		},
	}

	ev := &observation{ResourceName: "flap399"}
	o.pruneVolumeCacheToDesired(ev, res)

	if ev.Volumes != nil {
		t.Errorf("prune with all-present desired set emitted %+v, want no write", ev.Volumes)
	}

	o.volMu.Lock()
	_, vol0Present := o.volCache["flap399"][0]
	cacheLen := len(o.volCache["flap399"])
	o.volMu.Unlock()

	if !vol0Present || cacheLen != 1 {
		t.Errorf("volCache = len %d (vol0 present=%v), want exactly {0}", cacheLen, vol0Present)
	}
}

// TestWriteStatusConvergesDisklessVolumes is the end-to-end integration:
// it drives the real writeStatus path (the chokepoint both events and the
// 5s resync go through) and proves a diskless replica's Status.Volumes
// converges to the RD's surviving set. Pre-fix writeStatus published both
// cached volumes every tick (the phantom); post-fix it publishes only the
// survivor.
func TestWriteStatusConvergesDisklessVolumes(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	// Diskless replica whose RD has dropped volume 1: annotation lists
	// only "0", and a diskless replica carries no Spec.Volumes entries.
	existing := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: "flap399.worker-3",
			Annotations: map[string]string{
				blockstoriov1alpha1.ResourceAnnotationVolumeNumbers: "0",
			},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "flap399",
			NodeName:               "worker-3",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		WithStatusSubresource(existing).
		Build()

	o := &ObserverRunnable{Client: cli, Exec: storage.NewFakeExec(), NodeName: "worker-3"}

	// Seed the cache with both diskless volumes (no destroy frame ever
	// arrives for the tiebreaker's removed volume).
	o.mergeVolumes(&observation{
		ResourceName: "flap399",
		Volumes:      []volumeObservation{{VolumeNumber: 0, DiskState: "Diskless"}},
	})
	o.mergeVolumes(&observation{
		ResourceName: "flap399",
		Volumes:      []volumeObservation{{VolumeNumber: 1, DiskState: "Diskless"}},
	})

	// A resync-style write: snapshot carries the full cache.
	ev := o.snapshotFor("flap399")
	if err := o.writeStatus(context.Background(), &ev); err != nil {
		t.Fatalf("writeStatus: %v", err)
	}

	var got blockstoriov1alpha1.Resource
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "flap399.worker-3"}, &got); err != nil {
		t.Fatalf("get Resource: %v", err)
	}

	gotSet := map[int32]bool{}
	for i := range got.Status.Volumes {
		gotSet[got.Status.Volumes[i].VolumeNumber] = true
	}

	if gotSet[1] {
		t.Errorf("Status.Volumes still carries removed volume 1: %v — diskless flap", gotSet)
	}

	if !gotSet[0] {
		t.Errorf("Status.Volumes dropped surviving volume 0: %v", gotSet)
	}
}
