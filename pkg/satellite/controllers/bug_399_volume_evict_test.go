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
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
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
