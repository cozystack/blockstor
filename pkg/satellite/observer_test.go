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

package satellite_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
)

// TestObserverDeviceEventEmitsState: a `change device disk:UpToDate`
// event surfaces a ResourceObservedEvent with drbd_state populated.
func TestObserverDeviceEventEmitsState(t *testing.T) {
	obs := satellite.NewObserver("n1")

	events := []drbd.Event{
		{
			Action: "change",
			Kind:   "device",
			Fields: map[string]string{
				"name":   "pvc-1",
				"volume": "0",
				"disk":   "UpToDate",
			},
		},
	}

	out := collectObservations(obs, events)

	if len(out) != 1 {
		t.Fatalf("len(out): got %d, want 1", len(out))
	}

	if got := out[0].GetResourceName(); got != "pvc-1" {
		t.Errorf("ResourceName: got %q", got)
	}

	if got := out[0].GetNodeName(); got != "n1" {
		t.Errorf("NodeName: got %q", got)
	}

	if got := out[0].GetDrbdState(); got != "UpToDate" {
		t.Errorf("DrbdState: got %q", got)
	}
}

// TestObserverResourceRoleEmitsInUse: a `change resource role:Primary`
// event surfaces a ResourceObservedEvent with InUse=true. This is what
// drives the controller's auto-diskful path — without it, a manually
// promoted DISKLESS replica never gets reclassified as actively used.
func TestObserverResourceRoleEmitsInUse(t *testing.T) {
	obs := satellite.NewObserver("n1")

	events := []drbd.Event{
		{
			Action: "change",
			Kind:   "resource",
			Fields: map[string]string{
				"name": "pvc-promoted",
				"role": "Primary",
			},
		},
		{
			Action: "change",
			Kind:   "resource",
			Fields: map[string]string{
				"name": "pvc-promoted",
				"role": "Secondary",
			},
		},
	}

	out := collectObservations(obs, events)

	if len(out) != 2 {
		t.Fatalf("len(out): got %d, want 2", len(out))
	}

	if !out[0].GetInUse() {
		t.Errorf("Primary event should produce InUse=true; got %+v", out[0])
	}

	if out[1].GetInUse() {
		t.Errorf("Secondary event should produce InUse=false; got %+v", out[1])
	}

	if got := out[0].GetResourceName(); got != "pvc-promoted" {
		t.Errorf("ResourceName: got %q", got)
	}
}

// TestObserverIgnoresUnrelatedKinds: connection / peer-device kinds
// don't carry per-resource disk state, so they don't drive observations.
func TestObserverIgnoresUnrelatedKinds(t *testing.T) {
	obs := satellite.NewObserver("n1")

	events := []drbd.Event{
		{
			Action: "change",
			Kind:   "connection",
			Fields: map[string]string{
				"name":       "pvc-1",
				"connection": "Connected",
			},
		},
		{
			Action: "exists",
			Kind:   "-",
			Fields: map[string]string{},
		},
	}

	out := collectObservations(obs, events)

	if len(out) != 0 {
		t.Errorf("expected no observations; got %v", out)
	}
}

// TestObserverDropsEventsWithoutResource: a malformed event that lacks a
// `name` field can't be matched to a resource — silently skip.
func TestObserverDropsEventsWithoutResource(t *testing.T) {
	obs := satellite.NewObserver("n1")

	events := []drbd.Event{
		{
			Action: "change",
			Kind:   "device",
			Fields: map[string]string{
				"disk": "UpToDate",
			},
		},
		// Same defensive drop must apply to resource-kind events
		// too — pins translateResource's name-empty branch.
		{
			Action: "change",
			Kind:   "resource",
			Fields: map[string]string{
				"role": "Primary",
			},
		},
	}

	out := collectObservations(obs, events)

	if len(out) != 0 {
		t.Errorf("expected no observations from name-less events; got %v", out)
	}
}

// TestObserverResourceSecondaryEmitsInUseFalse: when a satellite
// demotes from Primary to Secondary, the events2 stream emits
// `role:Secondary` and the observer must reflect that as
// State.InUse=false. Pins the symmetric path of the auto-failover
// hook — without this the controller would never see "PVC unmounted"
// and the auto-diskful evictor wouldn't get the demotion signal.
func TestObserverResourceSecondaryEmitsInUseFalse(t *testing.T) {
	obs := satellite.NewObserver("n1")

	events := []drbd.Event{
		{
			Action: "change",
			Kind:   "resource",
			Fields: map[string]string{
				"name": "pvc-demoted",
				"role": "Secondary",
			},
		},
	}

	out := collectObservations(obs, events)

	if len(out) != 1 {
		t.Fatalf("observations: got %d, want 1", len(out))
	}

	if out[0].InUse {
		t.Errorf("InUse: got true on role:Secondary; want false")
	}

	if out[0].ResourceName != "pvc-demoted" {
		t.Errorf("ResourceName: got %q, want pvc-demoted", out[0].ResourceName)
	}
}

// TestObserverIncludesVolumeObservation: device events carry volume
// number — surface it in the VolumeObservation.
func TestObserverIncludesVolumeObservation(t *testing.T) {
	obs := satellite.NewObserver("n1")

	events := []drbd.Event{
		{
			Action: "change",
			Kind:   "device",
			Fields: map[string]string{
				"name":   "pvc-1",
				"volume": "0",
				"disk":   "UpToDate",
			},
		},
	}

	out := collectObservations(obs, events)

	if len(out) != 1 {
		t.Fatalf("len(out): got %d", len(out))
	}

	vols := out[0].GetVolumes()
	if len(vols) != 1 {
		t.Fatalf("len(volumes): got %d, want 1", len(vols))
	}

	if got := vols[0].GetVolumeNumber(); got != 0 {
		t.Errorf("VolumeNumber: got %d", got)
	}

	if got := vols[0].GetDiskState(); got != "UpToDate" {
		t.Errorf("DiskState: got %q", got)
	}
}

// collectObservations is a shared helper to drain Observer.Observe over
// a fixed input slice. We push into the input channel, close it, then
// read everything off the output channel.
func collectObservations(obs *satellite.Observer, events []drbd.Event) []*satellite.Observation {
	in := make(chan drbd.Event, len(events))
	for _, e := range events {
		in <- e
	}

	close(in)

	var out []*satellite.Observation

	for ev := range obs.Translate(in) {
		out = append(out, ev)
	}

	return out
}
