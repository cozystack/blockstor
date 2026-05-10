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

package satellite

import (
	"strconv"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
)

// Observation is the satellite-side view of one DRBD state change.
// It is a thin wrapper around the proto message so the rest of the
// satellite code can pass observations around without depending on
// generated gRPC types directly.
type Observation = satellitepb.ResourceObservedEvent

// Observer translates parsed `drbdsetup events2` lines into the proto
// observations the controller consumes. It is stateless across events
// — every output is derived from one input — which keeps the wire
// format simple and lets the controller dedupe/coalesce on its side.
type Observer struct {
	nodeName string
	now      func() time.Time
}

// NewObserver constructs an Observer that stamps `time.Now()` into
// every emitted observation.
func NewObserver(nodeName string) *Observer {
	return &Observer{nodeName: nodeName, now: time.Now}
}

// Translate consumes events from in, emits observations on the
// returned channel, and closes the output when in is closed. The
// goroutine exits when in closes; callers wanting cancellation should
// close in themselves.
func (o *Observer) Translate(in <-chan drbd.Event) <-chan *Observation {
	out := make(chan *Observation)

	go func() {
		defer close(out)

		for ev := range in {
			obs, ok := o.translate(ev)
			if !ok {
				continue
			}

			out <- obs
		}
	}()

	return out
}

// translate is the core mapping. Returns ok=false for events we
// shouldn't surface (wrong kind, missing resource name, etc.).
//
// Two event kinds matter to the controller:
//   - resource: role changes (Primary/Secondary). Drives InUse, which
//     is what the auto-diskful path keys off of.
//   - device:   per-volume disk-state changes (UpToDate, Diskless,
//     Failed, …). Drives DrbdState + per-volume DiskState.
//
// Other kinds (connection, peer-device, helper) are ignored for now.
func (o *Observer) translate(ev drbd.Event) (*Observation, bool) {
	switch ev.Kind {
	case "resource":
		return o.translateResource(ev)
	case "device":
		return o.translateDevice(ev)
	}

	return nil, false
}

// translateResource emits an observation when a resource's role
// changes. Sets InUse true on Primary so the controller's
// auto-diskful path runs.
func (o *Observer) translateResource(ev drbd.Event) (*Observation, bool) {
	name := ev.Fields["name"]
	if name == "" {
		return nil, false
	}

	role := ev.Fields["role"]

	return &Observation{
		ResourceName:  name,
		NodeName:      o.nodeName,
		InUse:         role == "Primary",
		TimestampUnix: o.now().Unix(),
	}, true
}

// translateDevice emits per-volume disk-state observations.
//
// Surfaces `current-uuid:` from `drbdsetup events2 --full` device
// frames as VolumeObservation.CurrentUuid so the controller can
// seed new replicas to skip the full initial-sync (Phase 8.1).
func (o *Observer) translateDevice(ev drbd.Event) (*Observation, bool) {
	name := ev.Fields["name"]
	if name == "" {
		return nil, false
	}

	disk := ev.Fields["disk"]

	obs := &Observation{
		ResourceName:  name,
		NodeName:      o.nodeName,
		DrbdState:     disk,
		TimestampUnix: o.now().Unix(),
	}

	volStr, hasVol := ev.Fields["volume"]
	if hasVol {
		volNum, err := strconv.Atoi(volStr)
		if err == nil {
			obs.Volumes = []*satellitepb.VolumeObservation{{
				VolumeNumber: int32(volNum), //nolint:gosec // volume numbers are small (<32) per drbd-9 limits
				DiskState:    disk,
				CurrentUuid:  ev.Fields["current-uuid"],
			}}
		}
	}

	return obs, true
}
