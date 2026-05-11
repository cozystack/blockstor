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
	"strconv"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// observation is the satellite-side translation of one parsed
// `drbdsetup events2` line — the minimal shape the
// `ObserverRunnable.writeStatus` SSA patch consumes. Lives in
// this package (rather than `pkg/satellite`) so the gRPC proto
// dependency stays local to the apply chain.
type observation struct {
	ResourceName string
	InUse        bool
	DrbdState    string
	Volumes      []volumeObservation
}

// volumeObservation carries per-volume DiskState + the
// `current-uuid` value the controller seeds new replicas with
// to skip the full initial-sync (Phase 8.1).
type volumeObservation struct {
	VolumeNumber int32
	DiskState    string
	CurrentUUID  string
}

// translateEvent maps one parsed events2 frame into the
// satellite-side observation shape. Returns ok=false for
// events we shouldn't surface (wrong kind, missing resource
// name, etc.).
//
// Two event kinds matter:
//   - resource: role changes (Primary/Secondary). Drives InUse,
//     which is what the controller's auto-diskful path keys off.
//   - device:   per-volume disk-state changes (UpToDate, Diskless,
//     Failed, …). Drives DrbdState + per-volume DiskState.
func translateEvent(ev drbd.Event) (observation, bool) {
	switch ev.Kind {
	case "resource":
		name := ev.Fields["name"]
		if name == "" {
			return observation{}, false
		}

		return observation{
			ResourceName: name,
			InUse:        ev.Fields["role"] == "Primary",
		}, true
	case "device":
		name := ev.Fields["name"]
		if name == "" {
			return observation{}, false
		}

		disk := ev.Fields["disk"]
		out := observation{ResourceName: name, DrbdState: disk}

		volStr, hasVol := ev.Fields["volume"]
		if hasVol {
			volNum, err := strconv.Atoi(volStr)
			if err == nil {
				out.Volumes = []volumeObservation{{
					VolumeNumber: int32(volNum), //nolint:gosec // drbd-9 volume numbers fit in int32
					DiskState:    disk,
					CurrentUUID:  ev.Fields["current-uuid"],
				}}
			}
		}

		return out, true
	}

	return observation{}, false
}

// observationsFrom transforms a stream of events2 lines into a
// stream of satellite observations. Returns when in closes.
func observationsFrom(in <-chan drbd.Event) <-chan observation {
	out := make(chan observation)

	go func() {
		defer close(out)

		for ev := range in {
			obs, ok := translateEvent(ev)
			if !ok {
				continue
			}

			out <- obs
		}
	}()

	return out
}

// observerEventBuffer bounds the events2 → translate goroutine
// queue. drbd-9 reconnect storms can burst dozens of events; 256
// matches the value the retired gRPC observer used.
const observerEventBuffer = 256

// ObserverRunnable tails `drbdsetup events2` and writes the parsed
// observations onto matching Resource CRDs' Status subresource
// via SSA. Phase 10.6: replaces the retired gRPC
// `Agent.runObserveLoop` + controller-side
// `pkg/satellitecontroller.Server.applyObserved` chain — the
// satellite now writes Status directly via the apiserver instead
// of streaming ResourceObservedEvent over gRPC.
//
// Implements `manager.Runnable` so the c-r manager owns the
// lifecycle: Start is invoked once when the manager's caches are
// in sync; Start returns when ctx cancels (manager teardown).
type ObserverRunnable struct {
	Client client.Client
	Exec   storage.Exec

	// NodeName is the satellite's own node identity — written
	// onto Resource.Status as the host signal the controller
	// uses to route observations to the right CRD.
	NodeName string
}

// NeedLeaderElection reports that this runnable does NOT need
// leader election — every satellite must run its own observer
// independently. The c-r manager has leader election disabled
// at the Config level anyway, so this is belt-and-braces.
func (*ObserverRunnable) NeedLeaderElection() bool { return false }

// Start launches the events2 watcher + observation translator +
// per-event Status SSA write loop. Returns when ctx cancels.
// Surface errors are logged but do not abort the runnable; the
// drbdsetup process is supervised externally by the satellite
// pod's restart policy.
func (o *ObserverRunnable) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("observer")

	watcher, cleanup, err := drbd.StartDrbdsetupEvents2(ctx)
	if err != nil {
		return errors.Wrap(err, "start drbdsetup events2")
	}
	defer cleanup()

	events := make(chan drbd.Event, observerEventBuffer)

	go func() {
		watchErr := watcher.Watch(ctx, events)
		if watchErr != nil && !errors.Is(watchErr, context.Canceled) {
			logger.Error(watchErr, "events2 watch")
		}
	}()

	adm := drbd.NewAdm(o.Exec)

	for ev := range observationsFrom(events) {
		o.handleObservation(ctx, adm, ev)
	}

	return nil
}

// handleObservation runs the per-event side-effects: the
// backing-device-failure auto-detach (kernel-reported disk:Failed
// → drbdadm detach) and the Resource.Status SSA write.
func (o *ObserverRunnable) handleObservation(ctx context.Context, adm *drbd.Adm, ev observation) {
	logger := log.FromContext(ctx).WithName("observer")

	if ev.DrbdState == "Failed" {
		err := adm.Detach(ctx, ev.ResourceName)
		if err != nil {
			logger.Error(err, "auto-detach on Failed", "resource", ev.ResourceName)
		} else {
			logger.Info("auto-detached failed replica", "resource", ev.ResourceName)
		}
	}

	err := o.writeStatus(ctx, ev)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "write Resource.Status", "resource", ev.ResourceName)
	}
}

// writeStatus applies the observation onto the matching Resource
// CRD's Status subresource via SSA. Replaces the retired
// controller-side `pkg/satellitecontroller.applyObserved` body:
// same field owner, same `+listType=map +listMapKey=volumeNumber`
// merge semantics for per-volume DiskState / CurrentGi.
//
// `NotFound` on the Get is normal during convergence — the
// satellite may observe state for a resource the controller
// hasn't yet created. Surface it so handleObservation drops
// the event without noise.
func (o *ObserverRunnable) writeStatus(ctx context.Context, ev observation) error {
	if ev.ResourceName == "" {
		return nil
	}

	name := k8s.Name(ev.ResourceName + "." + o.NodeName)

	var existing blockstoriov1alpha1.Resource

	err := o.Client.Get(ctx, client.ObjectKey{Name: name}, &existing)
	if err != nil {
		return errors.Wrapf(err, "get Resource %s", name)
	}

	apply := &blockstoriov1alpha1.Resource{
		TypeMeta:   metav1.TypeMeta{Kind: "Resource", APIVersion: blockstoriov1alpha1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: blockstoriov1alpha1.ResourceStatus{
			InUse:     ev.InUse,
			DrbdState: ev.DrbdState,
			Volumes:   buildObserverVolumeStatus(ev),
		},
	}

	err = o.Client.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available
		client.FieldOwner(k8s.SatelliteFieldOwner),
		client.ForceOwnership)
	if err != nil {
		return errors.Wrapf(err, "ssa apply Resource.Status %s", name)
	}

	return nil
}

// buildObserverVolumeStatus packs the per-volume observations
// from `ev` into the SSA-shaped Status.Volumes payload. Only
// non-empty fields propagate so the apply object stays narrow
// — broader claims would steal field ownership from other
// writers (controller-side seed allocator, etc.).
func buildObserverVolumeStatus(ev observation) []blockstoriov1alpha1.ResourceVolumeStatus {
	if len(ev.Volumes) == 0 {
		return nil
	}

	out := make([]blockstoriov1alpha1.ResourceVolumeStatus, 0, len(ev.Volumes))

	for _, v := range ev.Volumes {
		out = append(out, blockstoriov1alpha1.ResourceVolumeStatus{
			VolumeNumber: v.VolumeNumber,
			DiskState:    v.DiskState,
			CurrentGi:    v.CurrentUUID,
		})
	}

	return out
}

// Compile-time check that we satisfy the runnable contract.
var _ manager.Runnable = (*ObserverRunnable)(nil)
