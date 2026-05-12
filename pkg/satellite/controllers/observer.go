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
	"sync"

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
	Connections  []connectionObservation
}

// volumeObservation carries per-volume DiskState + the
// `current-uuid` value the controller seeds new replicas with
// to skip the full initial-sync (Phase 8.1).
//
// OutOfSyncKib is populated from `peer-device` events (kept
// separately from DiskState/CurrentUUID which come from `device`
// kind frames). When only one of the three sources fires, the
// other fields are left as their zero values — mergeVolumes
// stitches the per-volume picture together so SSA writes carry
// the full known state.
type volumeObservation struct {
	VolumeNumber int32
	DiskState    string
	CurrentUUID  string
	OutOfSyncKib int64
	HasSync      bool // true when this observation carried out-of-sync stats
}

// connectionObservation carries one per-peer DRBD connection state.
// Maps directly onto `ResourceStatus.Connections[i]` — the wire-side
// `linstor r list --faulty` reads `Connected` to color disconnected
// peers red.
type connectionObservation struct {
	PeerNodeName string
	Connected    bool
	Message      string
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
		return translateResourceEvent(ev)
	case "device":
		return translateDeviceEvent(ev)
	case eventKindPeerDevice:
		return translatePeerDeviceEvent(ev)
	case eventKindConnection:
		// `drbdsetup events2` emits:
		//   exists connection name:<rd> peer-node-id:<id> conn-name:<peer> connection:<state> ...
		//   change connection name:<rd> peer-node-id:<id> connection:<state> ...
		// `conn-name` is the LINSTOR peer node name; `connection` is
		// the DRBD-9 state (`Connected`, `StandAlone`, `BrokenPipe`,
		// `Connecting`, `NetworkFailure`, `Timeout`, ...). The Python
		// CLI's `--faulty` filter goes red on anything other than
		// `Connected`.
		name := ev.Fields["name"]
		peer := ev.Fields["conn-name"]
		state := ev.Fields[eventKindConnection]

		if name == "" || peer == "" || state == "" {
			return observation{}, false
		}

		return observation{
			ResourceName: name,
			Connections: []connectionObservation{{
				PeerNodeName: peer,
				Connected:    state == drbdStateConnected,
				Message:      state,
			}},
		}, true
	}

	return observation{}, false
}

// translateResourceEvent extracts the resource-kind frame: just
// the role transition (Primary → InUse=true). Helper for
// translateEvent's switch so the gocyclo budget stays under 15.
func translateResourceEvent(ev drbd.Event) (observation, bool) {
	name := ev.Fields["name"]
	if name == "" {
		return observation{}, false
	}

	return observation{
		ResourceName: name,
		InUse:        ev.Fields["role"] == "Primary",
	}, true
}

// translateDeviceEvent extracts the device-kind frame: per-volume
// DiskState + the current-uuid the controller seeds from.
func translateDeviceEvent(ev drbd.Event) (observation, bool) {
	name := ev.Fields["name"]
	if name == "" {
		return observation{}, false
	}

	disk := ev.Fields["disk"]
	out := observation{ResourceName: name, DrbdState: disk}

	volStr, hasVol := ev.Fields["volume"]
	if !hasVol {
		return out, true
	}

	volNum, err := strconv.Atoi(volStr)
	if err != nil {
		return out, true
	}

	out.Volumes = []volumeObservation{{
		VolumeNumber: int32(volNum), //nolint:gosec // drbd-9 volume numbers fit in int32
		DiskState:    disk,
		CurrentUUID:  ev.Fields["current-uuid"],
	}}

	return out, true
}

// translatePeerDeviceEvent extracts the peer-device frame from
// `drbdsetup events2 --statistics`:
//
//	exists peer-device name:<rd> peer-node-id:<id> volume:<v>
//	   conn-name:<peer> replication:<state> peer-disk:<state>
//	   out-of-sync:<kib> ...
//
// Only out-of-sync stats are surfaced; UI/CLI derive a sync-%
// from VolumeDefinition.SizeKib.
func translatePeerDeviceEvent(ev drbd.Event) (observation, bool) {
	name := ev.Fields["name"]
	volStr, hasVol := ev.Fields["volume"]
	oosStr, hasOOS := ev.Fields["out-of-sync"]

	if name == "" || !hasVol || !hasOOS {
		return observation{}, false
	}

	volNum, err := strconv.Atoi(volStr)
	if err != nil {
		return observation{}, false
	}

	oos, err := strconv.ParseInt(oosStr, 10, 64)
	if err != nil {
		return observation{}, false
	}

	return observation{
		ResourceName: name,
		Volumes: []volumeObservation{{
			VolumeNumber: int32(volNum), //nolint:gosec // drbd-9 volume numbers fit in int32
			OutOfSyncKib: oos,
			HasSync:      true,
		}},
	}, true
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

const (
	// eventKindConnection is the events2 `kind` token for peer
	// connection state-change frames.
	eventKindConnection = "connection"
	// eventKindPeerDevice is the events2 `kind` token for per-
	// (volume, peer) replication-state frames; carries the
	// `out-of-sync` byte counter the UI/CLI turns into a sync-%
	// progress bar.
	eventKindPeerDevice = "peer-device"
	// drbdStateConnected is the DRBD-9 connection-state token
	// meaning "handshake complete, replication active". Anything
	// else (`StandAlone`, `BrokenPipe`, `Connecting`, ...) lands
	// in the Python CLI's `--faulty` set.
	drbdStateConnected = "Connected"
)

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

	// connCache holds the latest observed per-peer connection state
	// keyed by `<resource>/<peer>`. We apply the full snapshot per
	// resource on every connection event because SSA with the same
	// FieldOwner re-applying `Connections=[<one>]` drops the other
	// peers from this owner's claims, deleting them. Aggregating
	// before apply preserves all peers.
	connMu    sync.Mutex
	connCache map[string]map[string]connectionObservation

	// volCache holds the latest observed per-volume aggregate keyed
	// by `<resource>/<volume>`. Same reason as connCache: SSA with
	// the same FieldOwner replaces the slice's per-key field-claims
	// each apply. Two events from different kinds (device-kind sets
	// DiskState; peer-device sets OutOfSyncKib) would otherwise drop
	// each other's fields between applies. Cache the union and emit
	// the full per-resource snapshot.
	volMu    sync.Mutex
	volCache map[string]map[int32]volumeObservation
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
		obs := ev
		o.handleObservation(ctx, adm, &obs)
	}

	return nil
}

// handleObservation runs the per-event side-effects: the
// backing-device-failure auto-detach (kernel-reported disk:Failed
// → drbdadm detach) and the Resource.Status SSA write.
func (o *ObserverRunnable) handleObservation(ctx context.Context, adm *drbd.Adm, ev *observation) {
	logger := log.FromContext(ctx).WithName("observer")

	if ev.DrbdState == "Failed" {
		err := adm.Detach(ctx, ev.ResourceName)
		if err != nil {
			logger.Error(err, "auto-detach on Failed", "resource", ev.ResourceName)
		} else {
			logger.Info("auto-detached failed replica", "resource", ev.ResourceName)
		}
	}

	// Connection observations arrive one peer at a time. SSA with the
	// same FieldOwner replaces the full list each apply, so we
	// aggregate per-resource state in-memory and emit the full
	// snapshot. Without the merge, Apply N drops Apply N-1's other
	// peers from this owner's claims and they vanish from Status.
	o.mergeConnections(ev)
	o.mergeVolumes(ev)

	err := o.writeStatus(ctx, ev)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "write Resource.Status", "resource", ev.ResourceName)
	}
}

// mergeVolumes folds the per-volume cache so SSA writes carry the
// full per-volume picture. Without this, two separate event kinds
// (`device` for DiskState/CurrentGi, `peer-device` for OutOfSyncKib)
// would each strip the other's field claims when SSA-applying the
// same listMap key — leaving Status.Volumes[i] alternating between
// "has disk-state, no sync" and "has sync, no disk-state".
//
// Throttle: when the incoming observation is identical to the
// cached value, mergeVolumes leaves ev.Volumes empty so writeStatus
// becomes a no-op for that slice. peer-device events fire on every
// drbdsetup statistics tick (~1Hz per peer); without this filter
// each idle resource would PATCH the apiserver every second.
func (o *ObserverRunnable) mergeVolumes(ev *observation) {
	if ev.ResourceName == "" {
		return
	}

	o.volMu.Lock()
	defer o.volMu.Unlock()

	if o.volCache == nil {
		o.volCache = map[string]map[int32]volumeObservation{}
	}

	cache, ok := o.volCache[ev.ResourceName]
	if !ok {
		cache = map[int32]volumeObservation{}
		o.volCache[ev.ResourceName] = cache
	}

	changed := false

	for _, incoming := range ev.Volumes {
		if mergeVolumeInto(cache, incoming) {
			changed = true
		}
	}

	if !changed {
		ev.Volumes = nil

		return
	}

	snapshot := make([]volumeObservation, 0, len(cache))
	for _, entry := range cache {
		snapshot = append(snapshot, entry)
	}

	ev.Volumes = snapshot
}

// mergeVolumeInto folds `incoming` into `cache` for its volume key.
// Returns true if any field actually changed — the caller uses that
// to decide whether to emit a fresh Status snapshot.
func mergeVolumeInto(cache map[int32]volumeObservation, incoming volumeObservation) bool {
	merged := cache[incoming.VolumeNumber]
	merged.VolumeNumber = incoming.VolumeNumber

	changed := false

	if incoming.DiskState != "" && merged.DiskState != incoming.DiskState {
		merged.DiskState = incoming.DiskState
		changed = true
	}

	if incoming.CurrentUUID != "" && merged.CurrentUUID != incoming.CurrentUUID {
		merged.CurrentUUID = incoming.CurrentUUID
		changed = true
	}

	if incoming.HasSync && (!merged.HasSync || merged.OutOfSyncKib != incoming.OutOfSyncKib) {
		merged.OutOfSyncKib = incoming.OutOfSyncKib
		merged.HasSync = true
		changed = true
	}

	cache[incoming.VolumeNumber] = merged

	return changed
}

// mergeConnections updates the per-resource peer-state cache from
// the latest event and replaces ev.Connections with the full
// snapshot the SSA apply must emit. Volume / role events still pass
// through their existing paths — only connection state needs
// aggregation because every peer is a separate listMap key under
// the same FieldOwner.
func (o *ObserverRunnable) mergeConnections(ev *observation) {
	if ev.ResourceName == "" {
		return
	}

	o.connMu.Lock()
	defer o.connMu.Unlock()

	if o.connCache == nil {
		o.connCache = map[string]map[string]connectionObservation{}
	}

	peers, ok := o.connCache[ev.ResourceName]
	if !ok {
		peers = map[string]connectionObservation{}
		o.connCache[ev.ResourceName] = peers
	}

	for _, c := range ev.Connections {
		peers[c.PeerNodeName] = c
	}

	// Only overwrite the slice when this event actually carried
	// connection data; otherwise role/disk events would re-broadcast
	// the cache redundantly on every kernel frame.
	if len(ev.Connections) == 0 {
		return
	}

	snapshot := make([]connectionObservation, 0, len(peers))
	for _, c := range peers {
		snapshot = append(snapshot, c)
	}

	ev.Connections = snapshot
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
func (o *ObserverRunnable) writeStatus(ctx context.Context, ev *observation) error {
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
		TypeMeta:   metav1.TypeMeta{Kind: resourceKind, APIVersion: blockstoriov1alpha1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: blockstoriov1alpha1.ResourceStatus{
			InUse:       ev.InUse,
			DrbdState:   ev.DrbdState,
			Volumes:     buildObserverVolumeStatus(ev),
			Connections: buildObserverConnectionStatus(ev),
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
func buildObserverVolumeStatus(ev *observation) []blockstoriov1alpha1.ResourceVolumeStatus {
	if len(ev.Volumes) == 0 {
		return nil
	}

	out := make([]blockstoriov1alpha1.ResourceVolumeStatus, 0, len(ev.Volumes))

	for _, v := range ev.Volumes {
		out = append(out, blockstoriov1alpha1.ResourceVolumeStatus{
			VolumeNumber: v.VolumeNumber,
			DiskState:    v.DiskState,
			CurrentGi:    v.CurrentUUID,
			OutOfSyncKib: v.OutOfSyncKib,
		})
	}

	return out
}

// buildObserverConnectionStatus packs the per-peer DRBD connection
// observations onto Status.Connections. With listMapKey=peerNodeName
// SSA merges per-peer — a single connection-changed event updates
// just that peer's entry, leaving others untouched.
func buildObserverConnectionStatus(ev *observation) []blockstoriov1alpha1.ResourceConnectionStatus {
	if len(ev.Connections) == 0 {
		return nil
	}

	out := make([]blockstoriov1alpha1.ResourceConnectionStatus, 0, len(ev.Connections))

	for _, c := range ev.Connections {
		out = append(out, blockstoriov1alpha1.ResourceConnectionStatus{
			PeerNodeName: c.PeerNodeName,
			Connected:    c.Connected,
			Message:      c.Message,
		})
	}

	return out
}

// Compile-time check that we satisfy the runnable contract.
var _ manager.Runnable = (*ObserverRunnable)(nil)
