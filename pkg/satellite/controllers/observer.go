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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
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
	// HasResource marks observations that carry a fresh resource-
	// kind frame (role transition → InUse, disk transition →
	// DrbdState). mergeResource only updates its cache from
	// observations with HasResource=true; for other event kinds it
	// re-emits the cached values so writeStatus' SSA apply keeps
	// the f:inUse / f:drbdState claims alive.
	HasResource bool
	Volumes     []volumeObservation
	Connections []connectionObservation
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
//
// `Removed` is an internal marker set by translateEvent for
// `destroy connection` frames (drbdsetup emits one after
// `drbdadm del-peer` resolves) so mergeConnections can prune the
// stale entry from the per-resource cache. Without it, a deleted
// peer's last-known state (typically StandAlone) lingers in
// view/resources forever — `linstor r l` keeps showing the dead
// peer as disconnected.
type connectionObservation struct {
	PeerNodeName     string
	Connected        bool
	Message          string
	ReplicationState string

	Removed bool
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
	case eventKindResource:
		return translateResourceEvent(ev)
	case eventKindDevice:
		return translateDeviceEvent(ev)
	case eventKindPeerDevice:
		return translatePeerDeviceEvent(ev)
	case eventKindConnection:
		// `drbdsetup events2` emits:
		//   exists connection name:<rd> peer-node-id:<id> conn-name:<peer> connection:<state> ...
		//   change connection name:<rd> peer-node-id:<id> connection:<state> ...
		//   destroy connection name:<rd> peer-node-id:<id> conn-name:<peer>
		// `conn-name` is the LINSTOR peer node name; `connection` is
		// the DRBD-9 state (`Connected`, `StandAlone`, `BrokenPipe`,
		// `Connecting`, `NetworkFailure`, `Timeout`, ...). The Python
		// CLI's `--faulty` filter goes red on anything other than
		// `Connected`. `destroy` arrives after `drbdadm del-peer`
		// resolves — surface it as a removal so mergeConnections can
		// prune the cache, otherwise `view/resources` keeps reporting
		// the deleted peer as StandAlone forever.
		name := ev.Fields["name"]
		peer := ev.Fields["conn-name"]
		state := ev.Fields[eventKindConnection]

		if name == "" || peer == "" {
			return observation{}, false
		}

		if ev.Action == eventActionDestroy {
			return observation{
				ResourceName: name,
				Connections: []connectionObservation{{
					PeerNodeName: peer,
					Removed:      true,
				}},
			}, true
		}

		if state == "" {
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
		InUse:        ev.Fields["role"] == drbdRolePrimary,
		HasResource:  true,
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
// Two pieces flow out: out-of-sync stats (for sync-progress %) and
// replication state (for the Python CLI's `linstor v l` Repl
// column). Both go through their respective merge caches; the
// observation produced here carries whichever the event provided.
func translatePeerDeviceEvent(ev drbd.Event) (observation, bool) {
	name := ev.Fields["name"]
	volStr, hasVol := ev.Fields["volume"]

	if name == "" || !hasVol {
		return observation{}, false
	}

	volNum, err := strconv.Atoi(volStr)
	if err != nil {
		return observation{}, false
	}

	out := observation{ResourceName: name}

	if oosStr, ok := ev.Fields["out-of-sync"]; ok {
		oos, parseErr := strconv.ParseInt(oosStr, 10, 64)
		if parseErr == nil {
			out.Volumes = []volumeObservation{{
				VolumeNumber: int32(volNum), //nolint:gosec // drbd-9 volume numbers fit in int32
				OutOfSyncKib: oos,
				HasSync:      true,
			}}
		}
	}

	if peer := ev.Fields["conn-name"]; peer != "" {
		if repl := ev.Fields["replication"]; repl != "" {
			out.Connections = []connectionObservation{{
				PeerNodeName:     peer,
				ReplicationState: repl,
			}}
		}
	}

	if len(out.Volumes) == 0 && len(out.Connections) == 0 {
		return observation{}, false
	}

	return out, true
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
	// eventKindResource is the events2 `kind` token for resource-
	// level role/disk transitions. Drives the InUse field.
	eventKindResource = "resource"
	// eventKindDevice is the events2 `kind` token for per-volume
	// disk-state frames (UpToDate, Diskless, Failed, …).
	eventKindDevice = "device"
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
	// drbdRolePrimary is the DRBD-9 role token meaning the
	// replica is open for write. Maps to ResourceStatus.InUse.
	drbdRolePrimary = "Primary"
	// eventActionDestroy is the events2 verb emitted after a peer
	// is removed via `drbdadm del-peer`. The observer uses this
	// to prune the connection cache so stale StandAlone entries
	// don't linger on view/resources after a replica delete.
	eventActionDestroy = "destroy"
	// drbdDiskStateFailed is the DRBD-9 device disk_state token
	// the kernel emits when the backing block device (LV / zvol /
	// loopfile / disk hardware) starts returning I/O errors. Two
	// auto-recovery actions key off this state: drbdadm detach to
	// stop bashing the dead device, and the SkipDisk prop write
	// to pin the next adjust onto `--skip-disk` (scenario 5.11).
	drbdDiskStateFailed = "Failed"
	// drbdDiskStateDiskless is the DRBD-9 device disk_state token
	// the kernel emits after a successful detach. Bug 280: an
	// operator-driven `drbdadm detach --force` transitions
	// UpToDate → Diskless directly (no Failed step), so the
	// observer's Failed-gated SkipDisk stamp doesn't fire and the
	// next reconcile re-attaches the disk via plain `drbdadm
	// adjust`. We treat that transition as an explicit operator
	// intent and stamp the same SkipDisk prop the Failed path uses.
	drbdDiskStateDiskless = "Diskless"
	// drbdDiskStateUpToDate is the steady-state token for a
	// healthy diskful replica. Used as the "before" half of the
	// Bug 280 transition gate.
	drbdDiskStateUpToDate = "UpToDate"
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

	// resourceCache holds the latest observed resource-kind fields
	// (InUse, DrbdState) keyed by resource name. Same SSA-merge
	// reason as the other caches: only the resource-kind event
	// carries InUse, but connection / peer-device events still go
	// through writeStatus — without re-emitting cached InUse the
	// next apply (with InUse=false, omitempty-stripped) drops this
	// owner's f:inUse claim and the apiserver deletes the field.
	resMu    sync.Mutex
	resCache map[string]resourceObservation

	// ReconcileTrigger is the channel the observer emits an
	// `event.GenericEvent` onto whenever a kernel-state change
	// for a local Resource lands (Bug 318). The
	// ResourceReconciler consumes it via
	// `WatchesRawSource(source.Channel(...))` so satellite-side
	// recovery decisions can wake on observed state even when no
	// apiserver write bumps Generation. Nil disables the trigger
	// (unit-test path).
	ReconcileTrigger chan<- event.GenericEvent
}

// resourceObservation is the cached per-resource state observer
// re-emits on every apply so SSA-merge doesn't drop InUse between
// connection / peer-device events.
type resourceObservation struct {
	InUse     bool
	DrbdState string
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

	go o.resyncLoop(ctx)

	for ev := range observationsFrom(events) {
		obs := ev
		o.handleObservation(ctx, adm, &obs)
	}

	return nil
}

// observerResyncInterval is how often the observer re-applies its
// cached per-resource state to the apiserver. Belt-and-braces
// against the race where the first `exists` device frame lands
// before the controller has created the Resource CRD: the SSA
// Patch returns NotFound, the event is silenced, and drbd-9
// never emits a follow-up `change` because the state hasn't
// moved. Without the periodic re-emit, the Resource lives its
// whole lifetime with Status.Volumes[i].diskState blank.
const observerResyncInterval = 5 * time.Second

// resyncLoop ticks every observerResyncInterval and re-applies
// every cached resource's full snapshot. Cheap — the SSA payload
// is small, and the apiserver's "same fields, same values" merge
// is a no-op on the wire.
func (o *ObserverRunnable) resyncLoop(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("observer-resync")

	ticker := time.NewTicker(observerResyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.resyncOnce(ctx, logger)
		}
	}
}

// resyncOnce snapshots both caches and re-applies each known
// resource. Called by resyncLoop and unit-tested directly.
func (o *ObserverRunnable) resyncOnce(ctx context.Context, logger logr.Logger) {
	names := o.cachedResourceNames()

	for _, name := range names {
		obs := o.snapshotFor(name)
		if obs.ResourceName == "" {
			continue
		}

		err := o.writeStatus(ctx, &obs)
		if err == nil {
			continue
		}

		if apierrors.IsNotFound(err) {
			continue
		}

		logger.Error(err, "resync Resource.Status", "resource", name)
	}
}

// cachedResourceNames returns the union of resource keys held by
// the per-volume, per-connection, and resource-state caches. Used
// by resyncOnce.
func (o *ObserverRunnable) cachedResourceNames() []string {
	seen := map[string]struct{}{}

	o.volMu.Lock()
	for name := range o.volCache {
		seen[name] = struct{}{}
	}
	o.volMu.Unlock()

	o.connMu.Lock()
	for name := range o.connCache {
		seen[name] = struct{}{}
	}
	o.connMu.Unlock()

	o.resMu.Lock()
	for name := range o.resCache {
		seen[name] = struct{}{}
	}
	o.resMu.Unlock()

	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}

	return out
}

// snapshotFor returns the full cached observation for one
// resource: every known volume, every known peer connection, and
// the resource-level InUse/DrbdState. Used by resyncOnce to
// rebuild the SSA payload from cache.
func (o *ObserverRunnable) snapshotFor(name string) observation {
	out := observation{ResourceName: name}

	o.volMu.Lock()
	if cache, ok := o.volCache[name]; ok {
		for _, v := range cache {
			out.Volumes = append(out.Volumes, v)
		}
	}
	o.volMu.Unlock()

	o.connMu.Lock()
	if peers, ok := o.connCache[name]; ok {
		for _, c := range peers {
			out.Connections = append(out.Connections, c)
		}
	}
	o.connMu.Unlock()

	o.resMu.Lock()
	if r, ok := o.resCache[name]; ok {
		out.InUse = r.InUse
		out.DrbdState = r.DrbdState
	}
	o.resMu.Unlock()

	return out
}

// mergeResource caches the resource-kind observation (InUse,
// DrbdState) so subsequent connection / peer-device event applies
// re-emit them. Without this, an event that doesn't carry InUse
// strips f:inUse off the observer's owner claim and the apiserver
// deletes the field — manifesting as the auto-diskful promotion
// regression where the controller never sees InUse=true even
// though DRBD reports the replica as Primary.
func (o *ObserverRunnable) mergeResource(ev *observation) {
	if ev.ResourceName == "" {
		return
	}

	o.resMu.Lock()
	defer o.resMu.Unlock()

	if o.resCache == nil {
		o.resCache = map[string]resourceObservation{}
	}

	cur := o.resCache[ev.ResourceName]

	// HasResource events (translateResourceEvent) carry an
	// authoritative role transition. Update cached InUse only
	// from these; other event kinds leave InUse at zero-value
	// which would falsely clear the cache.
	if ev.HasResource {
		cur.InUse = ev.InUse
	}

	// DrbdState flows from device-kind events (translateDeviceEvent
	// sets it from the `disk` field). Update whenever the event
	// carries a non-empty value.
	if ev.DrbdState != "" {
		cur.DrbdState = ev.DrbdState
	}

	o.resCache[ev.ResourceName] = cur

	// Re-emit cached values so writeStatus' apply sees them every
	// time, not just on the event kind that produced them. Without
	// this, a connection event right after a role transition
	// strips the f:inUse claim and the apiserver deletes the field.
	ev.InUse = cur.InUse
	ev.DrbdState = cur.DrbdState
}

// handleObservation runs the per-event side-effects: the
// backing-device-failure auto-detach (kernel-reported disk:Failed
// → drbdadm detach) plus the SkipDisk prop write that pins the
// next reconcile onto `drbdadm adjust --skip-disk`, and the
// Resource.Status SSA write.
//
// Scenario 5.11 (UG9 §4428-4460): kernel reports `change device
// disk:Failed` when the backing block device starts returning I/O
// errors (LV missing / zvol destroyed / disk physically gone).
// Two side-effects must converge before the resource is usable
// again on this node:
//
//  1. Detach the failed lower disk so the kernel stops bashing
//     dead I/O — this transitions the volume to Diskless (the
//     resource keeps serving I/O via DRBD's network path to
//     UpToDate peers).
//  2. Stamp `DrbdOptions/SkipDisk=True` onto Resource.Spec.Props
//     so the next `drbdadm adjust` call skips disk-level
//     reconfiguration (which would try to re-attach the dead
//     disk). The reconciler reads the prop and appends
//     `--skip-disk` to its Adjust invocation.
//
// Operator clears the prop with
// `linstor r sp <node> <rsc> DrbdOptions/SkipDisk` (no value);
// the existing prop-management path drops the key and the next
// reconcile resumes normal `drbdadm adjust` (which then tries to
// re-attach the disk if the underlying block device is back).
func (o *ObserverRunnable) handleObservation(ctx context.Context, adm *drbd.Adm, ev *observation) {
	logger := log.FromContext(ctx).WithName("observer")

	if ev.DrbdState == drbdDiskStateFailed {
		err := adm.Detach(ctx, ev.ResourceName)
		if err != nil {
			logger.Error(err, "auto-detach on Failed", "resource", ev.ResourceName)
		} else {
			logger.Info("auto-detached failed replica", "resource", ev.ResourceName)
		}

		err = o.writeSkipDiskProp(ctx, ev.ResourceName)
		switch {
		case err == nil:
			logger.Info("set DrbdOptions/SkipDisk on failed replica", "resource", ev.ResourceName)
		case apierrors.IsNotFound(err):
			// Resource CRD not yet created — convergence-pending; same
			// silence policy as writeStatus.
		default:
			logger.Error(err, "set SkipDisk prop on Failed", "resource", ev.ResourceName)
		}
	}

	// Bug 280 (P1): operator-driven `drbdadm detach --force`
	// transitions UpToDate → Diskless directly without going
	// through Failed, so the gate above doesn't fire. Without a
	// SkipDisk stamp the next reconcile pass runs bare `drbdadm
	// adjust` and re-attaches the lower disk before the operator
	// can observe Diskless. Detect the transition by comparing
	// the incoming DrbdState to the previously-cached value (the
	// cache is written by mergeResource further down — read BEFORE
	// that call lands so we still see the pre-event value).
	if ev.DrbdState == drbdDiskStateDiskless && ev.ResourceName != "" {
		o.resMu.Lock()
		prev := o.resCache[ev.ResourceName].DrbdState
		o.resMu.Unlock()

		if prev == drbdDiskStateUpToDate {
			err := o.writeSkipDiskProp(ctx, ev.ResourceName)
			switch {
			case err == nil:
				logger.Info("set DrbdOptions/SkipDisk on operator-detached replica",
					"resource", ev.ResourceName)
			case apierrors.IsNotFound(err):
				// Resource CRD not yet created — same silence policy.
			default:
				logger.Error(err, "set SkipDisk prop on operator-detach",
					"resource", ev.ResourceName)
			}
		}
	}

	// Connection observations arrive one peer at a time. SSA with the
	// same FieldOwner replaces the full list each apply, so we
	// aggregate per-resource state in-memory and emit the full
	// snapshot. Without the merge, Apply N drops Apply N-1's other
	// peers from this owner's claims and they vanish from Status.
	o.mergeConnections(ev)
	o.mergeVolumes(ev)
	o.mergeResource(ev)

	// Bug 318: wake the ResourceReconciler on every kernel-state
	// change (resource lifecycle, role, disk, conn, repl). The
	// satellite's recovery decisions depend on observed state but
	// many of them (e.g. peer flapping to StandAlone, the local
	// disk transitioning Failed → Diskless) generate no apiserver
	// Spec writes — so Generation never bumps and the primary For
	// watch sees nothing. The trigger channel closes that loop
	// architecturally; the primary watch's Status whitelist on
	// DRBDNodeID/Port/Minor handles controller-allocator stamps
	// separately.
	//
	// Done BEFORE writeStatus so an apiserver hiccup on Status SSA
	// doesn't suppress the wake-up — the recovery decision only
	// needs the kernel-state change to land in the reconcile
	// queue, not the corresponding Status PATCH to commit.
	o.emitReconcileTrigger(ev)

	err := o.writeStatus(ctx, ev)
	if err == nil {
		return
	}

	if apierrors.IsNotFound(err) {
		return
	}

	logger.Error(err, "write Resource.Status", "resource", ev.ResourceName)
}

// emitReconcileTrigger sends a GenericEvent for the affected
// Resource onto the observer-trigger channel so the
// ResourceReconciler wakes on kernel-state changes that produce no
// apiserver Spec write (Bug 318).
//
// Non-blocking: a full channel is treated as "reconciler is already
// behind, drop this wake-up" rather than back-pressuring the events2
// loop. The observer's 5-second resync ticker re-emits cached state
// onto Status so a coalesced wake-up still arrives within the same
// window; reconciler's RequeueAfter covers any per-resource hand-off.
//
// The emitted object carries only `Name` — the ResourceReconciler's
// SetupWithManager registers the channel as a raw source whose
// handler enqueues the named Resource for reconciliation, no
// per-event field inspection required.
func (o *ObserverRunnable) emitReconcileTrigger(ev *observation) {
	if o.ReconcileTrigger == nil || ev == nil || ev.ResourceName == "" {
		return
	}

	name := k8s.Name(ev.ResourceName + "." + o.NodeName)

	trigger := event.GenericEvent{
		Object: &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		},
	}

	select {
	case o.ReconcileTrigger <- trigger:
	default:
		// Channel full — reconciler is already behind. Drop this
		// wake-up; the 5-second resync ticker plus c-r's
		// per-resource debouncer guarantee a follow-up.
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

	// Field-wise merge so the two event-kinds (connection-kind sets
	// Connected/Message; peer-device-kind sets ReplicationState)
	// don't clobber each other's contributions. `Removed`
	// (from `destroy connection`) drops the peer entirely so a
	// deleted replica stops appearing in view/resources.
	for _, c := range ev.Connections {
		if c.Removed {
			delete(peers, c.PeerNodeName)

			continue
		}

		merged := peers[c.PeerNodeName]
		merged.PeerNodeName = c.PeerNodeName

		if c.Message != "" {
			merged.Connected = c.Connected
			merged.Message = c.Message
		}

		if c.ReplicationState != "" {
			merged.ReplicationState = c.ReplicationState
		}

		peers[c.PeerNodeName] = merged
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

	// Best-effort lookup for `Spec.StoragePool` so the observer's
	// per-volume Status entries carry the pool name on every tick
	// (Bug 75). NotFound / lookup error → leave the pool empty; the
	// SSA apply payload's `omitempty` keeps the observer from racing
	// the satellite-stamp owner on the same listMap entry. The
	// satellite-stamp path remains the primary writer.
	var storagePool string

	var lookup blockstoriov1alpha1.Resource

	lookupErr := o.Client.Get(ctx, client.ObjectKey{Name: name}, &lookup)
	if lookupErr == nil {
		storagePool = lookup.Spec.StoragePool
	}

	// No prior Get on the apply payload itself: SSA Patch is the
	// existence check. The cached client's local cache trails the
	// apiserver during the first seconds of a fresh Resource's
	// life; a Get round-trip through the cache returned NotFound,
	// the satellite silenced it, and the apply for the UpToDate
	// device-kind frame never reached the apiserver — leaving
	// Status.Volumes[i].diskState blank for the rest of the
	// lifetime. SSA Patch on a not-yet-existing Resource returns
	// NotFound which we silence the same way the Get used to.
	apply := &blockstoriov1alpha1.Resource{
		TypeMeta:   metav1.TypeMeta{Kind: resourceKind, APIVersion: blockstoriov1alpha1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: blockstoriov1alpha1.ResourceStatus{
			InUse:       ev.InUse,
			DrbdState:   ev.DrbdState,
			Volumes:     buildObserverVolumeStatus(ev, storagePool),
			Connections: buildObserverConnectionStatus(ev),
		},
	}

	// No ForceOwnership: observer only owns the runtime-state
	// subfields (diskState / currentGi / connections / inUse /
	// drbdState / outOfSyncKib / replicationState). The
	// reconciler-stamp owns devicePath under the same listMap key
	// volumes[volumeNumber=0]; ForceOwnership on either side would
	// kick the other's subfield claims off the listMap entry.
	// SSA's per-field merge already covers what we need.
	err := o.Client.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available
		client.FieldOwner(k8s.SatelliteFieldOwner))
	if err != nil {
		return errors.Wrapf(err, "ssa apply Resource.Status %s", name)
	}

	return nil
}

// skipDiskPropKey is the LINSTOR-compatible property path
// upstream's DrbdAdm.adjust consults. Matches
// `ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_SKIP_DISK`
// from upstream's StateSequenceDetector (which auto-stamps the
// prop on the same Failed→Diskless transition path we trigger off
// here). The value is upstream's `ApiConsts.VAL_TRUE = "True"` —
// match the literal so a same-cluster heterogeneous upgrade
// (some controllers still on upstream) reads consistent state.
const skipDiskPropKey = "DrbdOptions/SkipDisk"

// skipDiskPropValue is the literal upstream uses for the SkipDisk
// flag. Case-sensitive on write; upstream reads it
// case-insensitively (`VAL_TRUE.equalsIgnoreCase`) so the
// satellite's reconciler matches either spelling, but on write we
// pin the canonical form.
const skipDiskPropValue = "True"

// observerSkipDiskFieldOwner is the distinct SSA field-manager
// the observer uses for SkipDisk prop writes. Distinct from
// SatelliteFieldOwner (which owns Status fields) so a SkipDisk
// claim never collides with the reconciler's other Spec.Props
// writes — the reconciler is a different field-owner and SSA's
// per-key merge on the map keeps both alive. Operator clearing the
// prop via `r sp <n> <r> DrbdOptions/SkipDisk` (no value) is
// expected to use the controller's FieldOwner; without a distinct
// observer owner the apiserver would either reject the clear (key
// owned elsewhere) or silently re-apply on the next observer tick.
const observerSkipDiskFieldOwner = "blockstor-satellite-skipdisk"

// writeSkipDiskProp SSA-applies `Spec.Props["DrbdOptions/SkipDisk"]
// = "True"` onto the matching Resource CRD. Uses a distinct
// FieldOwner so the prop can be cleared by the operator's
// controller-side prop-management path (which uses
// ControllerFieldOwner) without an observer re-broadcast
// resurrecting the key.
//
// SSA on Spec.Props (a map) merges per-key — the apply object
// carries ONLY the SkipDisk key, so other Spec.Props entries
// owned by the controller-side reconciler stay untouched. The
// apiserver's NotFound on a not-yet-created Resource is surfaced
// to the caller; handleObservation silences it the same way
// writeStatus does.
func (o *ObserverRunnable) writeSkipDiskProp(ctx context.Context, resourceName string) error {
	if resourceName == "" {
		return nil
	}

	name := k8s.Name(resourceName + "." + o.NodeName)

	// Read the existing Resource so the SSA apply object can carry
	// the immutable required scalars (`resourceDefinitionName`,
	// `nodeName`) without claiming a new value for them — the
	// reconciler upstream of us authored those fields and SSA
	// validation will reject an apply that doesn't include them
	// (kubebuilder marks both `+required`, no `omitempty`).
	// Mirrors the pattern node_label_sync_controller.go uses when
	// SSA-applying Aux props onto Node.Spec.
	//
	// NotFound here is the convergence-pending case — Resource CRD
	// not yet created. Bubble up so handleObservation's
	// IsNotFound branch silences the event.
	var existing blockstoriov1alpha1.Resource

	err := o.Client.Get(ctx, client.ObjectKey{Name: name}, &existing)
	if err != nil {
		return errors.Wrapf(err, "get Resource %s", name)
	}

	// Unstructured (not the typed Resource) so the serialised SSA
	// apply object carries ONLY the SkipDisk key under
	// `spec.props` PLUS the immutable required scalars. Building
	// from the typed struct without omitempty on
	// resourceDefinitionName/nodeName would force claims even when
	// fields are omitted; unstructured gives us per-field shape
	// control.
	apply := &unstructured.Unstructured{}
	apply.SetGroupVersionKind(blockstoriov1alpha1.GroupVersion.WithKind(resourceKind))
	apply.SetName(name)
	apply.Object["spec"] = map[string]any{
		"resourceDefinitionName": existing.Spec.ResourceDefinitionName,
		"nodeName":               existing.Spec.NodeName,
		"props": map[string]any{
			skipDiskPropKey: skipDiskPropValue,
		},
	}

	// ForceOwnership: the SkipDisk key conflicts with the
	// controller-side FieldOwner ("blockstor-controller") which
	// authored Spec.Props from the dispatcher's resolve pass. The
	// SkipDisk gate is the satellite's auto-action on a kernel
	// disk-failure event — it MUST win against the resolved bag
	// the controller installed seconds before, otherwise the prop
	// flips back on the next dispatcher cycle and the SkipDisk
	// auto-recovery never takes hold. The required scalars stay
	// owned by their original writer because we don't change
	// their value (SSA's "value didn't change" merge leaves
	// ownership untouched).
	err = o.Client.Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available
		client.FieldOwner(observerSkipDiskFieldOwner),
		client.ForceOwnership)
	if err != nil {
		return errors.Wrapf(err, "ssa apply Resource.Spec.Props SkipDisk %s", name)
	}

	return nil
}

// buildObserverVolumeStatus packs the per-volume observations
// from `ev` into the SSA-shaped Status.Volumes payload. Only
// non-empty fields propagate so the apply object stays narrow
// — broader claims would steal field ownership from other
// writers (controller-side seed allocator, etc.).
//
// `storagePool` carries `Resource.Spec.StoragePool` so the
// observer's listMap entries don't blank out the pool name the
// satellite-stamp owner authored on the same `volumeNumber` key
// (Bug 75). An empty `storagePool` (e.g. parent Resource not yet
// resolvable) is intentionally not claimed — the `omitempty` on
// the wire field keeps SSA from staking a claim on `""`, which
// would race the satellite-stamp owner.
func buildObserverVolumeStatus(ev *observation, storagePool string) []blockstoriov1alpha1.ResourceVolumeStatus {
	if len(ev.Volumes) == 0 {
		return nil
	}

	out := make([]blockstoriov1alpha1.ResourceVolumeStatus, 0, len(ev.Volumes))

	for _, v := range ev.Volumes {
		out = append(out, blockstoriov1alpha1.ResourceVolumeStatus{
			VolumeNumber: v.VolumeNumber,
			StoragePool:  storagePool,
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
			PeerNodeName:     c.PeerNodeName,
			Connected:        c.Connected,
			Message:          c.Message,
			ReplicationState: c.ReplicationState,
		})
	}

	return out
}

// Compile-time check that we satisfy the runnable contract.
var _ manager.Runnable = (*ObserverRunnable)(nil)
