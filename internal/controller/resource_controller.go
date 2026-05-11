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

package controller

import (
	"context"
	"slices"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/effectiveprops"
	"github.com/cozystack/blockstor/pkg/store"
)

// resourceFinalizer is the legacy controller-side finalizer the
// reconciler used to manage. Phase 10.6 retires it — the
// satellite's own `blockstor.io.blockstor.io/satellite-resource`
// finalizer now owns teardown end-to-end. The constant + cleanup
// code stay so the controller strips the legacy finalizer off
// any Resource that still carries it (rolling upgrade case);
// the controller no longer stamps it on new Resources.
const resourceFinalizer = "blockstor.io.blockstor.io/resource"

// ResourceReconciler runs controller-side housekeeping on every
// Resource: DRBD-ID allocation (port/minor), seed-from-Gi for
// the initial-sync-skip pipeline, and auto-diskful promotion of
// actively-used DISKLESS replicas. Phase 10.6 removed the
// gRPC-dispatch path — the satellite picks the Resource up via
// its c-r watch and runs the apply chain locally.
type ResourceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Store is the shared blockstor store. Used by the auto-diskful
	// promotion path to look up storage pools per node without
	// requiring a separate StoragePool client cache.
	Store store.Store

	// APIReader is the uncached, apiserver-direct client. Used
	// specifically before DRBD node-id allocation so two replicas
	// reconciling in parallel can't both read a stale (nil)
	// nodeID off the cache and pick the same lowest-free value.
	// Wired from `mgr.GetAPIReader()` in SetupWithManager.
	APIReader client.Reader

	// allocMu serialises DRBD-ID allocation across replicas of the
	// same RD. APIReader bypasses the informer cache but doesn't
	// help if two goroutines fan-read simultaneously — both still
	// see the same not-yet-written state. A per-RD mutex held
	// across read+write makes the allocation atomic in the
	// single-controller process. Different RDs still allocate in
	// parallel.
	allocMu sync.Map // RD name → *sync.Mutex
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions,verbs=get;list;watch

// Reconcile reads a Resource and pushes the matching DesiredResource
// to the satellite that hosts it. Per-replica errors land in the
// log; transport faults trigger a 10s requeue.
func (r *ResourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var target blockstoriov1alpha1.Resource

	err := r.Get(ctx, req.NamespacedName, &target)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	// Deletion path: strip the legacy controller-side finalizer if
	// it's still around so the apiserver can finalise. Satellite
	// teardown runs under its own
	// `blockstor.io.blockstor.io/satellite-resource` finalizer.
	if !target.DeletionTimestamp.IsZero() {
		return r.stripLegacyFinalizer(ctx, &target)
	}

	// Drop a stale controller-side finalizer on a live Resource —
	// rolling-upgrade carry-over from the pre-Phase-10.6 code.
	if slices.Contains(target.Finalizers, resourceFinalizer) {
		target.Finalizers = slices.DeleteFunc(target.Finalizers,
			func(s string) bool { return s == resourceFinalizer })

		err = r.Update(ctx, &target)
		if err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true}, nil
	}

	// Skip housekeeping for malformed Resources — without a
	// parent RD reference the seed-from-Gi / DRBD-ID allocation
	// have nothing to anchor against. The scaffolded envtest
	// suite exercises this path.
	if target.Spec.ResourceDefinitionName == "" {
		return ctrl.Result{}, nil
	}

	return r.runApply(ctx, &target)
}

// runApply is the apply branch of Reconcile. Pulled out to keep
// Reconcile under the funlen budget.
func (r *ResourceReconciler) runApply(ctx context.Context, target *blockstoriov1alpha1.Resource) (ctrl.Result, error) {
	var resList blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &resList)
	if err != nil {
		return ctrl.Result{}, err
	}

	peers := make([]blockstoriov1alpha1.Resource, 0, len(resList.Items))

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName == target.Spec.ResourceDefinitionName {
			peers = append(peers, resList.Items[i])
		}
	}

	var nodeList blockstoriov1alpha1.NodeList

	err = r.List(ctx, &nodeList)
	if err != nil {
		return ctrl.Result{}, err
	}

	rdPtr, err := r.lookupRD(ctx, target.Spec.ResourceDefinitionName)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Allocate DRBD node-id (and port/minor on the first replica) and
	// persist via Status before pushing to the satellite. node-id MUST
	// be stable for the lifetime of the replica — re-numbering would
	// re-map DRBD bitmaps and corrupt data on resync. If allocation
	// changes Status we requeue so the next reconcile sees the
	// committed value before dispatching.
	allocated, err := r.ensureDRBDIDs(ctx, target, peers)
	if err != nil {
		return ctrl.Result{}, err
	}

	if allocated {
		return ctrl.Result{Requeue: true}, nil
	}

	// Initial-sync skip seeding (Phase 8.1): on a freshly-added
	// replica, pick the CurrentGi of an existing UpToDate peer and
	// stamp it into Spec.Volumes[i].SeedFromGi. The satellite
	// reconciler then pre-seeds the new replica's DRBD metadata
	// before drbdadm up so DRBD's GI handshake skips the full
	// initial-sync. Idempotent: re-runs on a Resource whose
	// SeedFromGi is already set leave Spec alone.
	seeded, err := r.ensureSeedFromGi(ctx, target, peers, rdPtr)
	if err != nil {
		return ctrl.Result{}, err
	}

	if seeded {
		return ctrl.Result{Requeue: true}, nil
	}

	// Auto-diskful: when a DISKLESS replica is actively used by a
	// consumer (InUse=true on this node) AND the hosting node has
	// a viable storage pool, promote it to diskful so reads stay
	// local. Cleanup (demote on idle) is intentionally not
	// automated yet — needs hysteresis to avoid flapping on
	// transient opens; operators demote via `linstor r d` until
	// then.
	promoted, err := r.maybeAutoDiskful(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}

	if promoted {
		return ctrl.Result{Requeue: true}, nil
	}

	// Housekeeping done. The satellite's c-r reconciler watches
	// Resource and runs Apply locally — the controller no longer
	// dispatches via gRPC (Phase 10.6).
	_ = peers
	_ = nodeList
	_ = rdPtr

	return ctrl.Result{}, nil
}

// maybeAutoDiskful flips a DISKLESS-but-actively-used replica to
// diskful when the hosting node has a usable storage pool. The
// satellite reconciler picks up the spec change on the next pass
// and creates the LV/zvol + attach. Returns true when Spec was
// mutated so the caller can requeue.
func (r *ResourceReconciler) maybeAutoDiskful(ctx context.Context, target *blockstoriov1alpha1.Resource) (bool, error) {
	if !slices.Contains(target.Spec.Flags, apiv1.ResourceFlagDiskless) {
		return false, nil
	}

	if !target.Status.InUse {
		return false, nil
	}

	if slices.Contains(target.Spec.Flags, apiv1.ResourceFlagTieBreaker) {
		// Tiebreaker witnesses must stay diskless — they're chosen
		// for the network presence, not local storage. Promoting a
		// tiebreaker would defeat the quorum semantic.
		return false, nil
	}

	pool, err := r.firstAvailablePool(ctx, target.Spec.NodeName)
	if err != nil {
		return false, err
	}

	if pool == "" {
		// No pool on this node → can't promote. Stay diskless.
		return false, nil
	}

	target.Spec.Flags = slices.DeleteFunc(target.Spec.Flags,
		func(s string) bool { return s == apiv1.ResourceFlagDiskless })

	// Phase 10.3 step: prefer typed Spec.StoragePool. Keep the
	// legacy Props key in sync for forward-compat with any reader
	// that hasn't migrated.
	target.Spec.StoragePool = pool

	if target.Spec.Props == nil {
		target.Spec.Props = map[string]string{}
	}

	target.Spec.Props["StorPoolName"] = pool

	err = r.Update(ctx, target)
	if err != nil {
		return false, err
	}

	return true, nil
}

// firstAvailablePool returns any non-diskless storage pool present on
// the named node. Used by the auto-diskful promotion to pick a
// destination for the freshly-attached LV. We don't try to be
// clever: production clusters typically have one pool per node.
func (r *ResourceReconciler) firstAvailablePool(ctx context.Context, nodeName string) (string, error) {
	pools, err := r.Store.StoragePools().List(ctx)
	if err != nil {
		return "", err
	}

	for i := range pools {
		if pools[i].NodeName != nodeName {
			continue
		}

		if pools[i].ProviderKind == apiv1.StoragePoolKindDiskless {
			continue
		}

		return pools[i].StoragePoolName, nil
	}

	return "", nil
}

// stripLegacyFinalizer removes the pre-Phase-10.6 controller-
// side finalizer when a Resource is being deleted. The
// satellite's own `blockstor.io.blockstor.io/satellite-resource`
// finalizer owns the teardown chain end-to-end now; this hook
// only exists to clean up Resources that still carry the old
// finalizer after a rolling upgrade.
func (r *ResourceReconciler) stripLegacyFinalizer(ctx context.Context, target *blockstoriov1alpha1.Resource) (ctrl.Result, error) {
	if !slices.Contains(target.Finalizers, resourceFinalizer) {
		return ctrl.Result{}, nil
	}

	target.Finalizers = slices.DeleteFunc(target.Finalizers,
		func(s string) bool { return s == resourceFinalizer })

	err := r.Update(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// lookupRD fetches the parent ResourceDefinition. A NotFound is
// converted to (nil, nil) so the dispatcher can still push the
// .res for connection setup; any other error bubbles.
func (r *ResourceReconciler) lookupRD(ctx context.Context, name string) (*blockstoriov1alpha1.ResourceDefinition, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: name}, &rd)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil //nolint:nilnil // intentional: caller treats nil RD as no-volume push
		}

		return nil, err
	}

	return &rd, nil
}

// ensureDRBDIDs allocates a stable DRBD node-id for target (and a
// port + minor for the RD if no sibling has one yet) and persists the
// values on Status. Returns true when Status was changed; the caller
// requeues so the next reconcile dispatches with the committed values.
//
// node-id allocation is the lowest free 0..MaxPeers-1 not held by
// any sibling Resource of the same RD — sibling ids stay put, only
// the unallocated target gets a new value. This is the load-bearing
// invariant: re-using ids on a different node would re-map DRBD
// bitmaps mid-flight and corrupt data.
//
// Port + minor allocation is cluster-wide: we scan every Resource
// across all RDs to gather taken values, then pick the lowest free
// from the pool range. Two RDs racing to pick the same port resolve
// deterministically (same taken set → same answer); the loser's
// Status update is rejected by Kube's optimistic concurrency check
// and the next reconcile picks the next free port.
func (r *ResourceReconciler) ensureDRBDIDs(ctx context.Context, target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) (bool, error) {
	original := target.Status.DeepCopy()

	// Serialise allocation across replicas of the same RD. The
	// APIReader-direct read alone doesn't fix the race — two
	// goroutines can both observe taken=[] simultaneously and
	// each pick 0. The mutex held across {read taken → pick →
	// Status().Update} forces a strict serial order so the
	// second goroutine reads the first one's committed Status.
	// Different RDs allocate in parallel.
	mu := r.rdAllocMu(target.Spec.ResourceDefinitionName)
	mu.Lock()
	defer mu.Unlock()

	if target.Status.DRBDNodeID == nil {
		id, err := r.allocateNodeIDLocked(ctx, target)
		if err != nil {
			return false, err
		}

		target.Status.DRBDNodeID = &id
	}

	if target.Status.DRBDPort == nil {
		port, err := r.allocatePort(ctx, target.Spec.NodeName)
		if err != nil {
			return false, err
		}

		target.Status.DRBDPort = &port
	}

	if target.Status.DRBDMinor == nil {
		minor, err := r.allocateMinor(ctx, target.Spec.NodeName)
		if err != nil {
			return false, err
		}

		target.Status.DRBDMinor = &minor
	}

	// `peers` is plumbed in for completeness with the rest of the
	// reconciler; its DRBDNodeID values aren't used (we re-read via
	// APIReader for correctness).
	_ = peers

	if equalStatus(original, &target.Status) {
		return false, nil
	}

	err := r.Status().Update(ctx, target)
	if err != nil {
		return false, err
	}

	return true, nil
}

func equalStatus(a, b *blockstoriov1alpha1.ResourceStatus) bool {
	return ptrEqI32(a.DRBDNodeID, b.DRBDNodeID) &&
		ptrEqI32(a.DRBDPort, b.DRBDPort) &&
		ptrEqI32(a.DRBDMinor, b.DRBDMinor)
}

// ensureSeedFromGi pre-seeds Spec.Volumes[i].SeedFromGi on a
// freshly-added replica with an existing UpToDate peer's CurrentGi
// so DRBD-9's GI handshake on first connect skips the full
// initial-sync. Returns true when Spec was mutated so the caller
// requeues with the persisted value (the next reconcile dispatches
// to the satellite, which consumes SeedFromGi via drbdmeta).
//
// Idempotency: any volume that already has SeedFromGi set is left
// alone — the satellite reconciler is responsible for consuming
// it once and the controller never rewrites. Volumes whose RD
// VolumeDefinition has no peer with a non-empty CurrentGi (fresh
// cluster, all-new replicas) get nothing set; they pay the
// (acceptable) full initial-sync cost on first activation.
//
// Skipped entirely for DISKLESS replicas — they have no metadata
// block to seed.
func (r *ResourceReconciler) ensureSeedFromGi(_ context.Context, target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) (bool, error) {
	if rd == nil || len(rd.Spec.VolumeDefinitions) == 0 {
		return false, nil
	}

	if slices.Contains(target.Spec.Flags, apiv1.ResourceFlagDiskless) {
		return false, nil
	}

	mutated := false

	for _, vd := range rd.Spec.VolumeDefinitions {
		if seedAlreadySet(target, vd.VolumeNumber) {
			continue
		}

		seed := pickSeedFromPeers(peers, target.Name, vd.VolumeNumber)
		if seed == "" {
			continue
		}

		setSeedFromGi(target, vd.VolumeNumber, seed)

		mutated = true
	}

	if !mutated {
		return false, nil
	}

	if err := r.Update(context.Background(), target); err != nil { //nolint:contextcheck // ctx-cancel survives Update — propagating it would race the requeue
		return false, err
	}

	return true, nil
}

// seedAlreadySet reports whether target.Spec.Volumes already has a
// SeedFromGi for the given volume number. Used to make
// ensureSeedFromGi idempotent.
func seedAlreadySet(target *blockstoriov1alpha1.Resource, volumeNumber int32) bool {
	for i := range target.Spec.Volumes {
		if target.Spec.Volumes[i].VolumeNumber == volumeNumber && target.Spec.Volumes[i].SeedFromGi != "" {
			return true
		}
	}

	return false
}

// pickSeedFromPeers picks an existing peer's CurrentGi for the given
// volume number. Deterministic: peers are sorted by Name and the
// first matching one wins, so two reconcile races converge on the
// same answer (no thrashing of Spec.Volumes[i].SeedFromGi).
//
// Excludes the target itself, peers without a CurrentGi for this
// volume, and peers whose Status.Volumes[i].DiskState != UpToDate
// (a peer that's still syncing wouldn't have the authoritative GI).
func pickSeedFromPeers(peers []blockstoriov1alpha1.Resource, targetName string, volumeNumber int32) string {
	candidates := make([]blockstoriov1alpha1.Resource, 0, len(peers))

	for i := range peers {
		if peers[i].Name == targetName {
			continue
		}

		gi := volumeCurrentGi(&peers[i], volumeNumber)
		if gi == "" {
			continue
		}

		if volumeDiskState(&peers[i], volumeNumber) != "UpToDate" {
			continue
		}

		candidates = append(candidates, peers[i])
	}

	if len(candidates) == 0 {
		return ""
	}

	slices.SortFunc(candidates, func(a, b blockstoriov1alpha1.Resource) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		default:
			return 0
		}
	})

	return volumeCurrentGi(&candidates[0], volumeNumber)
}

// volumeCurrentGi returns the CurrentGi for the given volume number
// from a Resource's Status, or "" if not present.
func volumeCurrentGi(res *blockstoriov1alpha1.Resource, volumeNumber int32) string {
	for i := range res.Status.Volumes {
		if res.Status.Volumes[i].VolumeNumber == volumeNumber {
			return res.Status.Volumes[i].CurrentGi
		}
	}

	return ""
}

// volumeDiskState returns the DiskState for the given volume number
// from a Resource's Status, or "" if not present.
func volumeDiskState(res *blockstoriov1alpha1.Resource, volumeNumber int32) string {
	for i := range res.Status.Volumes {
		if res.Status.Volumes[i].VolumeNumber == volumeNumber {
			return res.Status.Volumes[i].DiskState
		}
	}

	return ""
}

// setSeedFromGi mutates target.Spec.Volumes to record the seed GI
// for the given volume number. Appends a new entry if no
// ResourceVolumeSpec exists for the volume; otherwise updates in
// place.
func setSeedFromGi(target *blockstoriov1alpha1.Resource, volumeNumber int32, seed string) {
	for i := range target.Spec.Volumes {
		if target.Spec.Volumes[i].VolumeNumber == volumeNumber {
			target.Spec.Volumes[i].SeedFromGi = seed

			return
		}
	}

	target.Spec.Volumes = append(target.Spec.Volumes, blockstoriov1alpha1.ResourceVolumeSpec{
		VolumeNumber: volumeNumber,
		SeedFromGi:   seed,
	})
}

func ptrEqI32(a, b *int32) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

// allocatePort picks a TCP port from the hosting node's range.
// Upstream LINSTOR moved from per-RD to per-resource ports: each
// replica picks its own port from its node's local range. That way
// nodes can run unrelated TCP-port pools (port 7000 on n1 has nothing
// to do with port 7000 on n2), and a port collision on one node
// doesn't affect the rest of the cluster.
//
// Range source: the node's `DrbdOptions/TcpPortRange` prop ("min-max")
// with controller-wide defaults [DefaultPortMin, DefaultPortMax] when
// the prop is absent. Taken set: every Resource currently scheduled
// on the same node.
func (r *ResourceReconciler) allocatePort(ctx context.Context, nodeName string) (int32, error) {
	low, high, err := r.portRangeForNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	taken, err := r.takenPortsOnNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	return drbd.LowestFreePort(taken, low, high)
}

// allocateMinor mirrors allocatePort for /dev/drbd<N>. Minor numbers
// are local device-name suffixes; per-node scope is the natural fit.
func (r *ResourceReconciler) allocateMinor(ctx context.Context, nodeName string) (int32, error) {
	low, high, err := r.minorRangeForNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	taken, err := r.takenMinorsOnNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	return drbd.LowestFreeMinor(taken, low, high)
}

// portRangeForNode reads the DRBD TCP port range off the named Node
// CRD, falling back to the controller-wide default. Phase 10.3:
// typed `Spec.DRBDPortRange` wins; legacy `Props["DrbdOptions/
// TcpPortRange"]` is the forward-compat fallback. Default
// (7000-7999) covers fresh clusters with neither set.
func (r *ResourceReconciler) portRangeForNode(ctx context.Context, nodeName string) (int32, int32, error) {
	return r.nodeRange(ctx, nodeName,
		func(s *blockstoriov1alpha1.NodeSpec) *blockstoriov1alpha1.PortRange { return s.DRBDPortRange },
		"DrbdOptions/TcpPortRange",
		drbd.DefaultPortMin, drbd.DefaultPortMax)
}

func (r *ResourceReconciler) minorRangeForNode(ctx context.Context, nodeName string) (int32, int32, error) {
	return r.nodeRange(ctx, nodeName,
		func(s *blockstoriov1alpha1.NodeSpec) *blockstoriov1alpha1.PortRange { return s.DRBDMinorRange },
		"DrbdOptions/MinorNrRange",
		drbd.DefaultMinorMin, drbd.DefaultMinorMax)
}

// nodeRange resolves a port/minor range for the named Node. Reads
// the typed pointer first via the picker; on nil/missing falls back
// to the legacy "min-max" Props key; on absent both, returns
// defaults. Bad format on the Props side returns an error so the
// operator notices a typo. Missing Node CRD silently uses defaults
// (consistent with the legacy behaviour).
func (r *ResourceReconciler) nodeRange(
	ctx context.Context,
	nodeName string,
	pick func(*blockstoriov1alpha1.NodeSpec) *blockstoriov1alpha1.PortRange,
	legacyProp string,
	defLow, defHigh int32,
) (int32, int32, error) {
	var node blockstoriov1alpha1.Node
	if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
		if errors.IsNotFound(err) {
			return defLow, defHigh, nil
		}

		return 0, 0, err
	}

	if typed := pick(&node.Spec); typed != nil {
		return typed.Min, typed.Max, nil
	}

	raw := node.Spec.Props[legacyProp]
	if raw == "" {
		return defLow, defHigh, nil
	}

	low, high, err := drbd.ParseRange(raw)
	if err != nil {
		return 0, 0, err
	}

	return low, high, nil
}

// takenPortsOnNode scans every Resource scheduled on the given node
// and returns its persisted DRBDPort. The allocator uses this to
// guarantee no two replicas on the same node take the same port.
func (r *ResourceReconciler) takenPortsOnNode(ctx context.Context, nodeName string) ([]int32, error) {
	return r.takenOnNode(ctx, nodeName, func(s *blockstoriov1alpha1.ResourceStatus) *int32 { return s.DRBDPort })
}

// takenMinorsOnNode returns every minor consumed on the node. A
// multi-volume RD consumes N consecutive minors (the .res renderer
// emits volume k at base+k), so for each Resource we expand its
// recorded base minor to the full range based on the parent RD's
// VolumeDefinitions count. Without this expansion, a fresh
// Resource's allocator would happily pick base+1 on a node where a
// 2-volume sibling already owns base..base+1.
func (r *ResourceReconciler) takenMinorsOnNode(ctx context.Context, nodeName string) ([]int32, error) {
	list := &blockstoriov1alpha1.ResourceList{}
	if err := r.List(ctx, list); err != nil {
		return nil, err
	}

	rdVolCounts := map[string]int{}

	out := make([]int32, 0, len(list.Items))

	for i := range list.Items {
		if list.Items[i].Spec.NodeName != nodeName {
			continue
		}

		base := list.Items[i].Status.DRBDMinor
		if base == nil {
			continue
		}

		rdName := list.Items[i].Spec.ResourceDefinitionName
		volCount, cached := rdVolCounts[rdName]

		if !cached {
			volCount = 1 // safe default: at least the base is taken

			var rd blockstoriov1alpha1.ResourceDefinition
			if err := r.Get(ctx, client.ObjectKey{Name: rdName}, &rd); err == nil {
				if n := len(rd.Spec.VolumeDefinitions); n > 0 {
					volCount = n
				}
			}

			rdVolCounts[rdName] = volCount
		}

		for off := range int32(volCount) {
			out = append(out, *base+off)
		}
	}

	return out, nil
}

// takenOnNode is the shared scan: list every Resource on `nodeName`,
// pluck the int32 pointer the caller cares about, and return the
// non-nil set. Used by the per-node port and minor allocators.
func (r *ResourceReconciler) takenOnNode(ctx context.Context, nodeName string, pick func(*blockstoriov1alpha1.ResourceStatus) *int32) ([]int32, error) {
	list := &blockstoriov1alpha1.ResourceList{}
	if err := r.List(ctx, list); err != nil {
		return nil, err
	}

	out := make([]int32, 0, len(list.Items))

	for i := range list.Items {
		if list.Items[i].Spec.NodeName != nodeName {
			continue
		}

		if v := pick(&list.Items[i].Status); v != nil {
			out = append(out, *v)
		}
	}

	return out, nil
}

// resolveEffectiveProps delegates to the shared `pkg/effectiveprops`
// package (Phase 10.1 lift-out) so the satellite-side reconciler
// can use the same hierarchy resolution without duplicating
// 80 lines of merge logic. The wrapper survives because the
// existing call sites are `r.resolveEffectiveProps(ctx, target, rd)`
// and we don't want to churn them.
func (r *ResourceReconciler) resolveEffectiveProps(ctx context.Context, target *blockstoriov1alpha1.Resource, rdPtr *blockstoriov1alpha1.ResourceDefinition) (map[string]string, error) {
	return effectiveprops.Resolve(ctx, r.Client, target, rdPtr)
}

// resolveLayerStack walks the RD → RG hierarchy and returns the
// effective layer composition. Returns nil for the dispatcher's
// default-fall-through behaviour when nothing is set anywhere — the
// dispatcher then defaults to whatever rd.Spec.LayerStack contains
// (also possibly nil → satellite-side default ["DRBD","STORAGE"]).
func (r *ResourceReconciler) resolveLayerStack(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) []string {
	if rd == nil {
		return nil
	}

	if len(rd.Spec.LayerStack) > 0 {
		return rd.Spec.LayerStack
	}

	if rd.Spec.ResourceGroupName == "" {
		return nil
	}

	var rg blockstoriov1alpha1.ResourceGroup
	if err := r.Get(ctx, client.ObjectKey{Name: rd.Spec.ResourceGroupName}, &rg); err != nil {
		return nil
	}

	return rg.Spec.SelectFilter.LayerStack
}

// EnsureDRBDIDsForTest is an exported alias for ensureDRBDIDs. The
// allocator is package-private because it's an internal reconciler
// step, but the property tests live in package controller_test and
// need a way in. Production callers use Reconcile.
func (r *ResourceReconciler) EnsureDRBDIDsForTest(ctx context.Context, target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) (bool, error) {
	return r.ensureDRBDIDs(ctx, target, peers)
}

// SetupWithManager sets up the controller with the Manager.
//
// We Watch ResourceDefinitions and sibling Resources too:
//   - RD changes (volume size, prop bag, encryption passphrase, quorum
//     toggle) must re-fire every replica's reconcile so the satellite
//     gets the updated VolumeDefinitions / DRBD options bag.
//   - Sibling-Resource changes (a witness gets created or removed)
//     must re-fire the OTHER replicas so their rendered .res reflects
//     the new peer set. Without this, R1's .res keeps the pre-witness
//     peer list and R3 can't connect (R1 doesn't know it exists).
func (r *ResourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Resource{}).
		Watches(&blockstoriov1alpha1.ResourceDefinition{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueResourcesForRD)).
		Watches(&blockstoriov1alpha1.Resource{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueSiblings)).
		Named("resource").
		Complete(r)
}

// rdAllocMu returns the per-RD allocation mutex, lazily creating
// one on first use. Different RDs see different mutexes so they
// allocate in parallel; replicas of the same RD serialise.
func (r *ResourceReconciler) rdAllocMu(rdName string) *sync.Mutex {
	loaded, _ := r.allocMu.LoadOrStore(rdName, &sync.Mutex{})

	muTyped, _ := loaded.(*sync.Mutex)

	return muTyped
}

// allocateNodeIDLocked picks the next free DRBD node-id for target.
// Caller MUST hold rdAllocMu(target.Spec.ResourceDefinitionName).
//
// The APIReader bypasses the informer cache so we observe any
// sibling's freshly-committed Status. Combined with the mutex,
// concurrent reconciles of the same RD see a strict serial order:
// the second reconcile observes the first one's allocation.
func (r *ResourceReconciler) allocateNodeIDLocked(ctx context.Context, target *blockstoriov1alpha1.Resource) (int32, error) {
	taken, err := r.collectTakenNodeIDs(ctx, target)
	if err != nil {
		return 0, err
	}

	id, err := drbd.LowestFreeNodeID(taken)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// collectTakenNodeIDs returns the DRBDNodeIDs already assigned to
// sibling Resources of the same RD, reading directly from the
// apiserver (no informer cache) to avoid the stale-read race that
// otherwise lets two concurrent reconciles both pick the lowest
// free id.
func (r *ResourceReconciler) collectTakenNodeIDs(ctx context.Context, target *blockstoriov1alpha1.Resource) ([]int32, error) {
	// Fall back to the cached client when APIReader hasn't been
	// wired — tests construct `ResourceReconciler{}` directly with
	// a fake client and skip SetupWithManager; the race we're
	// guarding against only matters under real-cluster
	// informer-cache load, which the fake client doesn't simulate.
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

	var resList blockstoriov1alpha1.ResourceList

	err := reader.List(ctx, &resList)
	if err != nil {
		return nil, err
	}

	taken := make([]int32, 0, len(resList.Items))

	for i := range resList.Items {
		res := &resList.Items[i]

		if res.Name == target.Name {
			continue
		}

		if res.Spec.ResourceDefinitionName != target.Spec.ResourceDefinitionName {
			continue
		}

		if res.Status.DRBDNodeID != nil {
			taken = append(taken, *res.Status.DRBDNodeID)
		}
	}

	return taken, nil
}

// enqueueResourcesForRD maps an RD event to every Resource that
// references it via Spec.ResourceDefinitionName.
func (r *ResourceReconciler) enqueueResourcesForRD(ctx context.Context, obj client.Object) []reconcile.Request {
	rd, ok := obj.(*blockstoriov1alpha1.ResourceDefinition)
	if !ok {
		return nil
	}

	return r.requestsForRD(ctx, rd.Name, "")
}

// enqueueSiblings maps a Resource event to every OTHER Resource of
// the same RD. The originator's own reconcile fires through For(),
// so we exclude it from the fan-out to avoid the redundant requeue.
func (r *ResourceReconciler) enqueueSiblings(ctx context.Context, obj client.Object) []reconcile.Request {
	res, ok := obj.(*blockstoriov1alpha1.Resource)
	if !ok || res.Spec.ResourceDefinitionName == "" {
		return nil
	}

	return r.requestsForRD(ctx, res.Spec.ResourceDefinitionName, res.Name)
}

// requestsForRD returns reconcile.Request entries for every Resource
// of the named RD, optionally excluding `excludeName` (used when the
// originating Resource is already getting its own reconcile via For).
func (r *ResourceReconciler) requestsForRD(ctx context.Context, rdName, excludeName string) []reconcile.Request {
	var resList blockstoriov1alpha1.ResourceList

	if err := r.List(ctx, &resList); err != nil {
		return nil
	}

	out := make([]reconcile.Request, 0, len(resList.Items))

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		if resList.Items[i].Name == excludeName {
			continue
		}

		out = append(out, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: resList.Items[i].Name},
		})
	}

	return out
}
