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

	cerrors "github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
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

// controllerDRBDIDsFieldOwner is the SSA field-manager identity the
// controller-side allocator uses when it writes Status.DRBD{NodeID,
// Port,Minor}. Distinct from the satellite-side observer + reconciler
// owners so the apiserver merges the three claims cleanly. Replacing
// the old Status().Update path was necessary to stop the controller
// from clobbering observer-owned fields (disk_state, in_use, etc).
const controllerDRBDIDsFieldOwner = "blockstor-controller-drbd-ids"

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

	// Bug 149: orphan detection. `kubectl delete rd --cascade=orphan`
	// removes the parent RD CRD without touching its child Resources
	// — leaving Resources alive with no parent. They never get GC'd
	// because:
	//   - the controller-side rd-delete cascade (which would tear
	//     them down) doesn't run on the kubectl path;
	//   - the satellite reconciler's handleDelete only fires on a
	//     DeletionTimestamp, which the orphan never gets without
	//     us setting it.
	//
	// Trigger Delete on the orphan Resource so kube-apiserver stamps
	// a DeletionTimestamp; the satellite reconciler then runs its
	// teardown chain (with Bug 107's annotation-based volume-number
	// fallback for the case where the RD CRD really is gone).
	orphaned, err := r.handleOrphan(ctx, &target)
	if err != nil {
		return ctrl.Result{}, err
	}

	if orphaned {
		return ctrl.Result{}, nil
	}

	return r.runApply(ctx, &target)
}

// satelliteResourceFinalizer mirrors the
// `pkg/satellite/controllers.SatelliteResourceFinalizer` constant
// without an import — the satellite package imports
// `internal/controller` for shared helpers, so the reverse direction
// is forbidden to avoid a cycle. Duplicating one string is the
// pragmatic fix; a rename on either side breaks compile here via the
// related tests, which reference the same literal.
const satelliteResourceFinalizer = "blockstor.io.blockstor.io/satellite-resource"

// handleOrphan checks whether the Resource's parent ResourceDefinition
// CRD still exists. When the RD is gone — the kubectl-cascade-orphan
// state Bug 149 documents — Delete is invoked on the Resource so the
// satellite finalizer chain runs and the orphan eventually disappears.
// Returns true when the Resource was orphaned (caller short-circuits
// the rest of the reconcile chain because housekeeping on a doomed
// Resource is wasted work).
//
// Production-state gate: the orphan path only fires for Resources
// that have been successfully applied at least once — either they
// carry the satellite finalizer (`SatelliteResourceFinalizer`) or
// the Bug 107 `blockstor.io/volume-numbers` annotation. Fresh
// scaffolded Resources that have never reached a satellite (e.g.
// envtest scenarios that mint a Resource without an RD as a stub)
// are left alone — there's nothing for the satellite to tear down,
// and triggering Delete on them would surprise downstream test
// harnesses that build Resources without their parent RD.
func (r *ResourceReconciler) handleOrphan(ctx context.Context, target *blockstoriov1alpha1.Resource) (bool, error) {
	if !resourceWasApplied(target) {
		return false, nil
	}

	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: target.Spec.ResourceDefinitionName}, &rd)
	if err == nil {
		return false, nil
	}

	if !errors.IsNotFound(err) {
		return false, err
	}

	// Parent RD gone — invoke Delete so the satellite finalizer chain
	// runs. Idempotent: a second pass on an already-deleting Resource
	// surfaces a no-op because kube-apiserver doesn't re-stamp the
	// DeletionTimestamp once set.
	err = r.Delete(ctx, target)
	if err != nil && !errors.IsNotFound(err) {
		return false, err
	}

	return true, nil
}

// resourceWasApplied reports whether the Resource carries evidence
// of at least one successful satellite-side apply pass: either the
// satellite finalizer (stamped on first reconcile by the satellite
// reconciler) or the Bug 107 volume-numbers annotation (stamped on
// every successful apply). Without either, the Resource is a
// freshly-scaffolded stub and the orphan path is a no-op.
func resourceWasApplied(target *blockstoriov1alpha1.Resource) bool {
	if slices.Contains(target.Finalizers, satelliteResourceFinalizer) {
		return true
	}

	if target.Annotations != nil {
		if _, ok := target.Annotations[blockstoriov1alpha1.ResourceAnnotationVolumeNumbers]; ok {
			return true
		}
	}

	return false
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
	// Serialise allocation across replicas of the same RD. The
	// APIReader-direct read alone doesn't fix the race — two
	// goroutines can both observe taken=[] simultaneously and
	// each pick 0. The mutex held across {read taken → pick →
	// Status().Update} forces a strict serial order so the second
	// goroutine reads the first one's committed Status. Different
	// RDs allocate in parallel.
	mu := r.rdAllocMu(target.Spec.ResourceDefinitionName)
	mu.Lock()
	defer mu.Unlock()

	// `peers` is plumbed in for completeness with the rest of the
	// reconciler; its DRBDNodeID values aren't used (we re-read via
	// APIReader for correctness).
	_ = peers

	mutated := false

	// retry-on-conflict because the satellite-side observer
	// constantly writes Status.Volumes / Status.Connections via SSA
	// while we're trying to write Status.DRBD{NodeID,Port,Minor}.
	// Without a retry loop the allocator gives up after a single
	// stale-version conflict and the satellite stays stuck
	// "waiting for controller-side DRBD-ID allocation" until the
	// next controller-runtime backoff window, which can be minutes.
	// Tests construct ResourceReconciler{} directly with a fake
	// client and skip SetupWithManager; the cached client doubles
	// as the APIReader when the latter wasn't injected.
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var inner error

		mutated, inner = r.allocateAndApplyDRBDIDs(ctx, reader, target)

		return inner
	})
	if err != nil {
		return false, cerrors.Wrap(err, "allocate DRBD ids")
	}

	return mutated, nil
}

// allocateDRBDFields fills in any missing DRBD-{NodeID,Port,Minor}
// allocateAndApplyDRBDIDs runs the per-retry body of ensureDRBDIDs:
// refetch target, allocate any missing ids, and SSA-Patch the
// three fields back. Returns (mutated, err).
//
// SSA Apply (not Update) so the satellite-observer's per-volume
// diskState / connections / replicationState field-ownership
// survives. A plain Status().Update writes the whole status object
// and the apiserver drops observer's claims on the merge, leaving
// Status.Volumes[i].diskState blank for the rest of the resource
// lifetime.
func (r *ResourceReconciler) allocateAndApplyDRBDIDs(ctx context.Context, reader client.Reader, target *blockstoriov1alpha1.Resource) (bool, error) {
	err := reader.Get(ctx, client.ObjectKey{Name: target.Name}, target)
	if err != nil {
		// Resource gone between reconcile dispatch and direct
		// APIReader read — common race when the parent RD reconciler
		// just created the witness Resource and the workqueue fired
		// before the apiserver fully propagated, or when --force
		// deletion races against an in-flight reconcile. Nothing to
		// allocate; let the next event drive the allocation.
		if errors.IsNotFound(err) {
			return false, nil
		}

		return false, err
	}

	original := target.Status.DeepCopy()

	err = r.allocateDRBDFields(ctx, target)
	if err != nil {
		return false, err
	}

	if equalStatus(original, &target.Status) {
		return false, nil
	}

	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Resource",
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: target.Name},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: target.Status.DRBDNodeID,
			DRBDPort:   target.Status.DRBDPort,
			DRBDMinor:  target.Status.DRBDMinor,
		},
	}

	err = r.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available
		client.FieldOwner(controllerDRBDIDsFieldOwner),
		client.ForceOwnership)
	if err != nil {
		// SSA Patch on a Resource that was deleted between the Get
		// above and now returns NotFound. Same race-window as the
		// Get path — let the next event drive the next attempt.
		if errors.IsNotFound(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// fields on target.Status. Pulled out of ensureDRBDIDs so the retry
// loop body stays under the funlen budget.
//
// Bug 266+268 (CRITICAL, data-correctness): port and minor allocation
// runs at PER-RD (cluster) scope, not per-node. The satellite's `.res`
// renderer writes ONE port and ONE minor across every `on <node>`
// block in the file — divergent values across peers produce
// inconsistent .res files, drbdadm adjust rejects "minor mismatch" /
// conflicting node-ids, and the connection stays Connecting forever
// (no initial sync). The per-RD-scope allocator picks ONE value for
// the RD, persists it on the parent RD's Status, and every Resource
// inherits from there.
func (r *ResourceReconciler) allocateDRBDFields(ctx context.Context, target *blockstoriov1alpha1.Resource) error {
	if target.Status.DRBDNodeID == nil {
		id, err := r.allocateNodeIDLocked(ctx, target)
		if err != nil {
			return err
		}

		target.Status.DRBDNodeID = &id
	}

	rdPort, rdMinor, err := r.ensureRDPortMinor(ctx, target)
	if err != nil {
		return err
	}

	if target.Status.DRBDPort == nil || *target.Status.DRBDPort != rdPort {
		target.Status.DRBDPort = &rdPort
	}

	if target.Status.DRBDMinor == nil || *target.Status.DRBDMinor != rdMinor {
		target.Status.DRBDMinor = &rdMinor
	}

	return nil
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

// ensureRDPortMinor returns the cluster-scope port and minor for the
// RD that owns `target`, allocating both on the parent RD's Status
// the first time and reusing them on every subsequent call. The
// returned values are guaranteed identical across every Resource
// of the RD — that's the load-bearing invariant the satellite's
// `.res` renderer depends on (Bug 266 + Bug 268).
//
// Allocation strategy:
//
//  1. If RD.Status.DRBDPort/DRBDMinor are already set, return them.
//
//  2. Otherwise, compute the INTERSECTION of every hosting node's
//     range — a value must be allocatable on every node that hosts
//     a replica of this RD. Per-node `DrbdOptions/TcpPortRange` and
//     `DrbdOptions/MinorNrRange` props still constrain the choice;
//     cluster-scope `TcpPortAutoRange` / `MinorNrAutoRange` provide
//     the default when no per-node override exists.
//
//  3. Gather taken values cluster-wide:
//     - ports: every Resource.Status.DRBDPort across the cluster +
//     every RD.Status.DRBDPort already allocated.
//     - minors: every Resource.Status.DRBDMinor (expanded to its
//     RD's multi-volume range) + every RD.Status.DRBDMinor's
//     range across the cluster.
//
//  4. Pick the lowest free value from the intersected range that
//     isn't in the taken set, and stamp it on RD.Status via an
//     SSA-Patch — optimistic-concurrency loses cleanly to a
//     racing reconcile (which picked the same or a higher value),
//     and the next reconcile reads back the committed value.
//
// Multi-volume RDs reserve drbdMinor..drbdMinor+N-1 (the .res
// renderer emits volume k at base+k, so adjacent RDs must not land
// in the middle of an already-claimed range).
func (r *ResourceReconciler) ensureRDPortMinor(ctx context.Context, target *blockstoriov1alpha1.Resource) (int32, int32, error) {
	rdName := target.Spec.ResourceDefinitionName

	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: rdName}, &rd)
	if err != nil {
		if errors.IsNotFound(err) {
			// RD absent (unit-test fast path or rd-delete in flight).
			// Inherit from any sibling Resource that already has
			// values allocated — that's how we maintain the
			// per-RD invariant when no RD CRD exists to persist on.
			// First replica picks via the cluster-wide allocator;
			// every subsequent replica copies the sibling's values.
			return r.ensureRDPortMinorWithoutRD(ctx, target)
		}

		return 0, 0, err
	}

	if rd.Status.DRBDPort != nil && rd.Status.DRBDMinor != nil {
		return *rd.Status.DRBDPort, *rd.Status.DRBDMinor, nil
	}

	port, minor, err := r.allocateRDPortMinor(ctx, &rd)
	if err != nil {
		return 0, 0, err
	}

	rd.Status.DRBDPort = &port
	rd.Status.DRBDMinor = &minor

	err = r.Status().Update(ctx, &rd)
	if err != nil {
		// On conflict, a sibling reconcile already stamped values.
		// Re-fetch and return whatever they committed.
		if errors.IsConflict(err) {
			var fresh blockstoriov1alpha1.ResourceDefinition

			fetchErr := r.Get(ctx, client.ObjectKey{Name: rdName}, &fresh)
			if fetchErr != nil {
				return 0, 0, fetchErr
			}

			if fresh.Status.DRBDPort != nil && fresh.Status.DRBDMinor != nil {
				return *fresh.Status.DRBDPort, *fresh.Status.DRBDMinor, nil
			}
		}

		return 0, 0, err
	}

	return port, minor, nil
}

// ensureRDPortMinorWithoutRD is the fallback used when the parent
// RD CRD is absent — unit-test fast-paths and the legacy code paths
// where Resources exist without an RD CRD. The contract is the
// same as the main path: every Resource of the RD must observe the
// SAME (port, minor) pair.
//
// Algorithm: scan sibling Resources of the same RD. If any sibling
// already has Status.DRBDPort / Status.DRBDMinor stamped, inherit
// those values. Otherwise pick fresh values via the cluster-wide
// allocator (this becomes the seed every later replica copies).
//
// This branch is exercised primarily by tests that construct
// Resources without a parent RD; production reconciles always have
// the parent RD present (the REST handler creates RD → Resources
// in that order).
func (r *ResourceReconciler) ensureRDPortMinorWithoutRD(ctx context.Context, target *blockstoriov1alpha1.Resource) (int32, int32, error) {
	// Stable-on-already-set: target carries its own committed values
	// from a prior reconcile. Returning them keeps the per-RD
	// invariant stable across re-allocate passes (the test's
	// `allocate()` loop runs the allocator multiple times until no
	// further writes — without this short-circuit, every pass would
	// re-pick a fresh value and `LowestFreePort` would eventually
	// trip ErrPortPoolExhausted on the second iteration of a
	// narrow-range test).
	port, minor := readSiblingPortMinor(target)

	if port == nil || minor == nil {
		// Look at sibling Resources of the same RD: if any one of
		// them already stamped a value, inherit it.
		sp, sm, err := r.scanSiblingPortMinor(ctx, target)
		if err != nil {
			return 0, 0, err
		}

		if port == nil {
			port = sp
		}

		if minor == nil {
			minor = sm
		}
	}

	if port != nil && minor != nil {
		return *port, *minor, nil
	}

	// First replica of the RD: allocate fresh via the cluster-wide
	// path. The per-RD mutex held by the caller (ensureDRBDIDs)
	// serialises this so only one goroutine for the RD reaches the
	// fresh-allocation branch at a time.
	if port == nil {
		fresh, err := r.allocatePortAcrossCluster(ctx, target.Spec.NodeName)
		if err != nil {
			return 0, 0, err
		}

		port = &fresh
	}

	if minor == nil {
		fresh, err := r.allocateMinorAcrossCluster(ctx, target.Spec.NodeName)
		if err != nil {
			return 0, 0, err
		}

		minor = &fresh
	}

	return *port, *minor, nil
}

// readSiblingPortMinor returns target's own currently-stamped
// (port, minor) pointers. Pulled out for testability and to keep
// ensureRDPortMinorWithoutRD under the cyclomatic budget.
func readSiblingPortMinor(target *blockstoriov1alpha1.Resource) (*int32, *int32) {
	return target.Status.DRBDPort, target.Status.DRBDMinor
}

// scanSiblingPortMinor walks every Resource of the same RD as
// `target` (excluding target itself) and returns the first
// non-nil port and minor it finds. Used by the no-RD fallback to
// inherit the RD-scope values from an already-allocated sibling.
func (r *ResourceReconciler) scanSiblingPortMinor(ctx context.Context, target *blockstoriov1alpha1.Resource) (*int32, *int32, error) {
	rdName := target.Spec.ResourceDefinitionName

	var resList blockstoriov1alpha1.ResourceList
	if err := r.List(ctx, &resList); err != nil {
		return nil, nil, err
	}

	var (
		port  *int32
		minor *int32
	)

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		if resList.Items[i].Name == target.Name {
			continue
		}

		if port == nil && resList.Items[i].Status.DRBDPort != nil {
			port = resList.Items[i].Status.DRBDPort
		}

		if minor == nil && resList.Items[i].Status.DRBDMinor != nil {
			minor = resList.Items[i].Status.DRBDMinor
		}

		if port != nil && minor != nil {
			break
		}
	}

	return port, minor, nil
}

// allocateRDPortMinor picks an RD-scope port and minor that:
//   - fits the intersection of every hosting node's range
//   - is free cluster-wide (no other RD or Resource holds it)
//
// Returns the chosen (port, minor) pair. Used by ensureRDPortMinor
// to seed a fresh RD's Status.
func (r *ResourceReconciler) allocateRDPortMinor(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) (int32, int32, error) {
	hostNodes, err := r.hostingNodesForRD(ctx, rd.Name)
	if err != nil {
		return 0, 0, err
	}

	portLow, portHigh, err := r.intersectPortRange(ctx, hostNodes)
	if err != nil {
		return 0, 0, err
	}

	minorLow, minorHigh, err := r.intersectMinorRange(ctx, hostNodes)
	if err != nil {
		return 0, 0, err
	}

	portTaken, err := r.takenPortsCluster(ctx, rd.Name)
	if err != nil {
		return 0, 0, err
	}

	minorTaken, err := r.takenMinorsCluster(ctx, rd.Name)
	if err != nil {
		return 0, 0, err
	}

	port, err := drbd.LowestFreePort(portTaken, portLow, portHigh)
	if err != nil {
		return 0, 0, err
	}

	minor, err := drbd.LowestFreeMinor(minorTaken, minorLow, minorHigh)
	if err != nil {
		return 0, 0, err
	}

	return port, minor, nil
}

// hostingNodesForRD returns the set of nodes hosting a Resource of
// the named RD. Used by the per-RD allocator to intersect each
// node's port/minor range.
func (r *ResourceReconciler) hostingNodesForRD(ctx context.Context, rdName string) ([]string, error) {
	list := &blockstoriov1alpha1.ResourceList{}
	if err := r.List(ctx, list); err != nil {
		return nil, err
	}

	seen := map[string]bool{}
	out := make([]string, 0, len(list.Items))

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		node := list.Items[i].Spec.NodeName
		if seen[node] {
			continue
		}

		seen[node] = true
		out = append(out, node)
	}

	return out, nil
}

// intersectPortRange computes the intersection of every hosting
// node's TCP-port range. Empty node set → cluster-default range.
// Disjoint ranges produce (0,0) — `LowestFreePort` then returns
// `ErrPortPoolExhausted`, which is the operator-actionable signal
// that node ranges must be reconciled before any RD can land on
// the cross-section.
func (r *ResourceReconciler) intersectPortRange(ctx context.Context, hostNodes []string) (int32, int32, error) {
	return r.intersectRange(ctx, hostNodes,
		func(s *blockstoriov1alpha1.NodeSpec) *blockstoriov1alpha1.PortRange { return s.DRBDPortRange },
		"DrbdOptions/TcpPortRange", "TcpPortAutoRange",
		drbd.DefaultPortMin, drbd.DefaultPortMax)
}

// intersectMinorRange mirrors intersectPortRange for minors.
func (r *ResourceReconciler) intersectMinorRange(ctx context.Context, hostNodes []string) (int32, int32, error) {
	return r.intersectRange(ctx, hostNodes,
		func(s *blockstoriov1alpha1.NodeSpec) *blockstoriov1alpha1.PortRange { return s.DRBDMinorRange },
		"DrbdOptions/MinorNrRange", "MinorNrAutoRange",
		drbd.DefaultMinorMin, drbd.DefaultMinorMax)
}

// intersectRange walks every hosting node, resolves its
// port/minor range via the existing cluster-fallback chain, and
// returns the intersection (max-of-lows, min-of-highs). Empty node
// list falls back to the cluster defaults so the very first
// Resource of an empty cluster can still allocate.
func (r *ResourceReconciler) intersectRange(
	ctx context.Context,
	hostNodes []string,
	pick func(*blockstoriov1alpha1.NodeSpec) *blockstoriov1alpha1.PortRange,
	legacyProp, clusterProp string,
	defLow, defHigh int32,
) (int32, int32, error) {
	low := defLow
	high := defHigh

	first := true

	for _, node := range hostNodes {
		nLow, nHigh, err := r.nodeRangeWithClusterFallback(ctx, node,
			pick, legacyProp, clusterProp, defLow, defHigh)
		if err != nil {
			return 0, 0, err
		}

		if first {
			low = nLow
			high = nHigh
			first = false

			continue
		}

		if nLow > low {
			low = nLow
		}

		if nHigh < high {
			high = nHigh
		}
	}

	return low, high, nil
}

// takenPortsCluster returns every port already claimed cluster-wide:
//   - every OTHER RD's Status.DRBDPort
//   - every Resource.Status.DRBDPort (legacy / mid-migration shape)
//
// Excludes `selfRD` so an RD that's mid-allocation doesn't trip on
// its own draft.
func (r *ResourceReconciler) takenPortsCluster(ctx context.Context, selfRD string) ([]int32, error) {
	out := make([]int32, 0, 16)

	var rdList blockstoriov1alpha1.ResourceDefinitionList
	if err := r.List(ctx, &rdList); err != nil {
		return nil, err
	}

	for i := range rdList.Items {
		if rdList.Items[i].Name == selfRD {
			continue
		}

		if p := rdList.Items[i].Status.DRBDPort; p != nil {
			out = append(out, *p)
		}
	}

	var resList blockstoriov1alpha1.ResourceList
	if err := r.List(ctx, &resList); err != nil {
		return nil, err
	}

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName == selfRD {
			continue
		}

		if p := resList.Items[i].Status.DRBDPort; p != nil {
			out = append(out, *p)
		}
	}

	return out, nil
}

// takenMinorsCluster returns every minor claimed cluster-wide. A
// multi-volume RD reserves N consecutive minors (the .res renderer
// emits volume k at base+k), so we expand each base value to the
// full range via the parent RD's VolumeDefinitions count.
func (r *ResourceReconciler) takenMinorsCluster(ctx context.Context, selfRD string) ([]int32, error) {
	out := make([]int32, 0, 16)

	rdVolCounts := map[string]int{}

	var rdList blockstoriov1alpha1.ResourceDefinitionList
	if err := r.List(ctx, &rdList); err != nil {
		return nil, err
	}

	for i := range rdList.Items {
		volCount := 1
		if n := len(rdList.Items[i].Spec.VolumeDefinitions); n > 0 {
			volCount = n
		}

		rdVolCounts[rdList.Items[i].Name] = volCount

		if rdList.Items[i].Name == selfRD {
			continue
		}

		base := rdList.Items[i].Status.DRBDMinor
		if base == nil {
			continue
		}

		for off := range int32(volCount) {
			out = append(out, *base+off)
		}
	}

	var resList blockstoriov1alpha1.ResourceList
	if err := r.List(ctx, &resList); err != nil {
		return nil, err
	}

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName == selfRD {
			continue
		}

		base := resList.Items[i].Status.DRBDMinor
		if base == nil {
			continue
		}

		volCount, ok := rdVolCounts[resList.Items[i].Spec.ResourceDefinitionName]
		if !ok {
			volCount = 1
		}

		for off := range int32(volCount) {
			out = append(out, *base+off)
		}
	}

	return out, nil
}

// allocatePortAcrossCluster is the fallback used when the parent RD
// is absent (test fast-path / rd-delete in flight). Picks the lowest
// free port across the cluster's taken-set, using the node's local
// range for bounds.
func (r *ResourceReconciler) allocatePortAcrossCluster(ctx context.Context, nodeName string) (int32, error) {
	low, high, err := r.portRangeForNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	taken, err := r.takenPortsCluster(ctx, "")
	if err != nil {
		return 0, err
	}

	return drbd.LowestFreePort(taken, low, high)
}

// allocateMinorAcrossCluster mirrors allocatePortAcrossCluster.
func (r *ResourceReconciler) allocateMinorAcrossCluster(ctx context.Context, nodeName string) (int32, error) {
	low, high, err := r.minorRangeForNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	taken, err := r.takenMinorsCluster(ctx, "")
	if err != nil {
		return 0, err
	}

	return drbd.LowestFreeMinor(taken, low, high)
}

// portRangeForNode reads the DRBD TCP port range off the named Node
// CRD, falling back to the cluster-scope `TcpPortAutoRange` on the
// ControllerConfig singleton, then the compiled-in default. Order:
//
//  1. Node.Spec.DRBDPortRange typed pointer — operator's per-node
//     pin.
//  2. Node.Spec.Props["DrbdOptions/TcpPortRange"] — legacy
//     forward-compat per-node prop.
//  3. ControllerConfig.Spec.ExtraProps["TcpPortAutoRange"] —
//     cluster-scope dynamic-port range, the upstream-LINSTOR knob
//     (`linstor controller set-property TcpPortAutoRange ...`).
//  4. drbd.DefaultPortMin..drbd.DefaultPortMax (7000-7999).
//
// A malformed value at any tier surfaces as an error so the
// operator notices the typo. Scenario 3.W05.
func (r *ResourceReconciler) portRangeForNode(ctx context.Context, nodeName string) (int32, int32, error) {
	return r.nodeRangeWithClusterFallback(ctx, nodeName,
		func(s *blockstoriov1alpha1.NodeSpec) *blockstoriov1alpha1.PortRange { return s.DRBDPortRange },
		"DrbdOptions/TcpPortRange",
		"TcpPortAutoRange",
		drbd.DefaultPortMin, drbd.DefaultPortMax)
}

func (r *ResourceReconciler) minorRangeForNode(ctx context.Context, nodeName string) (int32, int32, error) {
	return r.nodeRangeWithClusterFallback(ctx, nodeName,
		func(s *blockstoriov1alpha1.NodeSpec) *blockstoriov1alpha1.PortRange { return s.DRBDMinorRange },
		"DrbdOptions/MinorNrRange",
		"MinorNrAutoRange",
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

// nodeRangeWithClusterFallback layers the cluster-scope
// `ControllerConfig.Spec.ExtraProps[clusterProp]` between the
// per-node fallback and the compiled-in defaults. Mirrors
// upstream LINSTOR's controller-scope `TcpPortAutoRange` /
// `MinorNrAutoRange` knobs: operators set them via
// `linstor controller set-property` to constrain dynamic
// allocation cluster-wide without touching every Node CRD.
//
// Precedence (highest first):
//
//  1. Node typed pointer (`pick(&node.Spec)`)
//  2. Node legacy props key (`legacyProp`)
//  3. ControllerConfig.Spec.ExtraProps[clusterProp]
//  4. (defLow, defHigh) compiled-in default
//
// Tier 1 and 2 are resolved by the existing `nodeRange` helper;
// this wrapper falls through to tier 3+ only when the node
// contributes nothing. Bad format at the cluster tier surfaces
// as an error — silent fallback would hide misconfig.
func (r *ResourceReconciler) nodeRangeWithClusterFallback(
	ctx context.Context,
	nodeName string,
	pick func(*blockstoriov1alpha1.NodeSpec) *blockstoriov1alpha1.PortRange,
	legacyProp, clusterProp string,
	defLow, defHigh int32,
) (int32, int32, error) {
	clusterLow, clusterHigh, ok, err := r.clusterRange(ctx, clusterProp, defLow, defHigh)
	if err != nil {
		return 0, 0, err
	}

	// Determine the "fall-through default" the node-tier
	// resolver returns when the Node CRD contributes nothing.
	// When the cluster tier supplies a value it wins over the
	// compiled-in default; the node tier can still override it.
	fallbackLow, fallbackHigh := defLow, defHigh
	if ok {
		fallbackLow, fallbackHigh = clusterLow, clusterHigh
	}

	return r.nodeRange(ctx, nodeName, pick, legacyProp, fallbackLow, fallbackHigh)
}

// clusterRange reads the cluster-scope range prop off the
// singleton ControllerConfig. Returns (low, high, set, err):
// `set` is false when the ControllerConfig is missing or the
// prop is absent — callers fall through to compiled-in
// defaults in that case. Malformed values surface as an error.
func (r *ResourceReconciler) clusterRange(ctx context.Context, prop string, defLow, defHigh int32) (int32, int32, bool, error) {
	var cfg blockstoriov1alpha1.ControllerConfig
	if err := r.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &cfg); err != nil {
		if errors.IsNotFound(err) {
			return defLow, defHigh, false, nil
		}

		return 0, 0, false, err
	}

	raw := cfg.Spec.ExtraProps[prop]
	if raw == "" {
		return defLow, defHigh, false, nil
	}

	low, high, err := drbd.ParseRange(raw)
	if err != nil {
		return 0, 0, false, err
	}

	return low, high, true, nil
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
