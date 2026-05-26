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
	"encoding/json"
	"slices"
	"strings"
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
// satellite's own `blockstor.cozystack.io/satellite-resource`
// finalizer now owns teardown end-to-end. The constant + cleanup
// code stay so the controller strips the legacy finalizer off
// any Resource that still carries it (rolling upgrade case);
// the controller no longer stamps it on new Resources.
const resourceFinalizer = "blockstor.cozystack.io/resource"

// takenPortsCluster / takenMinorsCluster pre-allocate this many slots
// for the result slice — sized to cover a typical small cluster (5-10
// RDs × 1-2 vols) without re-growing while not over-allocating for the
// single-RD common case.
const takenAllocsInitialCap = 16

// (formerly `controllerDRBDIDsFieldOwner`: the SSA field-manager
// identity the controller-side allocator used when it wrote
// Status.DRBD{NodeID,Port,Minor}. Phase 11.x switched to a raw JSON
// merge-patch — see `allocateAndApplyDRBDIDs` — so the SSA identity
// is no longer needed. The constant is retired; the
// satellite-observer field-managers remain disjoint from any future
// controller writes because merge-patch sets ownership to the
// requesting client without inheriting prior SSA claims.)

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

	// clusterAllocMu serialises CROSS-RD allocation of cluster-scoped
	// values (DRBD TCP port + minor). The per-RD `allocMu` only
	// serialises replicas of the SAME RD; two DIFFERENT RDs racing
	// to pick a fresh port both observe the same "taken set" because
	// neither has committed yet, both pick the lowest free value,
	// and both Status().Update succeed (each writes its own RD's
	// status — Kubernetes optimistic concurrency is per-object,
	// not cross-object). This is Bug 306: batch autoplace
	// (parallel `rd create`+autoplace) produces RDs with colliding
	// DRBD ports → satellite-side .res collision. Holding this mutex
	// across {list taken → pick free → Status patch} on the parent
	// RD makes the cross-RD allocation atomic in-process. Combined
	// with APIReader-direct reads, this guarantees collision-free
	// allocation under batch creation. Single-controller process
	// (Deployment replicas=1 + leader election in HA) is the
	// supported topology, so an in-process mutex is sufficient.
	clusterAllocMu sync.Mutex
}

// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resources/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resources/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resourcedefinitions,verbs=get;list;watch

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
	// `blockstor.cozystack.io/satellite-resource` finalizer.
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
	//
	// Order matters: the orphan check runs BEFORE DRBD-ID allocation
	// because allocating against a doomed Resource is wasted work
	// (the Delete that follows would just clean it up) and the
	// allocation's Status patch can change managedFields entries
	// that the satellite-side delete chain has to undo.
	orphaned, err := r.handleOrphan(ctx, &target)
	if err != nil {
		return ctrl.Result{}, err
	}

	if orphaned {
		return ctrl.Result{}, nil
	}

	// Bug 302: allocate DRBD-IDs unconditionally on every live
	// Resource Reconcile. The satellite's `waitForControllerAllocation`
	// gate stalls until Status.DRBD{NodeID,Port,Minor} are non-nil,
	// and gating allocation behind `runApply`'s long-tail housekeeping
	// (seed-from-Gi, auto-diskful) left a window where transient
	// errors deeper in the apply chain prevented allocation from
	// landing — the satellite then waited forever. Pulling allocation
	// out and running it as the FIRST live-Resource action makes the
	// invariant the satellite depends on robust against every
	// downstream branch.
	//
	// Idempotent on Status: `ensureDRBDIDs` is a no-op when every ID
	// is already stamped, so re-reconcile bursts (RD watch fan-out,
	// sibling watch fan-out, peer-changed bumps) don't churn the SSA
	// owner or thrash the apiserver.
	//
	// Operate on a copy: ensureDRBDIDs re-fetches via APIReader and
	// SSA-patches Status, which bumps the apiserver-side resource
	// version. Downstream gates in runApply may issue Spec.Update()
	// against the in-Reconcile target (auto-diskful promotion,
	// seed-from-Gi stamping); using our top-of-Reconcile snapshot
	// after a Status patch lands those Updates on a stale revision.
	// Allocate against a deepcopy so the top-of-Reconcile target's
	// resourceVersion stays valid for the downstream Spec writers;
	// requeue when allocation actually mutated state so the next
	// pass observes the fresh revision uniformly.
	allocCopy := target.DeepCopy()

	allocated, err := r.ensureDRBDIDs(ctx, allocCopy, nil)
	if err != nil {
		return ctrl.Result{}, err
	}

	if allocated {
		return ctrl.Result{Requeue: true}, nil
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
const satelliteResourceFinalizer = "blockstor.cozystack.io/satellite-resource"

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

	// Bug 302: DRBD-ID allocation moved to top-of-Reconcile (above the
	// orphan gate) so it runs unconditionally on every Reconcile pass —
	// the satellite's `waitForControllerAllocation` gate stalls until
	// Status.DRBD{NodeID,Port,Minor} are non-nil, and gating allocation
	// behind `runApply` left a window where a Resource that hit the
	// orphan branch or short-circuited before `runApply` could never
	// progress. `ensureDRBDIDs` is idempotent — when every ID is already
	// stamped it returns mutated=false without touching the apiserver,
	// so the early call has no fixed-state cost.

	// Initial-sync skip seeding (Phase 8.1): on a freshly-added
	// replica, pick the CurrentGI of an existing UpToDate peer and
	// stamp it into Spec.Volumes[i].SeedFromGI. The satellite
	// reconciler then pre-seeds the new replica's DRBD metadata
	// before drbdadm up so DRBD's GI handshake skips the full
	// initial-sync. Idempotent: re-runs on a Resource whose
	// SeedFromGI is already set leave Spec alone.
	seeded, err := r.ensureSeedFromGI(ctx, target, peers, rdPtr)
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
// satellite's own `blockstor.cozystack.io/satellite-resource`
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
// refetch target, run the one-time status→spec backfill migration,
// allocate any still-missing identities INTO SPEC (clusterIP model),
// patch the Resource Spec, ensure the parent RD's per-volume minors,
// and mirror the values onto Status for backward-compat readers.
// Returns (mutated, err).
//
// clusterIP model (Service.spec.clusterIP): Spec is authoritative.
// A non-nil Spec.DRBD{Port,NodeID} / VolumeDefinitions[].DRBDMinor is
// NEVER overwritten — that is exactly what makes a `kubectl get -o
// yaml` backup + `kubectl apply` restore preserve every identity with
// no resync/flap: the restored Spec already carries the value, the
// allocate-if-nil pass is a no-op, and the satellite renders the same
// .res it had before.
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

	// Hold the process-wide cross-RD/cross-node allocation mutex
	// across the whole {read taken-set → pick free → patch Spec}
	// sequence. Port allocation is now PER-NODE, so two DIFFERENT RDs
	// whose replicas land on the SAME node must not both observe the
	// same taken-set and pick the same port (Bug 306, same-node
	// variant); minor allocation is cluster-wide and races the same
	// way. The mutex makes both strictly serial in-process; the
	// APIReader-direct reads then observe the prior allocator's
	// committed Spec. (The per-RD allocMu held by ensureDRBDIDs only
	// serialises replicas of ONE RD — it does not cover cross-RD
	// same-node port collisions.)
	r.clusterAllocMu.Lock()
	defer r.clusterAllocMu.Unlock()

	originalSpec := target.Spec.DeepCopy()
	originalStatus := target.Status.DeepCopy()

	// (1) one-time status→spec backfill for objects created before
	// this refactor: copy the legacy Status identities into Spec when
	// Spec is still nil. Runs BEFORE the allocate-if-nil pass and is
	// idempotent (guarded on Spec==nil && Status!=nil).
	backfillResourceSpecFromStatus(target)

	// (2) allocate any still-missing per-replica identities INTO Spec.
	err = r.allocateResourceSpecFields(ctx, target)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}

		return false, err
	}

	// (3) ensure the parent RD's per-volume minors (separate object /
	// separate patch). Returns whether the RD Spec was mutated and the
	// volume-0 minor (mirrored onto Status below).
	minorMutated, vol0Minor, err := r.ensureRDVolumeMinors(ctx, target)
	if err != nil {
		return false, err
	}

	specMutated := !equalResourceIdentitySpec(originalSpec, &target.Spec)

	if specMutated {
		if perr := r.patchResourceSpecIdentities(ctx, target); perr != nil {
			if errors.IsNotFound(perr) {
				return false, nil
			}

			return false, perr
		}
	}

	// (4) mirror Spec identities onto Status for backward-compat
	// readers (REST drbdLayerFromStatus → TCPPorts, observers, tests).
	mirrorIdentitiesToStatus(target, vol0Minor)

	statusMutated := !equalStatusIdentities(originalStatus, &target.Status)

	if statusMutated {
		if perr := r.patchResourceStatusIdentities(ctx, target); perr != nil {
			if errors.IsNotFound(perr) {
				return false, nil
			}

			return false, perr
		}
	}

	return specMutated || statusMutated || minorMutated, nil
}

// backfillResourceSpecFromStatus copies the legacy per-replica Status
// identities onto Spec when Spec is still unset. One-time upgrade
// migration: a Resource created before the identity-to-spec refactor
// carries its allocated port/node-id only on Status; without this
// copy the allocate-if-nil pass below would re-allocate fresh values
// and the satellite would re-render a different .res (port/node-id
// change → reconnect → resync). Guard: only when Spec is nil AND
// Status is set, so it is idempotent and never clobbers a real Spec.
func backfillResourceSpecFromStatus(target *blockstoriov1alpha1.Resource) {
	if target.Spec.DRBDPort == nil && target.Status.DRBDPort != nil {
		v := *target.Status.DRBDPort
		target.Spec.DRBDPort = &v
	}

	if target.Spec.DRBDNodeID == nil && target.Status.DRBDNodeID != nil {
		v := *target.Status.DRBDNodeID
		target.Spec.DRBDNodeID = &v
	}
}

// allocateResourceSpecFields fills any still-nil per-replica Spec
// identity (node-id, port) on `target`. Authoritative non-nil values
// are left untouched (clusterIP respect-preset).
//
// node-id: per-RD scope (the DRBD-9 connection mesh). Lowest free id
// not held by any sibling Resource's Spec.DRBDNodeID nor observed as a
// still-live peer slot (Bug 342 union).
//
// port: PER-NODE scope (Bug 266 scaling fix). The taken-set is the
// ports already used by OTHER Resources ON THE SAME NODE
// (Spec.DRBDPort), so the same port number is reused across different
// nodes — a node can host 1000+ resources within the 7000-7999 window
// instead of the whole cluster sharing it. The optional RD.Spec.
// DRBDPort preferred seed is tried first on the node; on collision the
// allocator falls back to the lowest per-node-free port.
func (r *ResourceReconciler) allocateResourceSpecFields(ctx context.Context, target *blockstoriov1alpha1.Resource) error {
	if target.Spec.DRBDNodeID == nil {
		id, err := r.allocateNodeIDLocked(ctx, target)
		if err != nil {
			return err
		}

		target.Spec.DRBDNodeID = &id
	}

	if target.Spec.DRBDPort == nil {
		port, err := r.allocatePortForNode(ctx, target)
		if err != nil {
			return err
		}

		target.Spec.DRBDPort = &port
	}

	return nil
}

// mirrorIdentitiesToStatus copies the authoritative Spec identities
// onto Status so backward-compat readers (REST drbdLayerFromStatus,
// observers, e2e tests still grepping Status) keep working during and
// after the migration. Status.DRBDMinor mirrors the volume-0 minor —
// the per-volume authoritative source is RD.Spec.VolumeDefinitions.
func mirrorIdentitiesToStatus(target *blockstoriov1alpha1.Resource, vol0Minor *int32) {
	if target.Spec.DRBDPort != nil {
		v := *target.Spec.DRBDPort
		target.Status.DRBDPort = &v
	}

	if target.Spec.DRBDNodeID != nil {
		v := *target.Spec.DRBDNodeID
		target.Status.DRBDNodeID = &v
	}

	if vol0Minor != nil {
		v := *vol0Minor
		target.Status.DRBDMinor = &v
	}
}

// patchResourceSpecIdentities raw-merge-patches the per-replica Spec
// identities. Raw merge-patch (not SSA Apply) for the same reason the
// Status patch uses it: it bypasses controller-runtime's cached
// OpenAPI discovery schema, so a CRD schema upgrade that adds the new
// spec.drbd* fields takes effect without a controller restart.
func (r *ResourceReconciler) patchResourceSpecIdentities(ctx context.Context, target *blockstoriov1alpha1.Resource) error {
	patchTarget := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Resource",
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: target.Name},
	}

	body := map[string]any{"spec": map[string]any{
		"drbdNodeID": target.Spec.DRBDNodeID,
		"drbdPort":   target.Spec.DRBDPort,
	}}

	patchBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	return r.Patch(ctx, patchTarget, client.RawPatch(types.MergePatchType, patchBytes))
}

// patchResourceStatusIdentities raw-merge-patches the legacy Status
// identity mirrors. See the historical note on allocateAndApplyDRBDIDs:
// raw merge-patch survives a CRD schema upgrade without a controller
// restart and only writes the keys it carries, so the satellite
// observer's disjoint Status slots are never clobbered.
func (r *ResourceReconciler) patchResourceStatusIdentities(ctx context.Context, target *blockstoriov1alpha1.Resource) error {
	patchTarget := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Resource",
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: target.Name},
	}

	body := map[string]any{"status": map[string]any{
		"drbdNodeID": target.Status.DRBDNodeID,
		"drbdPort":   target.Status.DRBDPort,
		"drbdMinor":  target.Status.DRBDMinor,
	}}

	patchBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	return r.Status().Patch(ctx, patchTarget, client.RawPatch(types.MergePatchType, patchBytes))
}

// equalResourceIdentitySpec reports whether the per-replica Spec
// identities are unchanged.
func equalResourceIdentitySpec(a, b *blockstoriov1alpha1.ResourceSpec) bool {
	return ptrEqI32(a.DRBDNodeID, b.DRBDNodeID) &&
		ptrEqI32(a.DRBDPort, b.DRBDPort)
}

// equalStatusIdentities reports whether the mirrored Status identities
// are unchanged.
func equalStatusIdentities(a, b *blockstoriov1alpha1.ResourceStatus) bool {
	return ptrEqI32(a.DRBDNodeID, b.DRBDNodeID) &&
		ptrEqI32(a.DRBDPort, b.DRBDPort) &&
		ptrEqI32(a.DRBDMinor, b.DRBDMinor)
}

// ensureSeedFromGI pre-seeds Spec.Volumes[i].SeedFromGI on a
// freshly-added replica with an existing UpToDate peer's CurrentGI
// so DRBD-9's GI handshake on first connect skips the full
// initial-sync. Returns true when Spec was mutated so the caller
// requeues with the persisted value (the next reconcile dispatches
// to the satellite, which consumes SeedFromGI via drbdmeta).
//
// Idempotency: any volume that already has SeedFromGI set is left
// alone — the satellite reconciler is responsible for consuming
// it once and the controller never rewrites. Volumes whose RD
// VolumeDefinition has no peer with a non-empty CurrentGI (fresh
// cluster, all-new replicas) get nothing set; they pay the
// (acceptable) full initial-sync cost on first activation.
//
// Skipped entirely for DISKLESS replicas — they have no metadata
// block to seed.
func (r *ResourceReconciler) ensureSeedFromGI(_ context.Context, target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) (bool, error) {
	if rd == nil || len(rd.Spec.VolumeDefinitions) == 0 {
		return false, nil
	}

	if slices.Contains(target.Spec.Flags, apiv1.ResourceFlagDiskless) {
		return false, nil
	}

	// Bug 342 / seed-GI data-integrity gate: the day0 skip-sync seed
	// is ONLY valid when the RD is genuinely day0 — no existing
	// data-bearing (UpToDate/Consistent/Outdated) diskful peer. When a
	// data peer already exists (the relocate / physical `r d` then
	// `r c` recreate case) a fresh diskful replica MUST come up
	// Inconsistent and SyncTarget from that peer (full resync), NOT
	// adopt a seeded GI: stamping the peer's evolved Current UUID would
	// let DRBD skip the resync against a replica that holds ZERO data,
	// and a stale/missing stamp would drop the satellite into the
	// synthetic day0 fallback whose GI is unrelated to the survivor →
	// `uuid_compare()=unrelated-data` → StandAlone. Refuse to stamp any
	// SeedFromGI in that case; the satellite's resolveSeedGI enforces
	// the same gate race-free from observed peer state.
	if anyDataBearingDiskfulPeer(peers, target.Name) {
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

		setSeedFromGI(target, vd.VolumeNumber, seed)

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
// SeedFromGI for the given volume number. Used to make
// ensureSeedFromGI idempotent.
func seedAlreadySet(target *blockstoriov1alpha1.Resource, volumeNumber int32) bool {
	for i := range target.Spec.Volumes {
		if target.Spec.Volumes[i].VolumeNumber == volumeNumber && target.Spec.Volumes[i].SeedFromGI != "" {
			return true
		}
	}

	return false
}

// pickSeedFromPeers picks an existing peer's CurrentGI for the given
// volume number. Deterministic: peers are sorted by Name and the
// first matching one wins, so two reconcile races converge on the
// same answer (no thrashing of Spec.Volumes[i].SeedFromGI).
//
// Excludes the target itself, peers without a CurrentGI for this
// volume, and peers whose Status.Volumes[i].DiskState != UpToDate
// (a peer that's still syncing wouldn't have the authoritative GI).
func pickSeedFromPeers(peers []blockstoriov1alpha1.Resource, targetName string, volumeNumber int32) string {
	candidates := make([]blockstoriov1alpha1.Resource, 0, len(peers))

	for i := range peers {
		if peers[i].Name == targetName {
			continue
		}

		gi := volumeCurrentGI(&peers[i], volumeNumber)
		if gi == "" {
			continue
		}

		if volumeDiskState(&peers[i], volumeNumber) != string(drbd.DiskStateUpToDate) {
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

	return volumeCurrentGI(&candidates[0], volumeNumber)
}

// anyDataBearingDiskfulPeer reports whether any diskful PEER (excluding
// the target itself) already holds committed data, observed via
// Status.Volumes[].DiskState. UpToDate, Consistent, and Outdated all
// mean the peer carries real data with a real Current UUID — a fresh
// local replica must SyncTarget from it (full resync) rather than skip
// sync via a seeded GI. Mirrors dispatcher.anyDiskfulPeerHasData so the
// controller-side seed gate and the satellite-side resolveVolumeSeed
// gate agree on the same discriminator. Diskless peers never count.
//
// Day0-seeded fresh-sibling exception (staggered multi-replica create):
// a peer whose data-state volume reports a CurrentGI equal to the
// deterministic drbd.Day0GIFor value is a never-written day0 sibling —
// NOT a data source — so it does not count and the later replica may
// also day0-skip. The elected winner now seeds current=day0 too, so a
// fresh UpToDate winner reports CurrentGI==day0 and is correctly seen
// as a sibling. A real relocate survivor mints a runtime current-UUID
// that cannot equal the deterministic day0 (2^-64 collision) so it is
// still counted; a data-state volume with an empty / not-yet-observed
// CurrentGI is treated conservatively as data-bearing.
func anyDataBearingDiskfulPeer(peers []blockstoriov1alpha1.Resource, targetName string) bool {
	for i := range peers {
		if peers[i].Name == targetName {
			continue
		}

		if slices.Contains(peers[i].Spec.Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		rdName := peers[i].Spec.ResourceDefinitionName

		for j := range peers[i].Status.Volumes {
			vol := &peers[i].Status.Volumes[j]

			switch drbd.DiskState(vol.DiskState) {
			case drbd.DiskStateUpToDate,
				drbd.DiskStateConsistent,
				drbd.DiskStateOutdated:
				if !isDay0SeededDiskfulVolume(rdName, vol) {
					return true
				}
				// Fresh day0-seeded sibling, never written — keep scanning.
			case drbd.DiskStateDiskless,
				drbd.DiskStateAttaching,
				drbd.DiskStateDetaching,
				drbd.DiskStateFailed,
				drbd.DiskStateNegotiating,
				drbd.DiskStateInconsistent,
				drbd.DiskStateDUnknown:
				// No committed data we can seed from; keep scanning.
			default:
				// Unknown/empty state — treat as "no data".
			}
		}
	}

	return false
}

// isDay0SeededDiskfulVolume reports whether a peer's data-state volume
// carries nothing but the deterministic day0 GI — a fresh, never-
// written sibling that is NOT a data source. Empty CurrentGI returns
// false (conservative: not provably day0 ⇒ treat as data-bearing).
// Controller twin of dispatcher.isDay0SeededVolume; both compare
// against the single drbd.Day0GIFor derivation the satellite stamps.
func isDay0SeededDiskfulVolume(rdName string, vol *blockstoriov1alpha1.ResourceVolumeStatus) bool {
	if vol.CurrentGI == "" {
		return false
	}

	return strings.EqualFold(vol.CurrentGI, drbd.Day0GIFor(rdName, vol.VolumeNumber))
}

// volumeCurrentGI returns the CurrentGI for the given volume number
// from a Resource's Status, or "" if not present.
func volumeCurrentGI(res *blockstoriov1alpha1.Resource, volumeNumber int32) string {
	for i := range res.Status.Volumes {
		if res.Status.Volumes[i].VolumeNumber == volumeNumber {
			return res.Status.Volumes[i].CurrentGI
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

// setSeedFromGI mutates target.Spec.Volumes to record the seed GI
// for the given volume number. Appends a new entry if no
// ResourceVolumeSpec exists for the volume; otherwise updates in
// place.
func setSeedFromGI(target *blockstoriov1alpha1.Resource, volumeNumber int32, seed string) {
	for i := range target.Spec.Volumes {
		if target.Spec.Volumes[i].VolumeNumber == volumeNumber {
			target.Spec.Volumes[i].SeedFromGI = seed

			return
		}
	}

	target.Spec.Volumes = append(target.Spec.Volumes, blockstoriov1alpha1.ResourceVolumeSpec{
		VolumeNumber: volumeNumber,
		SeedFromGI:   seed,
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

// ensureRDVolumeMinors allocates a per-volume DRBD minor on the parent
// RD's Spec.VolumeDefinitions for every volume that still lacks one,
// following the clusterIP model: a non-nil minor is authoritative and
// NEVER overwritten (restore-safe / settable-once), a nil minor gets
// allocated from the cluster-wide free set and written back to the RD
// Spec under optimistic concurrency.
//
// Per-volume (not per-RD base+k): each VolumeDefinition carries its own
// minor, so adoption can preserve arbitrary, possibly non-contiguous
// LINSTOR minors verbatim. Native allocation stays contiguous-ish (it
// hands out the lowest free value per volume) but never assumes
// contiguity — the satellite renders each volume's own minor.
//
// One-time backfill: when every VolumeDefinition minor is nil but the
// legacy RD.Status.DRBDMinor base is set, the volumes are seeded with
// base+volumeIndex (preserving the pre-refactor base+k behaviour) so an
// upgraded cluster keeps its existing /dev/drbd<N> device paths.
//
// Returns (mutated, vol0Minor, err): mutated is true when the RD Spec
// was patched; vol0Minor is the minor of volume 0 (mirrored onto the
// Resource Status for backward-compat readers), nil when the RD is
// absent or has no volume 0.
//
// Cross-RD allocation is serialised by clusterAllocMu: two RDs
// reconciling at once would otherwise both observe the same free set
// and both pick the same minor (Kube optimistic concurrency is
// per-object, so writing to different RDs' Specs never conflicts).
func (r *ResourceReconciler) ensureRDVolumeMinors(ctx context.Context, target *blockstoriov1alpha1.Resource) (bool, *int32, error) {
	rdName := target.Spec.ResourceDefinitionName
	reader := r.apiReader()

	var rd blockstoriov1alpha1.ResourceDefinition

	err := reader.Get(ctx, client.ObjectKey{Name: rdName}, &rd)
	if err != nil {
		if errors.IsNotFound(err) {
			// RD absent (unit-test fast path / rd-delete in flight).
			// No RD Spec to allocate on; the .res renderer falls back
			// to the deterministic deriveMinor() until the RD exists.
			return false, nil, nil
		}

		return false, nil, err
	}

	if len(rd.Spec.VolumeDefinitions) == 0 {
		return false, nil, nil
	}

	if rdVolumeMinorsComplete(&rd) {
		return false, vol0MinorOf(&rd), nil
	}

	// Caller (allocateAndApplyDRBDIDs) holds clusterAllocMu across the
	// whole allocation sequence, so cross-RD minor allocation is
	// already serialised and the APIReader read above observed any
	// prior allocator's committed minors.
	mutated, err := r.allocateRDVolumeMinors(ctx, &rd)
	if err != nil {
		return false, nil, err
	}

	if !mutated {
		return false, vol0MinorOf(&rd), nil
	}

	if err = r.patchRDVolumeMinors(ctx, &rd); err != nil {
		if errors.IsConflict(err) {
			// Sibling reconcile committed minors first. Re-read and
			// return whatever it stamped — the next reconcile observes
			// the committed values uniformly.
			var fresh blockstoriov1alpha1.ResourceDefinition
			if gerr := reader.Get(ctx, client.ObjectKey{Name: rdName}, &fresh); gerr == nil {
				return false, vol0MinorOf(&fresh), nil
			}
		}

		return false, nil, err
	}

	return true, vol0MinorOf(&rd), nil
}

// rdVolumeMinorsComplete reports whether every VolumeDefinition on the
// RD already carries a (non-nil) DRBDMinor.
func rdVolumeMinorsComplete(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].DRBDMinor == nil {
			return false
		}
	}

	return true
}

// vol0MinorOf returns the minor of volume 0 (the one mirrored onto the
// Resource Status), or nil when there is no volume 0 / it is unset.
func vol0MinorOf(rd *blockstoriov1alpha1.ResourceDefinition) *int32 {
	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].VolumeNumber == 0 {
			return rd.Spec.VolumeDefinitions[i].DRBDMinor
		}
	}

	return nil
}

// allocateRDVolumeMinors fills any nil VolumeDefinition.DRBDMinor on
// the RD in place. Returns whether anything changed. Runs the one-time
// status→spec backfill first (legacy base+k), then allocates fresh
// minors for whatever is still nil from the cluster-wide free set.
// Caller holds clusterAllocMu.
func (r *ResourceReconciler) allocateRDVolumeMinors(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) (bool, error) {
	mutated := backfillRDMinorsFromStatus(rd)

	hostNodes, err := r.hostingNodesForRD(ctx, rd.Name)
	if err != nil {
		return false, err
	}

	low, high, err := r.intersectMinorRange(ctx, hostNodes)
	if err != nil {
		return false, err
	}

	taken, err := r.takenMinorsCluster(ctx, rd.Name)
	if err != nil {
		return false, err
	}

	// Treat any minor we just backfilled (or that an already-set
	// sibling volume holds) as taken so the fresh picks don't collide
	// within this RD.
	for i := range rd.Spec.VolumeDefinitions {
		if m := rd.Spec.VolumeDefinitions[i].DRBDMinor; m != nil {
			taken = append(taken, *m)
		}
	}

	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].DRBDMinor != nil {
			continue
		}

		minor, ferr := drbd.LowestFreeMinor(taken, low, high)
		if ferr != nil {
			return false, ferr
		}

		v := minor
		rd.Spec.VolumeDefinitions[i].DRBDMinor = &v
		mutated = true

		taken = append(taken, minor)
	}

	return mutated, nil
}

// backfillRDMinorsFromStatus seeds the RD's per-volume minors from the
// legacy RD.Status.DRBDMinor base (base+volumeIndex) when no volume yet
// has a minor and the base is set. One-time upgrade migration: preserves
// the pre-refactor base+k device paths so an upgraded cluster keeps its
// /dev/drbd<N> minors and never resyncs. Idempotent — a no-op once any
// volume already carries a minor. Returns whether it changed anything.
func backfillRDMinorsFromStatus(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	if rd.Status.DRBDMinor == nil {
		return false
	}

	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].DRBDMinor != nil {
			return false
		}
	}

	base := *rd.Status.DRBDMinor

	for i := range rd.Spec.VolumeDefinitions {
		v := base + rd.Spec.VolumeDefinitions[i].VolumeNumber
		rd.Spec.VolumeDefinitions[i].DRBDMinor = &v
	}

	return true
}

// patchRDVolumeMinors raw-merge-patches the RD Spec.VolumeDefinitions
// minors. Sends the full volumeDefinitions list (volumeNumber + minor)
// because a JSON merge-patch replaces the array wholesale; the
// satellite/REST owners of the other VolumeDefinition fields read them
// off Spec elsewhere, and we re-send the size/props verbatim to avoid
// dropping them.
func (r *ResourceReconciler) patchRDVolumeMinors(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) error {
	vols := make([]map[string]any, 0, len(rd.Spec.VolumeDefinitions))
	for i := range rd.Spec.VolumeDefinitions {
		vd := &rd.Spec.VolumeDefinitions[i]
		entry := map[string]any{
			"volumeNumber": vd.VolumeNumber,
			"sizeKib":      vd.SizeKib,
			"drbdMinor":    vd.DRBDMinor,
		}

		if len(vd.Props) > 0 {
			entry["props"] = vd.Props
		}

		if len(vd.Flags) > 0 {
			entry["flags"] = vd.Flags
		}

		vols = append(vols, entry)
	}

	body := map[string]any{"spec": map[string]any{"volumeDefinitions": vols}}

	patchBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	// Optimistic concurrency: include resourceVersion so a racing
	// minor write is rejected with Conflict and we re-read.
	patchTarget := &blockstoriov1alpha1.ResourceDefinition{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ResourceDefinition",
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: rd.Name},
	}

	return r.Patch(ctx, patchTarget, client.RawPatch(types.MergePatchType, patchBytes))
}

// allocatePortForNode picks the DRBD listen port for `target` on its
// node, PER-NODE (Bug 266 scaling fix). The taken-set is the ports
// already used by OTHER Resources ON THE SAME NODE (Spec.DRBDPort), so
// the same port is reused across different nodes. Tries the optional
// RD.Spec.DRBDPort preferred seed first; on a per-node collision (or
// no seed) falls back to the lowest per-node-free port in the node's
// range.
func (r *ResourceReconciler) allocatePortForNode(ctx context.Context, target *blockstoriov1alpha1.Resource) (int32, error) {
	low, high, err := r.portRangeForNode(ctx, target.Spec.NodeName)
	if err != nil {
		return 0, err
	}

	taken, err := r.takenPortsOnNode(ctx, target.Spec.NodeName, target.Name)
	if err != nil {
		return 0, err
	}

	// Try the RD's preferred-port seed first when it is free on this
	// node and inside the node's range.
	if seed := r.rdPreferredPort(ctx, target.Spec.ResourceDefinitionName); seed != nil {
		if *seed >= low && *seed <= high && !slices.Contains(taken, *seed) {
			return *seed, nil
		}
	}

	return drbd.LowestFreePort(taken, low, high)
}

// rdPreferredPort reads the optional RD.Spec.DRBDPort preferred-port
// seed. Returns nil when the RD is absent or sets no preference.
func (r *ResourceReconciler) rdPreferredPort(ctx context.Context, rdName string) *int32 {
	var rd blockstoriov1alpha1.ResourceDefinition
	if err := r.apiReader().Get(ctx, client.ObjectKey{Name: rdName}, &rd); err != nil {
		return nil
	}

	return rd.Spec.DRBDPort
}

// apiReader returns the uncached apiserver-direct client when
// available, falling back to the cached client for tests that
// construct ResourceReconciler{} directly without going through
// SetupWithManager. The fallback is safe because the fake client used
// in tests doesn't simulate informer-cache lag, so a single reader
// satisfies both the production and test paths.
func (r *ResourceReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}

	return r.Client
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

// intersectMinorRange computes the intersection of every hosting
// node's minor range. Minors are cluster-wide (the /dev/drbd<N>
// device identity, identical on every node), so a value must be
// allocatable on every node hosting a volume of the RD. Empty node
// set → cluster-default range.
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

// takenPortsOnNode returns every DRBD listen port already claimed by a
// Resource ON THE SAME NODE (PER-NODE port scope, Bug 266 scaling fix),
// excluding `selfName`. The taken-set is per-node — the same port
// number is reusable on a different node — so a node can host 1000+
// resources inside the 7000-7999 window instead of the whole cluster
// sharing it.
//
// Reads the authoritative Spec.DRBDPort. Uses APIReader (uncached) so
// concurrent allocations on the same node observe each other's
// freshly-committed Spec instead of a stale informer snapshot;
// combined with the per-RD/cluster mutexes this keeps same-node
// allocation collision-free.
func (r *ResourceReconciler) takenPortsOnNode(ctx context.Context, nodeName, selfName string) ([]int32, error) {
	out := make([]int32, 0, takenAllocsInitialCap)
	reader := r.apiReader()

	var resList blockstoriov1alpha1.ResourceList
	if err := reader.List(ctx, &resList); err != nil {
		return nil, err
	}

	for i := range resList.Items {
		res := &resList.Items[i]
		if res.Spec.NodeName != nodeName || res.Name == selfName {
			continue
		}

		if p := res.Spec.DRBDPort; p != nil {
			out = append(out, *p)
		}
	}

	return out, nil
}

// takenMinorsCluster returns every minor claimed CLUSTER-WIDE — minors
// are the /dev/drbd<N> device identity, identical on every node hosting
// a volume, so the scope is the whole cluster. The authoritative source
// is every RD's Spec.VolumeDefinitions[].DRBDMinor; the legacy
// RD.Status.DRBDMinor base (expanded base+k) is also folded in so
// not-yet-backfilled RDs on a mid-upgrade cluster still reserve their
// minors. Excludes `selfRD` so an RD mid-allocation doesn't trip on its
// own draft.
//
// Uses APIReader (uncached) so cross-RD batch allocation observes
// freshly-committed sibling minors rather than a stale cache.
func (r *ResourceReconciler) takenMinorsCluster(ctx context.Context, selfRD string) ([]int32, error) {
	out := make([]int32, 0, takenAllocsInitialCap)
	reader := r.apiReader()

	var rdList blockstoriov1alpha1.ResourceDefinitionList
	if err := reader.List(ctx, &rdList); err != nil {
		return nil, err
	}

	for i := range rdList.Items {
		rd := &rdList.Items[i]
		if rd.Name == selfRD {
			continue
		}

		// Authoritative per-volume minors on Spec.
		for j := range rd.Spec.VolumeDefinitions {
			if m := rd.Spec.VolumeDefinitions[j].DRBDMinor; m != nil {
				out = append(out, *m)
			}
		}

		// Legacy not-yet-backfilled base (expand base+k per volume).
		base := rd.Status.DRBDMinor
		if base != nil && !anyVolumeMinorSet(rd) {
			volCount := len(rd.Spec.VolumeDefinitions)
			if volCount == 0 {
				volCount = 1
			}

			for off := range volCount {
				out = append(out, *base+int32(off))
			}
		}
	}

	return out, nil
}

// anyVolumeMinorSet reports whether at least one VolumeDefinition on
// the RD already carries a Spec minor (i.e. it has been backfilled /
// allocated) — used to avoid double-counting the legacy Status base
// against the already-migrated Spec minors.
func anyVolumeMinorSet(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	for i := range rd.Spec.VolumeDefinitions {
		if rd.Spec.VolumeDefinitions[i].DRBDMinor != nil {
			return true
		}
	}

	return false
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

// CollectTakenNodeIDsForTest exposes the per-RD occupied node-id set
// builder (own Status.DRBDNodeID ∪ observed PeerDRBDNodeID across
// siblings) so the Bug 342 union invariant can be pinned directly
// without driving a full reconcile. The result is unordered; callers
// should sort before comparing.
func (r *ResourceReconciler) CollectTakenNodeIDsForTest(ctx context.Context, target *blockstoriov1alpha1.Resource) ([]int32, error) {
	return r.collectTakenNodeIDs(ctx, target)
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

// collectTakenNodeIDs returns the DRBD node-ids that are OCCUPIED
// for target's RD, reading directly from the apiserver (no informer
// cache) to avoid the stale-read race that otherwise lets two
// concurrent reconciles both pick the lowest free id.
//
// Bug 342 (CRITICAL, data-correctness): node-id space is per-RD (the
// DRBD-9 connection mesh). The occupied set is the UNION of two
// observation sources, both grounded in kernel-confirmed truth:
//
//	(a) every live sibling Resource's own Status.DRBDNodeID, and
//	(b) every observed peer node-id any sibling still reports under
//	    Status.Connections[].PeerDRBDNodeID.
//
// (b) is the load-bearing addition. When a replica is deleted
// (`r d <node>`) its Resource CRD — and its Status.DRBDNodeID — go
// away promptly, but the surviving peers keep a live DRBD-9 kernel
// connection slot bound to the departed peer's node-id until each
// runs `drbdadm del-peer` + `drbdmeta forget-peer` to reclaim the
// bitmap slot. The observer keeps PeerDRBDNodeID populated for
// exactly that window: it is sourced from live `drbdsetup status -j`
// / `events2` and drops out only on the `destroy connection` frame,
// which the kernel emits once the slot is truly forgotten.
//
// Treating those observed peer ids as occupied guarantees a
// new/relocated replica is never handed a node-id whose predecessor
// bitmap slot still exists in any peer's kernel — DRBD-9 refuses to
// handshake a fresh peer over such a slot, so reusing it would wedge
// the new replica in Connecting/disk:” forever (Bug 342). Once the
// slot is reclaimed and the connection drops out of every sibling's
// Status, the id frees up again.
//
// Tradeoff (acceptable): if a forget-peer is stuck, or a sibling is
// offline with a stale Status still listing the id, that id stays
// occupied longer — id-burn is bounded toward drbd.MaxPeers=16. This
// is conservative and self-healing; strictly better than the
// permanent wedge a premature reuse causes.
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

	// Dedup so the union of own-ids and observed-peer-ids doesn't
	// hand LowestFreeNodeID a slice with repeats (functionally
	// harmless, but keeps the taken set minimal and inspectable).
	occupied := make(map[int32]struct{}, len(resList.Items))

	for i := range resList.Items {
		res := &resList.Items[i]

		if res.Name == target.Name {
			continue
		}

		if res.Spec.ResourceDefinitionName != target.Spec.ResourceDefinitionName {
			continue
		}

		// (a) the sibling's own allocated node-id. Authoritative
		// source is Spec.DRBDNodeID (identity-to-spec refactor); the
		// legacy Status.DRBDNodeID is folded in for not-yet-backfilled
		// siblings on a mid-upgrade cluster.
		if res.Spec.DRBDNodeID != nil {
			occupied[*res.Spec.DRBDNodeID] = struct{}{}
		} else if res.Status.DRBDNodeID != nil {
			occupied[*res.Status.DRBDNodeID] = struct{}{}
		}

		// (b) every peer node-id this sibling still observes as a
		// live kernel connection slot — including zombie slots of a
		// departed peer whose Resource is already gone but whose
		// forget-peer hasn't completed.
		for j := range res.Status.Connections {
			if id := res.Status.Connections[j].PeerDRBDNodeID; id != nil {
				occupied[*id] = struct{}{}
			}
		}
	}

	taken := make([]int32, 0, len(occupied))
	for id := range occupied {
		taken = append(taken, id)
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
