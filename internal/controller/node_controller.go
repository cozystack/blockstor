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

package controller

import (
	"context"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// evictionRequeue is the wait between successive eviction reconciles
// while replacements are still in flight. Long enough that we don't
// burn CPU polling on the placer; short enough that an operator can
// see progress within a couple of pings.
const evictionRequeue = 30 * time.Second

// AnnotationMigrationBlocked is stamped onto the parent
// ResourceDefinition CRD when an eviction-driven migration cannot run
// — e.g. the RD has no ResourceGroup attached, so the placer has no
// SelectFilter to derive a topology / place_count from. Operators
// grep this annotation to find RDs that need manual intervention
// (attach an RG, then re-trigger the node eviction) rather than
// silently leaving replicas pinned to a draining node.
const AnnotationMigrationBlocked = "blockstor.io/migration-blocked"

// MigrationBlockedReasonNoRG is the value the NodeReconciler writes to
// AnnotationMigrationBlocked when the parent RD has no
// ResourceGroupName. Distinct constant so future reasons (e.g.
// insufficient candidates) get their own value rather than overloading
// a free-form string.
const MigrationBlockedReasonNoRG = "no-rg"

// NodeReconciler watches Node CRDs and drives replica migration when
// EVICTED / LOST flags appear. EVICTED is the soft "drain me" hint
// (operator initiated); LOST is the permanent "node is gone" mark.
//
// The reconciler owns the migration trigger only — actual replica
// teardown on the source node still flows through the normal Resource
// CRD delete path so the satellite gets a chance to clean up.
type NodeReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Store is the shared blockstor store used by the placer so a
	// migration uses the same data path as REST autoplace. Same
	// instance the rest of the controller manager wires.
	Store store.Store
}

// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=nodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=nodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=nodes/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resourcegroups,verbs=get;list;watch

// Reconcile drives the eviction migration. On every Node change we
// look for EVICTED; if set, every Resource on that node gets a
// replacement scheduled elsewhere via the placer. LOST adds a delete
// of the source Resource so the cluster doesn't keep waiting on a
// node that's never coming back.
//
// Idempotent: extra peers (>= placeCount) are not created on each
// pass — placer.Place treats existing replicas as already-placed and
// only fills the gap.
func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.Store == nil {
		// envtest scaffolding may construct without a Store —
		// keep the controller a no-op so the boilerplate test
		// suite stays green.
		return ctrl.Result{}, nil
	}

	var node blockstoriov1alpha1.Node

	err := r.Get(ctx, req.NamespacedName, &node)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	evicted := slices.Contains(node.Spec.Flags, apiv1.NodeFlagEvicted)
	lost := slices.Contains(node.Spec.Flags, apiv1.NodeFlagLost)

	if !evicted && !lost {
		return ctrl.Result{}, nil
	}

	resList, err := r.Store.Resources().List(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	for i := range resList {
		if resList[i].NodeName != node.Name {
			continue
		}

		err := r.migrateResource(ctx, &resList[i], lost)
		if err != nil {
			log.Error(err, "migrate resource",
				"resource", resList[i].Name,
				"node", node.Name)
			// Don't bail on one Resource — the next reconcile
			// retries the survivors.
			continue
		}
	}

	if evicted && !lost {
		// Schedule a follow-up reconcile in case migrations
		// partially landed (placer ran but replacement isn't
		// UpToDate yet).
		return ctrl.Result{RequeueAfter: evictionRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// migrateResource ensures the parent RD has place_count replicas on
// non-evicted nodes. The placer fills the gap honouring the same RG
// topology constraints the original autoplace used. For LOST, the
// source Resource is deleted via the K8s API so the Resource
// controller's finalizer cleans it up.
func (r *NodeReconciler) migrateResource(ctx context.Context, victim *apiv1.Resource, lost bool) error {
	rdName := victim.Name

	rd, err := r.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return err
	}

	// No RG → no SelectFilter → no topology constraints to migrate
	// against (which pools, which failure domains the replacement is
	// allowed to land on). Fail-safe: refuse the migration and stamp the
	// RD so an operator sees the gap rather than us placing a replacement
	// blind to the original placement constraints. They can attach an RG
	// and re-trigger the eviction.
	if rd.ResourceGroupName == "" {
		return r.annotateMigrationBlocked(ctx, rdName, MigrationBlockedReasonNoRG)
	}

	filter := apiv1.AutoSelectFilter{}

	rg, err := r.Store.ResourceGroups().Get(ctx, rd.ResourceGroupName)
	if err == nil {
		filter = rg.SelectFilter
	}

	// Derive the effective diskful target the drain must preserve.
	//
	// The RG's PlaceCount can be 0 — the default `DfltRscGrp` ships with
	// PlaceCount=0 and most CLI-created RDs inherit it. The previous
	// fallback of PlaceCount=1 was actively harmful: with 1, the placer
	// sees the surviving peer as "satisfied" and gap-fills NOTHING, while
	// evacuationReplacementReady is satisfied by that same surviving peer
	// — so pruneSource drops the source on the evacuated node and the RD
	// lands one diskful short (drop-without-add, worse than the original
	// bug). Anchor the target to the CURRENT diskful redundancy instead:
	// count the diskful replicas this RD has right now (INCLUDING the one
	// being evacuated, so it gets replaced; EXCLUDING diskless/tiebreaker
	// witnesses and replicas already pinned to other EVICTED/LOST nodes).
	// max() so an explicit, larger RG PlaceCount still wins, but a
	// zero/defaulted RG can never let redundancy silently drop.
	currentDiskful, err := r.currentDiskfulTarget(ctx, rdName, victim.NodeName)
	if err != nil {
		return err
	}

	if int(filter.PlaceCount) < currentDiskful {
		filter.PlaceCount = apiv1.LaxInt32(currentDiskful) //nolint:gosec // diskful count of an in-memory slice fits int32
	}

	_, _, err = placer.New(r.Store).Place(ctx, rdName, &filter)
	if err != nil {
		return err
	}

	if lost {
		// LOST node never returns. Delete the Resource on it via the
		// K8s API path unconditionally; the Resource controller's
		// finalizer will best-effort RPC-Delete to the (gone)
		// satellite, time out, and clear. There is no replacement to
		// wait on — the source is already gone, so redundancy cannot
		// be lowered by dropping a replica that no longer serves data.
		return r.pruneSource(ctx, rdName, victim.NodeName)
	}

	// EVICTED (online drain): mirror upstream LINSTOR's add-before-drop
	// ordering. The placer above gap-filled a replacement on a healthy
	// peer; we must NOT drop the source on the evacuated node until that
	// replacement is observed UpToDate, or redundancy would dip below
	// place_count mid-drain. Once enough healthy replicas are UpToDate,
	// prune the source so the node is left empty and `node delete`
	// completes cleanly. If the replacement is still syncing, leave the
	// source in place — the eviction requeue retries until it converges.
	ready, err := r.evacuationReplacementReady(ctx, &filter, rdName, victim.NodeName)
	if err != nil {
		return err
	}

	if !ready {
		return nil
	}

	return r.pruneSource(ctx, rdName, victim.NodeName)
}

// currentDiskfulTarget returns the diskful redundancy the evacuation
// drain must preserve: the number of diskful replicas this RD holds
// right now, INCLUDING the one on the evacuated node (it must be
// replaced, so it counts toward the target) but EXCLUDING:
//   - diskless / TIE_BREAKER witnesses — they carry no data, the same
//     exclusion splitDiskfulAndCandidates / placer.countDiskfulReplicas
//     apply (place_count is a diskful-replica target);
//   - replicas already pinned to OTHER draining (EVICTED / LOST) nodes —
//     those placements are themselves on the way out and can't backstop
//     redundancy.
//
// The evacuated node itself is the one exception to the disabled-node
// exclusion: its replica still serves data until pruneSource runs, so it
// must be counted so the placer is told to reach the SAME diskful count
// on a non-evacuated peer before the source is dropped.
//
// The raw count is capped at the number of non-disabled nodes available
// to host a diskful replica. This keeps the target STABLE across the
// requeue loop: once the placer has gap-filled the replacement on a
// healthy peer, that replacement is itself a diskful-on-a-healthy-node
// and would otherwise be counted IN ADDITION to the still-present source
// on the evacuated node — inflating the target every pass and demanding
// more diskful than there are nodes to host them (the placer would then
// report a capacity shortfall and the drain would never complete). The
// drain only needs to re-establish the pre-drain diskful count on healthy
// nodes; it can never need more diskful than non-disabled nodes exist.
func (r *NodeReconciler) currentDiskfulTarget(ctx context.Context, rdName, evictedNode string) (int, error) {
	drained, err := r.drainingNodes(ctx)
	if err != nil {
		return 0, err
	}

	nodes, err := r.Store.Nodes().List(ctx)
	if err != nil {
		return 0, err
	}

	healthyNodes := 0

	for i := range nodes {
		if _, off := drained[nodes[i].Name]; off {
			continue
		}

		healthyNodes++
	}

	replicas, err := r.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return 0, err
	}

	count := 0

	for i := range replicas {
		node := replicas[i].NodeName

		// Other draining nodes don't count; the evacuated node does (it
		// is the replica we are replacing).
		if node != evictedNode {
			if _, off := drained[node]; off {
				continue
			}
		}

		// Diskless replicas / tiebreaker witnesses carry no data.
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		count++
	}

	if count > healthyNodes {
		count = healthyNodes
	}

	return count, nil
}

// pruneSource deletes the Resource CRD for `rdName` on the evacuated /
// lost node via the K8s API path so the Resource controller's
// finalizer drives satellite teardown (or times out, for an
// unreachable node). A NotFound is swallowed — a previous reconcile
// pass may have already removed it.
func (r *NodeReconciler) pruneSource(ctx context.Context, rdName, node string) error {
	resCRD := &blockstoriov1alpha1.Resource{}

	err := r.Get(ctx, client.ObjectKey{Name: resourceCRDName(rdName, node)}, resCRD)
	if err != nil {
		return client.IgnoreNotFound(err)
	}

	return r.Delete(ctx, resCRD)
}

// evacuationReplacementReady reports whether the drain of `evictedNode`
// may now safely drop the source replica of `rdName`. It is the
// add-before-drop gate for the EVICTED path: true only once there are
// at least filter.PlaceCount diskful replicas on healthy (non-evicted,
// non-lost) nodes AND every one of them is observed UpToDate via the
// Resource CRD Status (the same DiskState gate the migration
// controller uses for `r td --migrate-from`). filter.PlaceCount here is
// the effective target migrateResource derived — max(RG place_count,
// current diskful count) — NOT the raw RG value, so a zero/defaulted RG
// still demands the pre-drain diskful count be re-established on healthy
// peers before the source goes. The evacuated node's own replica is
// explicitly NOT counted toward satisfaction (it is being drained).
// Until the gate passes the source on the evacuated node must live so
// redundancy never dips.
func (r *NodeReconciler) evacuationReplacementReady(ctx context.Context, filter *apiv1.AutoSelectFilter, rdName, evictedNode string) (bool, error) {
	drained, err := r.drainingNodes(ctx)
	if err != nil {
		return false, err
	}

	replicas, err := r.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return false, err
	}

	healthyUpToDate := 0

	for i := range replicas {
		node := replicas[i].NodeName

		// Skip the source on the evacuated node and any replica still
		// pinned to a draining (EVICTED / LOST) peer — neither counts
		// toward the post-drain redundancy we must reach first.
		if node == evictedNode {
			continue
		}

		if _, off := drained[node]; off {
			continue
		}

		// Diskless replicas (tiebreakers) carry no data; they don't
		// satisfy the diskful redundancy the drain has to preserve.
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		ready, err := r.replicaUpToDate(ctx, rdName, node)
		if err != nil {
			return false, err
		}

		if ready {
			healthyUpToDate++
		}
	}

	return healthyUpToDate >= int(filter.PlaceCount), nil
}

// replicaUpToDate reads the Resource CRD Status for `rdName` on `node`
// and reports whether every volume is UpToDate. A missing CRD or an
// empty Volumes slice returns false — we have no evidence the new copy
// is durable, so the source must not be dropped yet.
func (r *NodeReconciler) replicaUpToDate(ctx context.Context, rdName, node string) (bool, error) {
	resCRD := &blockstoriov1alpha1.Resource{}

	err := r.Get(ctx, client.ObjectKey{Name: resourceCRDName(rdName, node)}, resCRD)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}

		return false, err
	}

	return allVolumesUpToDate(resCRD), nil
}

// drainingNodes returns the set of node names currently flagged EVICTED
// or LOST. Replicas pinned to these nodes cannot count toward the
// post-drain redundancy target, so they are excluded from the
// add-before-drop readiness check.
func (r *NodeReconciler) drainingNodes(ctx context.Context) (map[string]struct{}, error) {
	nodes, err := r.Store.Nodes().List(ctx)
	if err != nil {
		return nil, err
	}

	out := map[string]struct{}{}

	for i := range nodes {
		if slices.Contains(nodes[i].Flags, apiv1.NodeFlagEvicted) ||
			slices.Contains(nodes[i].Flags, apiv1.NodeFlagLost) {
			out[nodes[i].Name] = struct{}{}
		}
	}

	return out, nil
}

// resourceCRDName mirrors the encoding used by the k8s store —
// `<rd>.<node>` is the documented composite key.
func resourceCRDName(rd, node string) string {
	return rd + "." + node
}

// annotateMigrationBlocked stamps the parent RD with the
// `blockstor.io/migration-blocked` annotation and the given reason so
// an operator (`kubectl get rd -o ...`) can surface every RD whose
// eviction migration refused to run. Idempotent: re-stamping with the
// same reason is a no-op write.
func (r *NodeReconciler) annotateMigrationBlocked(ctx context.Context, rdName, reason string) error {
	rd, err := r.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return err
	}

	if rd.Annotations[AnnotationMigrationBlocked] == reason {
		return nil
	}

	if rd.Annotations == nil {
		rd.Annotations = map[string]string{}
	}

	rd.Annotations[AnnotationMigrationBlocked] = reason

	return r.Store.ResourceDefinitions().Update(ctx, &rd)
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Node{}).
		Named("node").
		Complete(r)
}
