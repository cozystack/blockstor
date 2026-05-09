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

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=nodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=nodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=nodes/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcegroups,verbs=get;list;watch

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

	filter := apiv1.AutoSelectFilter{}

	if rd.ResourceGroupName != "" {
		rg, err := r.Store.ResourceGroups().Get(ctx, rd.ResourceGroupName)
		if err == nil {
			filter = rg.SelectFilter
		}
	}

	if filter.PlaceCount == 0 {
		filter.PlaceCount = 1
	}

	_, _, err = placer.New(r.Store).Place(ctx, rdName, &filter)
	if err != nil {
		return err
	}

	if !lost {
		return nil
	}

	// LOST node never returns. Delete the Resource on it via the
	// K8s API path; the Resource controller's finalizer will
	// best-effort RPC-Delete to the (gone) satellite, time out,
	// and clear.
	resCRD := &blockstoriov1alpha1.Resource{}

	err = r.Get(ctx, client.ObjectKey{Name: resourceCRDName(rdName, victim.NodeName)}, resCRD)
	if err != nil {
		return client.IgnoreNotFound(err)
	}

	return r.Delete(ctx, resCRD)
}

// resourceCRDName mirrors the encoding used by the k8s store —
// `<rd>.<node>` is the documented composite key.
func resourceCRDName(rd, node string) string {
	return rd + "." + node
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Node{}).
		Named("node").
		Complete(r)
}
