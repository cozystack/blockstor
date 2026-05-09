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
	"errors"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// ResourceDefinitionReconciler watches RD CRDs and maintains the
// tiebreaker invariant: an RD with exactly 2 diskful replicas in a
// cluster with 3+ satellite nodes auto-gains a 3rd DISKLESS replica
// on a remaining node so DRBD-9's `quorum: majority` always has a
// majority to compare against on a peer split.
//
// Without the tiebreaker, a 2-replica RD survives a single-node
// failure but freezes on quorum loss in a network partition — the
// surviving replica can't tell whether it's the majority or the
// outvoted minority. The diskless witness fixes that for free
// (no extra storage, just network presence).
type ResourceDefinitionReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Store is the shared blockstor store. Same instance the
	// NodeReconciler and REST server use.
	Store store.Store
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch;create;update;patch;delete

// Reconcile ensures the tiebreaker for a 2-replica RD. Idempotent:
// re-running on an RD that already has its tiebreaker is a no-op.
func (r *ResourceDefinitionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.Store == nil {
		return ctrl.Result{}, nil
	}

	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, req.NamespacedName, &rd)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !rd.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	replicas, listErr := r.Store.Resources().ListByDefinition(ctx, rd.Name)
	if listErr != nil {
		log.Error(listErr, "list replicas during RD reconcile", "rd", rd.Name)
	} else {
		diskful, diskless := splitByDiskless(replicas)
		log.Info("RD reconcile",
			"rd", rd.Name,
			"diskful", len(diskful),
			"diskless", len(diskless))
	}

	err = r.ensureTiebreaker(ctx, &rd)
	if err != nil {
		log.Error(err, "ensure tiebreaker", "rd", rd.Name)

		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// ensureTiebreaker maintains the parity-of-replicas invariant for
// DRBD's `quorum:majority`: the count of non-witness replicas (diskful
// + user-added diskless) PLUS the witness must always be odd, so a
// network partition has an unambiguous majority side.
//
// Rules:
//   - When non-witness count is EVEN (and ≥2) and no TIE_BREAKER
//     witness exists, create one on a healthy non-replica node.
//   - When non-witness count is ODD and a TIE_BREAKER witness exists,
//     delete it — it's redundant and wastes network presence.
//   - Single-replica RDs (count = 1) have no quorum to defend; no
//     witness is added or removed.
//
// Counts include diskless replicas the user added explicitly (via
// `linstor r c --diskless` or DisklessOnRemaining). Only the witness
// — flagged TIE_BREAKER — is excluded from the parity count, since
// its presence is what we're deciding.
func (r *ResourceDefinitionReconciler) ensureTiebreaker(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) error {
	replicas, err := r.Store.Resources().ListByDefinition(ctx, rd.Name)
	if err != nil {
		return err
	}

	nonWitness, witness := splitByTieBreaker(replicas)

	if len(nonWitness) <= 1 {
		// 0 or 1 replicas → no quorum question, but still drop a
		// stale witness if one is somehow lingering.
		return r.removeWitnesses(ctx, rd.Name, witness)
	}

	if len(nonWitness)%2 == 1 {
		// Odd → witness redundant. Drop it.
		return r.removeWitnesses(ctx, rd.Name, witness)
	}

	// Even → witness needed.
	if len(witness) > 0 {
		// Already have one. Idempotent no-op.
		return nil
	}

	hostingReplica := map[string]bool{}
	for _, repl := range replicas {
		hostingReplica[repl.NodeName] = true
	}

	tiebreakerNode, err := r.pickTiebreakerNode(ctx, hostingReplica)
	if err != nil {
		return err
	}

	if tiebreakerNode == "" {
		// No suitable node. The cluster is too small for a
		// tiebreaker; leave the RD as-is and the next reconcile
		// retries when nodes change.
		return nil
	}

	newWitness := apiv1.Resource{
		Name:     rd.Name,
		NodeName: tiebreakerNode,
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}

	err = r.Store.Resources().Create(ctx, &newWitness)
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) && !alreadyExists(err) {
		return err
	}

	return nil
}

// splitByTieBreaker partitions replicas into (non-witness, witness).
// Witnesses are diskless replicas flagged TIE_BREAKER.
func splitByTieBreaker(replicas []apiv1.Resource) ([]apiv1.Resource, []apiv1.Resource) {
	var nonWitness, witness []apiv1.Resource

	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagTieBreaker) {
			witness = append(witness, replicas[i])
		} else {
			nonWitness = append(nonWitness, replicas[i])
		}
	}

	return nonWitness, witness
}

// removeWitnesses deletes every TIE_BREAKER replica of the named RD.
// Best-effort: ErrNotFound is swallowed so concurrent reconciles
// converge.
func (r *ResourceDefinitionReconciler) removeWitnesses(ctx context.Context, rdName string, witnesses []apiv1.Resource) error {
	for i := range witnesses {
		err := r.Store.Resources().Delete(ctx, rdName, witnesses[i].NodeName)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}

	return nil
}

// splitByDiskless partitions replicas into (diskful, diskless) lists.
// DRBD treats DISKLESS replicas as connection-mesh participants only
// — they don't allocate storage but they vote in the quorum.
func splitByDiskless(replicas []apiv1.Resource) ([]apiv1.Resource, []apiv1.Resource) {
	var diskful, diskless []apiv1.Resource

	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDiskless) {
			diskless = append(diskless, replicas[i])
		} else {
			diskful = append(diskful, replicas[i])
		}
	}

	return diskful, diskless
}

// pickTiebreakerNode chooses any healthy satellite that is not
// already hosting a replica of this RD. Picks deterministically
// (lowest name first) so two reconcile races converge on the same
// answer instead of both creating a tiebreaker.
func (r *ResourceDefinitionReconciler) pickTiebreakerNode(ctx context.Context, hostingReplica map[string]bool) (string, error) {
	nodes, err := r.Store.Nodes().List(ctx)
	if err != nil {
		return "", err
	}

	candidates := make([]string, 0, len(nodes))

	for i := range nodes {
		if hostingReplica[nodes[i].Name] {
			continue
		}

		if isDisabledNode(&nodes[i]) {
			continue
		}

		if nodes[i].Type != "" && nodes[i].Type != apiv1.NodeTypeSatellite && nodes[i].Type != apiv1.NodeTypeCombined {
			continue
		}

		candidates = append(candidates, nodes[i].Name)
	}

	if len(candidates) == 0 {
		return "", nil
	}

	slices.Sort(candidates)

	return candidates[0], nil
}

// isDisabledNode mirrors placer.disabledNodes for the RD-level
// tiebreaker path so we don't pin an EVICTED/LOST node as the witness.
func isDisabledNode(node *apiv1.Node) bool {
	for _, f := range node.Flags {
		if f == apiv1.NodeFlagEvicted || f == apiv1.NodeFlagLost {
			return true
		}
	}

	return false
}

// alreadyExists is a string-based check for the wrapped errors the
// k8s store returns. The k8s store wraps errAlreadyExists from
// kube-apiserver in a cockroachdb/errors.Wrap — Is() doesn't tunnel
// through that, so we keyword-match on the message.
func alreadyExists(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "already exists")
}

// SetupWithManager sets up the controller with the Manager.
//
// We Watch Resources too — the tiebreaker logic needs to fire when
// child Resources land, not just on the RD's own creation. Without
// the watch, an `apply RD + 2 Resources` race never re-runs the RD
// reconciler after the Resources finish, and a 2-replica RD sits
// without its DISKLESS witness until the next periodic re-sync.
func (r *ResourceDefinitionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.ResourceDefinition{}).
		Watches(&blockstoriov1alpha1.Resource{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueRDForResource)).
		Named("resourcedefinition").
		Complete(r)
}

// enqueueRDForResource maps a Resource event to its parent RD.
// Resource.Spec.ResourceDefinitionName is the canonical link.
func (r *ResourceDefinitionReconciler) enqueueRDForResource(_ context.Context, obj client.Object) []reconcile.Request {
	res, ok := obj.(*blockstoriov1alpha1.Resource)
	if !ok || res.Spec.ResourceDefinitionName == "" {
		return nil
	}

	return []reconcile.Request{
		{NamespacedName: types.NamespacedName{Name: res.Spec.ResourceDefinitionName}},
	}
}
