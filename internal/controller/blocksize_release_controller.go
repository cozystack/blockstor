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
	"maps"
	"slices"
	"sync"
	"time"

	cerrors "github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/blocksize"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

// BlockSizeReleaseReconciler removes the block size LINSTOR's rule wrote
// on a volume definition once the definition's last replica is gone, as
// LINSTOR does when it deletes the last resource: the next replica then
// takes the size of whatever pool it lands in, and nothing is left frozen
// for a device that no longer exists.
//
// It runs on replica deletions, and on a sweep for deletions it did not
// see. A definition that simply has no replica yet keeps what it
// carries: a restore writes the source's size onto the definition before
// placing anything, and an operator may set a size before the first
// replica.
type BlockSizeReleaseReconciler struct {
	client.Client

	// APIReader lists replicas past the cache, so a replica created a
	// moment ago is not taken for an empty definition.
	APIReader client.Reader

	// Lock is the per-definition lock the block size decision for a new
	// replica holds, ResourceReconciler.rdAllocMu. The release holds it
	// from listing the replicas to the patch: a replica decided in
	// between under a size it kept writes nothing, so the patch would
	// otherwise take the size from under a replica that has just come up
	// with it. Nil only in tests that run nothing alongside.
	Lock func(rdName string) *sync.Mutex

	// SweepInterval is how often the sweep looks for definitions whose
	// last replica went while the controller was not watching. Zero is
	// blockSizeSweepInterval.
	SweepInterval time.Duration

	// conflicts counts each definition's back-to-back releases that lost
	// an optimistic-concurrency race, for releaseLoudAfter.
	conflictsMu sync.Mutex
	conflicts   map[string]int
}

// releaseLoudAfter is the run of back-to-back lost races on one
// definition after which they are logged at the default verbosity. A
// race from normal churn goes through within a retry or two; a run this
// long means another writer keeps rewriting the definition and the
// release is starving, and with the conflict kept out of the error log
// and the reconcile-error metric this line is what still shows it. It
// is conflict.LoudAfter of #316, which this branch does not carry.
const releaseLoudAfter = 10

// blockSizeSweepInterval is the default sweep period. A definition is
// released on the second sweep that finds it empty, so a definition whose
// replicas are being created right behind it, as linstor-migrate applies
// them, is never caught in between.
const blockSizeSweepInterval = 10 * time.Minute

// +kubebuilder:rbac:groups=blockstor.org,resources=resources,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.org,resources=resourcedefinitions,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=blockstor.org,resources=resourcegroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.org,resources=controllerconfigs,verbs=get;list;watch

// Reconcile runs for a definition one of whose replicas was deleted.
//
// A release whose patch carries an optimistic lock fails with a
// Conflict when another writer updated the definition since the read,
// which a burst of volume deletions does all the time. The next pass
// re-reads the definition and decides again, so that is not a failure:
// returned, controller-runtime logged it as "Reconciler error" at ERROR
// on every such burst. Requeue, not RequeueAfter, keeps the per-object
// backoff an error would get.
func (r *BlockSizeReleaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	result, err := r.release(ctx, req)
	if !apierrors.IsConflict(err) {
		r.forgetConflicts(req.Name)

		return result, err
	}

	streak := r.countConflict(req.Name)

	level := 1
	if streak >= releaseLoudAfter {
		level = 0
	}

	logf.FromContext(ctx).V(level).Info("block size release raced another writer, retrying",
		"resourceDefinition", req.Name, "conflicts", streak, "error", err.Error())

	return ctrl.Result{Requeue: true}, nil
}

// countConflict counts a lost race on the definition and returns the
// run so far.
func (r *BlockSizeReleaseReconciler) countConflict(name string) int {
	r.conflictsMu.Lock()
	defer r.conflictsMu.Unlock()

	if r.conflicts == nil {
		r.conflicts = map[string]int{}
	}

	r.conflicts[name]++

	return r.conflicts[name]
}

// forgetConflicts ends the definition's run after any other outcome, a
// deleted definition included, so the count does not outlive it.
func (r *BlockSizeReleaseReconciler) forgetConflicts(name string) {
	r.conflictsMu.Lock()
	defer r.conflictsMu.Unlock()

	delete(r.conflicts, name)
}

// forgetGoneConflicts drops the count of every definition no longer in
// rds. A lost race in the sweep is retried by the next sweep, not by a
// requeue, so a definition deleted in between never gets the pass that
// would otherwise forget it.
func (r *BlockSizeReleaseReconciler) forgetGoneConflicts(rds []blockstoriov1alpha1.ResourceDefinition) {
	r.conflictsMu.Lock()
	defer r.conflictsMu.Unlock()

	for name := range r.conflicts {
		if !slices.ContainsFunc(rds, func(rd blockstoriov1alpha1.ResourceDefinition) bool { return rd.Name == name }) {
			delete(r.conflicts, name)
		}
	}
}

func (r *BlockSizeReleaseReconciler) release(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Lock != nil {
		mu := r.Lock(req.Name)
		mu.Lock()
		defer mu.Unlock()
	}

	reader := r.reader()

	placed, err := hasReplica(ctx, reader, req.Name)
	if err != nil || placed {
		return ctrl.Result{}, err
	}

	var rd blockstoriov1alpha1.ResourceDefinition

	err = reader.Get(ctx, req.NamespacedName, &rd)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// A definition being restored keeps the size its snapshot gave it,
	// whatever happens to a replica placed meanwhile: the data it restores
	// was written at that size.
	if !rd.DeletionTimestamp.IsZero() || rd.Spec.Props[store.RestoreFromSnapshotProp] != "" {
		return ctrl.Result{}, nil
	}

	scope, err := loadBlockSizeScope(ctx, reader, &rd)
	if err != nil {
		return ctrl.Result{}, err
	}

	base := rd.DeepCopy()
	changed := false

	for i := range rd.Spec.VolumeDefinitions {
		vd := &rd.Spec.VolumeDefinitions[i]
		if !scope.auto(vd) {
			continue
		}

		props := maps.Clone(vd.Props)
		if blocksize.Release(props) {
			vd.Props = props
			changed = true
		}
	}

	if !changed {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.patchRelease(ctx, &rd, base)
}

// patchRelease writes the released definition under an optimistic
// lock. The wrapped error still answers apierrors.IsConflict, which is
// how Reconcile tells a lost race from a failure.
func (r *BlockSizeReleaseReconciler) patchRelease(ctx context.Context, rd, base *blockstoriov1alpha1.ResourceDefinition) error {
	err := r.Patch(ctx, rd, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	if err != nil {
		return cerrors.Wrapf(err, "release block size of %q", rd.Name)
	}

	return nil
}

// hasReplica reports whether any replica names the definition.
func hasReplica(ctx context.Context, reader client.Reader, rdName string) (bool, error) {
	var list blockstoriov1alpha1.ResourceList

	err := reader.List(ctx, &list)
	if err != nil {
		return false, cerrors.Wrap(err, "list resources")
	}

	return slices.ContainsFunc(list.Items, func(res blockstoriov1alpha1.Resource) bool {
		return naming.Same(res.Spec.ResourceDefinitionName, rdName)
	}), nil
}

func (r *BlockSizeReleaseReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}

	return r.Client
}

// SetupWithManager watches replica deletions and nothing else.
func (r *BlockSizeReleaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}

	deletesOnly := predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}

	err := mgr.Add(&blockSizeSweeper{release: r})
	if err != nil {
		return cerrors.Wrap(err, "register the block size sweep")
	}

	err = ctrl.NewControllerManagedBy(mgr).
		Named("blocksize-release").
		Watches(&blockstoriov1alpha1.Resource{},
			handler.EnqueueRequestsFromMapFunc(definitionOfReplica),
			builder.WithPredicates(deletesOnly)).
		Complete(r)
	if err != nil {
		return cerrors.Wrap(err, "register BlockSizeReleaseReconciler")
	}

	return nil
}

// definitionOfReplica maps a replica to its definition's object name.
func definitionOfReplica(_ context.Context, obj client.Object) []reconcile.Request {
	res, ok := obj.(*blockstoriov1alpha1.Resource)
	if !ok || res.Spec.ResourceDefinitionName == "" {
		return nil
	}

	return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: naming.Key(res.Spec.ResourceDefinitionName)}}}
}

// Sweep releases the definitions a replica deletion event never reached,
// those whose last replica went while the controller was down. LINSTOR
// removes the properties in the same transaction as the last resource,
// so it has no such gap.
//
// Only a definition that is frozen is a candidate. The stamp is written
// when a replica is in use, so a frozen definition with no replica has
// provably lost its last one; a size without the stamp may equally be
// one an operator set before placing anything, which LINSTOR keeps. A
// size left behind without the stamp is harmless: with no replica the
// size may still change, and the next replica raises it for its pool.
//
// seen holds the candidates of the previous sweep; only a definition
// found empty both times is released. The candidates of this sweep are
// returned for the next one.
func (r *BlockSizeReleaseReconciler) Sweep(ctx context.Context, seen map[string]bool) (map[string]bool, error) {
	reader := r.reader()

	var replicas blockstoriov1alpha1.ResourceList

	err := reader.List(ctx, &replicas)
	if err != nil {
		return seen, cerrors.Wrap(err, "list resources")
	}

	placed := make(map[string]bool, len(replicas.Items))
	for i := range replicas.Items {
		placed[naming.Key(replicas.Items[i].Spec.ResourceDefinitionName)] = true
	}

	var rds blockstoriov1alpha1.ResourceDefinitionList

	err = reader.List(ctx, &rds)
	if err != nil {
		return seen, cerrors.Wrap(err, "list resource definitions")
	}

	r.forgetGoneConflicts(rds.Items)

	candidates := map[string]bool{}

	for i := range rds.Items {
		rd := &rds.Items[i]
		if placed[rd.Name] || !rd.DeletionTimestamp.IsZero() ||
			rd.Spec.Props[store.RestoreFromSnapshotProp] != "" || !anyFrozen(rd) {
			continue
		}

		candidates[rd.Name] = true

		if !seen[rd.Name] {
			continue
		}

		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Name: rd.Name}})
		if err != nil {
			return candidates, err
		}
	}

	return candidates, nil
}

// anyFrozen reports whether a volume definition of rd carries the stamp.
func anyFrozen(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	for i := range rd.Spec.VolumeDefinitions {
		if blocksize.Frozen(rd.Spec.VolumeDefinitions[i].Props) {
			return true
		}
	}

	return false
}

// blockSizeSweeper runs the sweep on the leader.
type blockSizeSweeper struct {
	release *BlockSizeReleaseReconciler
}

// NeedLeaderElection is true: the sweep patches definitions.
func (*blockSizeSweeper) NeedLeaderElection() bool { return true }

// Start sweeps once at once and then every interval until ctx ends.
func (s *blockSizeSweeper) Start(ctx context.Context) error {
	interval := s.release.SweepInterval
	if interval <= 0 {
		interval = blockSizeSweepInterval
	}

	log := logf.FromContext(ctx).WithName("blocksize-sweep")

	var seen map[string]bool

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		next, err := s.release.Sweep(ctx, seen)
		if err != nil {
			log.Error(err, "block size sweep failed")
		}

		seen = next

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
