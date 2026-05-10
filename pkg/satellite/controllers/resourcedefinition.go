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

	"github.com/cockroachdb/errors"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// ResourceDefinitionReconciler exists to feed the watch cache:
// the Resource reconciler does `client.Get(rd)` when computing
// the desired state, and we want the RD already populated in
// the local informer cache before the Resource event fires.
//
// This reconciler has no own apply path — it just observes.
// On RD spec changes the predicate triggers a downstream
// Resource requeue, since the Resource reconciler watches RD
// updates too (wired in a follow-up commit via `Watches`).
//
// Phase 10.1.
type ResourceDefinitionReconciler struct {
	client.Client

	Config Config
}

// Reconcile is intentionally a no-op — see the type comment.
// The watch cache is populated as a side effect of the
// controller-runtime manager starting the informer; we don't
// need to actually act on RD events here.
func (r *ResourceDefinitionReconciler) Reconcile(_ context.Context, _ ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// SetupWithManager wires this reconciler. The predicate drops
// every event by default — the cache is populated via the
// list/watch the manager runs irrespective of our Reconcile
// being called. Keeping Reconcile cheap means the informer
// memory cost is the only overhead.
func (r *ResourceDefinitionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.ResourceDefinition{},
			builder.WithPredicates(dropAllEventsPredicate())).
		Named("satellite-resourcedefinition").
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register ResourceDefinitionReconciler")
	}

	return nil
}

// dropAllEventsPredicate is the cache-warming-only predicate:
// the watch fires and the cache fills, but the reconciler
// itself never runs. Used by reconcilers that exist purely to
// keep an informer alive (their downstream reconcilers do the
// real work).
func dropAllEventsPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(_ event.CreateEvent) bool { return false },
		UpdateFunc:  func(_ event.UpdateEvent) bool { return false },
		DeleteFunc:  func(_ event.DeleteEvent) bool { return false },
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}
