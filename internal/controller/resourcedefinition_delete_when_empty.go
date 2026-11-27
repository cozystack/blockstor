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

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

// deleteRequestedWhenEmpty finishes a delete a client asked for once the
// definition is empty (store.DeleteWhenEmptyProp), and reports whether the
// definition is gone. One that is still kept by the clone's snapshot waits
// for the snapshot's delete event. One that got a replica or a snapshot of
// its own since is in use again, and the request is dropped: LINSTOR would
// have deleted the definition before either could be made.
func (r *ResourceDefinitionReconciler) deleteRequestedWhenEmpty(
	ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition,
) (bool, error) {
	if rd.Spec.Props[store.DeleteWhenEmptyProp] == "" && rd.Spec.ExtraProps[store.DeleteWhenEmptyProp] == "" {
		return false, nil
	}

	contents, claimed, err := store.ClaimEmptyDefinition(ctx, r.Store, rd.Name)
	if err != nil {
		return false, errors.Wrap(err, "claim the empty definition")
	}

	if claimed {
		return true, errors.Wrap(store.DeleteClaimedDefinition(ctx, r.Store, rd.Name), "delete the empty definition")
	}

	if contents.OnlyCloneSnapshots {
		return false, nil
	}

	err = store.SetDeleteWhenEmpty(ctx, r.Store, rd.Name, false)
	if errors.Is(err, store.ErrNotFound) {
		return true, nil
	}

	return false, errors.Wrap(err, "drop the delete request of a definition in use again")
}

// enqueueRDForSnapshot maps a snapshot to its definition.
func (r *ResourceDefinitionReconciler) enqueueRDForSnapshot(_ context.Context, obj client.Object) []reconcile.Request {
	snap, ok := obj.(*blockstoriov1alpha1.Snapshot)
	if !ok || snap.Spec.ResourceDefinitionName == "" {
		return nil
	}

	return []reconcile.Request{
		{NamespacedName: types.NamespacedName{Name: naming.Key(snap.Spec.ResourceDefinitionName)}},
	}
}

// snapshotGone passes only the event of a snapshot leaving the API
// server: the one that can leave a definition marked
// store.DeleteWhenEmptyProp empty.
func snapshotGone() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
