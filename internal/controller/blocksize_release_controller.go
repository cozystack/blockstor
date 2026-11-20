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

	cerrors "github.com/cockroachdb/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/blocksize"
	"github.com/cozystack/blockstor/pkg/naming"
)

// BlockSizeReleaseReconciler removes the block size LINSTOR's rule wrote
// on a volume definition once the definition's last replica is gone, as
// LINSTOR does when it deletes the last resource: the next replica then
// takes the size of whatever pool it lands in, and nothing is left frozen
// for a device that no longer exists.
//
// It runs on replica deletions only. A definition that simply has no
// replica yet keeps what it carries: a restore writes the source's size
// onto the definition before placing anything, and an operator may set a
// size before the first replica.
type BlockSizeReleaseReconciler struct {
	client.Client

	// APIReader lists replicas past the cache, so a replica created a
	// moment ago is not taken for an empty definition.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=blockstor.org,resources=resources,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.org,resources=resourcedefinitions,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=blockstor.org,resources=resourcegroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.org,resources=controllerconfigs,verbs=get;list;watch

// Reconcile runs for a definition one of whose replicas was deleted.
func (r *BlockSizeReleaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reader := r.reader()

	var list blockstoriov1alpha1.ResourceList

	err := reader.List(ctx, &list)
	if err != nil {
		return ctrl.Result{}, cerrors.Wrap(err, "list resources")
	}

	for i := range list.Items {
		if naming.Same(list.Items[i].Spec.ResourceDefinitionName, req.Name) {
			return ctrl.Result{}, nil
		}
	}

	var rd blockstoriov1alpha1.ResourceDefinition

	err = reader.Get(ctx, req.NamespacedName, &rd)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !rd.DeletionTimestamp.IsZero() {
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

	err = r.Patch(ctx, &rd, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	if err != nil {
		return ctrl.Result{}, cerrors.Wrapf(err, "release block size of %q", rd.Name)
	}

	return ctrl.Result{}, nil
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

	err := ctrl.NewControllerManagedBy(mgr).
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
