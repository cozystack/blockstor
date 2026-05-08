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
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// SnapshotReconciler dispatches Snapshot CRDs to every diskful
// replica's satellite.
type SnapshotReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Dispatcher *dispatcher.Dispatcher
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=snapshots,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=snapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=snapshots/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch

// Reconcile fans the Snapshot out to every diskful replica's
// satellite via Dispatcher.CreateSnapshot. Failures requeue with a
// 10 s back-off so satellites that haven't registered yet eventually
// catch up.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.Dispatcher == nil {
		return ctrl.Result{}, nil
	}

	var snap blockstoriov1alpha1.Snapshot

	err := r.Get(ctx, req.NamespacedName, &snap)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	var resList blockstoriov1alpha1.ResourceList

	err = r.List(ctx, &resList)
	if err != nil {
		return ctrl.Result{}, err
	}

	replicas := make([]blockstoriov1alpha1.Resource, 0, len(resList.Items))

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName == snap.Spec.ResourceDefinitionName {
			replicas = append(replicas, resList.Items[i])
		}
	}

	var nodeList blockstoriov1alpha1.NodeList

	err = r.List(ctx, &nodeList)
	if err != nil {
		return ctrl.Result{}, err
	}

	results, err := r.Dispatcher.CreateSnapshot(ctx,
		snap.Spec.ResourceDefinitionName, snap.Spec.SnapshotName,
		replicas, nodeList.Items)
	if err != nil {
		log.Error(err, "CreateSnapshot dispatch failed", "snapshot", snap.Name)

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	for _, res := range results {
		if !res.GetOk() {
			log.Info("satellite rejected snapshot", "msg", res.GetMessage(), "snapshot", snap.Name)
		}
	}

	log.Info("snapshot dispatched", "snapshot", snap.Name, "replicas", len(results))

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Snapshot{}).
		Named("snapshot").
		Complete(r)
}
