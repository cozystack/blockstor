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

package controllers

import (
	"context"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// A pass renders a replica's .res from more than its own Resource, its
// peers and its definition: the ResourceGroup and the ControllerConfig
// give it DRBD options, the Nodes (and the corev1 Nodes' InternalIPs)
// the peer addresses, the StoragePools the backing devices. LINSTOR's
// controller pushes a change of any of these to every satellite that
// holds an affected resource. With no periodic pass left, these watches
// are what carries such a change to the replicas already up, so
// `linstor rg drbd-options` or `linstor controller drbd-options` reaches
// their .res and the kernel the way it does under LINSTOR.

// watchRenderInputs registers the watches for the objects a pass renders
// a replica from besides its Resource, its peers and its definition.
// Only a spec change (a new generation) of a blockstor object counts,
// since their status writes (the heartbeat, the pool capacity) change
// nothing a pass renders. A ResourceGroup or the ControllerConfig that
// appears or goes away changes the effective options too; a Node or a
// StoragePool that appears or goes away comes with Resource events of
// its own.
func (r *ResourceReconciler) watchRenderInputs(bldr *builder.Builder) *builder.Builder {
	scope := builder.WithPredicates(predicate.GenerationChangedPredicate{})
	specOnly := builder.WithPredicates(predicate.GenerationChangedPredicate{}, updatesOnly())

	return bldr.
		Watches(&blockstoriov1alpha1.ResourceGroup{},
			handler.EnqueueRequestsFromMapFunc(r.passes.mapFunc(passConfig, r.enqueueLocalForGroup)), scope).
		Watches(&blockstoriov1alpha1.ControllerConfig{},
			handler.EnqueueRequestsFromMapFunc(r.passes.mapFunc(passConfig, r.enqueueAllLocal)), scope).
		Watches(&blockstoriov1alpha1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.passes.mapFunc(passConfig, r.enqueueLocalForNodeObject)), specOnly).
		Watches(&blockstoriov1alpha1.StoragePool{},
			handler.EnqueueRequestsFromMapFunc(r.passes.mapFunc(passConfig, r.enqueueLocalForPool)), specOnly).
		Watches(&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.passes.mapFunc(passConfig, r.enqueueLocalForNodeObject)),
			builder.WithPredicates(internalIPChanged()))
}

// updatesOnly passes updates and nothing else.
func updatesOnly() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// internalIPChanged passes a corev1 Node update whose InternalIPs
// changed, the one field of it a pass renders. The kubelet rewrites the
// rest of the status on its own schedule.
func internalIPChanged() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, okOld := e.ObjectOld.(*corev1.Node)
			newNode, okNew := e.ObjectNew.(*corev1.Node)

			return okOld && okNew && !slices.Equal(internalIPs(oldNode), internalIPs(newNode))
		},
	}
}

func internalIPs(node *corev1.Node) []string {
	var out []string

	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			out = append(out, addr.Address)
		}
	}

	return out
}

// enqueueAllLocal enqueues every local replica.
func (r *ResourceReconciler) enqueueAllLocal(ctx context.Context, _ client.Object) []reconcile.Request {
	return r.enqueueLocalOf(ctx, func(*blockstoriov1alpha1.Resource) bool { return true })
}

// enqueueLocalForGroup enqueues the local replicas of every definition in
// the group.
func (r *ResourceReconciler) enqueueLocalForGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	var rds blockstoriov1alpha1.ResourceDefinitionList

	err := r.List(ctx, &rds)
	if err != nil {
		return nil
	}

	inGroup := map[string]bool{}

	for i := range rds.Items {
		rd := &rds.Items[i]
		if rd.Spec.ResourceGroupName != "" && naming.Same(rd.Spec.ResourceGroupName, obj.GetName()) {
			inGroup[naming.Key(rd.Name)] = true
		}
	}

	if len(inGroup) == 0 {
		return nil
	}

	return r.enqueueLocalOf(ctx, func(res *blockstoriov1alpha1.Resource) bool {
		return inGroup[naming.Key(res.Spec.ResourceDefinitionName)]
	})
}

// enqueueLocalForNodeObject enqueues the local replicas that render the
// node named like obj (a blockstor or a corev1 Node).
func (r *ResourceReconciler) enqueueLocalForNodeObject(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.enqueueLocalSharingNode(ctx, obj.GetName())
}

// enqueueLocalForPool enqueues the local replicas that render the pool's
// node.
func (r *ResourceReconciler) enqueueLocalForPool(ctx context.Context, obj client.Object) []reconcile.Request {
	pool, ok := obj.(*blockstoriov1alpha1.StoragePool)
	if !ok {
		return nil
	}

	return r.enqueueLocalSharingNode(ctx, pool.Spec.NodeName)
}

// enqueueLocalSharingNode enqueues the local replicas of every definition
// with a replica on node: those are the .res files that name it. A
// change of the local node itself enqueues every local replica.
func (r *ResourceReconciler) enqueueLocalSharingNode(ctx context.Context, node string) []reconcile.Request {
	if node == "" {
		return nil
	}

	var all blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &all)
	if err != nil {
		return nil
	}

	onNode := map[string]bool{}

	for i := range all.Items {
		if naming.Same(all.Items[i].Spec.NodeName, node) {
			onNode[naming.Key(all.Items[i].Spec.ResourceDefinitionName)] = true
		}
	}

	return r.localOf(all.Items, func(res *blockstoriov1alpha1.Resource) bool {
		return onNode[naming.Key(res.Spec.ResourceDefinitionName)]
	})
}

// enqueueLocalOf enqueues the local replicas keep selects.
func (r *ResourceReconciler) enqueueLocalOf(ctx context.Context, keep func(*blockstoriov1alpha1.Resource) bool) []reconcile.Request {
	var all blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &all)
	if err != nil {
		return nil
	}

	return r.localOf(all.Items, keep)
}

func (r *ResourceReconciler) localOf(items []blockstoriov1alpha1.Resource, keep func(*blockstoriov1alpha1.Resource) bool) []reconcile.Request {
	var reqs []reconcile.Request

	for i := range items {
		res := &items[i]
		if naming.Same(res.Spec.NodeName, r.Config.NodeName) && keep(res) {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKey{Name: res.Name}})
		}
	}

	return reqs
}
