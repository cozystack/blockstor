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

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// EnqueueResourcesForRD exposes the internal RD-watch fan-out for
// the *_test.go suite. Pure forward; the production wiring (in
// SetupWithManager) keeps using the unexported method.
func (r *ResourceReconciler) EnqueueResourcesForRD(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.enqueueResourcesForRD(ctx, obj)
}

// EnqueueSiblings exposes the internal sibling fan-out for tests.
func (r *ResourceReconciler) EnqueueSiblings(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.enqueueSiblings(ctx, obj)
}

// EnqueueRDForResource exposes the RD-side parent lookup for tests.
// Production wiring stays unexported via SetupWithManager.
func (r *ResourceDefinitionReconciler) EnqueueRDForResource(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.enqueueRDForResource(ctx, obj)
}

// AlreadyExists exposes the wrapped-error keyword check the RD
// reconciler uses to tolerate kube-apiserver "already exists"
// races. Tests pin both the positive and the false-positive paths.
func AlreadyExists(err error) bool {
	return alreadyExists(err)
}

// ResolveLayerStack exposes the four-tier layer-stack resolver:
// RD spec → RG spec → nil (dispatcher default). Tests pin each
// tier so a refactor that swapped precedence (RG over RD, say)
// would surface immediately.
func (r *ResourceReconciler) ResolveLayerStack(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) []string {
	return r.resolveLayerStack(ctx, rd)
}
