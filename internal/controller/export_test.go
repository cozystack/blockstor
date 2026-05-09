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
