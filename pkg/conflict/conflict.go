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

// Package conflict keeps optimistic-concurrency conflicts out of the
// error log of a controller-runtime reconciler.
package conflict

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Reconcile runs one pass of pass and retries it, rather than reports
// it, when it failed on a 409 Conflict. A conflict means another writer
// updated the object between our read and our write; the next pass
// re-reads it and goes through, so it is not a failure anyone can act
// on. Returned as an error it makes controller-runtime log "Reconciler
// error" at ERROR level on every volume create and delete, and an alert
// on ERROR lines then fires on normal churn. The retry goes through the
// same rate limiter an error would, and the conflict is logged at V(1).
func Reconcile(ctx context.Context, req reconcile.Request, pass reconcile.Func) (reconcile.Result, error) {
	result, err := pass(ctx, req)
	if err == nil || !apierrors.IsConflict(err) {
		return result, err
	}

	logf.FromContext(ctx).V(1).Info("write raced another writer, retrying", "error", err.Error())

	return reconcile.Result{Requeue: true}, nil
}
