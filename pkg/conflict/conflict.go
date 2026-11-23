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
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// LoudAfter is the run of back-to-back conflicts on one object after
// which they are logged at the default verbosity. A conflict from
// normal churn goes through within a retry or two; a run this long
// means two writers keep overwriting each other, and with the error
// gone from the log and from the reconcile-error metric this line is
// what still shows it.
const LoudAfter = 10

// Retrier keeps optimistic-concurrency conflicts out of a reconciler's
// error log. The zero value is ready to use; a reconciler holds one
// for its whole lifetime, because it counts conflicts per object
// across passes.
type Retrier struct {
	mu     sync.Mutex
	streak map[types.NamespacedName]int
}

// Reconcile runs one pass of pass and retries it, rather than reports
// it, when it failed on a 409 Conflict. A conflict means another writer
// updated the object between our read and our write; the next pass
// re-reads it and goes through, so it is not a failure anyone can act
// on. Returned as an error it makes controller-runtime log "Reconciler
// error" at ERROR level on every volume create and delete, and an alert
// on ERROR lines then fires on normal churn.
//
// The retry uses the deprecated Requeue, not RequeueAfter, on purpose.
// controller-runtime sends a Requeue through the same rate limiter as
// an error, backing off per object, while RequeueAfter resets that
// backoff and would turn a writer fight into a hot loop.
//
// A conflict is logged at V(1) until LoudAfter of them in a row on the
// same object, then at the default level.
func (r *Retrier) Reconcile(ctx context.Context, req reconcile.Request, pass reconcile.Func) (reconcile.Result, error) {
	result, err := pass(ctx, req)
	if err == nil || !apierrors.IsConflict(err) {
		r.reset(req.NamespacedName)

		return result, err
	}

	streak := r.record(req.NamespacedName)

	level := 1
	if streak >= LoudAfter {
		level = 0
	}

	logf.FromContext(ctx).V(level).Info("write raced another writer, retrying",
		"conflicts", streak, "error", err.Error())

	return reconcile.Result{Requeue: true}, nil
}

func (r *Retrier) record(key types.NamespacedName) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.streak == nil {
		r.streak = map[types.NamespacedName]int{}
	}

	r.streak[key]++

	return r.streak[key]
}

func (r *Retrier) reset(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.streak, key)
}
