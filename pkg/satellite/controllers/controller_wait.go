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
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// controllerWaitReportAfter is how long an apply may wait for the
// controller's stamp before the wait is reported on the Resource. A
// fresh replica gets its stamp within seconds; one that waits a minute
// has a controller that is down or cannot stamp it, and an Info line in
// the satellite log is not where an operator looks for that.
const controllerWaitReportAfter = time.Minute

// waitingForControllerReason is the Event reason for an apply that has
// waited longer than controllerWaitReportAfter for the controller to
// stamp the replica.
const waitingForControllerReason = "WaitingForController"

// controllerWaits remembers since when the apply of each replica has
// been waiting for the controller's stamp, and whether that wait was
// reported. Like applyBackoff it lives in satellite memory, so a
// restart starts the wait over. The zero value is ready to use.
type controllerWaits struct {
	mu      sync.Mutex
	pending map[string]controllerWait

	// now is the clock, time.Now when nil. Tests set it.
	now func() time.Time
}

type controllerWait struct {
	since    time.Time
	reported bool
}

// overdue records that the apply of name waits, and returns how long
// it has waited and true the first time that is past after.
func (w *controllerWaits) overdue(name string, after time.Duration) (time.Duration, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	clock := w.now
	if clock == nil {
		clock = time.Now
	}

	now := clock()

	if w.pending == nil {
		w.pending = map[string]controllerWait{}
	}

	wait, waiting := w.pending[name]
	if !waiting {
		w.pending[name] = controllerWait{since: now}

		return 0, false
	}

	waited := now.Sub(wait.since)
	if wait.reported || waited < after {
		return waited, false
	}

	wait.reported = true
	w.pending[name] = wait

	return waited, true
}

// forget drops the wait of name: its apply went on, or the Resource is
// gone.
func (w *controllerWaits) forget(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.pending, name)
}

// trackControllerWait reports, once, an apply that has waited for the
// controller's stamp longer than controllerWaitReportAfter, as a
// Warning Event on the Resource. The short waits every fresh replica
// meets leave nothing, as before; any pass that does not wait ends the
// wait.
func (r *ResourceReconciler) trackControllerWait(
	res *blockstoriov1alpha1.Resource, results []*intent.ResourceApplyResult, waiting bool,
) {
	if !waiting {
		r.waits.forget(res.Name)

		return
	}

	waited, report := r.waits.overdue(res.Name, controllerWaitReportAfter)
	if !report || r.Config.Recorder == nil {
		return
	}

	var message string

	for _, result := range results {
		if result.GetWaitingForController() {
			message = result.GetMessage()

			break
		}
	}

	r.Config.Recorder.Eventf(res, nil, corev1.EventTypeWarning, waitingForControllerReason, "Apply",
		"apply held for %s until the controller stamps the replica: %s", waited.Round(time.Second), message)
}
