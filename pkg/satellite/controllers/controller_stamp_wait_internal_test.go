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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// scriptedCreateProvider fails its creates with the next message in
// script, an empty one meaning the create goes through, and goes
// through once the script runs out.
type scriptedCreateProvider struct {
	flakyCreateProvider

	script []string
}

func (p *scriptedCreateProvider) CreateVolume(_ context.Context, _ storage.Volume) error {
	if len(p.script) == 0 {
		return nil
	}

	message := p.script[0]
	p.script = p.script[1:]

	switch message {
	case "":
		return nil
	case stampWait:
		return fmt.Errorf("%w: %w", errScriptedCreate, satellite.ErrWaitingForController)
	}

	return fmt.Errorf("%w: %s", errScriptedCreate, message)
}

// errScriptedCreate is a create scriptedCreateProvider fails.
var errScriptedCreate = errors.New("create volume")

// stampWait scripts a create held for the controller's stamp, the
// error a fresh replica's apply gets before the controller stamped
// Spec.SkipInitialSync on it.
const stampWait = "wait for the stamp"

// A fresh replica whose apply waits for the controller to stamp it is
// not a failed apply: no Warning Event, no back-off, and no "succeeded
// after failures" once it goes through. It is retried on the short
// timer like the allocation waits ahead of the apply. Reported as an
// ApplyFailed Warning, the wait read as a fault on every few new
// replicas, and counting it stretched the retries of a replica that
// then failed for real.
func TestAWaitForTheControllerStampIsNotAFailedApply(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD("STORAGE")
	res := memberReplica()
	r := newPassphraseTestReconciler(t, luksGroup(), rd, res)

	recorder := events.NewFakeRecorder(16)
	r.Config.Recorder = recorder

	provider := &scriptedCreateProvider{script: []string{
		stampWait, stampWait, stampWait, stampWait, "",
		stampWait, stampWait, stampWait, stampWait, "storage pool transiently unavailable",
	}}
	r.Config.Exec = storage.NewFakeExec()
	r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool-a": provider},
		NodeName:  "node-a",
	})

	now := time.Unix(0, 0)
	r.retries.now = func() time.Time { return now }

	run := func() time.Duration {
		t.Helper()

		result, err := r.runApply(t.Context(), res, logr.Discard())
		if err != nil {
			t.Fatalf("runApply: %v", err)
		}

		now = now.Add(result.RequeueAfter)

		return result.RequeueAfter
	}

	for i := range 4 {
		if got := run(); got != applyFailureRequeue {
			t.Fatalf("wait %d retried after %v, want %v", i+1, got, applyFailureRequeue)
		}
	}

	if got := run(); got != 0 {
		t.Fatalf("the apply that went through requeued after %v, want no requeue", got)
	}

	select {
	case ev := <-recorder.Events:
		t.Fatalf("waits for the controller stamp emitted %q, want no Event", ev)
	default:
	}

	for range 4 {
		run()
	}

	if got := run(); got != applyFailureRequeue {
		t.Fatalf("the first real failure after waits retried after %v, want %v: the waits counted toward the back-off",
			got, applyFailureRequeue)
	}
}

// A wait for the controller's stamp during a diskless-to-diskful
// conversion is neither a failed attempt nor the one that went
// through: Status.ToggleDiskRetries keeps its count. A witness the
// placer promotes before the controller stamped it meets this wait,
// and counting it spent the conversion's retries on a replica with no
// fault.
func TestAWaitForTheControllerStampKeepsTheToggleDiskRetries(t *testing.T) {
	t.Parallel()

	const (
		rdName = "pvc-toggle-wait"
		node   = "n-toggle"
		pool   = "lvm-thin"
	)

	res := makeToggledResource(rdName, node, pool)
	res.Status.ToggleDiskRetries = 2

	cli := fake.NewClientBuilder().
		WithScheme(newToggleDiskTestScheme(t)).
		WithObjects(makeStorageOnlyRD(rdName), res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName: node,
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{pool: &scriptedCreateProvider{script: []string{stampWait}}},
				NodeName:  node,
			}),
			Exec: storage.NewFakeExec(),
		},
	}

	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: res.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if result.RequeueAfter != applyFailureRequeue {
		t.Fatalf("the pass requeued after %v, want the wait's %v: the apply did not wait", result.RequeueAfter, applyFailureRequeue)
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(t.Context(), client.ObjectKey{Name: res.Name}, &after)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if after.Status.ToggleDiskRetries != 2 {
		t.Errorf("ToggleDiskRetries = %d after a wait, want 2", after.Status.ToggleDiskRetries)
	}
}

// An apply that waits for the controller's stamp for more than a
// minute is reported once, as a WaitingForController Warning on the
// Resource: the controller is down or cannot stamp it, and the short
// waits leave nothing, so without it the Resource showed nothing at
// all. Waits after the report, and the apply that then goes through,
// add no further Event.
func TestAWaitForTheControllerPastAMinuteIsReportedOnce(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD("STORAGE")
	res := memberReplica()
	r := newPassphraseTestReconciler(t, luksGroup(), rd, res)

	recorder := events.NewFakeRecorder(32)
	r.Config.Recorder = recorder

	const waits = 20

	script := make([]string, 0, waits+1)
	for range waits {
		script = append(script, stampWait)
	}

	r.Config.Exec = storage.NewFakeExec()
	r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool-a": &scriptedCreateProvider{script: append(script, "")}},
		NodeName:  "node-a",
	})

	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r.retries.now = clock
	r.waits.now = clock

	var reported []string

	for pass := range waits + 1 {
		result, err := r.runApply(t.Context(), res, logr.Discard())
		if err != nil {
			t.Fatalf("runApply: %v", err)
		}

		select {
		case ev := <-recorder.Events:
			reported = append(reported, fmt.Sprintf("pass %d at %v: %s", pass+1, now.Sub(time.Unix(0, 0)), ev))
		default:
		}

		now = now.Add(result.RequeueAfter)
	}

	if len(reported) != 1 {
		t.Fatalf("Events: %q, want one WaitingForController Warning", reported)
	}

	if !strings.Contains(reported[0], "Warning "+waitingForControllerReason) ||
		!strings.HasPrefix(reported[0], "pass 13 at 1m0s:") {
		t.Errorf("Event %q, want a WaitingForController Warning on the pass a minute into the wait", reported[0])
	}
}

// A fresh replica the satellite sees before it has its storage pool
// waits for the pool like for the controller's other stamps: no
// ApplyFailed Warning, no back-off, and no "succeeded after failures"
// once the pool lands and the apply goes through.
func TestAReplicaWithoutItsPoolYetIsNotAFailedApply(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD("STORAGE")
	res := memberReplica()
	res.Spec.StoragePool = ""
	r := newPassphraseTestReconciler(t, luksGroup(), rd, res)

	recorder := events.NewFakeRecorder(16)
	r.Config.Recorder = recorder
	r.Config.Exec = storage.NewFakeExec()
	r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool-a": &scriptedCreateProvider{}},
		NodeName:  "node-a",
	})

	result, err := r.runApply(t.Context(), res, logr.Discard())
	if err != nil {
		t.Fatalf("runApply: %v", err)
	}

	if result.RequeueAfter != applyFailureRequeue {
		t.Fatalf("the pass without a pool requeued after %v, want the wait's %v", result.RequeueAfter, applyFailureRequeue)
	}

	res.Spec.StoragePool = "pool-a"

	result, err = r.runApply(t.Context(), res, logr.Discard())
	if err != nil {
		t.Fatalf("runApply: %v", err)
	}

	if result.RequeueAfter != 0 {
		t.Fatalf("the pass with the pool requeued after %v, want no requeue", result.RequeueAfter)
	}

	select {
	case ev := <-recorder.Events:
		t.Fatalf("a replica waiting for its pool emitted %q, want no Event", ev)
	default:
	}
}
