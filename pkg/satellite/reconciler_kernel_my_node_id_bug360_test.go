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

package satellite

import (
	"context"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// drFor360 builds a minimal DesiredResource carrying the
// controller-allocated local node-id in DrbdOptions, the field
// reconcileKernelMyNodeID compares against the kernel my-id.
func drFor360(name string, allocatedID string) *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     name,
		NodeName: "n1",
		Props:    map[string]string{},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": allocatedID,
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}
}

// TestReconcileKernelMyNodeIDDownsOnMismatch (Bug 360): the kernel
// slot was burned to my-node-id 0 at a pre-allocation first-
// activation, but the controller has since allocated id 2 (rendered
// into DrbdOptions node-id). adjust cannot rewrite a loaded
// resource's own my-id, so the self-heal MUST drbdadm-down the slot
// — the FSM dispatch that follows re-ups it with id 2.
func TestReconcileKernelMyNodeIDDownsOnMismatch(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-360 --json", storage.FakeResponse{
		Stdout: []byte(`[{"name":"pvc-360","node-id":0,"role":"Secondary","devices":[],"connections":[]}]`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	if err := rec.reconcileKernelMyNodeID(context.Background(), drFor360("pvc-360", "2")); err != nil {
		t.Fatalf("reconcileKernelMyNodeID mismatch: %v", err)
	}

	want := "drbdadm down pvc-360"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("mismatch: expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestReconcileKernelMyNodeIDNoOpOnMatch (Bug 360): steady state —
// kernel my-id already equals the allocated id. The self-heal MUST
// be a no-op (no down), or every reconcile would tear down a healthy
// slot.
func TestReconcileKernelMyNodeIDNoOpOnMatch(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-360 --json", storage.FakeResponse{
		Stdout: []byte(`[{"name":"pvc-360","node-id":2,"role":"Secondary","devices":[],"connections":[]}]`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	if err := rec.reconcileKernelMyNodeID(context.Background(), drFor360("pvc-360", "2")); err != nil {
		t.Fatalf("reconcileKernelMyNodeID match: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm down pvc-360") {
		t.Errorf("match: down must NOT fire on a converged slot; got %v", fx.CommandLines())
	}
}

// TestReconcileKernelMyNodeIDNoOpOnAbsentSlot (Bug 360): kernel has
// no slot yet (fresh provision). The self-heal MUST be a no-op — the
// bring-up path ups with the correct id directly; tearing down a
// non-existent slot is pointless and drbdadm-down would error.
func TestReconcileKernelMyNodeIDNoOpOnAbsentSlot(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-360 --json", storage.FakeResponse{
		Stdout: []byte("[]\n"),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	if err := rec.reconcileKernelMyNodeID(context.Background(), drFor360("pvc-360", "2")); err != nil {
		t.Fatalf("reconcileKernelMyNodeID absent: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm down pvc-360") {
		t.Errorf("absent slot: down must NOT fire; got %v", fx.CommandLines())
	}
}

// TestReconcileKernelMyNodeIDNoOpOnUnparseableDesiredID (Bug 360):
// when DrbdOptions node-id is absent/malformed (pre-allocation /
// bypassed gate) the self-heal declines to act — the controller-side
// allocation gate owns blocking that case; here we never act on a
// guess.
func TestReconcileKernelMyNodeIDNoOpOnUnparseableDesiredID(t *testing.T) {
	fx := storage.NewFakeExec()
	// Even if the kernel reports a slot, an unresolved desired id
	// must short-circuit before the status probe acts.
	fx.Expect("drbdsetup status pvc-360 --json", storage.FakeResponse{
		Stdout: []byte(`[{"name":"pvc-360","node-id":0,"role":"Secondary","devices":[],"connections":[]}]`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := drFor360("pvc-360", "")
	delete(dr.DrbdOptions, "node-id")

	if err := rec.reconcileKernelMyNodeID(context.Background(), dr); err != nil {
		t.Fatalf("reconcileKernelMyNodeID unparseable: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm down pvc-360") {
		t.Errorf("unparseable desired id: down must NOT fire; got %v", fx.CommandLines())
	}
}
