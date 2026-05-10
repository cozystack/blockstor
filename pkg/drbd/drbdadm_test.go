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

package drbd_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestAdmUpInvokesDrbdadm: Up("pvc-1") shells out to `drbdadm up pvc-1`.
// Resource state changes are kernel-side; the wrapper's whole job is to
// translate Go calls into drbdadm CLI invocations.
func TestAdmUpInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Up(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Up: %v", err)
	}

	want := "drbdadm up pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmDownInvokesDrbdadm: Down → `drbdadm down <res>`.
func TestAdmDownInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Down(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Down: %v", err)
	}

	want := "drbdadm down pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmAdjustInvokesDrbdadm: Adjust → `drbdadm adjust <res>`. This is
// the reload-on-config-change path; runs after the .res file is rewritten.
func TestAdmAdjustInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Adjust(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Adjust: %v", err)
	}

	want := "drbdadm adjust pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmCreateMD: `drbdadm create-md --force <res>` (used on first
// activation; --force is needed when there is leftover signature from a
// previous resource).
func TestAdmCreateMD(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.CreateMD(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("CreateMD: %v", err)
	}

	want := "drbdadm create-md --force pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmPrimary: `drbdadm primary <res>` to flip role for mount.
func TestAdmPrimary(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Primary(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Primary: %v", err)
	}

	want := "drbdadm primary pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmPrimaryForce pins the initial-sync seed command shape:
// `drbdadm primary --force <res>`. Used on a brand-new diskful
// replica when no peer is UpToDate — without --force, drbd refuses
// to promote and the resource sits permanently Inconsistent.
//
// The --force flag MUST appear in the args; a regression that
// accidentally dropped it would silently turn first-Apply into a
// no-op promotion and leave the auto-primary seed broken.
func TestAdmPrimaryForce(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.PrimaryForce(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("PrimaryForce: %v", err)
	}

	want := "drbdadm primary --force pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}

	// And the plain `drbdadm primary pvc-1` (no --force) must NOT
	// appear — the regression risk is reverting to non-forced.
	for _, line := range fx.CommandLines() {
		if line == "drbdadm primary pvc-1" {
			t.Errorf("PrimaryForce emitted non-forced primary: %s", line)
		}
	}
}

// TestAdmSecondary: `drbdadm secondary <res>` after unmount.
func TestAdmSecondary(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Secondary(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Secondary: %v", err)
	}

	want := "drbdadm secondary pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmPropagatesError: exec failure surfaces wrapped — caller needs
// to distinguish "drbdadm not found" from a config-rejection.
func TestAdmPropagatesError(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm up pvc-1",
		storage.FakeResponse{Err: errFakeFailure})

	adm := drbd.NewAdm(fx)

	err := adm.Up(t.Context(), "pvc-1")
	if err == nil {
		t.Fatalf("Up: expected error, got nil")
	}
}

var errFakeFailure = errors.New("drbdadm: simulated failure")

// TestAdmDetachInvokesDrbdadm: Detach → `drbdadm detach --force <res>`.
// --force is required because the disk is already in a transient
// (Failed) state when this gets called; without it drbdadm refuses.
func TestAdmDetachInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.Detach(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("Detach: %v", err)
	}

	want := "drbdadm detach --force pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestAdmSetGiInvokesDrbdmeta pins the initial-sync skip seeding
// command shape: `drbdmeta --force <res>/<vol> v09 <device>
// internal set-gi <peer_gi>:<peer_gi>:0:0`. Phase 8.1.
//
// The format MUST be peer-gi twice (current_uuid + bitmap_uuid both
// match the peer's current_uuid), then two zero history slots — a
// regression that emits just the bare GI or that swaps current/
// bitmap order would silently break the GI-handshake match and
// re-introduce the full initial-sync this whole pipeline exists to
// avoid.
func TestAdmSetGiInvokesDrbdmeta(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.SetGi(t.Context(), "pvc-1", 0, "/dev/dm-3", "78A0DDDABCDEF000")
	if err != nil {
		t.Fatalf("SetGi: %v", err)
	}

	want := "drbdmeta --force pvc-1/0 v09 /dev/dm-3 internal set-gi 78A0DDDABCDEF000:78A0DDDABCDEF000:0:0"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestAdmResizeInvokesDrbdadm: Resize → `drbdadm resize --assume-clean <res>`.
// --assume-clean skips re-syncing the new bytes (they're freshly
// allocated zeros) — without it growing 3 replicas serialises on
// every resync.
func TestAdmResizeInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.Resize(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("Resize: %v", err)
	}

	want := "drbdadm resize --assume-clean pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}
