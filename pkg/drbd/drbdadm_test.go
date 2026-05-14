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
	"context"
	"errors"
	"fmt"
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

// TestAdmDownForceHappyPath: when `drbdadm down <res>` returns
// successfully under the budget, DownForce stops after one shell-out
// and does NOT invoke the kernel-direct fallback. Pins Bug 82's
// happy-path expectation — the new bounded teardown stays a pure
// `drbdadm down` on healthy resources so we don't churn netlink
// state on every delete.
func TestAdmDownForceHappyPath(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.DownForce(t.Context(), "pvc-1", []int32{0, 1}); err != nil {
		t.Fatalf("DownForce: %v", err)
	}

	calls := fx.CommandLines()
	if !slices.Contains(calls, "drbdadm down pvc-1") {
		t.Errorf("missing drbdadm down call: %v", calls)
	}

	// Fallback path MUST NOT run when drbdadm down succeeded.
	for _, line := range calls {
		if line == "drbdsetup detach --force pvc-1/0" ||
			line == "drbdsetup detach --force pvc-1/1" ||
			line == "drbdsetup down pvc-1" {
			t.Errorf("fallback path ran on happy success: %q in %v", line, calls)
		}
	}
}

// errSuspendedQuorum is the static error used as a surrogate for
// "drbdadm down hung on suspended:quorum" in the DownForce fallback
// tests. DownForce only matches substrings against the drbdsetup
// error text (not drbdadm), so this message just needs to make the
// happy path fail; the exact wording is irrelevant.
var errSuspendedQuorum = errors.New("test: resource is suspended: quorum")

// errNoSuchResource is the static error used as a surrogate for
// drbdsetup down's "missing slot" exit. DownForce matches the
// title-case substring "No such resource" against the drbdsetup
// error text, so we embed that exact phrase here (the leading
// "test:" satisfies the ST1005 "no leading capital" lint without
// hiding the substring DownForce's matcher looks for).
var errNoSuchResource = errors.New("test: No such resource")

// TestAdmDownForceFallback pins the Bug 82 recovery: when
// `drbdadm down` fails (suspended-quorum surrogate: we just inject a
// generic error), DownForce must invoke `drbdsetup detach --force`
// for every volume and then `drbdsetup down <res>`, and must return
// nil because the goal state (kernel slot released) was reached via
// the fallback path.
func TestAdmDownForceFallback(t *testing.T) {
	fx := storage.NewFakeExec()
	// Make `drbdadm down` fail; this is the surrogate for the
	// real-world "suspended:quorum hung the call until ctx deadline"
	// — the wrapper's contract is identical either way (error from
	// the bounded sub-context triggers fallback).
	fx.Expect("drbdadm down pvc-suspended", storage.FakeResponse{
		Err: errSuspendedQuorum,
	})

	adm := drbd.NewAdm(fx)

	err := adm.DownForce(t.Context(), "pvc-suspended", []int32{0, 1})
	if err != nil {
		t.Fatalf("DownForce: expected nil after fallback recovery, got %v", err)
	}

	calls := fx.CommandLines()

	want := []string{
		"drbdadm down pvc-suspended",
		"drbdsetup detach --force pvc-suspended/0",
		"drbdsetup detach --force pvc-suspended/1",
		"drbdsetup down pvc-suspended",
	}
	for _, w := range want {
		if !slices.Contains(calls, w) {
			t.Errorf("missing %q in fallback sequence: %v", w, calls)
		}
	}

	// Ordering: every detach must precede the final drbdsetup down,
	// otherwise the kernel slot release runs before lower disks are
	// released and the suspended-state retry stalls again.
	downIdx := slices.Index(calls, "drbdsetup down pvc-suspended")
	for _, vol := range []string{"pvc-suspended/0", "pvc-suspended/1"} {
		detachIdx := slices.Index(calls, "drbdsetup detach --force "+vol)
		if detachIdx < 0 || downIdx < 0 || detachIdx > downIdx {
			t.Errorf("detach %s must precede drbdsetup down; got %v", vol, calls)
		}
	}
}

// TestAdmDownForceFallbackMissingSlot: when `drbdsetup down` reports
// the resource is already gone ("No such resource"), DownForce returns
// nil — the goal state is "kernel slot absent", and a missing slot
// satisfies it. Without this branch, an idempotent re-issue (second
// handleDelete pass after a partial first) would surface as an error.
func TestAdmDownForceFallbackMissingSlot(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm down pvc-gone", storage.FakeResponse{
		Err: errSuspendedQuorum,
	})
	fx.Expect("drbdsetup down pvc-gone", storage.FakeResponse{
		Err: errNoSuchResource,
	})

	adm := drbd.NewAdm(fx)

	err := adm.DownForce(t.Context(), "pvc-gone", []int32{0})
	if err != nil {
		t.Fatalf("DownForce: missing-slot must be a clean no-op, got %v", err)
	}
}

// TestAdmDownForceBoundedDeadline guarantees the happy-path call
// receives a context with a deadline set by DownForceTimeout — this
// is the load-bearing invariant of the Bug 82 fix. The fake exec
// inspects ctx.Deadline() on the recorded call to verify the
// timeout was applied; without a deadline, a suspended-quorum
// resource would block the satellite finalizer-strip path
// indefinitely.
func TestAdmDownForceBoundedDeadline(t *testing.T) {
	var (
		deadlineSeen bool
		hasDeadline  bool
	)

	deadlineFx := &deadlineCapturingExec{
		fx: storage.NewFakeExec(),
		onCall: func(name string, _ []string, hasDl bool) {
			if name == "drbdadm" {
				deadlineSeen = true
				hasDeadline = hasDl
			}
		},
	}

	adm := drbd.NewAdm(deadlineFx)
	if err := adm.DownForce(t.Context(), "pvc-1", nil); err != nil {
		t.Fatalf("DownForce: %v", err)
	}

	if !deadlineSeen {
		t.Fatal("drbdadm down was never invoked")
	}

	if !hasDeadline {
		t.Error("DownForce must invoke drbdadm down with a deadline (Bug 82 bounded teardown)")
	}
}

// deadlineCapturingExec wraps a FakeExec to inspect ctx.Deadline()
// per call — needed by TestAdmDownForceBoundedDeadline because
// FakeExec drops the context.
type deadlineCapturingExec struct {
	fx     *storage.FakeExec
	onCall func(name string, args []string, hasDeadline bool)
}

func (d *deadlineCapturingExec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	_, hasDeadline := ctx.Deadline()
	if d.onCall != nil {
		d.onCall(name, args, hasDeadline)
	}

	out, err := d.fx.Run(ctx, name, args...)
	if err != nil {
		return out, fmt.Errorf("fake exec %s: %w", name, err)
	}

	return out, nil
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

	// --max-peers pinned to MaxPeers-1 so the kernel can hold the
	// connection mesh the allocator hands out.
	want := fmt.Sprintf("drbdadm create-md --force --max-peers=%d pvc-1", drbd.MaxPeers-1)
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

// TestAdmStatusResourcesParsesNames pins the kernel-state listing
// the orphan sweeper (Scenario 5.34) relies on. `drbdsetup status`
// puts every resource name at column 0 followed by `role:<role>`;
// per-volume / per-peer lines are indented. The parser must:
//   - pull the resource name from column-0 lines,
//   - skip indented continuation lines,
//   - skip blank separators between resource blocks.
//
// A regression that returned indented tokens (volume / peer-node
// names) would feed the sweeper false orphans and trigger
// drbdadm-down on healthy volumes.
func TestAdmStatusResourcesParsesNames(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status", storage.FakeResponse{Stdout: []byte(`pvc-aaa role:Primary
  volume:0 disk:UpToDate
  worker-2 role:Secondary
    volume:0 peer-disk:UpToDate

pvc-bbb role:Secondary
  volume:0 disk:Diskless
`)})

	adm := drbd.NewAdm(fx)

	names, err := adm.StatusResources(t.Context())
	if err != nil {
		t.Fatalf("StatusResources: %v", err)
	}

	want := []string{"pvc-aaa", "pvc-bbb"}
	if !slices.Equal(names, want) {
		t.Errorf("StatusResources: got %v, want %v", names, want)
	}
}

// TestAdmStatusResourcesEmptyKernel pins the no-resources path:
// drbdsetup exits non-zero with `No currently configured DRBD found.`
// when the kernel module is loaded but holds nothing. The sweeper
// must treat this as "empty kernel, no orphans" — not an error.
// Otherwise every sweep on a freshly-rebooted node would log a
// failure.
func TestAdmStatusResourcesEmptyKernel(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status", storage.FakeResponse{
		Stdout: []byte("No currently configured DRBD found.\n"),
		Err:    errFakeFailure,
	})

	adm := drbd.NewAdm(fx)

	names, err := adm.StatusResources(t.Context())
	if err != nil {
		t.Fatalf("StatusResources empty: unexpected error %v", err)
	}

	if len(names) != 0 {
		t.Errorf("StatusResources empty: got %v, want []", names)
	}
}

// TestAdmIsLoadedTrue pins the kernel-loaded case: `drbdsetup status
// <rd>` exits zero with a real status block → IsLoaded returns true.
// Used by the reconciler's Bug-47 fix to decide between `drbdadm
// adjust` (loaded → reconcile diff) and `drbdadm up` (absent →
// bootstrap from .res + metadata).
func TestAdmIsLoadedTrue(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1", storage.FakeResponse{Stdout: []byte(`pvc-1 role:Primary
  volume:0 disk:UpToDate
  worker-2 role:Secondary
    volume:0 peer-disk:UpToDate
`)})

	adm := drbd.NewAdm(fx)

	loaded, err := adm.IsLoaded(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("IsLoaded: unexpected error %v", err)
	}

	if !loaded {
		t.Errorf("IsLoaded(loaded resource): got false, want true")
	}
}

// TestAdmIsLoadedFalseNoResource pins the post-`drbdadm down` case:
// `drbdsetup status <rd>` returns non-zero with the verbatim "No
// currently configured DRBD found" message → IsLoaded must report
// false + nil error. The reconciler keys its `drbdadm up` fallback
// off this exact "absent but not broken" signal; a bubbled error
// here would surface as a misleading "satellite probe failed" in
// the reconcile loop instead of "kernel slot is just gone".
func TestAdmIsLoadedFalseNoResource(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-down", storage.FakeResponse{
		Stdout: []byte("No currently configured DRBD found.\n"),
		Err:    errFakeFailure,
	})

	adm := drbd.NewAdm(fx)

	loaded, err := adm.IsLoaded(t.Context(), "pvc-down")
	if err != nil {
		t.Fatalf("IsLoaded(absent): unexpected error %v", err)
	}

	if loaded {
		t.Errorf("IsLoaded(absent): got true, want false")
	}
}

// TestAdmIsLoadedFalseEmptyStdout pins the defensive empty-output
// case: a zero-exit `drbdsetup status` with no payload is treated
// as "not loaded" even though real drbdsetup never produces that
// — fake exec in unit tests can, and we'd rather err on "absent
// → reconciler will call up" than mis-report as loaded and adjust
// against a slot the kernel doesn't know.
func TestAdmIsLoadedFalseEmptyStdout(t *testing.T) {
	fx := storage.NewFakeExec()
	// No Expect → FakeExec returns nil stdout + nil error.

	adm := drbd.NewAdm(fx)

	loaded, err := adm.IsLoaded(t.Context(), "pvc-empty")
	if err != nil {
		t.Fatalf("IsLoaded(empty): unexpected error %v", err)
	}

	if loaded {
		t.Errorf("IsLoaded(empty stdout): got true, want false")
	}
}
