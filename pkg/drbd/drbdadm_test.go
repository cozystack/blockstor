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

package drbd_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
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

// TestAdmSetupDownInvokesDrbdsetup pins the kernel-direct teardown
// shape: SetupDown → `drbdsetup down <res>`. Distinct from Down
// because drbdsetup operates on kernel state directly, with no
// /etc/drbd.d/<rsc>.res lookup — this is the only path that
// actually works once the .res file has been removed.
//
// Issue 288: the orphan sweeper used to call `drbdadm down` on
// resources discovered via `drbdsetup status`, which always
// failed (".res file missing → 'not defined in your config'")
// and left the kernel slot leaked. The sweeper now routes
// through SetupDown to close that hole.
func TestAdmSetupDownInvokesDrbdsetup(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.SetupDown(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("SetupDown: %v", err)
	}

	want := "drbdsetup down pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}

	// Regression guard: SetupDown MUST NOT shell out to drbdadm.
	// The whole reason this method exists is to skip drbdadm's
	// .res-file lookup; a regression that fell back to drbdadm
	// would re-introduce the kernel-slot leak.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm ") {
			t.Errorf("SetupDown shelled out to drbdadm (defeats the .res-less recovery purpose): %s",
				line)
		}
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

// TestAdmSetGIInvokesDrbdmeta pins the initial-sync skip seeding
// command shape: `drbdmeta --force <res>/<vol> v09 <device>
// internal set-gi --node-id <peer> <peer_gi>:<peer_gi>:0:0`.
//
// Two invariants this regression guard pins:
//
//   - The GI tuple MUST be peer-gi twice (current_uuid + bitmap_uuid
//     both match the peer's current_uuid), then two zero history
//     slots. Swapping current/bitmap order or emitting a bare GI
//     would silently break the GI-handshake match and re-introduce
//     the full initial-sync the day0-skip pipeline exists to avoid.
//
//   - `--node-id <peer>` MUST be on the command line. DRBD 9.2+'s
//     drbdmeta refuses the legacy single-call form with "The set-gi
//     command requires the --node-id option" because the GI tuple
//     lives in a per-peer bitmap slot in the modern v09 layout. A
//     regression to the no-node-id shape would silently re-introduce
//     a fall-through to the full initial-sync on DRBD 9.2+.
func TestAdmSetGIInvokesDrbdmeta(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.SetGI(t.Context(), "pvc-1", 0, "/dev/dm-3", 1, "78A0DDDABCDEF000")
	if err != nil {
		t.Fatalf("SetGI: %v", err)
	}

	want := "drbdmeta --force pvc-1/0 v09 /dev/dm-3 internal set-gi --node-id 1 78A0DDDABCDEF000:78A0DDDABCDEF000:0:0"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestAdmForgetPeerInvokesDrbdmeta pins the on-disk slot
// reclaim command shape: `drbdmeta --force <res>/<vol> v09
// <device> internal forget-peer <peer-node-id>`.
//
// Without forget-peer, `drbdadm del-peer` only severs the kernel
// connection; the metadata block still records the departed
// peer's GI / bitmap slot. After enough permanent node-removal
// cycles the v09 metadata layout exhausts its
// `--max-peers=MaxPeers-1` slot pool and the next replica add
// fails with drbdmeta refusing to allocate.
//
// Two invariants pinned:
//
//   - `<peer-node-id>` MUST be passed as the trailing positional
//     argument (not `--node-id`). The forget-peer subcommand
//     takes the node-id positionally; a regression that copied
//     SetGI's `--node-id <N>` shape would surface as drbdmeta
//     refusing the call with a usage error.
//   - The `--force` flag MUST be present so drbdmeta accepts the
//     mutation against in-use metadata (the resource is still
//     loaded when forget-peer runs).
func TestAdmForgetPeerInvokesDrbdmeta(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.ForgetPeer(t.Context(), "pvc-1", 0, "/dev/dm-3", 2)
	if err != nil {
		t.Fatalf("ForgetPeer: %v", err)
	}

	want := "drbdmeta --force pvc-1/0 v09 /dev/dm-3 internal forget-peer 2"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestAdmResizeInvokesDrbdadm: Resize(assumeClean=true) →
// `drbdadm resize --assume-clean <res>`. --assume-clean skips
// re-syncing the new bytes (zero-fill providers expose freshly-zeroed
// extents) — without it growing 3 replicas serialises on every resync.
func TestAdmResizeInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.Resize(t.Context(), "pvc-1", true)
	if err != nil {
		t.Fatalf("Resize: %v", err)
	}

	want := "drbdadm resize --assume-clean pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestAdmResizeWithoutAssumeClean: Resize(assumeClean=false) →
// `drbdadm resize <res>` (no --assume-clean). Bug 395 (P1, data
// integrity): non-zero-fill providers (thick LVM) MUST omit
// --assume-clean so DRBD marks the grown region out-of-sync and
// resyncs it from the UpToDate source — otherwise replicas silently
// disagree on the grown region (0xA1/0xB2 divergence confirmed on the
// stand).
func TestAdmResizeWithoutAssumeClean(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.Resize(t.Context(), "pvc-1", false)
	if err != nil {
		t.Fatalf("Resize: %v", err)
	}

	want := "drbdadm resize pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}

	for _, cl := range fx.CommandLines() {
		if strings.Contains(cl, "--assume-clean") {
			t.Errorf("did not expect --assume-clean in calls; got %v", fx.CommandLines())
		}
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

// TestAdmHasDisklessVolumeTrue pins the post-detach case used by
// the Bug 280 fix: when the operator runs `drbdadm detach --force`
// against the satellite shell, the kernel transitions UpToDate →
// Diskless. The reconciler's runAdjust probes the kernel via
// HasDisklessVolume before dispatching adjust; a Diskless local
// volume must coerce the dispatch onto `adjust --skip-disk` so a
// reconcile-in-flight with a stale prop view doesn't re-attach
// the disk before the operator's SkipDisk-stamp has propagated.
func TestAdmHasDisklessVolumeTrue(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose pvc-detached", storage.FakeResponse{
		Stdout: []byte(`pvc-detached node-id:0 role:Primary suspended:no force-io-failures:no
  volume:0 minor:1002 disk:Diskless backing_dev:none quorum:yes blocked:no
      worker-2 node-id:1 connection:Connected role:Secondary congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:UpToDate resync-suspended:no
`),
	})

	adm := drbd.NewAdm(fx)

	diskless, err := adm.HasDisklessVolume(t.Context(), "pvc-detached")
	if err != nil {
		t.Fatalf("HasDisklessVolume: unexpected error %v", err)
	}

	if !diskless {
		t.Errorf("HasDisklessVolume(post-detach): got false, want true")
	}
}

// TestAdmHasDisklessVolumeFalseUpToDate pins the steady-state
// case: a healthy diskful replica reports disk:UpToDate, the probe
// returns false, and runAdjust dispatches plain `drbdadm adjust` as
// before. Guards against a regression where the probe over-trips
// on `peer-disk:Diskless` (a peer-side state we don't care about
// for our local skip-disk dispatch).
func TestAdmHasDisklessVolumeFalseUpToDate(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose pvc-healthy", storage.FakeResponse{
		Stdout: []byte(`pvc-healthy node-id:0 role:Primary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/loop6 quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`),
	})

	adm := drbd.NewAdm(fx)

	diskless, err := adm.HasDisklessVolume(t.Context(), "pvc-healthy")
	if err != nil {
		t.Fatalf("HasDisklessVolume: unexpected error %v", err)
	}

	if diskless {
		t.Errorf("HasDisklessVolume(UpToDate): got true, want false")
	}
}

// TestAdmHasDisklessVolumeFalseAbsentSlot pins the convergence-
// pending case: the kernel doesn't have a slot yet for the named
// resource (first activation, pre-`drbdadm up`). `drbdsetup status
// --verbose` returns non-zero — we treat it the same way IsLoaded
// does (false + nil error) so the caller doesn't have to branch on
// the kernel-absent signal.
func TestAdmHasDisklessVolumeFalseAbsentSlot(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose pvc-absent", storage.FakeResponse{
		Stdout: []byte("No currently configured DRBD found.\n"),
		Err:    errFakeFailure,
	})

	adm := drbd.NewAdm(fx)

	diskless, err := adm.HasDisklessVolume(t.Context(), "pvc-absent")
	if err != nil {
		t.Fatalf("HasDisklessVolume(absent): unexpected error %v", err)
	}

	if diskless {
		t.Errorf("HasDisklessVolume(absent slot): got true, want false")
	}
}

// TestAdmHasDisklessVolumeTrueDetaching pins the Bug 280 follow-up
// case: there is a sub-second window after `drbdadm detach --force`
// where the kernel reports `disk:Detaching` rather than
// `disk:Diskless`. A reconcile that probes the kernel during this
// window must still coerce the adjust onto `--skip-disk` — plain
// `drbdadm adjust` here would schedule attach_cmd via drbd-utils'
// compare_volume (kern->disk=="none" + conf->disk=<path>) and
// race the operator's detach, re-attaching the disk before any
// external poll can observe Diskless.
//
// Regression: the original Bug 280 fix only matched `disk:Diskless`
// and explicitly skipped `disk:Detaching` as "transient" — but
// transient is exactly when the race window is open, not a reason
// to ignore it. The e2e5 disk-replace-internal-metadata scenario
// caught this empirically: `drbdadm detach --force` would fire,
// then 15 s later the test's poll would still see `UpToDate`
// because a reconcile probed during the Detaching window, missed
// the signal, and re-attached.
func TestAdmHasDisklessVolumeTrueDetaching(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose pvc-detaching", storage.FakeResponse{
		Stdout: []byte(`pvc-detaching node-id:0 role:Primary suspended:no force-io-failures:no
  volume:0 minor:1003 disk:Detaching backing_dev:/dev/loop8 quorum:yes blocked:no
      worker-2 node-id:1 connection:Connected role:Secondary congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:UpToDate resync-suspended:no
`),
	})

	adm := drbd.NewAdm(fx)

	diskless, err := adm.HasDisklessVolume(t.Context(), "pvc-detaching")
	if err != nil {
		t.Fatalf("HasDisklessVolume: unexpected error %v", err)
	}

	if !diskless {
		t.Errorf("HasDisklessVolume(mid-Detaching): got false, want true (would re-attach via plain adjust during detach race window)")
	}
}

// TestAdmHasDisklessVolumeTrueFailed pins the Failed-disk arm:
// when the lower disk returned an I/O error the kernel transitions
// to `disk:Failed` before settling on `disk:Diskless`. The observer
// stamps `DrbdOptions/SkipDisk=True` on Failed→Diskless, but until
// that prop write propagates the kernel probe is the only safety
// net. Plain `drbdadm adjust` on a Failed lower disk would attempt
// a re-attach that DRBD refuses (or worse, succeeds against a
// corrupt-but-not-yet-Failed disk); coerce to `--skip-disk` until
// the operator clears the prop.
func TestAdmHasDisklessVolumeTrueFailed(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose pvc-failed", storage.FakeResponse{
		Stdout: []byte(`pvc-failed node-id:0 role:Primary suspended:no force-io-failures:no
  volume:0 minor:1004 disk:Failed backing_dev:/dev/loop9 quorum:yes blocked:no
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`),
	})

	adm := drbd.NewAdm(fx)

	diskless, err := adm.HasDisklessVolume(t.Context(), "pvc-failed")
	if err != nil {
		t.Fatalf("HasDisklessVolume: unexpected error %v", err)
	}

	if !diskless {
		t.Errorf("HasDisklessVolume(Failed): got false, want true (Failed lower disk must coerce skip-disk)")
	}
}

// TestAdmHasDisklessVolumeFalsePeerDiskless pins the per-peer
// distinction: when the local volume is UpToDate but a PEER reports
// peer-disk:Diskless (e.g., the operator detached the OTHER replica,
// or the peer is a diskless quorum-tiebreaker), HasDisklessVolume
// must NOT trip. Tripping here would falsely coerce the adjust on
// the healthy local replica onto `--skip-disk`, leaving the local
// disk's reconfig pinned even though the local kernel reports
// UpToDate. We only care about the local-side `disk:` token, not
// the `peer-disk:` token.
func TestAdmHasDisklessVolumeFalsePeerDiskless(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose pvc-peer-down", storage.FakeResponse{
		Stdout: []byte(`pvc-peer-down node-id:0 role:Primary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/loop6 quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:Diskless
`),
	})

	adm := drbd.NewAdm(fx)

	diskless, err := adm.HasDisklessVolume(t.Context(), "pvc-peer-down")
	if err != nil {
		t.Fatalf("HasDisklessVolume: unexpected error %v", err)
	}

	if diskless {
		t.Errorf("HasDisklessVolume(peer-disk:Diskless only): got true, want false")
	}
}

// TestAdmHasDisklessVolumeFalsePeerDetachingOrFailed pins the
// per-peer distinction for the Detaching / Failed arms added in
// the Bug 280 follow-up: when the local replica is UpToDate but
// a PEER reports peer-disk:Detaching (operator just detached the
// OTHER replica) or peer-disk:Failed (peer's lower disk died),
// HasDisklessVolume must NOT trip. The local disk is healthy and
// the next reconcile must run plain `drbdadm adjust` to converge
// peer state — coercing skip-disk here would freeze the local
// disk-level reconfig and trap the local replica's view of the
// peer's transition (e.g., demoting peer to TIE_BREAKER) behind
// the prop-clear, which never comes because nobody set the prop.
func TestAdmHasDisklessVolumeFalsePeerDetachingOrFailed(t *testing.T) {
	cases := []struct {
		name    string
		peerSt  string
		resName string
	}{
		{name: "peer-detaching", peerSt: "Detaching", resName: "pvc-peer-detaching"},
		{name: "peer-failed", peerSt: "Failed", resName: "pvc-peer-failed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status --verbose "+tc.resName, storage.FakeResponse{
				Stdout: []byte(`` + tc.resName + ` node-id:0 role:Primary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/loop6 quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:` + tc.peerSt + `
`),
			})

			adm := drbd.NewAdm(fx)

			diskless, err := adm.HasDisklessVolume(t.Context(), tc.resName)
			if err != nil {
				t.Fatalf("HasDisklessVolume: unexpected error %v", err)
			}

			if diskless {
				t.Errorf("HasDisklessVolume(peer-disk:%s only): got true, want false (local is UpToDate)", tc.peerSt)
			}
		})
	}
}

// TestAdmSuspendIOInvokesDrbdadm pins the Bug-351 freeze
// command: SuspendIO → `drbdadm suspend-io <res>`.
//
// Why drbdadm not drbdsetup: drbdsetup's `suspend-io` takes a
// kernel minor or `/dev/drbdN` path, not a resource name —
// passing a resource name yields `exit 20: Cannot determine
// minor device number of device '<res>'` on a real satellite.
// drbdadm resolves the resource name to its kernel minor via
// the local .res file and forwards to drbdsetup correctly.
func TestAdmSuspendIOInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.SuspendIO(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("SuspendIO: %v", err)
	}

	want := "drbdadm suspend-io pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}

	// Regression guard: SuspendIO MUST NOT shell out to drbdsetup
	// directly with the resource name — that's the bug we just
	// fixed (drbdsetup needs a minor, not a resource name).
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdsetup suspend-io ") {
			t.Errorf("SuspendIO shelled out to drbdsetup directly (Bug 351 regression — exit 20 on resource name): %s",
				line)
		}
	}
}

// TestAdmResumeIOInvokesDrbdadm pins ResumeIO →
// `drbdadm resume-io <res>`. Same drbdadm-resolves-minor rationale
// as SuspendIO; the orchestration's Phase-3 resume MUST fire on
// every targeted node even on the abort path or application I/O
// hangs forever on the still-frozen siblings.
func TestAdmResumeIOInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.ResumeIO(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("ResumeIO: %v", err)
	}

	want := "drbdadm resume-io pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdsetup resume-io ") {
			t.Errorf("ResumeIO shelled out to drbdsetup directly: %s", line)
		}
	}
}

// errCreateMdConfiguredCollisionReal is the verbatim drbdadm
// create-md error string captured from the e2e2 stand:
// `two-volume-rd.sh` failing because `cli-matrix-multi-life-b`
// zombie kernel slot owned minor 20001 (held open by an lvs
// process stuck in D-state inside __drbd_make_request).
var errCreateMdConfiguredCollisionReal = errors.New("drbdadm create-md: drbdadm create-md --force " +
	"--max-peers=15 e2e-twovol: You want me to create a v09 style " +
	"flexible-size internal meta data block.\nThere appears to be a " +
	"v09 flexible-size internal meta data block already in place on " +
	"/dev/loop8 at byte offset 67104768\nDo you really want to overwrite " +
	"the existing meta-data?\n*** confirmation forced via --force " +
	"option ***\ninitializing bitmap (32 KB) to all zero\n# Output " +
	"might be stale, since minor 20001 is attached\nDevice '20001' is " +
	"configured!\nCommand 'drbdmeta 20001 v09 /dev/loop9 internal " +
	"create-md 15 --force' terminated with exit code 20: exit status 20")

var (
	errCreateMdUnrelated     = errors.New("drbdadm adjust: unknown resource pvc-1")
	errCreateMdPartialMarker = errors.New("device '20001' is something-else")
	errCreateMdNonNumeric    = errors.New("device 'foo' is configured")
)

// TestParseConfiguredDeviceMinorMatchesRealErrorShape pins the
// drbdmeta create-md collision error shape: when the kernel already
// owns the target minor for a different resource, drbdmeta surfaces
// `Device '<N>' is configured!` (typically immediately before
// `Command 'drbdmeta <N> ... terminated with exit code 20`). The
// reconciler's create-md recovery path keys off this exact substring
// to identify the offending minor and free it via drbdsetup down.
//
// Without this guard a regression that changes the parser (e.g.
// case-folding, regex tweak) would silently make the create-md path
// stop self-healing collisions and revert to the manual-reboot
// recovery this fix replaces.
func TestParseConfiguredDeviceMinorMatchesRealErrorShape(t *testing.T) {
	minor, ok := drbd.ParseConfiguredDeviceMinor(errCreateMdConfiguredCollisionReal)
	if !ok {
		t.Fatalf("ParseConfiguredDeviceMinor: ok=false on real error shape")
	}

	if minor != 20001 {
		t.Errorf("ParseConfiguredDeviceMinor: got %d, want 20001", minor)
	}
}

// TestParseConfiguredDeviceMinorRejectsNonMatch pins the dominant
// negative case: every OTHER drbdadm error (unknown resource, .res
// parse error, transient netlink hiccup) must NOT trigger the
// collision-recovery path. Returning a false-positive minor here
// would make the reconciler issue `drbdsetup down` against an
// unrelated kernel slot and break healthy resources.
func TestParseConfiguredDeviceMinorRejectsNonMatch(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"unrelated", errCreateMdUnrelated},
		{"partial-marker", errCreateMdPartialMarker},
		{"non-numeric", errCreateMdNonNumeric},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := drbd.ParseConfiguredDeviceMinor(tc.err)
			if ok {
				t.Errorf("ParseConfiguredDeviceMinor: ok=true on %q", tc.err)
			}
		})
	}
}

// TestAdmResourceOwningMinorFindsZombie pins the inverse-lookup the
// create-md collision-recovery path needs: given a minor that the
// kernel already owns, name the resource holding it. The parser
// must:
//   - track the most recent column-0 resource line,
//   - match the indented `minor:<N>` token exactly (space-bounded so
//     `minor:200` doesn't accidentally fire on `minor:20001`),
//   - return the paired resource name.
func TestAdmResourceOwningMinorFindsZombie(t *testing.T) {
	fx := storage.NewFakeExec()
	// Real-shape drbdsetup status --verbose output from the e2e2
	// stand: `cli-matrix-multi-life-b` is the zombie owning minor
	// 20001 that blocks new RDs reusing that slot. Multi-resource
	// listing ensures the parser correctly tracks the current
	// resource-block boundary across multiple column-0 lines.
	fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte(
		`cli-matrix-multi-life-b node-id:0 role:Secondary suspended:user
    force-io-failures:no
  volume:0 minor:20001 disk:UpToDate backing_dev:/dev/loop6 quorum:yes
      blocked:upper

pvc-healthy node-id:0 role:Secondary suspended:no
  volume:0 minor:20000 disk:UpToDate backing_dev:/dev/loop5 quorum:yes
`)})

	adm := drbd.NewAdm(fx)

	owner, err := adm.ResourceOwningMinor(t.Context(), 20001)
	if err != nil {
		t.Fatalf("ResourceOwningMinor: %v", err)
	}

	if owner != "cli-matrix-multi-life-b" {
		t.Errorf("ResourceOwningMinor(20001): got %q, want %q", owner, "cli-matrix-multi-life-b")
	}
}

// TestAdmResourceOwningMinorRejectsPrefixOverlap pins the
// space-bounded match: a kernel slot with `minor:200` must NOT be
// returned when the caller asks about minor 2 (or any other prefix
// overlap). Without strict bounding the recovery path would
// `drbdsetup down` an unrelated resource on every fresh allocation.
func TestAdmResourceOwningMinorRejectsPrefixOverlap(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte(
		`pvc-aaa node-id:0 role:Secondary
  volume:0 minor:20001 disk:UpToDate backing_dev:/dev/loop5
`)})

	adm := drbd.NewAdm(fx)

	owner, err := adm.ResourceOwningMinor(t.Context(), 2)
	if err != nil {
		t.Fatalf("ResourceOwningMinor: %v", err)
	}

	if owner != "" {
		t.Errorf("ResourceOwningMinor(2) on minor:20001 listing: got %q, want \"\"", owner)
	}
}

// TestAdmResourceOwningMinorEmptyKernel pins the no-resources path:
// drbdsetup exits non-zero with `No currently configured DRBD found.`
// when the kernel has nothing loaded. The lookup must return
// "" + nil (not an error) so the recovery path treats the empty
// kernel as "no owner, fall through to the original create-md
// error".
func TestAdmResourceOwningMinorEmptyKernel(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose", storage.FakeResponse{
		Stdout: []byte("No currently configured DRBD found.\n"),
		Err:    errFakeFailure,
	})

	adm := drbd.NewAdm(fx)

	owner, err := adm.ResourceOwningMinor(t.Context(), 20001)
	if err != nil {
		t.Fatalf("ResourceOwningMinor empty: unexpected error %v", err)
	}

	if owner != "" {
		t.Errorf("ResourceOwningMinor empty: got %q, want \"\"", owner)
	}
}

// TestAdmKernelMyNodeIDParsesSelfNodeID (Bug 360): KernelMyNodeID
// parses the resource object's top-level `node-id` from
// `drbdsetup status <res> --json` — the kernel's OWN my-node-id
// burned into the slot at `drbdadm up` time. This is the field the
// self-heal compares against the controller-allocated id to decide
// whether the slot must be torn down and re-`up`d.
func TestAdmKernelMyNodeIDParsesSelfNodeID(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte(`[{"name":"pvc-1","node-id":2,"role":"Secondary","suspended":false,"devices":[],"connections":[]}]`),
	})

	adm := drbd.NewAdm(fx)

	id, ok := adm.KernelMyNodeID(t.Context(), "pvc-1")
	if !ok {
		t.Fatalf("KernelMyNodeID: ok=false, want true")
	}

	if id != 2 {
		t.Errorf("KernelMyNodeID: got %d, want 2", id)
	}
}

// TestAdmKernelMyNodeIDAbsentSlot (Bug 360): an empty JSON array
// (kernel has no slot for the resource) yields ok=false so the
// self-heal declines to act — the first `drbdadm up` will load the
// correct id directly, nothing to recreate.
func TestAdmKernelMyNodeIDAbsentSlot(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte("[]\n"),
	})

	adm := drbd.NewAdm(fx)

	if _, ok := adm.KernelMyNodeID(t.Context(), "pvc-1"); ok {
		t.Errorf("KernelMyNodeID absent: ok=true, want false")
	}
}

// TestAdmKernelMyNodeIDStatusError (Bug 360): a non-zero drbdsetup
// exit (no configured DRBD / netlink hiccup) yields ok=false rather
// than acting on a guess.
func TestAdmKernelMyNodeIDStatusError(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte("No currently configured DRBD found.\n"),
		Err:    errFakeFailure,
	})

	adm := drbd.NewAdm(fx)

	if _, ok := adm.KernelMyNodeID(t.Context(), "pvc-1"); ok {
		t.Errorf("KernelMyNodeID error: ok=true, want false")
	}
}

// TestAdmEvaluateDownVetoResyncOnSyncTarget (Bug 350): a peer-device
// in SyncTarget means the local replica is actively pulling resync
// data — EvaluateDownVeto must return DownVetoResync so applyInactive
// refuses to `drbdadm down` and abort the resync.
func TestAdmEvaluateDownVetoResyncOnSyncTarget(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte(`[{"name":"pvc-1","connections":[
			{"name":"worker-1","connection-state":"Connected","peer_devices":[
				{"volume":0,"replication-state":"SyncTarget","peer-disk-state":"UpToDate"}]}]}]`),
	})

	if got := drbd.NewAdm(fx).EvaluateDownVeto(t.Context(), "pvc-1"); got != drbd.DownVetoResync {
		t.Errorf("EvaluateDownVeto SyncTarget: got %d, want DownVetoResync", got)
	}
}

// TestAdmEvaluateDownVetoResyncOnSyncSource (Bug 350): SyncSource
// means the local replica is the resync feeder — downing it strands
// the SyncTarget peer Inconsistent. Must veto.
func TestAdmEvaluateDownVetoResyncOnSyncSource(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte(`[{"name":"pvc-1","connections":[
			{"name":"worker-2","connection-state":"Connected","peer_devices":[
				{"volume":0,"replication-state":"SyncSource","peer-disk-state":"Inconsistent"}]}]}]`),
	})

	if got := drbd.NewAdm(fx).EvaluateDownVeto(t.Context(), "pvc-1"); got != drbd.DownVetoResync {
		t.Errorf("EvaluateDownVeto SyncSource: got %d, want DownVetoResync", got)
	}
}

// TestAdmEvaluateDownVetoAllowedOnEstablished (Bug 350): a steady-
// state Established connection carries no in-flight resync — the down
// must be allowed to proceed (a genuine deactivate).
func TestAdmEvaluateDownVetoAllowedOnEstablished(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte(`[{"name":"pvc-1","connections":[
			{"name":"worker-1","connection-state":"Connected","peer_devices":[
				{"volume":0,"replication-state":"Established","peer-disk-state":"UpToDate"}]}]}]`),
	})

	if got := drbd.NewAdm(fx).EvaluateDownVeto(t.Context(), "pvc-1"); got != drbd.DownAllowed {
		t.Errorf("EvaluateDownVeto Established: got %d, want DownAllowed", got)
	}
}

// TestAdmEvaluateDownVetoAllowedOnNotLoaded (Bug 350): a non-zero
// drbdsetup exit carrying the verbatim "No such resource" not-loaded
// message is CONCLUSIVE absence — the slot is gone, down is a no-op,
// so we must allow it. This is the anti-infinite-defer guard: once a
// slot has actually been downed, the probe stays conclusive and the
// caller never defers forever.
func TestAdmEvaluateDownVetoAllowedOnNotLoaded(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte("No such resource: pvc-1\n"),
		Err:    errFakeFailure,
	})

	if got := drbd.NewAdm(fx).EvaluateDownVeto(t.Context(), "pvc-1"); got != drbd.DownAllowed {
		t.Errorf("EvaluateDownVeto not-loaded: got %d, want DownAllowed", got)
	}
}

// TestAdmEvaluateDownVetoInconclusiveOnProbeError (Bug 350 cold-start):
// a non-zero drbdsetup exit that is NOT a not-loaded message (timeout,
// netlink hiccup under cold-satellite timing) is INCONCLUSIVE — we
// cannot confirm the slot is quiescent, so EvaluateDownVeto must fail
// CLOSED with DownVetoInconclusive and the caller defers the down.
func TestAdmEvaluateDownVetoInconclusiveOnProbeError(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte(""),
		Err:    errFakeFailure,
	})

	if got := drbd.NewAdm(fx).EvaluateDownVeto(t.Context(), "pvc-1"); got != drbd.DownVetoInconclusive {
		t.Errorf("EvaluateDownVeto probe error: got %d, want DownVetoInconclusive (fail-closed)", got)
	}
}

// TestAdmEvaluateDownVetoInconclusiveOnEmptyJSON (Bug 350 cold-start):
// exit 0 yet an empty/truncated JSON array means drbdsetup produced
// partial output for a slot that almost certainly still exists (a
// truly-absent slot exits non-zero). Fail CLOSED.
func TestAdmEvaluateDownVetoInconclusiveOnEmptyJSON(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte("[]"),
	})

	if got := drbd.NewAdm(fx).EvaluateDownVeto(t.Context(), "pvc-1"); got != drbd.DownVetoInconclusive {
		t.Errorf("EvaluateDownVeto empty JSON: got %d, want DownVetoInconclusive (fail-closed)", got)
	}
}

// TestAdmEvaluateDownVetoInconclusiveOnMalformedJSON (Bug 350 cold-
// start): exit 0 with unparseable JSON (truncated mid-stream) is the
// same ambiguous-but-likely-present case. Fail CLOSED.
func TestAdmEvaluateDownVetoInconclusiveOnMalformedJSON(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-1 --json", storage.FakeResponse{
		Stdout: []byte(`[{"name":"pvc-1","connec`),
	})

	if got := drbd.NewAdm(fx).EvaluateDownVeto(t.Context(), "pvc-1"); got != drbd.DownVetoInconclusive {
		t.Errorf("EvaluateDownVeto malformed JSON: got %d, want DownVetoInconclusive (fail-closed)", got)
	}
}

// errHasMDDumpMdEBUSY mirrors the verbatim shape drbdmeta returns
// when the lower disk is attached to the kernel — exclusive open
// fails with "Device or resource busy" and drbdmeta exits 20.
var errHasMDDumpMdEBUSY = errors.New("open(/dev/loop5) failed: Device or resource busy\nExclusive open failed.\nOperation canceled.\nCommand 'drbdmeta 20000 v09 /dev/loop5 internal dump-md' terminated with exit code 20: exit status 20")

// errHasMDDumpMdConfigured mirrors the alternate attached-device
// surface ("Device 'X' is configured!") drbdmeta emits when the
// kernel already owns the minor. The capitalised leading word is
// intentional — verbatim drbdmeta wire shape, the HasMD parser
// matches the substring at any offset so casing must be preserved.
//
//nolint:staticcheck // verbatim drbdmeta error wire shape; capitalisation is load-bearing
var errHasMDDumpMdConfigured = errors.New("Device '20000' is configured!\nCommand 'drbdmeta 20000 v09 /dev/loop5 internal dump-md' terminated with exit code 20")

// errHasMDDumpMdNoMeta mirrors the verbatim "no metadata" exit
// drbdadm returns when the lower disk has no DRBD-9 superblock.
var errHasMDDumpMdNoMeta = errors.New("drbdadm: No valid meta data found")

// TestHasMDReturnsTrueOnAttachedDevice pins the Bug B.4 (P0)
// carve-out: when `drbdadm dump-md <rd>/<vol>` errors because the
// lower disk is exclusive-held by the kernel (volume already
// attached), HasMD MUST return true. Skipping that case routes
// the caller into create-md against an attached minor and
// EBUSY-loops the reconciler at ~10 Hz, suspending the whole
// resource on quorum. The kernel could not have brought the
// volume up unless metadata was already present, so attached =
// metadata-exists by definition.
func TestHasMDReturnsTrueOnAttachedDevice(t *testing.T) {
	// Two error shapes drbdmeta surfaces when the lower disk is
	// attached: pre-create-md probe (`dump-md` exits 20 with
	// "Device or resource busy" + "Operation canceled") and the
	// "Device 'X' is configured!" stale-output marker. Both mean
	// the kernel owns the device, so metadata is present.
	cases := []struct {
		name string
		out  string
		err  error
	}{
		{
			name: "EBUSY error",
			out:  "Operation canceled.\n",
			err:  errHasMDDumpMdEBUSY,
		},
		{
			name: "Device configured",
			out:  "# Output might be stale, since minor 20000 is attached\n",
			err:  errHasMDDumpMdConfigured,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			fx.Expect("drbdadm dump-md pvc-b4/0",
				storage.FakeResponse{Stdout: []byte(tc.out), Err: tc.err})

			has, hasErr := drbd.NewAdm(fx).HasMD(t.Context(), "pvc-b4/0")
			if hasErr != nil {
				t.Fatalf("HasMD: %v", hasErr)
			}

			if !has {
				t.Errorf("HasMD on attached device: got false, want true "+
					"(attached lower disk implies metadata exists; Bug B.4 EBUSY-loop surface). out=%q err=%v",
					tc.out, tc.err)
			}
		})
	}
}

// TestHasMDReturnsFalseOnNoMetaData pins the existing
// "metadata-absent" signal: `dump-md` exits non-zero with
// "No valid meta data found" and HasMD returns false so the
// caller's create-md fires on the truly-missing case.
func TestHasMDReturnsFalseOnNoMetaData(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm dump-md pvc-fresh/0",
		storage.FakeResponse{Err: errHasMDDumpMdNoMeta})

	has, err := drbd.NewAdm(fx).HasMD(t.Context(), "pvc-fresh/0")
	if err != nil {
		t.Fatalf("HasMD: %v", err)
	}

	if has {
		t.Errorf("HasMD on missing metadata: got true, want false")
	}
}

// TestHasMDToleratesAbsentWordings: the strict direction fails closed,
// so a wording this list does not know stalls every new replica on
// every node rather than one volume. Casing and the hyphenated spelling
// of the noun are the variations drbd-utils has actually shipped, so
// pin that they all still read as "absent".
func TestHasMDToleratesAbsentWordings(t *testing.T) {
	for _, wording := range errHasMDAbsentWordings {
		fx := storage.NewFakeExec()
		fx.Expect("drbdadm dump-md pvc-fresh/0",
			storage.FakeResponse{Err: wording})

		has, err := drbd.NewAdm(fx).HasMD(t.Context(), "pvc-fresh/0")
		if err != nil {
			t.Errorf("%v: HasMD returned an error instead of reading it as absent: %v", wording, err)

			continue
		}

		if has {
			t.Errorf("%v: HasMD = true, want false", wording)
		}
	}
}

// The "no metadata" spellings drbd-utils has shipped, verbatim. Named
// individually so each is a static sentinel, matching how the other
// wire shapes in this file are written.
var (
	errHasMDAbsentTitleCase = errors.New("drbdadm: No valid meta data found")
	errHasMDAbsentLowerCase = errors.New("drbdadm: no valid meta data found")
	errHasMDAbsentHyphen    = errors.New("drbdadm: No valid meta-data found")
	//nolint:staticcheck // verbatim drbdmeta wire shape; the capital is load-bearing
	errHasMDAbsentShouted = errors.New(
		"Command 'drbdmeta 0 v09 /dev/loop5 internal dump-md' failed: NO VALID META DATA")
)

//nolint:gochecknoglobals // verbatim wire shapes, matched in one test
var errHasMDAbsentWordings = []error{
	errHasMDAbsentTitleCase,
	errHasMDAbsentLowerCase,
	errHasMDAbsentHyphen,
	errHasMDAbsentShouted,
}

// errHasMDDumpMdUnclean mirrors the verbatim exit drbdadm returns for a
// volume whose activity log was never closed. Captured from an e2e run:
// every lower disk materialised from a ZFS snapshot, clone or send/recv
// arrives in this state, because the snapshot caught the source's AL
// mid-flight.
var errHasMDDumpMdUnclean = errors.New(
	`drbdadm dump-md ship-restored/0: Found meta data is "unclean", ` +
		`please apply-al first: exit status 1`)

// TestHasMDReturnsFalseOnUncleanActivityLog: a superblock that cannot be
// read until the activity log is applied must not surface as an
// unrecognised failure. Every volume built from a ZFS snapshot carries
// one, so failing closed here strands the clone, ship and restore paths
// outright — the resource never attaches and the reconciler toggles the
// disk until the scenario times out. Re-initialising is what the
// destination of a clone wants: it is a new resource and needs metadata
// of its own.
func TestHasMDReturnsFalseOnUncleanActivityLog(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm dump-md ship-restored/0",
		storage.FakeResponse{Err: errHasMDDumpMdUnclean})

	has, err := drbd.NewAdm(fx).HasMD(t.Context(), "ship-restored/0")
	if err != nil {
		t.Fatalf("HasMD on an unclean activity log: %v", err)
	}

	if has {
		t.Errorf("HasMD on an unclean activity log: got true, want false")
	}
}

// errHasMDDumpMdParseError mirrors drbdadm refusing to read a resource
// whose .res file does not parse. The probe never reaches the disk, so
// it cannot know whether metadata is there.
var errHasMDDumpMdParseError = errors.New(
	"drbdadm dump-md pvc-live/0: drbd.d/pvc-live.res:4: " +
		"Parse error: ';' expected, but got 'k6fjase' (TK 281): exit status 10")

// TestHasMDFailsClosedOnUnparseableConfig: a config-level failure must
// surface as an error, never as "no metadata". The caller's next move on
// a false negative is `create-md --force`, which destroys the metadata of
// a volume that is full of data — so this probe has to fail closed.
func TestHasMDFailsClosedOnUnparseableConfig(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm dump-md pvc-live/0",
		storage.FakeResponse{Err: errHasMDDumpMdParseError})

	has, err := drbd.NewAdm(fx).HasMD(t.Context(), "pvc-live/0")
	if err == nil {
		t.Fatal("HasMD on unparseable config: want error, got nil")
	}

	if has {
		t.Errorf("HasMD on unparseable config: got true, want false")
	}
}

// errHasMDDumpMdKilled mirrors dump-md dying for a reason that has
// nothing to do with the disk — OOM-killed under memory pressure here.
var errHasMDDumpMdKilled = errors.New("drbdadm dump-md pvc-live/0: signal: killed")

// TestHasMDFailsClosedOnUnrecognisedFailure: the probe guards
// `create-md --force`, so it may answer "no metadata" only for the exits
// that positively say so. Every other failure has to surface.
//
// Treating them all as "absent" is worse than the unparseable-config
// case that first exposed this: nothing downstream trips over an
// OOM-kill or a held drbdmeta lock a moment later, so create-md --force
// goes on to succeed and wipes the GI tuple and dirty bitmap of a
// healthy replica.
func TestHasMDFailsClosedOnUnrecognisedFailure(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm dump-md pvc-live/0",
		storage.FakeResponse{Err: errHasMDDumpMdKilled})

	has, err := drbd.NewAdm(fx).HasMD(t.Context(), "pvc-live/0")
	if err == nil {
		t.Fatal("HasMD on an unrecognised failure: want error, got nil")
	}

	if has {
		t.Errorf("HasMD on an unrecognised failure: got true, want false")
	}
}
