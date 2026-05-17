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

// TestAdmSetGiInvokesDrbdmeta pins the initial-sync skip seeding
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
func TestAdmSetGiInvokesDrbdmeta(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	err := adm.SetGi(t.Context(), "pvc-1", 0, "/dev/dm-3", 1, "78A0DDDABCDEF000")
	if err != nil {
		t.Fatalf("SetGi: %v", err)
	}

	want := "drbdmeta --force pvc-1/0 v09 /dev/dm-3 internal set-gi --node-id 1 78A0DDDABCDEF000:78A0DDDABCDEF000:0:0"
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
