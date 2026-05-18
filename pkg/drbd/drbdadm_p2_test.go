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

// Phase 11.4.b P2: operator-recovery wrappers — argv-shape pin tests.
//
// These wrappers exist to expose `drbdadm verify / invalidate /
// new-current-uuid / pause-sync / resume-sync / outdate / apply-al`
// and the `drbdmeta ... internal wipe-md / show-gi / get-gi`
// counterparts as Go-callable helpers. They have no business-logic
// consumer today (operator-recovery surface, not satellite-driven),
// so each test pins ONLY the exact CLI invocation shape — a
// regression in argv ordering / flag presence is what the operator
// would actually notice when manually triaging a broken replica.

import (
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestAdmVerifyInvokesDrbdadm: Verify → `drbdadm verify <res>`.
// Schedules an online data scan; out-of-sync blocks surface in
// subsequent events2 frames. No flags — drbdadm picks the peer.
func TestAdmVerifyInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Verify(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	want := "drbdadm verify pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmInvalidateInvokesDrbdadm: Invalidate → `drbdadm
// invalidate <res>`. Forces a full resync from a peer. No
// `--force` flag in this batch — `drbdadm invalidate` is the
// safe form (requires at least one UpToDate peer; refuses
// otherwise).
func TestAdmInvalidateInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Invalidate(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	want := "drbdadm invalidate pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmNewCurrentUUIDInvokesDrbdadm: NewCurrentUUID →
// `drbdadm new-current-uuid <res>`. Split-brain recovery step
// (UG9 §7.4.1).
func TestAdmNewCurrentUUIDInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.NewCurrentUUID(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("NewCurrentUUID: %v", err)
	}

	want := "drbdadm new-current-uuid pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmPauseSyncInvokesDrbdadm: PauseSync → `drbdadm
// pause-sync <res>`. Operator throttle.
func TestAdmPauseSyncInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.PauseSync(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("PauseSync: %v", err)
	}

	want := "drbdadm pause-sync pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmResumeSyncInvokesDrbdadm: ResumeSync → `drbdadm
// resume-sync <res>`. Counterpart to PauseSync.
func TestAdmResumeSyncInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.ResumeSync(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("ResumeSync: %v", err)
	}

	want := "drbdadm resume-sync pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmOutdateInvokesDrbdadm: Outdate → `drbdadm outdate <res>`.
// Explicit Outdated mark for fencing patterns (UG9 §7.6).
func TestAdmOutdateInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.Outdate(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Outdate: %v", err)
	}

	want := "drbdadm outdate pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmApplyALInvokesDrbdadm: ApplyAL → `drbdadm apply-al <res>`.
// Replays the on-disk activity log onto the lower disk — required
// before promote-after-crash when the kernel surfaces
// ERR_NEED_APPLY_AL (drbdsetup exit 167).
func TestAdmApplyALInvokesDrbdadm(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.ApplyAL(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("ApplyAL: %v", err)
	}

	want := "drbdadm apply-al pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestAdmWipeMdInvokesDrbdmeta pins the deliberate metadata wipe
// shape: `drbdmeta --force <res>/<vol> v09 <device> internal
// wipe-md`. Three invariants pinned:
//
//   - `<res>/<vol>` per-volume target (NOT just `<res>`); the v09
//     metadata block is per-volume, addressing the resource alone
//     would refuse.
//   - `v09` literal (the metadata version drbd-9 uses).
//   - `--force` present so drbdmeta accepts the in-place mutation.
//
// Counterpart to CreateMD; the safety-rail variant for recycling
// a lower disk that previously held DRBD metadata.
func TestAdmWipeMdInvokesDrbdmeta(t *testing.T) {
	fx := storage.NewFakeExec()
	adm := drbd.NewAdm(fx)

	if err := adm.WipeMd(t.Context(), "pvc-1", 0, "/dev/dm-3"); err != nil {
		t.Fatalf("WipeMd: %v", err)
	}

	want := "drbdmeta --force pvc-1/0 v09 /dev/dm-3 internal wipe-md"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestAdmShowGiInvokesDrbdmeta pins the raw GI dump shape:
// `drbdmeta --force <res>/<vol> v09 <device> internal show-gi`.
// The verbose human-readable variant; counterpart to GetGi (which
// emits the terser machine-parseable tuple).
func TestAdmShowGiInvokesDrbdmeta(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdmeta --force pvc-1/0 v09 /dev/dm-3 internal show-gi", storage.FakeResponse{
		Stdout: []byte(`+--<  Current data generation UUID  >-
| 78A0DDDABCDEF000
`),
	})

	adm := drbd.NewAdm(fx)

	out, err := adm.ShowGi(t.Context(), "pvc-1", 0, "/dev/dm-3")
	if err != nil {
		t.Fatalf("ShowGi: %v", err)
	}

	want := "drbdmeta --force pvc-1/0 v09 /dev/dm-3 internal show-gi"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}

	if len(out) == 0 {
		t.Errorf("ShowGi: expected stdout to be returned, got empty")
	}
}

// TestAdmGetGiInvokesDrbdmeta pins the parsed-tuple shape:
// `drbdmeta --force <res>/<vol> v09 <device> internal get-gi`.
// Output is `<current>:<bitmap>:<history0>:<history1>` matching
// the format SetGi accepts — so the operator can read with GetGi,
// pick a survivor, write with SetGi on each peer.
func TestAdmGetGiInvokesDrbdmeta(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdmeta --force pvc-1/0 v09 /dev/dm-3 internal get-gi", storage.FakeResponse{
		Stdout: []byte("78A0DDDABCDEF000:78A0DDDABCDEF000:0:0\n"),
	})

	adm := drbd.NewAdm(fx)

	tuple, err := adm.GetGi(t.Context(), "pvc-1", 0, "/dev/dm-3")
	if err != nil {
		t.Fatalf("GetGi: %v", err)
	}

	want := "drbdmeta --force pvc-1/0 v09 /dev/dm-3 internal get-gi"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}

	// Pin the trim-trailing-newline contract: callers comparing
	// tuples across replicas should not have to .TrimSpace() the
	// result themselves.
	if tuple != "78A0DDDABCDEF000:78A0DDDABCDEF000:0:0" {
		t.Errorf("GetGi: got %q, want trimmed tuple", tuple)
	}
}
