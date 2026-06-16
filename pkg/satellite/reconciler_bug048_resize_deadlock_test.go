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

package satellite_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// TestBUG048SteadyStateDiskfulReconcileIssuesNoMetadataProbeOrSelfHeal is the
// regression pin for the BUG-048 resize deadlock (P0, availability — a
// reboot-proof cluster-wide DRBD deadlock that PR #164 shipped to main with a
// fully-green CI).
//
// ROOT CAUSE: PR #164 widened the ensurePerVolumeMetadata trigger gate from
// the .res-count hasLateAddedVolume() to the unconditional
// dr.GetMetadataCreated(), so EVERY post-activation diskful reconcile ran a
// per-volume `drbdadm dump-md` (an md_buffer consumer) plus the cluster-wide
// late-add self-heals. During a `vd s` resize DRBD holds md_buffer for the
// whole cluster-wide size change (change_cluster_wide_device_size /
// drbd_determine_dev_size); a per-reconcile dump-md + the self-heals' cluster-
// wide state-change actions then perpetually lose the cluster-wide state-
// change arbitration, the resize never completes, md_buffer is never
// released, and the resource deadlocks cluster-wide, reboot-proof.
//
// THE FIX: the per-volume metadata pass fires ONLY when some DESIRED volume
// is NOT present-and-attached in the kernel (an attached volume already has
// metadata — the kernel cannot attach a lower disk without it — so dump-md on
// it is pointless and is the md_buffer consumer that contends with the
// resize). The late-add self-heals are also gated out of a resize pass.
//
// WHAT THIS TEST OBSERVES: on a converged steady-state diskful reconcile where
// EVERY desired volume is present-and-attached (UpToDate), the reconciler must
// issue NO `drbdadm dump-md` and NO cluster-wide self-heal command
// (disconnect / new-current-uuid --clear-bitmap / invalidate). The mock
// records every issued command.
//
// FAIL-ON-BUG PROOF: under the pre-fix `dr.GetMetadataCreated()` gate this
// reconcile fired ensurePerVolumeMetadata unconditionally, so `drbdadm
// dump-md pvc-bug048-steady/0` (and /1) WAS issued — this test's
// "no dump-md" assertion FAILS on pre-fix and PASSES on the fix. See the
// PROOF note in the PR / task report.
func TestBUG048SteadyStateDiskfulReconcileIssuesNoMetadataProbeOrSelfHeal(t *testing.T) {
	const rd = "pvc-bug048-steady"

	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Both volumes already provisioned at the desired size (no grow → no
	// resize): the converged steady-state shape `vd s` settles into and that
	// the reconcile loop revisits on every Status/event tick.
	for _, vn := range []string{"00000", "00001"} {
		fx.Expect(fmt.Sprintf("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/%s_%s", rd, vn),
			storage.FakeResponse{Stdout: []byte(rd + "_" + vn + "\n")})
		fx.Expect(fmt.Sprintf("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/%s_%s", rd, vn),
			storage.FakeResponse{Stdout: []byte(fmt.Sprintf("/dev/vg/%s_%s|1048576\n", rd, vn))})
	}

	// KERNEL TRUTH: both desired volumes are present-and-attached UpToDate —
	// the converged state. The new AttachedVolumes probe reads THIS, sees
	// {0,1} all attached, and the gate finds no unattached desired volume.
	fx.Expect("drbdsetup status "+rd+" --json", storage.FakeResponse{Stdout: []byte(`[{
	  "name":"` + rd + `","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"minor":1000,"disk-state":"UpToDate"},
	    {"volume":1,"minor":1001,"disk-state":"UpToDate"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":1,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// Converged on-disk state: .res + .md-created already stamped from the
	// first activation, both volumes in the .res. MetadataCreated=true.
	seedConvergedTwoVolumeRD(t, dir, rd)

	dr := []*intent.DesiredResource{convergedTwoVolumeDR(rd)}

	results, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("Apply: expected Ok=true; got results=%+v", results)
	}

	assertNoMetadataProbe(t, fx.CommandLines(), rd)
	assertNoLateAddSelfHeal(t, fx.CommandLines(), rd)
}

// TestBUG048ResizeInFlightIssuesNoMetadataProbeOrSelfHeal pins the deadlock-
// shape directly: a pickup-time `vd s` resize (the storage layer grew, so the
// reconcile issues `drbdadm resize`) MUST NOT also fire the per-volume
// dump-md probe nor any cluster-wide late-add self-heal in the same pass —
// those are exactly the md_buffer / cluster-wide-state-change operations that
// contend with DRBD's in-flight change_cluster_wide_device_size and produce
// the reboot-proof deadlock.
//
// The kernel status here shows the grown volume transiently dipped to
// Inconsistent while a sibling stays UpToDate — the worst case, because that
// is precisely the shape NeedsLateAddPromote would misfire on (UpToDate
// sibling + Inconsistent volume + no peer data). The fix's resize guard keeps
// the self-heals out regardless, and the attached-volume gate keeps dump-md
// out (both volumes are attached).
//
// FAIL-ON-BUG PROOF: under the pre-fix gate the resize pass ALSO ran
// ensurePerVolumeMetadata → `drbdadm dump-md` (md_buffer contention), and the
// ungated self-heals would mint a source mid-resize. This test's assertions
// FAIL on pre-fix and PASS on the fix.
func TestBUG048ResizeInFlightIssuesNoMetadataProbeOrSelfHeal(t *testing.T) {
	const rd = "pvc-bug048-resize"

	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Both volumes exist but on-disk at 1 GiB while the spec asks 2 GiB →
	// applyStorageIfDiskful grows them and pins resized=true → the reconcile
	// issues `drbdadm resize` (the in-flight resize the deadlock needs).
	for _, vn := range []string{"00000", "00001"} {
		fx.Expect(fmt.Sprintf("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/%s_%s", rd, vn),
			storage.FakeResponse{Stdout: []byte(rd + "_" + vn + "\n")})
		fx.Expect(fmt.Sprintf("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/%s_%s", rd, vn),
			storage.FakeResponse{Stdout: []byte(fmt.Sprintf("/dev/vg/%s_%s|1048576\n", rd, vn))})
	}

	// KERNEL TRUTH mid-resize: vol-0 UpToDate, vol-1 Inconsistent (grown
	// region resyncing / transiently dipped). Both are ATTACHED (Inconsistent
	// is an attached, metadata-bearing state).
	fx.Expect("drbdsetup status "+rd+" --json", storage.FakeResponse{Stdout: []byte(`[{
	  "name":"` + rd + `","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"minor":1000,"disk-state":"UpToDate"},
	    {"volume":1,"minor":1001,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":1,"peer-disk-state":"Inconsistent","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	seedConvergedTwoVolumeRD(t, dir, rd)

	// Desired 2 GiB per volume → a grow → resized=true.
	dr := convergedTwoVolumeDR(rd)
	for _, v := range dr.Volumes {
		v.SizeKib = 2 * 1024 * 1024
	}

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("Apply: expected Ok=true; got results=%+v", results)
	}

	calls := fx.CommandLines()

	// Sanity: the resize MUST actually have fired — otherwise this test is
	// not exercising the resize-in-flight path at all.
	if !slices.Contains(calls, "drbdadm resize --assume-clean "+rd) {
		t.Fatalf("test precondition: expected `drbdadm resize --assume-clean %s` to fire (the in-flight resize); got %v", rd, calls)
	}

	assertNoMetadataProbe(t, calls, rd)
	assertNoLateAddSelfHeal(t, calls, rd)
}

// TestBUG048LateAddStillRunsMetadataPassForUnattachedVolume is the BUG-048-
// preserved bookend: when a DESIRED volume is genuinely NOT attached in the
// kernel (a real late `vd c` — the kernel device list lacks it), the per-
// volume metadata pass MUST still fire (dump-md + create-md for the missing
// volume), so the fix does not reintroduce the original BUG-048 wedge it was
// shipped to fix. This is the same invariant
// TestApplyLateAddedVolumePerVolumeMetadataRunsDespitePreRenderedRes pins,
// proven here through the NEW attached-volume gate (drbdsetup status --json
// shows only vol-0).
func TestBUG048LateAddStillRunsMetadataPassForUnattachedVolume(t *testing.T) {
	const rd = "pvc-bug048-lateadd"

	dir := t.TempDir()
	fx := storage.NewFakeExec()

	for _, vn := range []string{"00000", "00001"} {
		fx.Expect(fmt.Sprintf("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/%s_%s", rd, vn),
			storage.FakeResponse{Stdout: []byte(rd + "_" + vn + "\n")})
		fx.Expect(fmt.Sprintf("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/%s_%s", rd, vn),
			storage.FakeResponse{Stdout: []byte(fmt.Sprintf("/dev/vg/%s_%s|1048576\n", rd, vn))})
	}

	// vol-0 stamped, vol-1 has no metadata yet (the late-added volume).
	fx.Expect("drbdadm dump-md "+rd+"/0",
		storage.FakeResponse{Stdout: []byte("version \"v09\";\nla-size-sect 2048;\n")})
	fx.Expect("drbdadm dump-md "+rd+"/1",
		storage.FakeResponse{Err: errDrbdadmDumpMdNoMeta})
	fx.Expect(fmt.Sprintf("drbdadm create-md --force --max-peers=%d %s/1", drbd.MaxPeers-1, rd),
		storage.FakeResponse{})

	// KERNEL TRUTH: only vol-0 is attached; vol-1 is absent from the device
	// list (the genuine late-add window). AttachedVolumes returns {0}, so the
	// gate finds vol-1 unattached and the metadata pass fires.
	fx.Expect("drbdsetup status "+rd+" --json", storage.FakeResponse{Stdout: []byte(`[{
	  "name":"` + rd + `","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"minor":1000,"disk-state":"UpToDate"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	seedConvergedTwoVolumeRD(t, dir, rd)

	dr := []*intent.DesiredResource{convergedTwoVolumeDR(rd)}

	results, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("Apply: expected Ok=true; got results=%+v", results)
	}

	calls := fx.CommandLines()

	wantCreateMD := fmt.Sprintf("drbdadm create-md --force --max-peers=%d %s/1", drbd.MaxPeers-1, rd)
	if !slices.Contains(calls, wantCreateMD) {
		t.Errorf("BUG-048 REGRESSED: late-added unattached vol-1 got NO metadata pass — it would come up Diskless / Inconsistent with no winner seed and wedge; want %q in %v", wantCreateMD, calls)
	}

	// The dump-md probe MUST have run for the unattached volume.
	if !slices.Contains(calls, "drbdadm dump-md "+rd+"/1") {
		t.Errorf("expected the per-volume dump-md probe to run for the unattached vol-1; got %v", calls)
	}

	// vol-0 (attached) must NOT be re-created (would wipe its GI + bitmap).
	forbidden := fmt.Sprintf("drbdadm create-md --force --max-peers=%d %s/0", drbd.MaxPeers-1, rd)
	if slices.Contains(calls, forbidden) {
		t.Errorf("re-ran create-md on the attached vol-0 (would wipe operator-stamped metadata): %v", calls)
	}
}

// --- shared fixtures -------------------------------------------------------

// seedConvergedTwoVolumeRD writes the on-disk state a 2-volume RD settles
// into after first activation: a .res carrying both volume blocks and the
// .md-created marker (so dr.GetMetadataCreated()==true drives firstActivation
// false — the post-activation reconcile shape the deadlock lived in).
func seedConvergedTwoVolumeRD(t *testing.T, dir, rd string) {
	t.Helper()

	resBody := "resource " + rd + " {\n" +
		"  on n1 {\n" +
		"    volume 0 {\n    }\n" +
		"    volume 1 {\n    }\n" +
		"  }\n}\n"
	if err := os.WriteFile(filepath.Join(dir, rd+".res"), []byte(resBody), 0o600); err != nil {
		t.Fatalf("seed .res: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, rd+".md-created"), nil, 0o600); err != nil {
		t.Fatalf("seed md-created: %v", err)
	}
}

// convergedTwoVolumeDR is the DesiredResource for a past-first-activation
// 2-diskful RD with one peer: MetadataCreated=true, both volumes 1 GiB.
func convergedTwoVolumeDR(rd string) *intent.DesiredResource {
	peerID := int32(1)

	return &intent.DesiredResource{
		Name:            rd,
		NodeName:        "n1",
		MetadataCreated: true,
		SkipInitialSync: skipInitTrue(),
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			{VolumeNumber: 1, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n2", NodeID: &peerID}},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			"peer.n2.address": "10.0.0.2", "peer.n2.node-id": "1", "peer.n2.port": "7000",
		},
	}
}

// assertNoMetadataProbe fails the test if the reconcile issued ANY per-volume
// `drbdadm dump-md <rd>/<n>` — the md_buffer consumer that deadlocks a resize.
func assertNoMetadataProbe(t *testing.T, calls []string, rd string) {
	t.Helper()

	for _, line := range calls {
		if strings.HasPrefix(line, "drbdadm dump-md "+rd+"/") {
			t.Errorf("BUG-048 resize deadlock: per-volume metadata probe %q fired on a converged/resizing diskful reconcile — "+
				"dump-md is an md_buffer consumer that contends with an in-flight `vd s` resize and deadlocks the cluster reboot-proof; all calls: %v", line, calls)
		}
	}
}

// assertNoLateAddSelfHeal fails the test if the reconcile issued any cluster-
// wide late-add self-heal action: MintLateAddSource (disconnect +
// new-current-uuid --clear-bitmap + adjust) or InvalidateVolume. These are
// the cluster-wide state-change operations that contend with an in-flight
// resize's md_buffer hold.
func assertNoLateAddSelfHeal(t *testing.T, calls []string, rd string) {
	t.Helper()

	for _, line := range calls {
		switch {
		case line == "drbdadm disconnect "+rd:
			t.Errorf("BUG-048: late-add self-heal `disconnect %s` fired on a converged/resizing reconcile (MintLateAddSource) — would contend with an in-flight resize; calls: %v", rd, calls)
		case strings.HasPrefix(line, "drbdsetup new-current-uuid --clear-bitmap "):
			t.Errorf("BUG-048: late-add self-heal `%s` fired on a converged/resizing reconcile (MintLateAddSource) — would contend with an in-flight resize; calls: %v", line, calls)
		case strings.HasPrefix(line, "drbdadm invalidate "+rd):
			t.Errorf("BUG-048: late-add resync-kick `%s` fired on a converged/resizing reconcile (InvalidateVolume) — would contend with an in-flight resize; calls: %v", line, calls)
		}
	}
}
