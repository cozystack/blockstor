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
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Bug 319 (root-cause fix for Bug 303): `linstor r td --migrate-from
// <src>` (UG9 §"Migrating a resource to another node") flips a
// Resource's Spec.Flags from [DISKLESS] to [] AND stamps
// Spec.StoragePool. The satellite then runs the diskful Apply chain:
// applyStorage carves the backing zvol/LV, applyDRBD writes the .res
// file (now with `disk /dev/zvol/...` instead of `disk none;`), and
// `ensureMetadata` stamps fresh DRBD-9 metadata on the new disk
// BEFORE `drbdadm adjust` runs.
//
// Upstream LINSTOR's DrbdLayer pipeline is `createMetaData` →
// `drbdadm adjust`. drb-utils' `adjust` then crosses the
// diskless→diskful boundary on its own via compare_volume:
// kern->disk=="none" + conf->disk pointing at a real path schedules
// attach_cmd. Bug 303's earlier workaround (explicit `drbdadm
// attach` AFTER adjust) papered over a satellite gate that prevented
// create-md from running on the flip; Bug 319 lifts the gate and
// removes the explicit attach.
//
// This test pins the upstream-aligned shape: create-md MUST run on
// the flip BEFORE adjust, and the explicit `drbdadm attach` MUST be
// gone (the test that previously asserted attach is the bug being
// fixed).
func TestBug319CreateMDBeforeAdjustOnDisklessToDiskfulFlip(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Pre-stage the lvs probes the storage provider runs.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-bug303_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-bug303_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-bug303_00000|1048576\n")})

	// Kernel state probes: `drbdsetup status` returns a multi-line
	// dump showing the slot loaded with `disk:Diskless client:yes`
	// — the intentional-diskless shape this fix targets.
	kernelDump := []byte(`pvc-bug303 role:Secondary
  volume:0 disk:Diskless client:yes
  n2 role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`)
	fx.Expect("drbdsetup status pvc-bug303",
		storage.FakeResponse{Stdout: kernelDump})
	fx.Expect("drbdsetup status --verbose pvc-bug303",
		storage.FakeResponse{Stdout: kernelDump})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// Diskful DesiredResource — the shape the dispatcher emits AFTER
	// the migrate-disk REST handler cleared DISKLESS and stamped
	// StoragePool. No DISKLESS in Flags.
	dr := &intent.DesiredResource{
		Name:     "pvc-bug303",
		NodeName: "n1",
		Flags:    []string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []string{"n2"},
		DrbdOptions: map[string]string{
			"port":            "7000",
			"node-id":         "0",
			"address":         "10.0.0.1",
			"minor":           "1000",
			"peer.n2.address": "10.0.0.2",
			"peer.n2.node-id": "1",
			"peer.n2.port":    "7000",
		},
	}

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("apply result not ok: %+v", results)
	}

	cmds := fx.CommandLines()

	// Upstream-aligned invariant: `drbdadm create-md` MUST run on the
	// diskless→diskful flip, BEFORE `drbdadm adjust`. drb-utils'
	// compare_volume then schedules attach_cmd automatically from the
	// kern->disk=="none" + conf->disk path diff. The Bug 303 fix
	// papered over a missing create-md re-entry with an explicit
	// `drbdadm attach`; Bug 319 lifts the gate so create-md is the
	// load-bearing step.
	createMDIdx := slices.IndexFunc(cmds, func(s string) bool {
		return strings.HasPrefix(s, "drbdadm create-md") && strings.HasSuffix(s, "pvc-bug303")
	})
	if createMDIdx < 0 {
		t.Errorf("expected `drbdadm create-md ... pvc-bug303` on the diskless→diskful flip; got: %v", cmds)
	}

	adjustIdx := slices.IndexFunc(cmds, func(s string) bool {
		return strings.HasPrefix(s, "drbdadm adjust pvc-bug303")
	})
	if adjustIdx < 0 {
		t.Errorf("expected `drbdadm adjust pvc-bug303` on the flip; got: %v", cmds)
	}

	if createMDIdx >= 0 && adjustIdx >= 0 && createMDIdx > adjustIdx {
		t.Errorf("create-md must run BEFORE adjust (upstream DrbdLayer pipeline); "+
			"got create-md@%d adjust@%d in:\n%v", createMDIdx, adjustIdx, cmds)
	}

	// Adjust must be the plain `drbdadm adjust` (not `--skip-disk`).
	// `--skip-disk` would prevent the auto-attach that the
	// kern->disk vs conf->disk diff in compare_volume schedules; the
	// Bug 280 race-close that coerces `--skip-disk` when the kernel
	// reports Diskless must NOT fire on the flip path.
	if slices.Contains(cmds, "drbdadm adjust --skip-disk pvc-bug303") {
		t.Errorf("plain `drbdadm adjust` expected on the flip — `--skip-disk` "+
			"suppresses the auto-attach the create-md was for; got: %v", cmds)
	}

	// Bug 303's explicit attach is GONE — the upstream pipeline
	// (create-md → adjust) does it via compare_volume's attach_cmd.
	// Leaving the explicit attach in place would be a redundant
	// shell-out at best and a race with the kernel's own attach
	// completion at worst.
	if slices.Contains(cmds, "drbdadm attach pvc-bug303") {
		t.Errorf("explicit `drbdadm attach` must NOT run — adjust auto-attaches via "+
			"drb-utils' compare_volume after create-md; got: %v", cmds)
	}
}

// TestBug303NoAttachOnDisklessResource guards the inverse: a Resource
// whose Spec is still DISKLESS must NOT trigger an attach. Without
// this guard the fix would spuriously call attach on every diskless
// replica's reconcile pass.
func TestBug303NoAttachOnDisklessResource(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Kernel state is loaded + diskless; same shape as the bug case.
	kernelDump := []byte(`pvc-bug303-d role:Secondary
  volume:0 disk:Diskless client:yes
  n2 role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`)
	fx.Expect("drbdsetup status pvc-bug303-d",
		storage.FakeResponse{Stdout: kernelDump})
	fx.Expect("drbdsetup status --verbose pvc-bug303-d",
		storage.FakeResponse{Stdout: kernelDump})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-bug303-d",
		NodeName: "n1",
		Flags:    []string{"DISKLESS"},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: ""},
		},
		Peers: []string{"n2"},
		DrbdOptions: map[string]string{
			"port":            "7000",
			"node-id":         "0",
			"address":         "10.0.0.1",
			"minor":           "1000",
			"peer.n2.address": "10.0.0.2",
			"peer.n2.node-id": "1",
			"peer.n2.port":    "7000",
		},
	}

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	cmds := fx.CommandLines()
	if slices.Contains(cmds, "drbdadm attach pvc-bug303-d") {
		t.Errorf("attach must NOT run on diskless-spec Resource; got: %v", cmds)
	}
}

// TestBug303NoAttachWhenKernelAlreadyDiskful guards the second
// inverse: when the kernel slot is already diskful (the steady
// state after the first migrate succeeded), the attach probe
// returns false and we skip the shell-out. Without this we'd
// spam `drbdadm attach` on every reconcile pass of an already-
// converged replica.
func TestBug303NoAttachWhenKernelAlreadyDiskful(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-bug303-up_00000",
		storage.FakeResponse{Stdout: []byte("pvc-bug303-up_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-bug303-up_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-bug303-up_00000|1048576\n")})

	// Kernel state: already diskful UpToDate — the post-conversion
	// steady state. HasDisklessVolume must return false here.
	kernelDump := []byte(`pvc-bug303-up role:Secondary
  volume:0 disk:UpToDate
  n2 role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`)
	fx.Expect("drbdsetup status pvc-bug303-up",
		storage.FakeResponse{Stdout: kernelDump})
	fx.Expect("drbdsetup status --verbose pvc-bug303-up",
		storage.FakeResponse{Stdout: kernelDump})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// Touch the md-marker so firstActivation is false (steady-state
	// re-reconcile, not the first-conversion pass).
	mdMarker := dir + "/pvc-bug303-up.md-created"

	err := mkEmptyFile(mdMarker)
	if err != nil {
		t.Fatalf("mkEmptyFile: %v", err)
	}

	dr := &intent.DesiredResource{
		Name:     "pvc-bug303-up",
		NodeName: "n1",
		Flags:    []string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []string{"n2"},
		DrbdOptions: map[string]string{
			"port":            "7000",
			"node-id":         "0",
			"address":         "10.0.0.1",
			"minor":           "1000",
			"peer.n2.address": "10.0.0.2",
			"peer.n2.node-id": "1",
			"peer.n2.port":    "7000",
		},
	}

	_, err = rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	cmds := fx.CommandLines()
	if slices.Contains(cmds, "drbdadm attach pvc-bug303-up") {
		t.Errorf("attach must NOT run when kernel is already diskful; got: %v", cmds)
	}
}

// TestApplyDRBDRunsCreateMdOnDisklessToDiskfulFlip pins the Bug 319
// invariant against the marker-present case: even when `.md-created`
// is already on disk from a prior incarnation (re-created RD with the
// same name, satellite carrying over state, or any path that wrote
// the marker before the resource went diskless), the diskless→
// diskful flip MUST re-enter create-md so the freshly-carved lower
// disk gets valid DRBD-9 metadata. The historical Bug 303 fix gated
// create-md on `firstActivation` (i.e. marker absence) and so missed
// this case entirely — `drbdadm adjust` would see kernel Diskless
// and no metadata, refuse to attach, and the explicit attach
// workaround would fail with "No valid meta data found".
//
// FakeExec assertion: `drbdadm create-md` MUST appear in the command
// history BEFORE `drbdadm adjust`, regardless of marker state.
func TestApplyDRBDRunsCreateMdOnDisklessToDiskfulFlip(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-bug319_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-bug319_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-bug319_00000|1048576\n")})

	// Kernel reports intentional-Diskless — the shape after the
	// previous diskless apply brought the slot up with `disk none`.
	kernelDump := []byte(`pvc-bug319 role:Secondary
  volume:0 disk:Diskless client:yes
  n2 role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`)
	fx.Expect("drbdsetup status pvc-bug319",
		storage.FakeResponse{Stdout: kernelDump})
	fx.Expect("drbdsetup status --verbose pvc-bug319",
		storage.FakeResponse{Stdout: kernelDump})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// Pre-stage `.md-created` so firstActivation reports false. The
	// flip MUST still re-enter create-md via the diskful-flip gate
	// (kernel probe), independent of the marker.
	err := mkEmptyFile(dir + "/pvc-bug319.md-created")
	if err != nil {
		t.Fatalf("mkEmptyFile: %v", err)
	}

	dr := &intent.DesiredResource{
		Name:     "pvc-bug319",
		NodeName: "n1",
		Flags:    []string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []string{"n2"},
		DrbdOptions: map[string]string{
			"port":            "7000",
			"node-id":         "0",
			"address":         "10.0.0.1",
			"minor":           "1000",
			"peer.n2.address": "10.0.0.2",
			"peer.n2.node-id": "1",
			"peer.n2.port":    "7000",
		},
	}

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("apply result not ok: %+v", results)
	}

	cmds := fx.CommandLines()

	createMDIdx := slices.IndexFunc(cmds, func(s string) bool {
		return strings.HasPrefix(s, "drbdadm create-md") && strings.HasSuffix(s, "pvc-bug319")
	})
	if createMDIdx < 0 {
		t.Errorf("expected `drbdadm create-md ... pvc-bug319` on the flip even with "+
			".md-created marker present; got: %v", cmds)
	}

	adjustIdx := slices.IndexFunc(cmds, func(s string) bool {
		return strings.HasPrefix(s, "drbdadm adjust pvc-bug319")
	})
	if adjustIdx < 0 {
		t.Errorf("expected `drbdadm adjust pvc-bug319` on the flip; got: %v", cmds)
	}

	if createMDIdx >= 0 && adjustIdx >= 0 && createMDIdx > adjustIdx {
		t.Errorf("create-md must run BEFORE adjust on the flip; "+
			"got create-md@%d adjust@%d in:\n%v", createMDIdx, adjustIdx, cmds)
	}

	if slices.Contains(cmds, "drbdadm attach pvc-bug319") {
		t.Errorf("explicit `drbdadm attach` must NOT run (Bug 303 workaround removed); "+
			"got: %v", cmds)
	}

	if slices.Contains(cmds, "drbdadm adjust --skip-disk pvc-bug319") {
		t.Errorf("plain `drbdadm adjust` expected on the flip — `--skip-disk` "+
			"suppresses the auto-attach the create-md was for; got: %v", cmds)
	}

	// Bug 319: primary --force MUST NOT run on a flag flip. The peer
	// is already UpToDate; force-primary here would regenerate the
	// local Current UUID out from under the cluster.
	for _, c := range cmds {
		if strings.HasPrefix(c, "drbdadm primary --force") {
			t.Errorf("primary --force must NOT run on a diskless→diskful flip; "+
				"got: %s in %v", c, cmds)
		}
	}
}

// mkEmptyFile writes a zero-byte file at the given path. Helper for
// pre-staging the .md-created marker so applyDRBD's firstActivation
// gate reports false (steady-state re-reconcile).
func mkEmptyFile(path string) error {
	return os.WriteFile(path, nil, 0o600) //nolint:wrapcheck // test helper bubbles raw os error
}
