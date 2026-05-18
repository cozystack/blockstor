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

// Bug 303: `linstor r td --migrate-from <src>` (UG9 §"Migrating a
// resource to another node") flips a Resource's Spec.Flags from
// [DISKLESS] to [] AND stamps Spec.StoragePool. The satellite then
// runs the diskful Apply chain: applyStorage carves the backing
// zvol/LV, applyDRBD writes the .res file (now with `disk
// /dev/zvol/...` instead of `disk none;`), runFirstActivation
// stamps fresh DRBD-9 metadata on the new disk, and
// runApplyDRBDVerb dispatches `drbdadm adjust`.
//
// But the kernel slot was previously brought up as
// `disk:Diskless client:yes` (intentional diskless). `drbdadm
// adjust` reconciles network/peer state and resource-level options
// against the new .res, but the kernel treats an intentional-
// diskless slot's current state as deliberate and adjust does NOT
// cross the diskless→diskful boundary on its own. The replica
// stays `disk:Diskless client:yes` forever; the
// lifecycle-toggle-migrate e2e times out with "never reached
// UpToDate".
//
// Fix: after the .res write + create-md + adjust, when the local
// kernel slot is loaded AND HasDisklessVolume reports a Diskless
// volume AND the spec is now diskful, run `drbdadm attach <rd>`
// to explicitly cross the boundary. Idempotent: attach on an
// already-diskful slot is a kernel-level no-op; the gate prevents
// us shelling out in that case.
func TestBug303AttachAfterIntentionalDisklessFlip(t *testing.T) {
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

	// Assert the explicit attach landed. Without the Bug 303 fix the
	// satellite stops at `drbdadm adjust` and never crosses the
	// diskless→diskful boundary, so this slice membership check is
	// the load-bearing invariant.
	if !slices.Contains(cmds, "drbdadm attach pvc-bug303") {
		t.Errorf("expected `drbdadm attach pvc-bug303` after diskless→diskful flip; got: %v", cmds)
	}

	// Order matters: attach must run AFTER adjust. Adjust reconciles
	// the .res file's network/peer state into the kernel; running
	// attach before adjust risks attaching against a stale .res view
	// of the peer mesh.
	adjustIdx := slices.IndexFunc(cmds, func(s string) bool {
		return strings.HasPrefix(s, "drbdadm adjust pvc-bug303")
	})
	attachIdx := slices.Index(cmds, "drbdadm attach pvc-bug303")

	if adjustIdx >= 0 && attachIdx >= 0 && attachIdx < adjustIdx {
		t.Errorf("attach must run AFTER adjust; got adjust@%d attach@%d in:\n%v",
			adjustIdx, attachIdx, cmds)
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

// mkEmptyFile writes a zero-byte file at the given path. Helper for
// pre-staging the .md-created marker so applyDRBD's firstActivation
// gate reports false (steady-state re-reconcile).
func mkEmptyFile(path string) error {
	return os.WriteFile(path, nil, 0o600) //nolint:wrapcheck // test helper bubbles raw os error
}
