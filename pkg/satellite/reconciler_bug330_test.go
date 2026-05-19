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

// Bug 330 (P1, observable on real stand): `linstor r td --diskless
// <node> <rd>` returns SUCCESS at the REST layer (Spec.Flags is
// correctly flipped to include DISKLESS), but the satellite NEVER
// actually transitions the local DRBD slot to Diskless. `linstor r l`
// keeps showing the replica as `DRBD,STORAGE Ok UpToDate` indefinitely.
//
// Root cause: the reconciler has no detach path. The FSM dispatch on a
// loaded, currently-UpToDate slot whose Spec just flipped to DISKLESS
// emits ActionAdjust → plain `drbdadm adjust`. The .res file is
// re-rendered with `disk none` for the local host, but the kernel
// slot will not detach via adjust alone: drbd-utils' compare_volume
// has no scheduled action to transition kern->disk=<path> →
// conf->disk="none" — the inverse direction of Bug 319 — without an
// explicit `drbdadm detach <rd>` call.
//
// Compounded with Bug 267 (which already deletes the backing LV via
// reclaimVolumesForDiskless on the same Apply pass), this leaves
// DRBD pinning a now-missing block device: the kernel still believes
// it owns the disk and any subsequent I/O fails, while `r l` reports
// stale UpToDate.
//
// Fix: when Spec.Flags has DISKLESS, kernel slot is loaded, and the
// kernel still reports the local volume as non-Diskless (i.e. has not
// detached yet on its own), the satellite must invoke
// `drbdadm detach --force <rd>` BEFORE reclaimVolumesForDiskless
// deletes the lower disk so DRBD has a chance to release the device
// gracefully. After detach the kernel slot transitions to Diskless,
// reclaimVolumesForDiskless can safely DeleteVolume, and the .res
// re-render leaves a `disk none` config that adjust then no-ops.
//
// This test pins the contract at the satellite reconciler layer: feed
// the dispatcher's toggle-to-diskless shape (Spec.Flags=[DISKLESS],
// historical pool stamped on volumes) AND a kernel-state probe that
// reports the slot is loaded but still UpToDate, then assert
// `drbdadm detach --force <rd>` appears in the recorded command
// stream.
//
// MUST fail before the fix lands.
func TestBug330ToggleDisklessIssuesDrbdadmDetach(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// IsLoaded probe: non-empty stdout signals the kernel owns the
	// slot. The trimmed body is what runAdjust later branches on too.
	fx.Expect("drbdsetup status pvc-bug330", storage.FakeResponse{
		Stdout: []byte("pvc-bug330 role:Secondary\n  volume:0 disk:UpToDate\n"),
	})

	// Kernel reports the local volume as UpToDate — the diskful
	// steady state that opens the bug. Spec just flipped to DISKLESS;
	// we need the satellite to detach before the LV is reclaimed.
	fx.Expect("drbdsetup status --verbose pvc-bug330", storage.FakeResponse{
		Stdout: []byte(`pvc-bug330 node-id:0 role:Secondary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/vg/pvc-bug330_00000 quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`),
	})

	// LV lookup for reclaimVolumesForDiskless. Provider must answer
	// the lvs probe before DeleteVolume is dispatched; we leave the
	// thin LVM provider wired so the satellite's diskless-cleanup
	// path is exercised against a realistic command stream.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-bug330_00000",
		storage.FakeResponse{Stdout: []byte("pvc-bug330_00000\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-bug330",
		NodeName: "n1",
		// Spec just flipped to DISKLESS via `linstor r td --diskless`.
		Flags: []string{"DISKLESS"},
		// Dispatcher stamps the historical pool on toggle-to-diskless
		// (Bug 267) so the satellite knows which provider to reclaim
		// against. We use that same shape here.
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	cmds := fx.CommandLines()

	wantDetach := "drbdadm detach --force pvc-bug330"
	if !slices.Contains(cmds, wantDetach) {
		t.Errorf("Bug 330: expected %q in command stream so DRBD releases the lower disk "+
			"before `r td --diskless` reclaims the backing LV; commands seen:\n%v",
			wantDetach, cmds)
	}

	// Belt-and-braces: the .res file must also have flipped to
	// `disk none` for the local host so a subsequent `drbdadm adjust`
	// finds nothing to do. Without this the kernel could re-attach on
	// the next reconcile pass.
	body, readErr := os.ReadFile(filepath.Join(dir, "pvc-bug330.res"))
	if readErr != nil {
		t.Fatalf("read .res: %v", readErr)
	}

	if !strings.Contains(string(body), "none;") {
		t.Errorf("Bug 330: .res must render `disk ... none;` for the local diskless host; got:\n%s", body)
	}
}
