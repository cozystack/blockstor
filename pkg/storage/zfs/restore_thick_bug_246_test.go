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

package zfs_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// Bug 246 (P2, capacity safety): ZFS-thick RestoreVolumeFromSnapshot
// issued plain `zfs clone` without follow-up
// `zfs set refreservation=<volsize>`. `zfs clone` always produces a
// sparse zvol regardless of origin reservation, so the "thick" cloned
// volume lost its space guarantee and could hit ENOSPC mid-write —
// defeating the entire point of thick provisioning.
//
// Fix: after `zfs clone`, in thick mode, set
// `refreservation=<volsize-bytes>` on the clone. ENOSPC at restore time
// propagates as the thick contract working as intended.

// TestThickRestoreSetsRefreservationAfterClone pins the fix: thick mode
// MUST issue `zfs set refreservation=<bytes> <clone>` after the clone
// is created, so the cloned zvol is fully reserved (thick) instead of
// sparse (the default for `zfs clone`).
func TestThickRestoreSetsRefreservationAfterClone(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target dataset does not yet exist (proceed past idempotency).
	fx.Expect("zfs list -H -o name tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Source snapshot dataset exists.
	fx.Expect("zfs list -H -o name tank/pvc-1_00000@snap-1",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000@snap-1\n")})
	// volsize lookup on the clone → 1 GiB in bytes.
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: false}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024, // 1 GiB
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	cloneCmd := "zfs clone tank/pvc-1_00000@snap-1 tank/pvc-2_00000"
	if !slices.Contains(fx.CommandLines(), cloneCmd) {
		t.Errorf("Bug 246 fix: expected clone cmd %q in calls; got %v",
			cloneCmd, fx.CommandLines())
	}

	wantSet := "zfs set refreservation=1073741824 tank/pvc-2_00000"
	if !slices.Contains(fx.CommandLines(), wantSet) {
		t.Errorf("Bug 246 fix: thick clone must restore refreservation; "+
			"expected %q in calls; got %v",
			wantSet, fx.CommandLines())
	}

	// Ordering matters — refreservation must come AFTER clone so the
	// dataset already exists when we set the property.
	cloneIdx, setIdx := -1, -1

	for i, line := range fx.CommandLines() {
		switch line {
		case cloneCmd:
			cloneIdx = i
		case wantSet:
			setIdx = i
		}
	}

	if cloneIdx >= 0 && setIdx >= 0 && setIdx < cloneIdx {
		t.Errorf("Bug 246 fix: refreservation must be set AFTER clone; "+
			"got clone at idx %d, set at idx %d", cloneIdx, setIdx)
	}
}

// TestThinRestoreDoesNotSetRefreservation is the negative-pair: ZFS_THIN
// must NOT set refreservation after clone, because the thin contract is
// explicit sparse-everywhere (oversubscription enabled). A regression
// that leaked the thick refreservation into the thin path would defeat
// thin's whole point.
func TestThinRestoreDoesNotSetRefreservation(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("zfs list -H -o name tank/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("zfs list -H -o name tank/pvc-1_00000@snap-1",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000@snap-1\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "refreservation=") {
			t.Errorf("THIN restore must NOT set refreservation (thin = sparse "+
				"oversubscription); got %q", line)
		}
	}
}
