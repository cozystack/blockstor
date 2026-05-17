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

package lvm

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// ThickConfig parametrises a Thick (LINSTOR `LVM`) provider with just
// the volume group — there's no thin pool. The VG must already exist
// on the host.
type ThickConfig struct {
	VolumeGroup string
}

// Thick implements storage.Provider for LINSTOR's classic `LVM` kind.
// Differs from Thin: no thin pool, no `--virtualsize`, allocates real
// extents up-front, no snapshot via copy-on-write.
type Thick struct {
	cfg  ThickConfig
	exec storage.Exec
}

// NewThick constructs a Thick provider.
func NewThick(cfg ThickConfig, ex storage.Exec) *Thick {
	return &Thick{cfg: cfg, exec: ex}
}

// Kind returns the upstream LINSTOR provider kind string.
func (*Thick) Kind() string { return "LVM" }

// CreateVolume allocates a thick LV. Idempotent: existing LV is a no-op.
func (t *Thick) CreateVolume(ctx context.Context, vol storage.Volume) error {
	if t.lvExists(ctx, volumeLVName(vol)) {
		return nil
	}

	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	_, err := t.exec.Run(ctx, "lvcreate",
		Args("--size", strconv.FormatInt(sizeMiB, 10)+"MiB",
			"--name", volumeLVName(vol),
			// Skip the optional zero-on-create step — the satellite
			// container has no udev daemon, and the wipe trips on the
			// missing /dev/<vg>/<lv> symlink. Same trick as install-pools.sh.
			"--config", "activation{udev_sync=0 udev_rules=0}",
			"-Wn", "-Zn",
			t.cfg.VolumeGroup)...)
	if err != nil {
		return errors.Wrapf(err, "lvcreate %s", volumeLVName(vol))
	}

	return nil
}

// ResizeVolume grows the LV to vol.SizeKib. Bug 269 (P1): inherits
// the udev-less workaround CreateVolume uses; satellite container
// has no udev daemon so `activation{udev_sync=0 udev_rules=0}` keeps
// `lvextend` from blocking on a sync that never completes (see
// Thin.ResizeVolume doc for the @drbd_ru repro chain).
func (t *Thick) ResizeVolume(ctx context.Context, vol storage.Volume) error {
	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	_, err := t.exec.Run(ctx, "lvextend",
		Args("--size", strconv.FormatInt(sizeMiB, 10)+"MiB",
			"--config", "activation{udev_sync=0 udev_rules=0}",
			t.cfg.VolumeGroup+"/"+volumeLVName(vol))...)
	if err != nil {
		return errors.Wrapf(err, "lvextend %s", volumeLVName(vol))
	}

	return nil
}

// DeleteVolume idempotently removes the LV.
func (t *Thick) DeleteVolume(ctx context.Context, vol storage.Volume) error {
	if !t.lvExists(ctx, volumeLVName(vol)) {
		return nil
	}

	_, err := t.exec.Run(ctx, "lvremove",
		Args("--force",
			t.cfg.VolumeGroup+"/"+volumeLVName(vol))...)
	if err != nil {
		return errors.Wrapf(err, "lvremove %s", volumeLVName(vol))
	}

	return nil
}

// VolumeStatus reports observed disk state via lvs.
func (t *Thick) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return volumeStatusViaLVS(ctx, t.exec, t.cfg.VolumeGroup+"/"+volumeLVName(vol))
}

// ListVolumeNames enumerates every LV in the configured VG that
// matches blockstor's `<resource>_<vol5digits>` naming. Used by the
// orphan-storage sweeper (Bug 43) — see Thin.ListVolumeNames doc.
func (t *Thick) ListVolumeNames(ctx context.Context) ([]storage.VolumeRef, error) {
	return listLVMVolumes(ctx, t.exec, t.cfg.VolumeGroup)
}

// PoolStatus reports the VG's free/total capacity. Bug 270: bounded
// ctx so a stuck `vgs` can't wedge the satellite — see
// withProbeTimeout in lvm_common.go.
func (t *Thick) PoolStatus(ctx context.Context) (storage.PoolStatus, error) {
	ctx, cancel := withProbeTimeout(ctx)
	defer cancel()

	out, err := t.exec.Run(ctx, "vgs",
		Args("--noheadings",
			"--separator", "|",
			"-o", "vg_size,vg_free",
			"--units", "k",
			"--nosuffix",
			t.cfg.VolumeGroup)...)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "vgs")
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		// Bug 282: tag with ErrPoolGone so writeCapacity flips
		// PoolMissing=true. Transient vgs errors (timeout, locked)
		// MUST NOT carry this tag.
		return storage.PoolStatus{}, errors.Wrapf(storage.ErrPoolGone,
			"vg %s not found", t.cfg.VolumeGroup)
	}

	parts := strings.SplitN(line, "|", lvsCols)
	if len(parts) != lvsCols {
		return storage.PoolStatus{}, errors.Errorf("vgs: unexpected line %q", line)
	}

	totalKib, err := parseFloatToInt64(parts[0])
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse vg_size")
	}

	freeKib, err := parseFloatToInt64(parts[1])
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse vg_free")
	}

	return storage.PoolStatus{
		FreeCapacityKib:   freeKib,
		TotalCapacityKib:  totalKib,
		SupportsSnapshots: false, // LVM-classic: no copy-on-write
	}, nil
}

// CreateSnapshot is `lvcreate --snapshot --size <N>` for thick LV.
// We allocate the snapshot at 25 % of the source LV's size — bigger
// snapshots waste extents, smaller ones overflow on heavy churn.
//
// Idempotent: pre-existing snapshot LV → no-op (Bug 216). See
// Thin.CreateSnapshot for rationale; the reconcile-loop trap is the
// same on LVM-classic.
func (t *Thick) CreateSnapshot(ctx context.Context, snap storage.Snapshot) error {
	if t.lvExists(ctx, snapshotLVName(snap)) {
		return nil
	}

	source := fmt.Sprintf("%s_%05d", snap.ResourceName, 0)

	_, err := t.exec.Run(ctx, "lvcreate",
		Args("--snapshot",
			"--extents", "25%ORIGIN",
			"--name", snapshotLVName(snap),
			t.cfg.VolumeGroup+"/"+source)...)
	if err != nil {
		return errors.Wrapf(err, "lvcreate --snapshot %s", snapshotLVName(snap))
	}

	return nil
}

// DeleteSnapshot mirrors Thin's, including the missing-LV idempotency
// short-circuit so the satellite reconciler can strip the Snapshot
// CRD's finalizer instead of looping on a "not found" surface error.
func (t *Thick) DeleteSnapshot(ctx context.Context, snap storage.Snapshot) error {
	if !t.lvExists(ctx, snapshotLVName(snap)) {
		return nil
	}

	_, err := t.exec.Run(ctx, "lvremove",
		Args("--force",
			t.cfg.VolumeGroup+"/"+snapshotLVName(snap))...)
	if err != nil {
		return errors.Wrapf(err, "lvremove -f %s", snapshotLVName(snap))
	}

	return nil
}

// RestoreVolumeFromSnapshot materialises target as an independent,
// fully-allocated thick LV holding the snapshot's bytes.
//
// Bug 245 (P1): the previous implementation used
// `lvcreate --snapshot --extents 25%ORIGIN`, producing a COW overlay
// capped at 25 % of origin size. Writes exceeding that silently
// invalidated the LV (lv_attr → I), corrupting the restored PV.
// The thin variant can do this because thin snapshots are uncapped
// CoW; thick has no such shortcut.
//
// Canonical thick-restore path:
//  1. `lvcreate --size <origin_size> --addtag @blockstor-restore-incomplete
//     --name <new>` — fully-allocated independent LV tagged
//     mid-operation.
//  2. `dd if=/dev/<vg>/<snap> of=/dev/<vg>/<new> bs=1M conv=fsync` —
//     copy the snapshot bytes onto the new LV.
//  3. `lvchange --deltag @blockstor-restore-incomplete <vg>/<new>` —
//     clear the sentinel.
//
// Heavy I/O, but the correct semantic for "restore snapshot to a new
// independent volume on thick LVM". The new volume survives arbitrary
// write volume because it isn't a COW overlay.
//
// Bug 257 (P1, data integrity): the pre-fix idempotency check was a
// bare lvExists — a crash BETWEEN lvcreate and dd left the LV existing
// with garbage content, and the next reconcile mis-trusted it as the
// restored volume. The fix is a completion sentinel: the lvcreate
// carries `--addtag @blockstor-restore-incomplete` inline so the
// sentinel is present from the first byte of the LV's existence, and
// only the post-dd deltag clears it. Idempotent-skip checks the tag:
// present → previous run crashed mid-dd, re-run the dd; absent →
// previous run completed cleanly, short-circuit.
func (t *Thick) RestoreVolumeFromSnapshot(ctx context.Context, target storage.Volume, src storage.Snapshot) error {
	tgtName := volumeLVName(target)
	srcName := snapshotLVName(src)

	if t.lvExists(ctx, tgtName) {
		// Bug 257: idempotent-skip is conditional on the sentinel
		// being absent. Present → previous run crashed mid-dd; re-run
		// just the dd + deltag steps (lvcreate is skipped since the LV
		// already holds an allocation).
		if !lvHasRestoreIncompleteTag(ctx, t.exec, t.cfg.VolumeGroup, tgtName) {
			return nil
		}

		if !t.lvExists(ctx, srcName) {
			return errors.Wrapf(storage.ErrNotFound,
				"snapshot LV %s/%s for restore re-run", t.cfg.VolumeGroup, srcName)
		}

		return t.copyAndClearSentinel(ctx, srcName, tgtName)
	}

	if !t.lvExists(ctx, srcName) {
		return errors.Wrapf(storage.ErrNotFound, "snapshot LV %s/%s for clone", t.cfg.VolumeGroup, srcName)
	}

	// Size the new LV at the origin snapshot's full size — the restored
	// volume must hold every byte the snapshot could have, regardless
	// of what the caller passed in vol.SizeKib (CSI controllers usually
	// pre-fill it to match, but we trust the source size to keep dd's
	// dst large enough for the copy).
	srcStatus, err := volumeStatusViaLVS(ctx, t.exec, t.cfg.VolumeGroup+"/"+srcName)
	if err != nil {
		return errors.Wrapf(err, "lvs %s for restore-size lookup", srcName)
	}

	sizeMiB := max(srcStatus.UsableKib/mibPerKib, 1)

	// lvcreate carries `--addtag` inline (Bug 257): the sentinel is set
	// in the same LVM transaction that allocates the LV, so a crash
	// BEFORE dd cannot leave an un-tagged LV that would be mis-trusted
	// on the next reconcile.
	_, err = t.exec.Run(ctx, "lvcreate",
		Args("--size", strconv.FormatInt(sizeMiB, 10)+"MiB",
			"--name", tgtName,
			"--addtag", RestoreIncompleteTag,
			// Same udev-less satellite workaround as CreateVolume.
			"--config", "activation{udev_sync=0 udev_rules=0}",
			"-Wn", "-Zn",
			t.cfg.VolumeGroup)...)
	if err != nil {
		return errors.Wrapf(err, "lvcreate --size %s for restore", tgtName)
	}

	return t.copyAndClearSentinel(ctx, srcName, tgtName)
}

// copyAndClearSentinel runs the dd byte-copy and, on success, clears
// the completion sentinel (Bug 257). Failure of the dd leaves the
// sentinel in place so the next reconcile re-runs the sequence; failure
// of the deltag itself is propagated because a successful dd with a
// surviving tag would re-trigger the dd on every subsequent reconcile.
func (t *Thick) copyAndClearSentinel(ctx context.Context, srcName, tgtName string) error {
	srcPath := "/dev/" + t.cfg.VolumeGroup + "/" + srcName
	tgtPath := "/dev/" + t.cfg.VolumeGroup + "/" + tgtName

	_, err := t.exec.Run(ctx, "dd",
		"if="+srcPath,
		"of="+tgtPath,
		"bs=1M",
		"conv=fsync")
	if err != nil {
		return errors.Wrapf(err, "dd %s → %s for restore", srcName, tgtName)
	}

	_, err = t.exec.Run(ctx, "lvchange",
		Args("--deltag", RestoreIncompleteTag,
			t.cfg.VolumeGroup+"/"+tgtName)...)
	if err != nil {
		return errors.Wrapf(err, "lvchange --deltag %s/%s", t.cfg.VolumeGroup, tgtName)
	}

	return nil
}

// lvExists is the same idempotency primitive Thin uses. Bug 270:
// bounded ctx so a stuck `lvs` cannot block the create/delete path.
func (t *Thick) lvExists(ctx context.Context, lvName string) bool {
	ctx, cancel := withProbeTimeout(ctx)
	defer cancel()

	out, err := t.exec.Run(ctx, "lvs",
		Args("--noheadings",
			"-o", "lv_name",
			t.cfg.VolumeGroup+"/"+lvName)...)
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(out)) != ""
}
