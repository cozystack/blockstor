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

// Package lvm provides LVM and LVM-thin storage backends. The LINSTOR
// `LVM_THIN` provider is the default for cozystack-style clusters because
// it gives proper snapshots without the storage-overhead of LVM-classic.
package lvm

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// ThinConfig parametrises a Thin provider with the LVM volume group and
// thin pool that back it. Both must already exist on the host; the
// satellite never auto-creates VGs (that's a host-prep concern).
type ThinConfig struct {
	VolumeGroup string
	ThinPool    string
}

// Thin implements storage.Provider for LINSTOR's `LVM_THIN` kind.
type Thin struct {
	cfg  ThinConfig
	exec storage.Exec
}

// NewThin constructs a Thin provider. The Exec is injected so unit tests
// can drive it without a real LVM stack.
func NewThin(cfg ThinConfig, ex storage.Exec) *Thin {
	return &Thin{cfg: cfg, exec: ex}
}

// Kind returns the upstream LINSTOR provider kind string.
func (*Thin) Kind() string { return "LVM_THIN" }

// volumeLVName mirrors upstream LINSTOR's naming: `<resource>_<vol5digits>`.
// We zero-pad the volume number to keep lexical sort meaningful.
func volumeLVName(vol storage.Volume) string {
	return fmt.Sprintf("%s_%05d", vol.ResourceName, vol.VolumeNumber)
}

// snapshotLVName for `lvcreate -s` of a Volume's first volume (#0). LINSTOR
// snapshots are per-resource, so the LV name is `<rd>_<snap>_00000`.
func snapshotLVName(snap storage.Snapshot) string {
	return fmt.Sprintf("%s_%s_00000", snap.ResourceName, snap.SnapshotName)
}

// CreateVolume idempotently creates the thin LV. If the LV already exists
// (regardless of size) we return nil — resize lives on a separate code
// path which Phase 4 introduces.
func (t *Thin) CreateVolume(ctx context.Context, vol storage.Volume) error {
	if t.lvExists(ctx, volumeLVName(vol)) {
		return nil
	}

	// LINSTOR rounds to MiB internally; we follow.
	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	_, err := t.exec.Run(ctx, "lvcreate",
		Args("--thin",
			"--virtualsize", strconv.FormatInt(sizeMiB, 10)+"MiB",
			"--name", volumeLVName(vol),
			t.cfg.VolumeGroup+"/"+t.cfg.ThinPool)...)
	if err != nil {
		return errors.Wrapf(err, "lvcreate %s", volumeLVName(vol))
	}

	return nil
}

// ResizeVolume grows the LV to vol.SizeKib (rounded up to MiB to
// match LINSTOR's reporting). Shrinks are rejected — DRBD doesn't
// support online shrink and CSI ControllerExpandVolume is grow-only.
// `lvextend --size` is a no-op when the requested size matches, so
// the call stays idempotent.
func (t *Thin) ResizeVolume(ctx context.Context, vol storage.Volume) error {
	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	_, err := t.exec.Run(ctx, "lvextend",
		Args("--size", strconv.FormatInt(sizeMiB, 10)+"MiB",
			t.cfg.VolumeGroup+"/"+volumeLVName(vol))...)
	if err != nil {
		return errors.Wrapf(err, "lvextend %s", volumeLVName(vol))
	}

	return nil
}

// DeleteVolume idempotently removes the LV. Missing → no-op (reconcile
// loops re-call this).
func (t *Thin) DeleteVolume(ctx context.Context, vol storage.Volume) error {
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
func (t *Thin) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return volumeStatusViaLVS(ctx, t.exec, t.cfg.VolumeGroup+"/"+volumeLVName(vol))
}

// PoolStatus reports the thin pool's free/total capacity.
func (t *Thin) PoolStatus(ctx context.Context) (storage.PoolStatus, error) {
	out, err := t.exec.Run(ctx, "lvs",
		Args("--noheadings",
			"--separator", "|",
			"-o", "lv_size,data_percent",
			"--units", "k",
			"--nosuffix",
			t.cfg.VolumeGroup+"/"+t.cfg.ThinPool)...)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "lvs (pool)")
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return storage.PoolStatus{}, errors.Errorf("thin pool %s/%s not found",
			t.cfg.VolumeGroup, t.cfg.ThinPool)
	}

	parts := strings.SplitN(line, "|", lvsCols)
	if len(parts) != lvsCols {
		return storage.PoolStatus{}, errors.Errorf("lvs (pool): unexpected line %q", line)
	}

	totalKib, err := parseFloatToInt64(parts[0])
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse lv_size")
	}

	usedPct, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse data_percent")
	}

	usedKib := int64(float64(totalKib) * usedPct / pctMax)

	return storage.PoolStatus{
		FreeCapacityKib:   totalKib - usedKib,
		TotalCapacityKib:  totalKib,
		SupportsSnapshots: true,
	}, nil
}

// CreateSnapshot is `lvcreate -s` of the resource's volume 0. Multi-volume
// resources land in Phase 4.
func (t *Thin) CreateSnapshot(ctx context.Context, snap storage.Snapshot) error {
	source := fmt.Sprintf("%s_%05d", snap.ResourceName, 0)

	_, err := t.exec.Run(ctx, "lvcreate",
		Args("--snapshot",
			"--name", snapshotLVName(snap),
			t.cfg.VolumeGroup+"/"+source)...)
	if err != nil {
		return errors.Wrapf(err, "lvcreate -s %s", snapshotLVName(snap))
	}

	return nil
}

// DeleteSnapshot mirrors DeleteVolume on the snapshot LV.
func (t *Thin) DeleteSnapshot(ctx context.Context, snap storage.Snapshot) error {
	_, err := t.exec.Run(ctx, "lvremove",
		Args("--force",
			t.cfg.VolumeGroup+"/"+snapshotLVName(snap))...)
	if err != nil {
		return errors.Wrapf(err, "lvremove -f %s", snapshotLVName(snap))
	}

	return nil
}

// RestoreVolumeFromSnapshot materialises target as a thin-pool LV
// clone of the snapshot. Thin snapshots in LVM are CoW by default
// (no `--addtag pvmove` flag toggling required) — instant create,
// lazy allocation on diverging writes.
//
// Upstream LINSTOR equivalent:
//
//	lvcreate -s --kernel --activate y --name <tgt> <vg>/<src-snap>
//
// Idempotent: target LV present → resumed reconcile, no-op.
func (t *Thin) RestoreVolumeFromSnapshot(ctx context.Context, target storage.Volume, src storage.Snapshot) error {
	tgtName := volumeLVName(target)
	if t.lvExists(ctx, tgtName) {
		return nil
	}

	srcName := snapshotLVName(src)
	if !t.lvExists(ctx, srcName) {
		return errors.Wrapf(storage.ErrNotFound, "snapshot LV %s/%s for clone", t.cfg.VolumeGroup, srcName)
	}

	_, err := t.exec.Run(ctx, "lvcreate",
		Args("--snapshot",
			"--kernel",
			"--setactivationskip", "n",
			"--activate", "y",
			"--name", tgtName,
			t.cfg.VolumeGroup+"/"+srcName)...)
	if err != nil {
		return errors.Wrapf(err, "lvcreate -s %s → %s", srcName, tgtName)
	}

	return nil
}

// lvExists is the idempotency primitive used by Create/Delete. Errors
// from `lvs` are folded into "missing": we can't distinguish "lv not
// found" from "vg locked" via stdout, and the caller's subsequent op
// surfaces the real cause anyway.
func (t *Thin) lvExists(ctx context.Context, lvName string) bool {
	out, err := t.exec.Run(ctx, "lvs",
		Args("--noheadings",
			"-o", "lv_name",
			t.cfg.VolumeGroup+"/"+lvName)...)
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(out)) != ""
}

const (
	mibPerKib = 1024
	pctMax    = 100.0
	lvsCols   = 2
)

// parseFloatToInt64 parses a numeric string LVM emits with `--units k
// --nosuffix`, which is e.g. "104857600.00".
func parseFloatToInt64(raw string) (int64, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, errors.Wrap(err, "parse number")
	}

	return int64(v), nil
}
