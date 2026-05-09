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
		"--size", strconv.FormatInt(sizeMiB, 10)+"MiB",
		"--name", volumeLVName(vol),
		// Skip the optional zero-on-create step — the satellite
		// container has no udev daemon, and the wipe trips on the
		// missing /dev/<vg>/<lv> symlink. Same trick as install-pools.sh.
		"--config", "activation{udev_sync=0 udev_rules=0}",
		"-Wn", "-Zn",
		t.cfg.VolumeGroup)
	if err != nil {
		return errors.Wrapf(err, "lvcreate %s", volumeLVName(vol))
	}

	return nil
}

// ResizeVolume grows the LV to vol.SizeKib. Same flag set as Thin's
// resize; LV-thick resize is just `lvextend` over the requested size.
func (t *Thick) ResizeVolume(ctx context.Context, vol storage.Volume) error {
	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	_, err := t.exec.Run(ctx, "lvextend",
		"--size", strconv.FormatInt(sizeMiB, 10)+"MiB",
		t.cfg.VolumeGroup+"/"+volumeLVName(vol))
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
		"--force",
		t.cfg.VolumeGroup+"/"+volumeLVName(vol))
	if err != nil {
		return errors.Wrapf(err, "lvremove %s", volumeLVName(vol))
	}

	return nil
}

// VolumeStatus reports observed disk state via lvs.
func (t *Thick) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return volumeStatusViaLVS(ctx, t.exec, t.cfg.VolumeGroup+"/"+volumeLVName(vol))
}

// PoolStatus reports the VG's free/total capacity.
func (t *Thick) PoolStatus(ctx context.Context) (storage.PoolStatus, error) {
	out, err := t.exec.Run(ctx, "vgs",
		"--noheadings",
		"--separator", "|",
		"-o", "vg_size,vg_free",
		"--units", "k",
		"--nosuffix",
		t.cfg.VolumeGroup)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "vgs")
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return storage.PoolStatus{}, errors.Errorf("vg %s not found", t.cfg.VolumeGroup)
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
func (t *Thick) CreateSnapshot(ctx context.Context, snap storage.Snapshot) error {
	source := fmt.Sprintf("%s_%05d", snap.ResourceName, 0)

	_, err := t.exec.Run(ctx, "lvcreate",
		"--snapshot",
		"--extents", "25%ORIGIN",
		"--name", snapshotLVName(snap),
		t.cfg.VolumeGroup+"/"+source)
	if err != nil {
		return errors.Wrapf(err, "lvcreate --snapshot %s", snapshotLVName(snap))
	}

	return nil
}

// DeleteSnapshot mirrors Thin's.
func (t *Thick) DeleteSnapshot(ctx context.Context, snap storage.Snapshot) error {
	_, err := t.exec.Run(ctx, "lvremove",
		"--force",
		t.cfg.VolumeGroup+"/"+snapshotLVName(snap))
	if err != nil {
		return errors.Wrapf(err, "lvremove -f %s", snapshotLVName(snap))
	}

	return nil
}

// lvExists is the same idempotency primitive Thin uses.
func (t *Thick) lvExists(ctx context.Context, lvName string) bool {
	out, err := t.exec.Run(ctx, "lvs",
		"--noheadings",
		"-o", "lv_name",
		t.cfg.VolumeGroup+"/"+lvName)
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(out)) != ""
}
