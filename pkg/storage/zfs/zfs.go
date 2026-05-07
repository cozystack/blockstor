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

// Package zfs is the ZFS-pool storage backend. It supports both the
// LINSTOR `ZFS` (thick) and `ZFS_THIN` (sparse zvols) provider kinds via
// the same Provider; Config.Thin toggles between them.
package zfs

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Config parametrises a Provider with the ZFS pool name and whether it
// runs in thin (sparse zvol) mode.
type Config struct {
	Pool string
	Thin bool
}

// Provider implements storage.Provider against ZFS on the host.
type Provider struct {
	cfg  Config
	exec storage.Exec
}

// NewProvider constructs a Provider. The Exec is injected so unit tests
// can drive it without ZFS installed.
func NewProvider(cfg Config, ex storage.Exec) *Provider {
	return &Provider{cfg: cfg, exec: ex}
}

// Kind returns "ZFS" or "ZFS_THIN" depending on Config.Thin.
func (p *Provider) Kind() string {
	if p.cfg.Thin {
		return "ZFS_THIN"
	}

	return "ZFS"
}

// CreateVolume creates a zvol. Idempotent: existing dataset → no-op.
func (p *Provider) CreateVolume(ctx context.Context, vol storage.Volume) error {
	if p.datasetExists(ctx, p.volumeDataset(vol)) {
		return nil
	}

	sizeMiB := max(vol.SizeKib/mibPerKib, 1)

	args := []string{"create"}
	if p.cfg.Thin {
		args = append(args, "-s")
	}

	args = append(args, "-V", strconv.FormatInt(sizeMiB, 10)+"M", p.volumeDataset(vol))

	_, err := p.exec.Run(ctx, "zfs", args...)
	if err != nil {
		return errors.Wrapf(err, "zfs create %s", p.volumeDataset(vol))
	}

	return nil
}

// DeleteVolume `zfs destroy -r`s the zvol (recursive to clean up any
// dependent snapshots automatically). Missing → no-op.
func (p *Provider) DeleteVolume(ctx context.Context, vol storage.Volume) error {
	if !p.datasetExists(ctx, p.volumeDataset(vol)) {
		return nil
	}

	_, err := p.exec.Run(ctx, "zfs", "destroy", "-r", p.volumeDataset(vol))
	if err != nil {
		return errors.Wrapf(err, "zfs destroy %s", p.volumeDataset(vol))
	}

	return nil
}

// VolumeStatus parses `zfs list -p` output (bytes, no suffixes).
func (p *Provider) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	out, err := p.exec.Run(ctx, "zfs",
		"list", "-H", "-p",
		"-o", "name,volsize,used",
		p.volumeDataset(vol))
	if err != nil {
		return storage.VolumeStatus{State: stateNotProvisioned}, nil //nolint:nilerr // missing == not provisioned
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return storage.VolumeStatus{State: stateNotProvisioned}, nil
	}

	parts := strings.Split(line, "\t")
	if len(parts) != zfsListCols {
		return storage.VolumeStatus{}, errors.Errorf("zfs list: unexpected line %q", line)
	}

	volSizeBytes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return storage.VolumeStatus{}, errors.Wrap(err, "parse volsize")
	}

	usedBytes, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return storage.VolumeStatus{}, errors.Wrap(err, "parse used")
	}

	return storage.VolumeStatus{
		DevicePath:   "/dev/zvol/" + p.volumeDataset(vol),
		UsableKib:    volSizeBytes / bytesPerKib,
		AllocatedKib: usedBytes / bytesPerKib,
		State:        "PROVISIONED",
	}, nil
}

// PoolStatus reads `zpool list -p` for free/total bytes.
func (p *Provider) PoolStatus(ctx context.Context) (storage.PoolStatus, error) {
	out, err := p.exec.Run(ctx, "zpool", "list", "-H", "-p", "-o", "size,free", p.cfg.Pool)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrapf(err, "zpool list %s", p.cfg.Pool)
	}

	parts := strings.Split(strings.TrimSpace(string(out)), "\t")
	if len(parts) != zpoolListCols {
		return storage.PoolStatus{}, errors.Errorf("zpool list: unexpected output %q", out)
	}

	sizeBytes, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse size")
	}

	freeBytes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrap(err, "parse free")
	}

	return storage.PoolStatus{
		FreeCapacityKib:   freeBytes / bytesPerKib,
		TotalCapacityKib:  sizeBytes / bytesPerKib,
		SupportsSnapshots: true,
	}, nil
}

// CreateSnapshot is `zfs snapshot <pool>/<rd>_00000@<snap>`.
func (p *Provider) CreateSnapshot(ctx context.Context, snap storage.Snapshot) error {
	_, err := p.exec.Run(ctx, "zfs", "snapshot", p.snapshotDataset(snap))
	if err != nil {
		return errors.Wrapf(err, "zfs snapshot %s", p.snapshotDataset(snap))
	}

	return nil
}

// DeleteSnapshot is the inverse `zfs destroy <pool>/<rd>_00000@<snap>`.
func (p *Provider) DeleteSnapshot(ctx context.Context, snap storage.Snapshot) error {
	_, err := p.exec.Run(ctx, "zfs", "destroy", p.snapshotDataset(snap))
	if err != nil {
		return errors.Wrapf(err, "zfs destroy %s", p.snapshotDataset(snap))
	}

	return nil
}

// datasetExists is the idempotency primitive — analogous to lvExists.
func (p *Provider) datasetExists(ctx context.Context, ds string) bool {
	out, err := p.exec.Run(ctx, "zfs", "list", "-H", "-o", "name", ds)
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(out)) != ""
}

// volumeDataset is `<pool>/<resource>_<vol5digits>`.
func (p *Provider) volumeDataset(vol storage.Volume) string {
	return fmt.Sprintf("%s/%s_%05d", p.cfg.Pool, vol.ResourceName, vol.VolumeNumber)
}

// snapshotDataset is `<pool>/<resource>_00000@<snap>`. ZFS snapshots are
// per-dataset, so they're addressed via the parent zvol; we always
// snapshot volume 0 (multi-volume support lands in Phase 4).
func (p *Provider) snapshotDataset(snap storage.Snapshot) string {
	return fmt.Sprintf("%s/%s_00000@%s", p.cfg.Pool, snap.ResourceName, snap.SnapshotName)
}

const (
	stateNotProvisioned = "NOT_PROVISIONED"
	mibPerKib           = 1024
	bytesPerKib         = 1024
	zfsListCols         = 3
	zpoolListCols       = 2
)
