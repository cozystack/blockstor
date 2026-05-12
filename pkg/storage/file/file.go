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

// Package file is the FILE / FILE_THIN storage backend. Volumes are
// regular files under a configured directory, attached to a
// /dev/loopN via `losetup --find --show` so DRBD (and any other
// consumer that needs a block device) can use them. Thick volumes
// are fallocate-pre-allocated; thin volumes are sparse (truncated
// to size, allocated on first write).
//
// This mirrors upstream LINSTOR's FILE / FILE_THIN providers — the
// file is the backing store, the loop dev is the consumer-facing
// `disk` path. `losetup --find` goes through /dev/loop-control so
// the same dir can hold hundreds of files without hardcoded numbers.
package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Config parametrises the file backend.
type Config struct {
	// Dir is the on-disk directory where volume files live. Must
	// already exist; the provider does not create it.
	Dir string

	// Thin selects sparse (truncate-only) vs. thick (fallocate)
	// allocation.
	Thin bool
}

// Provider implements storage.Provider against regular files + losetup.
type Provider struct {
	cfg  Config
	exec storage.Exec
}

// NewProvider constructs a file Provider. The Exec is injected so
// tests can drive fallocate/truncate/losetup without touching the
// kernel.
func NewProvider(cfg Config, ex storage.Exec) *Provider {
	return &Provider{cfg: cfg, exec: ex}
}

// Kind returns "FILE" or "FILE_THIN" per Config.Thin.
func (p *Provider) Kind() string {
	if p.cfg.Thin {
		return "FILE_THIN"
	}

	return "FILE"
}

// CreateVolume idempotently:
//   - creates the backing file at full size (fallocate, thick) or
//     as a sparse file (truncate, thin)
//   - attaches it to a /dev/loopN via `losetup --find --show`
//
// Re-runs no-op cleanly: stat skips the allocation step and the
// attach helper reuses any existing loop dev for the file.
func (p *Provider) CreateVolume(ctx context.Context, vol storage.Volume) error {
	path := p.volumePath(vol)
	sizeBytes := vol.SizeKib * bytesPerKib

	_, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrapf(err, "stat %s", path)
		}

		tool := "fallocate"
		flag := "-l"

		if p.cfg.Thin {
			tool = "truncate"
			flag = "-s"
		}

		_, err = p.exec.Run(ctx, tool, flag, strconv.FormatInt(sizeBytes, 10), path)
		if err != nil {
			return errors.Wrapf(err, "%s %s", tool, path)
		}
	}

	_, err = p.attach(ctx, path)
	if err != nil {
		return errors.Wrapf(err, "losetup %s", path)
	}

	return nil
}

// ResizeVolume grows the backing file to vol.SizeKib bytes and
// refreshes the loop device's reported capacity with `losetup -c`.
// truncate handles both thick (fallocate-created) and thin
// (truncate-created) cases. Shrinks are rejected.
func (p *Provider) ResizeVolume(ctx context.Context, vol storage.Volume) error {
	path := p.volumePath(vol)

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Wrapf(storage.ErrNotFound, "resize %s", path)
		}

		return errors.Wrapf(err, "stat %s", path)
	}

	target := vol.SizeKib * bytesPerKib

	if info.Size() >= target {
		return nil
	}

	_, err = p.exec.Run(ctx, "truncate", "-s", strconv.FormatInt(target, 10), path)
	if err != nil {
		return errors.Wrapf(err, "truncate %s", path)
	}

	dev, lerr := p.lookupLoop(ctx, path)
	if lerr == nil && dev != "" {
		_, err = p.exec.Run(ctx, "losetup", "-c", dev)
		if err != nil {
			return errors.Wrapf(err, "losetup -c %s", dev)
		}
	}

	return nil
}

// DeleteVolume detaches the loop device (if any) and removes the
// backing file. Missing file → no-op.
func (p *Provider) DeleteVolume(ctx context.Context, vol storage.Volume) error {
	path := p.volumePath(vol)

	dev, err := p.lookupLoop(ctx, path)
	if err == nil && dev != "" {
		_, detachErr := p.exec.Run(ctx, "losetup", "-d", dev)
		if detachErr != nil {
			return errors.Wrapf(detachErr, "losetup -d %s", dev)
		}
	}

	err = os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "remove %s", path)
	}

	return nil
}

// VolumeStatus stats the file and reports DevicePath = the current
// /dev/loopN. Missing file → NOT_PROVISIONED. The status path also
// attaches the loop dev if it is not currently associated, so a
// satellite restart re-establishes the loop before DRBD picks the
// .res back up.
func (p *Provider) VolumeStatus(ctx context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	path := p.volumePath(vol)

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return storage.VolumeStatus{State: stateNotProvisioned}, nil
		}

		return storage.VolumeStatus{}, errors.Wrapf(err, "stat %s", path)
	}

	dev, err := p.attach(ctx, path)
	if err != nil {
		return storage.VolumeStatus{}, errors.Wrapf(err, "losetup %s", path)
	}

	sizeKib := info.Size() / bytesPerKib

	return storage.VolumeStatus{
		DevicePath:   dev,
		UsableKib:    sizeKib,
		AllocatedKib: sizeKib,
		State:        "PROVISIONED",
	}, nil
}

// PoolStatus reports the directory's free / total bytes via statfs.
func (p *Provider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	free, total, err := diskFree(p.cfg.Dir)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrapf(err, "statfs %s", p.cfg.Dir)
	}

	return storage.PoolStatus{
		FreeCapacityKib:   free / bytesPerKib,
		TotalCapacityKib:  total / bytesPerKib,
		SupportsSnapshots: false,
	}, nil
}

// CreateSnapshot captures the volume by copying its backing file
// with `cp --reflink=auto`. On a reflink-capable FS (XFS, btrfs,
// most modern ext4 on copy-on-write FS, ZFS-backed) this is O(1)
// + CoW; otherwise cp falls back to a full byte copy. Snapshots
// always live next to the volume in the same pool dir.
//
// The CSI snapshot-restore path needs this to function — `linstor
// snapshot create` over a FILE_THIN pool used to refuse outright,
// which broke clone / snapshot-restore-cross-node e2e on the dev
// stand (the only pool there is file_thin).
func (p *Provider) CreateSnapshot(ctx context.Context, snap storage.Snapshot) error {
	srcPath := p.volumePathByResource(snap.ResourceName, 0)
	dstPath := p.snapshotPath(snap)

	_, err := os.Stat(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Wrapf(storage.ErrNotFound, "snapshot source %s", srcPath)
		}

		return errors.Wrapf(err, "stat %s", srcPath)
	}

	// Idempotent: presence of the snapshot file → resumed reconcile.
	_, err = os.Stat(dstPath)
	if err == nil {
		return nil
	}

	_, err = p.exec.Run(ctx, "cp", "--reflink=auto", srcPath, dstPath)
	if err != nil {
		return errors.Wrapf(err, "cp --reflink %s → %s", srcPath, dstPath)
	}

	return nil
}

// DeleteSnapshot removes the .img copy. Missing → no-op.
func (*Provider) DeleteSnapshot(_ context.Context, snap storage.Snapshot) error {
	path := snapshotPathRaw(snap)

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "remove %s", path)
	}

	return nil
}

// RestoreVolumeFromSnapshot materialises target.img by reflink-copying
// the snapshot .img then attaching it to a loop device, matching the
// CreateVolume tail. Pre-existing target → resumed reconcile, no-op.
//
// Upstream LINSTOR for FILE/FILE_THIN: `cp --reflink=auto <snap>.img
// <vol>.img`. Reflink keeps the copy O(1); writes diverge lazily.
func (p *Provider) RestoreVolumeFromSnapshot(ctx context.Context, target storage.Volume, src storage.Snapshot) error {
	dstPath := p.volumePath(target)

	_, err := os.Stat(dstPath)
	if err == nil {
		// Target file exists — still need to ensure the loop is up.
		_, err = p.attach(ctx, dstPath)
		if err != nil {
			return errors.Wrapf(err, "losetup %s", dstPath)
		}

		return nil
	}

	if !os.IsNotExist(err) {
		return errors.Wrapf(err, "stat %s", dstPath)
	}

	srcPath := p.snapshotPath(src)

	_, sErr := os.Stat(srcPath)
	if sErr != nil {
		if os.IsNotExist(sErr) {
			return errors.Wrapf(storage.ErrNotFound, "snapshot %s for clone", srcPath)
		}

		return errors.Wrapf(sErr, "stat %s", srcPath)
	}

	_, err = p.exec.Run(ctx, "cp", "--reflink=auto", srcPath, dstPath)
	if err != nil {
		return errors.Wrapf(err, "cp --reflink %s → %s", srcPath, dstPath)
	}

	_, err = p.attach(ctx, dstPath)
	if err != nil {
		return errors.Wrapf(err, "losetup %s", dstPath)
	}

	return nil
}

// snapshotPath is `<dir>/<resource>_<snap>_00000.img`. Matches the
// LV-side `<rd>_<snap>_00000` naming (volume #0 only — multi-volume
// snapshots land in Phase 4).
func (p *Provider) snapshotPath(snap storage.Snapshot) string {
	return filepath.Join(p.cfg.Dir, fmt.Sprintf("%s_%s_00000.img", snap.ResourceName, snap.SnapshotName))
}

// snapshotPathRaw is the package-level shape used by DeleteSnapshot
// (which doesn't have access to cfg.Dir at that callsite when the
// receiver is the bare type).
func snapshotPathRaw(snap storage.Snapshot) string {
	return filepath.Join(snap.PoolName, fmt.Sprintf("%s_%s_00000.img", snap.ResourceName, snap.SnapshotName))
}

// volumePathByResource is volumePath but indexed by resource name +
// vol number directly, for snapshot-source lookups.
func (p *Provider) volumePathByResource(rd string, vol int32) string {
	return filepath.Join(p.cfg.Dir, fmt.Sprintf("%s_%05d.img", rd, vol))
}

// attach is the idempotent loop-attach step. We pre-check via
// `losetup -j <path>` and reuse the existing /dev/loopN if there
// is one — `--find --show` always allocates a fresh dev, which on
// reconcile-heavy paths would leak hundreds of loop nodes pointing
// at the same backing file.
func (p *Provider) attach(ctx context.Context, path string) (string, error) {
	dev, err := p.lookupLoop(ctx, path)
	if err != nil {
		return "", err
	}

	if dev != "" {
		return dev, nil
	}

	out, err := p.exec.Run(ctx, "losetup", "--find", "--show", path)
	if err != nil {
		return "", errors.Wrapf(err, "losetup --find --show %s", path)
	}

	dev = strings.TrimSpace(string(out))
	if dev == "" {
		return "", errors.Errorf("losetup returned empty device for %s", path)
	}

	return dev, nil
}

// lookupLoop greps `losetup -j <file>` for an existing loop device.
// Empty result → no attach. Returns ("", nil) cleanly when nothing
// matches; only real exec failures bubble up.
func (p *Provider) lookupLoop(ctx context.Context, path string) (string, error) {
	out, err := p.exec.Run(ctx, "losetup", "-j", path)
	if err != nil {
		return "", errors.Wrapf(err, "losetup -j %s", path)
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return "", nil
	}

	// Format: `/dev/loopN: [hex] inode (path)`. We only need the
	// /dev/loopN prefix.
	colon := strings.Index(line, ":")
	if colon <= 0 {
		return "", nil
	}

	return line[:colon], nil
}

// volumePath is `<dir>/<resource>_<vol5digits>.img`.
func (p *Provider) volumePath(vol storage.Volume) string {
	return filepath.Join(p.cfg.Dir, fmt.Sprintf("%s_%05d.img", vol.ResourceName, vol.VolumeNumber))
}

const (
	stateNotProvisioned = "NOT_PROVISIONED"
	bytesPerKib         = 1024
)
