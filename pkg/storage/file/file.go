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
	"io"
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
// Shrinks are rejected (silent no-op) — shrinking a backing file
// under DRBD would corrupt the replicated state.
//
// The grow step honours cfg.Thin: thin → `truncate -s N` widens the
// file with sparse holes (matching the FILE_THIN overcommit
// contract); thick → `truncate -s N` followed by `fallocate -l N` so
// the extended bytes are actually reserved on the backing filesystem.
// Without the fallocate, a thick resize would silently downgrade
// space guarantees — the first write into the extended range could
// ENOSPC even though the file's apparent size grew.
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

	if !p.cfg.Thin {
		// Reserve the extended range on disk so the thick space
		// guarantee survives the resize. fallocate -l N is idempotent
		// against an already-allocated file (no-op if blocks are
		// reserved) and matches the CreateVolume thick path.
		_, err = p.exec.Run(ctx, "fallocate", "-l", strconv.FormatInt(target, 10), path)
		if err != nil {
			return errors.Wrapf(err, "fallocate %s", path)
		}
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
//
// Detach is best-effort: a failing `losetup -d` (e.g. EBUSY while a
// kernel consumer is still tearing down, or the device was already
// auto-cleared) MUST NOT block the unlink. Returning early on a detach
// error would leave the `.img` permanently on disk — the satellite
// reconciler drops its reference once the Resource CRD is removed, so
// the FILE_THIN pool would leak one `<rd>_00000.img` per RD ever
// created and `stand` would eventually run out of free capacity.
func (p *Provider) DeleteVolume(ctx context.Context, vol storage.Volume) error {
	path := p.volumePath(vol)

	dev, lookupErr := p.lookupLoop(ctx, path)
	if lookupErr == nil && dev != "" {
		// Detach is best-effort; the unlink below is the load-bearing
		// step. losetup -d failures are non-fatal so the .img is
		// always reaped on every delete pass.
		_, _ = p.exec.Run(ctx, "losetup", "-d", dev)
	}

	err := os.Remove(path)
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
//
// SupportsSnapshots tracks the thin variant: FILE_THIN volumes are
// sparse, and CreateSnapshot below copies them via `cp --reflink=auto`,
// which is O(1) on reflink-capable filesystems (XFS, btrfs, ZFS-backed)
// and a transparent full-copy fallback otherwise. Upstream LINSTOR
// reports the same — FILE_THIN advertises CanSnapshots=True so
// `linstor s c <rd> <snap>` doesn't refuse the request out-of-hand
// for thin pools. Plain (thick) FILE pools keep SupportsSnapshots=false
// to match upstream and avoid promising a feature the backend can't
// implement efficiently (fallocate-backed files don't reflink).
func (p *Provider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	// An ENOENT on statfs (operator deleted the backing directory
	// out of band) is surfaced as a `pool dir %q not found` error
	// so the satellite's writeCapacity loop flips
	// Status.PoolMissing=true and the wire view in `linstor sp l`
	// lands state=Faulty rather than silently staying state=Ok
	// with zeroed capacity.
	_, err := os.Stat(p.cfg.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return storage.PoolStatus{}, errors.Errorf("pool dir %s not found", p.cfg.Dir)
		}

		return storage.PoolStatus{}, errors.Wrapf(err, "stat %s", p.cfg.Dir)
	}

	free, total, err := diskFree(p.cfg.Dir)
	if err != nil {
		return storage.PoolStatus{}, errors.Wrapf(err, "statfs %s", p.cfg.Dir)
	}

	return storage.PoolStatus{
		FreeCapacityKib:   free / bytesPerKib,
		TotalCapacityKib:  total / bytesPerKib,
		SupportsSnapshots: p.cfg.Thin,
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
//
// The path MUST be resolved through the provider's configured pool
// dir (p.cfg.Dir). The satellite reconciler calls DeleteSnapshot with
// snap.PoolName="" (only ResourceName + SnapshotName are populated),
// so deriving the path from snap.PoolName produces a relative name
// like `proptest_snap1_00000.img` and os.Remove silently no-op's
// against the satellite's cwd — every `linstor s delete` then leaks
// its snapshot file in /var/lib/blockstor-pool/.
func (p *Provider) DeleteSnapshot(_ context.Context, snap storage.Snapshot) error {
	path := p.snapshotPath(snap)

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "remove %s", path)
	}

	return nil
}

// RestoreVolumeFromSnapshot materialises target.img by copying the
// snapshot .img then attaching it to a loop device, matching the
// CreateVolume tail. Pre-existing target → resumed reconcile, no-op.
//
// The copy honours cfg.Thin: thin → `cp --reflink=auto` (upstream
// LINSTOR FILE_THIN behaviour — O(1) on reflink-capable filesystems
// like XFS, btrfs, cow-enabled ext4, with writes diverging lazily);
// thick → plain `cp` (no --reflink) so the new file gets its own
// allocated blocks. Without the reflink-strip on thick, the restored
// "thick" volume would CoW-share blocks with the snapshot and the
// first divergent write could ENOSPC despite the operator-visible
// size reporting full allocation.
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

	if p.cfg.Thin {
		_, err = p.exec.Run(ctx, "cp", "--reflink=auto", srcPath, dstPath)
	} else {
		// Thick: force a full byte copy so the new file has its own
		// allocated blocks (no CoW share with the snapshot).
		_, err = p.exec.Run(ctx, "cp", srcPath, dstPath)
	}

	if err != nil {
		return errors.Wrapf(err, "cp %s → %s", srcPath, dstPath)
	}

	_, err = p.attach(ctx, dstPath)
	if err != nil {
		return errors.Wrapf(err, "losetup %s", dstPath)
	}

	return nil
}

// SendSnapshot opens the snapshot's .img file as a byte stream. For
// FILE/FILE_THIN this is the raw file contents — the receiver's
// RecvSnapshot writes them verbatim into the target.img path. No
// special framing; suitable for piping through HTTP, ssh, or any
// transparent transport.
//
// Returns storage.ErrNotFound when the snapshot file is missing
// (e.g. the calling satellite picked a peer that doesn't host this
// snapshot — caller should pick another peer or fall through to
// DRBD resync).
func (p *Provider) SendSnapshot(_ context.Context, snap storage.Snapshot) (io.ReadCloser, error) {
	path := p.snapshotPath(snap)

	snapshotFile, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Wrapf(storage.ErrNotFound, "snapshot %s", path)
		}

		return nil, errors.Wrapf(err, "open %s", path)
	}

	return snapshotFile, nil
}

// RecvSnapshot writes the byte stream into target's .img and attaches
// a loop device, idempotent across satellite restarts and atomic
// against a mid-transfer crash on either side.
//
// Resilience: stream into `<target>.partial`, then rename to the final
// path on success. On the next reconcile:
//
//   - final exists, .partial absent → resumed; attach loop, return.
//   - final absent, .partial present → previous run aborted before
//     rename; drop the .partial and re-stream from scratch.
//   - both absent → first run; open .partial, stream, rename.
//
// The rename is atomic on Linux ext4/xfs/btrfs/zfs (single-directory,
// same filesystem) so an interrupted recv never leaves the final
// path pointing at a truncated copy.
//
// drbdmeta drop-md + create-md follow on the caller side to stamp
// this node's DRBD node-id over the metadata block embedded in the
// stream.
//
// Bug 250 (P2, space-guarantee, cross-node): in thick mode the .partial
// is pre-allocated via `fallocate -l <SizeKib*1024>` BEFORE io.Copy so
// the backing range is reserved on the host filesystem. Without this,
// a cross-node-cloned thick FILE volume had no on-disk reservation —
// the io.Copy could out-allocate the FS mid-stream and a subsequent
// divergent write could ENOSPC even though the file's apparent size
// looked fine. Thin path skips fallocate (sparse / overcommit is the
// FILE_THIN contract).
func (p *Provider) RecvSnapshot(ctx context.Context, target storage.Volume, src io.Reader) error {
	finalPath := p.volumePath(target)
	partialPath := finalPath + ".partial"

	resumed, err := p.recvSnapshotResumeIfFinal(ctx, finalPath)
	if err != nil || resumed {
		return err
	}

	// Either no prior attempt or one that aborted before rename. Drop
	// the leftover .partial (if any) so OpenFile O_EXCL succeeds and
	// we re-stream from byte zero — partial bytes from a previous
	// crash are unsafe to trust.
	err = os.Remove(partialPath)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "remove stale %s", partialPath)
	}

	dst, err := os.OpenFile(partialPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, volumeFilePerm)
	if err != nil {
		return errors.Wrapf(err, "create %s", partialPath)
	}

	if !p.cfg.Thin {
		// Reserve the full expected range on disk BEFORE streaming so
		// the io.Copy can never out-allocate the FS — peer-cloned thick
		// FILE volumes must honour the thick space guarantee (Bug 250).
		// fallocate -l is idempotent on a freshly-created empty file.
		sizeBytes := target.SizeKib * bytesPerKib

		_, falErr := p.exec.Run(ctx, "fallocate", "-l", strconv.FormatInt(sizeBytes, 10), partialPath)
		if falErr != nil {
			_ = dst.Close()
			_ = os.Remove(partialPath)

			return errors.Wrapf(falErr, "fallocate %s", partialPath)
		}
	}

	_, copyErr := io.Copy(dst, src)
	closeErr := dst.Close()

	if copyErr != nil {
		_ = os.Remove(partialPath)

		return errors.Wrapf(copyErr, "stream into %s", partialPath)
	}

	if closeErr != nil {
		_ = os.Remove(partialPath)

		return errors.Wrapf(closeErr, "close %s", partialPath)
	}

	// Atomic publish: the .partial → final rename is the single point
	// after which a future reconcile must consider this recv complete.
	err = os.Rename(partialPath, finalPath)
	if err != nil {
		_ = os.Remove(partialPath)

		return errors.Wrapf(err, "rename %s → %s", partialPath, finalPath)
	}

	_, err = p.attach(ctx, finalPath)
	if err != nil {
		return errors.Wrapf(err, "losetup %s", finalPath)
	}

	return nil
}

// recvSnapshotResumeIfFinal handles the resumed-reconcile branch of
// RecvSnapshot: if `finalPath` already exists, re-attach the loop dev
// and signal "done" (resumed=true). Returns (false, nil) when there's
// no prior recv to resume — the caller proceeds to the fresh-stream
// path. Extracted from RecvSnapshot to keep funlen under control after
// the Bug 250 fallocate addition.
func (p *Provider) recvSnapshotResumeIfFinal(ctx context.Context, finalPath string) (bool, error) {
	_, statErr := os.Stat(finalPath)
	if statErr == nil {
		_, err := p.attach(ctx, finalPath)
		if err != nil {
			return false, errors.Wrapf(err, "losetup %s", finalPath)
		}

		return true, nil
	}

	if !os.IsNotExist(statErr) {
		return false, errors.Wrapf(statErr, "stat %s", finalPath)
	}

	return false, nil
}

// snapshotPath is `<dir>/<resource>_<snap>_00000.img`. Matches the
// LV-side `<rd>_<snap>_00000` naming (volume #0 only — multi-volume
// snapshots land in Phase 4).
func (p *Provider) snapshotPath(snap storage.Snapshot) string {
	return filepath.Join(p.cfg.Dir, fmt.Sprintf("%s_%s_00000.img", snap.ResourceName, snap.SnapshotName))
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
	// volumeFilePerm — owner-only read+write. The FILE storage pool
	// dir already restricts access; the satellite runs as root.
	volumeFilePerm = 0o600
)
