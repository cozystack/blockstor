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

// Package loopfile is a sparse-file-backed-by-losetup storage backend.
// It fits the dev stand: Talos workers don't expose a free block
// device, but they do mount /var/lib/* as ext4 — fallocate a file,
// run losetup, and DRBD has a `disk` it can attach.
//
// Snapshots aren't supported (the underlying FS rarely has reflinks
// the satellite can rely on); callers that want them should route the
// resource onto an LVM-thin / ZFS pool instead.
package loopfile

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

// Config parametrises a loopfile Provider.
type Config struct {
	// Dir is the on-disk directory where backing sparse files live.
	// Must already exist; the provider does not create it.
	Dir string
}

// Provider implements storage.Provider against a directory of sparse
// files mapped via losetup.
type Provider struct {
	cfg  Config
	exec storage.Exec
}

// NewProvider constructs a Provider. Exec runs `truncate` and
// `losetup`; tests inject FakeExec.
func NewProvider(cfg Config, ex storage.Exec) *Provider {
	return &Provider{cfg: cfg, exec: ex}
}

// Kind reports the LINSTOR-shaped provider kind. We expose this as
// FILE_THIN — there's no upstream "LOOPFILE" kind, and FILE_THIN is
// the closest semantic match (sparse, single-file backed).
func (*Provider) Kind() string { return "FILE_THIN" }

// CreateVolume idempotently:
//   - truncates a sparse file at <Dir>/<resource>_<vol5>.img
//   - runs `losetup --find --show -P <file>` to attach
//
// `losetup --show` already returns the existing loop dev when one is
// associated, so re-runs no-op cleanly.
func (p *Provider) CreateVolume(ctx context.Context, vol storage.Volume) error {
	path := p.volumePath(vol)
	sizeBytes := vol.SizeKib * bytesPerKib

	_, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrapf(err, "stat %s", path)
		}

		_, err = p.exec.Run(ctx, "truncate", "-s", strconv.FormatInt(sizeBytes, 10), path)
		if err != nil {
			return errors.Wrapf(err, "truncate %s", path)
		}
	}

	_, err = p.attach(ctx, path)
	if err != nil {
		return errors.Wrapf(err, "losetup %s", path)
	}

	return nil
}

// ResizeVolume grows the backing file and re-runs losetup --set-capacity
// so the kernel picks up the new size on the loop device. Shrinks are
// rejected. Idempotent: a no-op when current size already meets target.
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

	dev, err := p.lookupLoop(ctx, path)
	if err == nil && dev != "" {
		_, err = p.exec.Run(ctx, "losetup", "-c", dev)
		if err != nil {
			return errors.Wrapf(err, "losetup -c %s", dev)
		}
	}

	return nil
}

// DeleteVolume detaches the loop device (if any) and removes the file.
// Missing → no-op.
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

// VolumeStatus reports DevicePath = the current /dev/loopN. Missing
// file → NOT_PROVISIONED.
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

// PoolStatus reports total/free for the underlying directory's
// filesystem. We reuse the same statfs path the file backend uses; if
// callers need finer-grained accounting they should be on LVM/ZFS.
func (*Provider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	// Intentionally minimal — the loopfile pool is for the dev stand
	// where capacity tracking isn't load-bearing. Phase 6 wires
	// statfs(2) here once a use case shows up.
	return storage.PoolStatus{
		FreeCapacityKib:   0,
		TotalCapacityKib:  0,
		SupportsSnapshots: false,
	}, nil
}

// CreateSnapshot returns "not supported"; route via LVM-thin / ZFS.
func (*Provider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error {
	return errors.New("loopfile backend does not support snapshots")
}

// DeleteSnapshot mirrors CreateSnapshot.
func (*Provider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error {
	return errors.New("loopfile backend does not support snapshots")
}

// attach is the idempotent loop-attach step. We pre-check via
// `losetup -j <path>` and reuse the existing /dev/loopN if there is
// one — `--find --show` always allocates a fresh dev, which on
// reconcile-heavy paths would leak hundreds of loop nodes pointing at
// the same backing file (observed in practice).
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
