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
// regular files under a configured directory, mounted by DRBD via
// loop devices. Thick volumes are fallocate-pre-allocated; thin volumes
// are sparse (truncated to size, allocated on first write).
//
// This is the simplest provider — useful as the dev-stand backend
// because it needs no LVM/ZFS host setup.
package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

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

// Provider implements storage.Provider against regular files.
type Provider struct {
	cfg  Config
	exec storage.Exec
}

// NewProvider constructs a file Provider. The Exec is injected so
// tests can drive fallocate/truncate without actually allocating.
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

// CreateVolume idempotently creates the backing file at full size
// (fallocate) or as a sparse file (truncate). Existing file → no-op.
func (p *Provider) CreateVolume(ctx context.Context, vol storage.Volume) error {
	path := p.volumePath(vol)

	_, err := os.Stat(path)
	if err == nil {
		return nil
	}

	if !os.IsNotExist(err) {
		return errors.Wrapf(err, "stat %s", path)
	}

	sizeBytes := vol.SizeKib * bytesPerKib

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

	return nil
}

// ResizeVolume grows the backing file to vol.SizeKib bytes. truncate
// is the right tool for both thick (fallocate-created) and thin
// (truncate-created) cases — when the file is shorter than the new
// size, the OS extends with sparse zero-bytes. Shrinks are rejected
// to match the rest of the provider contract.
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

	return nil
}

// DeleteVolume removes the backing file. Missing → no-op.
func (p *Provider) DeleteVolume(_ context.Context, vol storage.Volume) error {
	path := p.volumePath(vol)

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "remove %s", path)
	}

	return nil
}

// VolumeStatus stats the file. Missing file → NOT_PROVISIONED.
func (p *Provider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	path := p.volumePath(vol)

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return storage.VolumeStatus{State: stateNotProvisioned}, nil
		}

		return storage.VolumeStatus{}, errors.Wrapf(err, "stat %s", path)
	}

	sizeKib := info.Size() / bytesPerKib

	return storage.VolumeStatus{
		DevicePath:   path,
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

// CreateSnapshot — file backend has no first-class snapshot. We could
// hard-link or `cp --reflink=auto` but that's lossy for thick volumes.
// Until we know the underlying FS supports reflinks, we surface "not
// supported" via the error so callers don't silently get a coherent
// but uncorrelated copy.
func (*Provider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error {
	return errors.New("file backend does not support snapshots; use a reflink-capable FS or LVM/ZFS")
}

// DeleteSnapshot mirrors CreateSnapshot.
func (*Provider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error {
	return errors.New("file backend does not support snapshots")
}

// volumePath is `<dir>/<resource>_<vol5digits>.img`.
func (p *Provider) volumePath(vol storage.Volume) string {
	return filepath.Join(p.cfg.Dir, fmt.Sprintf("%s_%05d.img", vol.ResourceName, vol.VolumeNumber))
}

const (
	stateNotProvisioned = "NOT_PROVISIONED"
	bytesPerKib         = 1024
)
