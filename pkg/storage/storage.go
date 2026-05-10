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

// Package storage defines the satellite-side storage provider contract.
// Implementations live under pkg/storage/{lvm,zfs,file} and translate
// Volume / Snapshot intent into shell-out calls (lvcreate, zfs create,
// dd, ...) wrapped behind the Exec interface so unit tests can drive
// them without root or real block devices.
package storage

import (
	"context"

	"github.com/cockroachdb/errors"
)

// Sentinel errors returned by Provider implementations. REST handlers and
// reconcilers map these to HTTP statuses / Status conditions.
var (
	// ErrNotFound — the named volume/snapshot/pool does not exist on
	// this node.
	ErrNotFound = errors.New("storage object not found")
	// ErrAlreadyExists — Create called on an object that already exists.
	ErrAlreadyExists = errors.New("storage object already exists")
)

// Volume identifies a block-level volume on a satellite. The triple
// (PoolName, ResourceName, VolumeNumber) uniquely names it; the
// implementation maps that to a backend-specific path (LVM LV, ZFS
// dataset, file).
type Volume struct {
	PoolName       string
	ResourceName   string
	VolumeNumber   int32
	SizeKib        int64
	StoragePoolDir string // for FILE providers
}

// Snapshot is a captured-at-a-point-in-time copy of a Volume on the same
// node. Snapshot shipping (intra-cluster clone / replica expansion) lives
// on the Provider too because zfs/thin tooling differ per provider.
type Snapshot struct {
	PoolName     string
	ResourceName string
	SnapshotName string
}

// VolumeStatus is the observed state the satellite reports back to the
// controller. Empty DevicePath means the volume is not yet provisioned.
type VolumeStatus struct {
	DevicePath   string
	AllocatedKib int64
	UsableKib    int64
	State        string // PROVISIONED / ERROR / NOT_PROVISIONED
}

// PoolStatus mirrors `linstor sp l` output for one pool on this node.
type PoolStatus struct {
	FreeCapacityKib   int64
	TotalCapacityKib  int64
	SupportsSnapshots bool
}

// Provider is the per-storage-kind interface every backend implements.
// Implementations are constructed once per pool by the satellite agent
// and re-used across reconcile cycles.
type Provider interface {
	// Kind returns the LINSTOR provider kind string (e.g. "LVM_THIN").
	Kind() string

	// PoolStatus reports free/total capacity and snapshot capability.
	// Used by the satellite's per-pool reporter.
	PoolStatus(ctx context.Context) (PoolStatus, error)

	// CreateVolume materialises the volume on disk. Idempotent: if the
	// volume already exists with the same size, returns nil.
	CreateVolume(ctx context.Context, vol Volume) error

	// DeleteVolume removes the volume. Idempotent: ErrNotFound is
	// silently swallowed so repeated reconciles converge.
	DeleteVolume(ctx context.Context, vol Volume) error

	// ResizeVolume grows the volume to vol.SizeKib in place. Shrinks
	// MUST be rejected (DRBD doesn't support online shrink and the
	// CSI contract forbids it). Idempotent: a no-op when the volume
	// already matches the target size. The satellite layers
	// cryptsetup resize and drbdadm resize on top — the provider
	// only owns the underlying block device.
	ResizeVolume(ctx context.Context, vol Volume) error

	// VolumeStatus reports observed state. DevicePath empty + State
	// NOT_PROVISIONED means the volume hasn't been created yet.
	VolumeStatus(ctx context.Context, vol Volume) (VolumeStatus, error)

	// CreateSnapshot captures the volume at a point in time. The
	// implementation chooses the on-disk representation (LV snapshot,
	// zfs snapshot, COW copy).
	CreateSnapshot(ctx context.Context, snap Snapshot) error

	// DeleteSnapshot is the inverse of CreateSnapshot.
	DeleteSnapshot(ctx context.Context, snap Snapshot) error

	// Destroy tears the pool itself down on disk: `vgremove
	// --force` for LVM, `zpool destroy` for ZFS, recursive
	// directory removal for FILE/LOOPFILE. Idempotent — a
	// missing pool returns nil so a re-run after a partial
	// teardown finishes cleanly. The satellite's StoragePool
	// reconciler runs this when `Spec.DestroyOnDelete=true` on
	// a CRD with non-zero DeletionTimestamp. Phase 10.8.
	Destroy(ctx context.Context) error
}
