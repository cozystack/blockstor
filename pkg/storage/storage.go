// SPDX-License-Identifier: Apache-2.0

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
	"io"

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
	// ErrTerminal — the provider has determined the operation cannot
	// succeed on a retry. The satellite snapshot reconciler tests for
	// this via errors.Is and stamps `Status.Flags=["FAILED"]` on the
	// Snapshot CRD rather than requeueing indefinitely. Wrap a
	// transient cause (lvm temporary lock, busy dataset) WITHOUT this
	// sentinel to keep retry semantics intact.
	ErrTerminal = errors.New("provider terminal error")
	// ErrPoolGone — PoolStatus reports the backing pool is definitely
	// not present (operator ran `zpool destroy` / `vgremove` / removed
	// the file-thin dir). The satellite controller writeCapacity
	// path tests for this via errors.Is and flips
	// Status.PoolMissing=true. Any OTHER PoolStatus error preserves
	// the prior Status (Bug 282): a transient lvs/zfs hiccup, a
	// withProbeTimeout (Bug 270/271) cancel under load, or a "pool
	// is full but reachable" probe must NOT be mis-classified as
	// "pool gone".
	ErrPoolGone = errors.New("storage pool not present")
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

	// DeletedSuffix is the suffix the snapshot's zvol was renamed with
	// when its replica was deleted (see SuffixedDeleter), recorded so
	// the snapshot is found under `_deleted_<name>_<suffix>`. Empty means
	// the zvol is looked for under its own name, and then by search.
	DeletedSuffix string
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

	// RestoreVolumeFromSnapshot materialises target as a clone of
	// sourceSnapshot. Both must live in the same pool on the same
	// node (cross-node restore happens via DRBD network resync after
	// at least one peer was restored locally). Idempotent: if target
	// already exists with the same size it returns nil. Upstream
	// LINSTOR pattern:
	//   ZFS / ZFS_THIN: `zfs clone <pool>/<src>@<snap> <pool>/<tgt>`
	//   LVM_THIN:       `lvcreate -s --kernel --activate y \
	//                       --name <tgt> <pool>/<src-snap>`
	//   FILE / FILE_THIN: `cp --reflink=auto <src-snap>.img <tgt>.img`
	RestoreVolumeFromSnapshot(ctx context.Context, target Volume, sourceSnapshot Snapshot) error
}

// SuffixedDeleter is the optional capability of a provider that cannot
// always delete a volume at once. ZFS renames a zvol that still has
// snapshots to `_deleted_<name>_<suffix>` instead; the suffix is recorded
// before the delete so the snapshots are found under the new name later.
// An empty suffix lets the provider pick one, as DeleteVolume does.
type SuffixedDeleter interface {
	DeleteVolumeWithSuffix(ctx context.Context, vol Volume, deletedSuffix string) error
}

// SnapshotShipper is the optional cross-node-clone capability — a
// provider that implements it can stream a snapshot to a peer
// satellite and reconstitute it on the receiving side. ZFS / LVM
// thin / FILE all support this; legacy backends don't. Callers do a
// type-assert and fall back to DRBD initial-sync when the assert
// fails.
//
// Pulled out of Provider so the base interface stays under the
// interfacebloat budget — most call sites don't need the shipper
// methods at all.
type SnapshotShipper interface {
	// SendSnapshot opens a byte stream of the snapshot's contents
	// suitable for receiving on another node. ZFS returns
	// `zfs send` (incremental-replay format); LVM thin returns
	// `thin_send` (binary delta); FILE returns `dd` of the
	// file (raw bytes). The caller closes the reader when done.
	SendSnapshot(ctx context.Context, snap Snapshot) (io.ReadCloser, error)

	// RecvSnapshot materialises target from a stream produced by
	// SendSnapshot on a peer satellite. After this returns, target
	// is fully populated with the snapshot's bytes; the caller
	// wires drbdmeta drop-md + create-md to stamp fresh DRBD
	// metadata for the local node-id.
	RecvSnapshot(ctx context.Context, target Volume, src io.Reader) error
}

// ResizeZeroFiller is the optional "grown region reads back as zeros"
// capability (Bug 395, P1 data integrity). A provider implements it to
// declare whether the bytes exposed by a grow (the region
// [old_size, new_size)) are deterministically zero on every replica.
//
// This gates the `--assume-clean` fast path on `drbdadm resize`: that
// flag tells DRBD to mark the grown region UpToDate on every replica
// WITHOUT a resync. That is sound ONLY when the grown bytes are
// guaranteed identical across replicas. Zero-on-allocate backends
// (ZFS/ZFS_THIN refreservation reads zeros for never-written blocks,
// LVM_THIN zeroes new thin chunks, FILE/FILE_THIN grow into a hole that
// reads as zeros) satisfy this. Classic thick `LVM` does NOT:
// `lvextend` exposes recycled VG extents whose prior content differs
// per node, so `--assume-clean` would silently leave the replicas
// disagreeing on [old_size, new_size) with no out-of-sync flag — a
// failover then changes the bytes an application reads.
//
// A provider that does NOT implement this interface is treated as
// NOT zero-fill (the safe default): the satellite omits `--assume-clean`
// so DRBD marks the grown region out-of-sync and resyncs it from the
// UpToDate source, guaranteeing every replica agrees on the grown
// bytes. Implemented as an optional capability so the base Provider
// interface stays under the interfacebloat budget.
type ResizeZeroFiller interface {
	// ResizeZeroFills reports whether a grow on this provider yields a
	// region that reads back as zeros on every replica. true → the
	// `drbdadm resize --assume-clean` fast path is sound; false →
	// DRBD must resync the grown region.
	ResizeZeroFills() bool
}
