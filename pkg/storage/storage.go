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

// VolumeLister is the optional "enumerate every volume this provider
// currently holds on disk" capability. The orphan-storage sweeper
// (see pkg/satellite/controllers/storage_sweeper.go) uses it to
// diff on-disk volumes against the Resource CRDs scheduled to this
// satellite and reap any volume whose owning CRD vanished without
// the satellite's finalizer running its DeleteVolume — the failure
// mode Bug 43 documents (controller force-strips the finalizer
// after a hung satellite, the satellite-side DeleteResource never
// fires, the ZVOL / LV survives).
//
// Pulled out of Provider for the same interfacebloat reason as
// SnapshotShipper — most call sites don't need to enumerate, and a
// provider that genuinely cannot enumerate (e.g. an opaque
// network-backed kind) keeps working without implementing this.
type VolumeLister interface {
	// ListVolumeNames returns one entry per on-disk volume this
	// provider owns. ResourceName + VolumeNumber are parsed out of
	// the backend-specific path naming `<resource>_<vol5digits>`;
	// PoolName is filled by the caller (the sweeper, which knows
	// which pool it's iterating). Implementations skip artefacts
	// they don't recognise (e.g. ZFS deferred-delete markers
	// containing `__DELETED__`) so the sweeper never tries to GC
	// a name that doesn't match the CRD-owned convention.
	ListVolumeNames(ctx context.Context) ([]VolumeRef, error)
}

// VolumeRef is the lightweight identifier the sweeper consumes —
// just enough to call DeleteVolume on the matching provider when an
// orphan is detected. Distinct from Volume (which carries size and
// other materialise-time fields) so the listing path doesn't have
// to fabricate stub values that would later confuse the caller.
type VolumeRef struct {
	// PoolName is filled by the sweeper from the storage-pool the
	// provider is registered under, NOT parsed from the backend.
	// Two providers backed by the same ZFS pool would be the same
	// physical storage but a different logical pool from blockstor's
	// PoV; the registry name is the source of truth.
	PoolName string

	// ResourceName matches the RD name (e.g. "pvc-aaa"). The
	// satellite-side DeleteResource takes the RD name, not the
	// per-node CRD name `<rd>.<node>`.
	ResourceName string

	// VolumeNumber is the per-RD volume index (0-based). RDs
	// today have at most one volume; the field exists so the
	// sweeper stays correct when multi-volume RDs land.
	VolumeNumber int32
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
