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

// Package intent hosts the in-process value objects the controller
// dispatcher passes to the satellite apply chain. Phase 10.6 retired
// the gRPC wire that lived under `pkg/satellite/proto`; these types
// are plain Go structs the controller-runtime reconcilers, the
// satellite `Reconciler`, and `dispatcher.BuildDesired` exchange
// in-process.
//
// Getter methods (`GetX`) are kept so the migration from the
// generated-proto shape was a no-op at every call site.
package intent

// DesiredResource is the satellite-facing apply payload for one
// per-node Resource: which RD, which node, the flags
// (DISKLESS/EVICTED/...), the resolved DRBD options + peer list +
// per-volume slots, and the layer composition.
type DesiredResource struct {
	Name        string
	NodeName    string
	Flags       []string
	Props       map[string]string
	Volumes     []*DesiredVolume
	Peers       []string
	DrbdOptions map[string]string

	// LayerStack is the resolved composition (["DRBD","STORAGE"]
	// = default; ["LUKS","STORAGE"] = no DRBD; ["STORAGE"] =
	// single-replica local mode). Empty == default.
	LayerStack []string

	// Connections carries the explicit multi-path entries the
	// dispatcher decoded off RD.Spec.Props (scenario 3.7,
	// UG9 §"Creating multiple DRBD paths with LINSTOR"). Empty when
	// the RD has no explicit per-peer paths — the satellite then
	// falls back to drbd-9's default single-host-pair render.
	Connections []DesiredConnection

	// MetadataCreated mirrors the `MetadataCreated` Status Condition
	// on the parent Resource CRD. True means `drbdmeta create-md`
	// has previously succeeded on this node for this RD; the
	// satellite reconciler's `firstActivation` predicate flips false
	// when this is set. Cache of on-disk metadata state — the
	// authoritative source is the kernel-side `drbdmeta dump-md`
	// probe (`Adm.HasMD`). Phase 11.3 Stage 1.
	MetadataCreated bool

	// FilesystemFormatted mirrors the `FilesystemFormatted` Status
	// Condition on the parent Resource CRD. True means the RG-driven
	// auto-mkfs path (scenario 9.W14) has previously reported every
	// diskful volume of this RD as carrying a filesystem on this
	// node; the satellite reconciler's `needsAutoMkfsRetry` predicate
	// and the `runAutoMkfs` fast-path skip the per-volume blkid
	// probe when this is set. Hot-path optimisation — NOT the
	// double-mkfs safety net. The authoritative guard against
	// re-formatting a populated volume remains the per-volume
	// `blkid -o export` probe inside `runAutoMkfs`. Phase 11.3
	// Stage 2.
	FilesystemFormatted bool

	// KernelLoaded mirrors the `KernelLoaded` Status Condition on
	// the parent Resource CRD. True means the observer's events2
	// stream most recently saw an `exists resource <name>` frame
	// (and no subsequent `destroy resource`) for this RD on this
	// node — i.e. the DRBD kernel slot is loaded. The satellite
	// reconciler's `observeForFsm` reads this to short-circuit the
	// `drbdsetup status` round-trip inside the hot path. Hot-path
	// optimisation only — the kernel-direct probe via Adm.IsLoaded
	// remains the authoritative fallback when the Condition is
	// absent (cluster just upgraded, observer restarting). Phase
	// 11.3 Stage 3.
	KernelLoaded bool
}

// DesiredConnection is one (peer-pair, paths) entry on a
// DesiredResource. The satellite's `.res` renderer turns each entry
// into one `connection { … }` block carrying one `path { … }`
// sub-block per Path.
type DesiredConnection struct {
	NodeA string
	NodeB string
	Paths []DesiredConnectionPath
}

// DesiredConnectionPath is one path inside a DesiredConnection.
// Name is operator-facing (path1, path2, …) and not emitted into
// the .res file — drbd identifies paths positionally.
type DesiredConnectionPath struct {
	Name     string
	AddressA string
	AddressB string
}

// GetName returns the RD name. Nil-safe.
func (x *DesiredResource) GetName() string {
	if x == nil {
		return ""
	}

	return x.Name
}

// GetNodeName returns the satellite node name. Nil-safe.
func (x *DesiredResource) GetNodeName() string {
	if x == nil {
		return ""
	}

	return x.NodeName
}

// GetFlags returns the flag list (DISKLESS, EVICTED, ...).
func (x *DesiredResource) GetFlags() []string {
	if x == nil {
		return nil
	}

	return x.Flags
}

// GetProps returns the Props bag.
func (x *DesiredResource) GetProps() map[string]string {
	if x == nil {
		return nil
	}

	return x.Props
}

// GetVolumes returns the per-volume slots.
func (x *DesiredResource) GetVolumes() []*DesiredVolume {
	if x == nil {
		return nil
	}

	return x.Volumes
}

// GetPeers returns the peer node-name list.
func (x *DesiredResource) GetPeers() []string {
	if x == nil {
		return nil
	}

	return x.Peers
}

// GetDrbdOptions returns the resolved DRBD options bag.
func (x *DesiredResource) GetDrbdOptions() map[string]string {
	if x == nil {
		return nil
	}

	return x.DrbdOptions
}

// GetLayerStack returns the resolved layer composition.
func (x *DesiredResource) GetLayerStack() []string {
	if x == nil {
		return nil
	}

	return x.LayerStack
}

// GetConnections returns the explicit multi-path connection list.
// Nil-safe; empty slice == no overrides (satellite falls back to the
// default single-host-pair render).
func (x *DesiredResource) GetConnections() []DesiredConnection {
	if x == nil {
		return nil
	}

	return x.Connections
}

// GetMetadataCreated returns whether the parent Resource CRD carries
// a `MetadataCreated=True` Status Condition — the cache flag the
// satellite reconciler reads to short-circuit `firstActivation`.
// Nil-safe.
func (x *DesiredResource) GetMetadataCreated() bool {
	if x == nil {
		return false
	}

	return x.MetadataCreated
}

// GetFilesystemFormatted returns whether the parent Resource CRD
// carries a `FilesystemFormatted=True` Status Condition — the cache
// flag the satellite reconciler reads to short-circuit the
// `needsAutoMkfsRetry` probe and the `runAutoMkfs` fast-path.
// Nil-safe. Phase 11.3 Stage 2.
func (x *DesiredResource) GetFilesystemFormatted() bool {
	if x == nil {
		return false
	}

	return x.FilesystemFormatted
}

// GetKernelLoaded returns whether the parent Resource CRD carries a
// `KernelLoaded=True` Status Condition — the cache flag the
// satellite reconciler reads to short-circuit the kernel-side
// `drbdsetup status` probe inside `observeForFsm`. Nil-safe.
// Phase 11.3 Stage 3.
func (x *DesiredResource) GetKernelLoaded() bool {
	if x == nil {
		return false
	}

	return x.KernelLoaded
}

// DesiredVolume describes one of an RD's volumes from the apply
// payload's perspective: which volume number, what size, which
// pool to provision on, and (Phase 8.1) the GI seed an existing
// UpToDate peer offered so the satellite can pre-seed DRBD
// metadata and skip the full initial-sync.
type DesiredVolume struct {
	VolumeNumber int32
	SizeKib      int64
	StoragePool  string
	SeedFromGi   string
	// SourceSnapshot, when non-empty, tells the satellite to
	// materialise this volume by cloning the named snapshot via the
	// provider's RestoreVolumeFromSnapshot instead of CreateVolume.
	// Carries the SOURCE RD + snapshot name in `<rd>:<snap>` form;
	// the satellite splits before issuing the storage call. Used by
	// the CSI clone + snapshot-restore-resource paths.
	SourceSnapshot string

	// MetaPool, when non-empty, names the storage pool the satellite
	// must provision a SECOND backing volume on to hold this DRBD
	// volume's activity-log + bitmap + GI state. Set by the
	// dispatcher when the resolved props carry
	// `StorPoolNameDrbdMeta` (UG9 §"Using external DRBD metadata",
	// scenario 6.18). Empty == internal metadata (default).
	//
	// The satellite carves a sibling volume named
	// `<rd>_<vol>_meta` on this pool BEFORE the data volume's
	// drbdadm create-md so the `meta-disk <path>;` line in the .res
	// file points at an already-existing block device. The path is
	// then propagated via the .res renderer's Volume.MetaDisk field.
	MetaPool string
}

// GetVolumeNumber returns the per-RD volume index.
func (x *DesiredVolume) GetVolumeNumber() int32 {
	if x == nil {
		return 0
	}

	return x.VolumeNumber
}

// GetSizeKib returns the volume size in KiB.
func (x *DesiredVolume) GetSizeKib() int64 {
	if x == nil {
		return 0
	}

	return x.SizeKib
}

// GetStoragePool returns the target pool name.
func (x *DesiredVolume) GetStoragePool() string {
	if x == nil {
		return ""
	}

	return x.StoragePool
}

// GetSeedFromGi returns the GI an existing peer offered as the
// seed for skip-initial-sync, or "" when no seed was found.
func (x *DesiredVolume) GetSeedFromGi() string {
	if x == nil {
		return ""
	}

	return x.SeedFromGi
}

// GetSourceSnapshot returns the `<rd>:<snap>` clone source, or
// "" when this volume should be created blank.
func (x *DesiredVolume) GetSourceSnapshot() string {
	if x == nil {
		return ""
	}

	return x.SourceSnapshot
}

// GetMetaPool returns the external-metadata pool name (scenario
// 6.18 / StorPoolNameDrbdMeta), or "" when this volume should use
// internal metadata.
func (x *DesiredVolume) GetMetaPool() string {
	if x == nil {
		return ""
	}

	return x.MetaPool
}

// ResourceApplyResult is the per-resource outcome of one
// Reconciler.Apply call.
type ResourceApplyResult struct {
	Name     string
	NodeName string
	Ok       bool
	Message  string

	// Volumes carries per-volume DevicePath the satellite materialised
	// during this apply. Empty when the resource is DISKLESS or the
	// apply chain failed before assigning a device. The c-r reconciler
	// surfaces these via SSA into Resource.Status.Volumes[i].devicePath
	// so linstor-csi / consumers reading the CRD can resolve the
	// backing /dev path without an extra round-trip.
	Volumes []*ResourceApplyVolumeResult
}

// ResourceApplyVolumeResult is the per-volume slice item the
// satellite emits for the consumer side. Only the fields the
// satellite owns end up here — DRBD-side state (DiskState,
// CurrentGi) flows via the separate events2 observer.
type ResourceApplyVolumeResult struct {
	VolumeNumber int32
	DevicePath   string
}

// GetVolumes returns the per-volume slice — nil-safe.
func (x *ResourceApplyResult) GetVolumes() []*ResourceApplyVolumeResult {
	if x == nil {
		return nil
	}

	return x.Volumes
}

// GetVolumeNumber returns the volume index — nil-safe.
func (v *ResourceApplyVolumeResult) GetVolumeNumber() int32 {
	if v == nil {
		return 0
	}

	return v.VolumeNumber
}

// GetDevicePath returns the materialised device — nil-safe.
func (v *ResourceApplyVolumeResult) GetDevicePath() string {
	if v == nil {
		return ""
	}

	return v.DevicePath
}

// GetName returns the resource name the result refers to.
func (x *ResourceApplyResult) GetName() string {
	if x == nil {
		return ""
	}

	return x.Name
}

// GetNodeName returns the satellite node the result refers to.
func (x *ResourceApplyResult) GetNodeName() string {
	if x == nil {
		return ""
	}

	return x.NodeName
}

// GetOk reports whether the apply succeeded.
func (x *ResourceApplyResult) GetOk() bool {
	if x == nil {
		return false
	}

	return x.Ok
}

// GetMessage returns the error/diagnostic message when Ok=false.
func (x *ResourceApplyResult) GetMessage() string {
	if x == nil {
		return ""
	}

	return x.Message
}

// DeleteResourceRequest is the apply chain's input for tearing
// down a Resource on one satellite. Carries the RD name, the
// pool the volumes were provisioned on, and the per-RD volume
// numbers the satellite-side `DeleteVolume` chain walks.
type DeleteResourceRequest struct {
	Name          string
	StoragePool   string
	VolumeNumbers []int32
}

// GetName returns the RD name.
func (x *DeleteResourceRequest) GetName() string {
	if x == nil {
		return ""
	}

	return x.Name
}

// GetStoragePool returns the pool the satellite's DeleteVolume
// chain routes through. Empty for DISKLESS replicas.
func (x *DeleteResourceRequest) GetStoragePool() string {
	if x == nil {
		return ""
	}

	return x.StoragePool
}

// GetVolumeNumbers returns the per-RD volume numbers the
// satellite should drop.
func (x *DeleteResourceRequest) GetVolumeNumbers() []int32 {
	if x == nil {
		return nil
	}

	return x.VolumeNumbers
}

// DeleteResourceResponse mirrors ResourceApplyResult for the
// delete path.
type DeleteResourceResponse struct {
	Ok      bool
	Message string
}

// GetOk reports whether the teardown succeeded.
func (x *DeleteResourceResponse) GetOk() bool {
	if x == nil {
		return false
	}

	return x.Ok
}

// GetMessage returns the diagnostic when Ok=false.
func (x *DeleteResourceResponse) GetMessage() string {
	if x == nil {
		return ""
	}

	return x.Message
}

// CreateSnapshotRequest carries the per-RD snapshot create
// parameters into Reconciler.CreateSnapshot.
type CreateSnapshotRequest struct {
	ResourceName  string
	SnapshotName  string
	VolumeNumbers []int32
}

// GetResourceName returns the source RD name.
func (x *CreateSnapshotRequest) GetResourceName() string {
	if x == nil {
		return ""
	}

	return x.ResourceName
}

// GetSnapshotName returns the snapshot name.
func (x *CreateSnapshotRequest) GetSnapshotName() string {
	if x == nil {
		return ""
	}

	return x.SnapshotName
}

// GetVolumeNumbers returns the volume numbers to snapshot.
// Empty == snapshot every RD volume.
func (x *CreateSnapshotRequest) GetVolumeNumbers() []int32 {
	if x == nil {
		return nil
	}

	return x.VolumeNumbers
}

// CreateSnapshotResponse mirrors ResourceApplyResult for the
// snapshot-create path. The optional CreateTimestampUnix is
// stamped on success so callers can surface "snapshot taken at"
// in upstream-LINSTOR Status responses.
//
// Terminal carries the satellite reconciler's verdict on whether
// a follow-up Reconcile would have any chance of succeeding. True
// means "do not retry": the failure is a missing parent volume,
// an unknown resource, or a provider-level ErrTerminal. False on
// an Ok=false body means "transient — back off and try again"
// (lvm temporary lock, busy dataset, exec wrapper failure).
// SnapshotReconciler stamps Status.Flags=["FAILED"] only when
// Terminal=true; transient failures keep the snapshot in the
// Incomplete state and rely on controller-runtime's rate limiter
// for backoff.
type CreateSnapshotResponse struct {
	Ok                  bool
	Terminal            bool
	Message             string
	CreateTimestampUnix int64
}

// GetOk reports whether the snapshot create succeeded.
func (x *CreateSnapshotResponse) GetOk() bool {
	if x == nil {
		return false
	}

	return x.Ok
}

// GetTerminal reports whether the failure is dead-letter (true) or
// transient (false). Meaningless when Ok=true.
func (x *CreateSnapshotResponse) GetTerminal() bool {
	if x == nil {
		return false
	}

	return x.Terminal
}

// GetMessage returns the diagnostic when Ok=false.
func (x *CreateSnapshotResponse) GetMessage() string {
	if x == nil {
		return ""
	}

	return x.Message
}

// DeleteSnapshotRequest carries the per-RD snapshot teardown
// parameters into Reconciler.DeleteSnapshot.
type DeleteSnapshotRequest struct {
	ResourceName string
	SnapshotName string
}

// GetResourceName returns the source RD name.
func (x *DeleteSnapshotRequest) GetResourceName() string {
	if x == nil {
		return ""
	}

	return x.ResourceName
}

// GetSnapshotName returns the snapshot name.
func (x *DeleteSnapshotRequest) GetSnapshotName() string {
	if x == nil {
		return ""
	}

	return x.SnapshotName
}

// DeleteSnapshotResponse mirrors ResourceApplyResult for the
// snapshot-delete path.
type DeleteSnapshotResponse struct {
	Ok      bool
	Message string
}

// GetOk reports whether the snapshot teardown succeeded.
func (x *DeleteSnapshotResponse) GetOk() bool {
	if x == nil {
		return false
	}

	return x.Ok
}

// GetMessage returns the diagnostic when Ok=false.
func (x *DeleteSnapshotResponse) GetMessage() string {
	if x == nil {
		return ""
	}

	return x.Message
}
