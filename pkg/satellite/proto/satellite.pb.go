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

// Package satellitepb hosts the in-process wire format for the
// satellite apply chain. Phase 10.6 retired the gRPC contract; the
// types here are now plain Go structs the controller-runtime
// reconcilers, the satellite `Reconciler`, and the
// `dispatcher.BuildDesired` translator pass between each other.
// Getter methods (`GetX`) are kept so the migration off the
// generated-proto shape was a no-op at call sites.
//
// The package name and import path stay as `satellitepb` for
// historical reasons; a future rename to `pkg/satellite/applyspec`
// (or similar) is fine — there are no out-of-tree consumers.
package satellitepb

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

// ResourceApplyResult is the per-resource outcome of one
// Reconciler.Apply call.
type ResourceApplyResult struct {
	Name     string
	NodeName string
	Ok       bool
	Message  string
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
type CreateSnapshotResponse struct {
	Ok                  bool
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
