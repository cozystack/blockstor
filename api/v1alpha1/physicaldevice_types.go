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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PhysicalDeviceLabelNode is the metadata.label key under which the
// satellite stamps the owning node name on every PhysicalDevice it
// publishes. Used by `client.MatchingLabels{LabelNode: nodeName}`
// for efficient per-node listing.
const PhysicalDeviceLabelNode = "blockstor.io/node"

// PhysicalDeviceSpec is the desired state of one raw block device on
// a satellite node. Phase 10.7: replaces the upstream-LINSTOR
// `physical-storage` PropsContainers + gRPC pattern with a
// satellite-publish + controller-attach + satellite-execute flow.
//
// An empty Spec (zero-value AttachTo) means "this device is
// published as available — pick me up when an operator wants to
// turn me into a pool". The controller (REST shim or operator
// via `kubectl edit`) populates Spec.AttachTo to ask the
// satellite to allocate this device into the named pool. On
// successful attach the satellite DELETES the CRD (delete-as-
// completion) — a reattachment requires re-discovery.
type PhysicalDeviceSpec struct {
	// attachTo is the controller-side request that this device be
	// added to the named pool. Empty/nil means "available". Phase
	// 10.7 step 2 wires the REST endpoint that flips this; until
	// then operators can edit the CRD directly.
	// +optional
	AttachTo *AttachToPool `json:"attachTo,omitempty"`
}

// AttachToPool carries the parameters the satellite needs to fold
// a device into a pool. Most of the fields are kind-specific and
// only one sub-block is meaningful per request (the CRD validation
// admission rule enforces "exactly one of vg / thinPool / zpool /
// directory matches the providerKind").
type AttachToPool struct {
	// storagePoolName is the target pool name (matches the
	// upstream LINSTOR pool name a Resource refers to).
	// +required
	StoragePoolName string `json:"storagePoolName"`

	// providerKind picks the storage backend ("LVM", "LVM_THIN",
	// "ZFS", "ZFS_THIN", "FILE", "FILE_THIN").
	// +kubebuilder:validation:Enum=LVM;LVM_THIN;ZFS;ZFS_THIN;FILE;FILE_THIN
	// +required
	ProviderKind string `json:"providerKind"`

	// vgName is the LVM volume group to create (LVM / LVM_THIN
	// kinds). Ignored otherwise.
	// +optional
	VGName string `json:"vgName,omitempty"`

	// thinPoolName is the LVM thin pool to create on top of VG
	// (LVM_THIN only). Ignored otherwise.
	// +optional
	ThinPoolName string `json:"thinPoolName,omitempty"`

	// zPoolName is the ZFS zpool to create (ZFS / ZFS_THIN
	// kinds). Ignored otherwise.
	// +optional
	ZPoolName string `json:"zPoolName,omitempty"`

	// directory is the mount-point where a FILE / FILE_THIN
	// pool's backing files live. The satellite formats + mounts
	// the device on this path before flipping the pool to
	// available.
	// +optional
	Directory string `json:"directory,omitempty"`

	// wipe is the explicit consent flag the operator must set to
	// `true` to allow the satellite to overwrite existing on-disk
	// signatures (LVM/ZFS/MD/filesystems). Without it the attach
	// fails with `Status.Phase=Failed` rather than risk data loss.
	// +optional
	Wipe bool `json:"wipe,omitempty"`
}

// PhysicalDeviceStatus is the observed state populated by the
// satellite's discovery loop. The CRD itself only exists while the
// device is `Available` or `Attaching` — successful attachment
// deletes it (delete-as-completion).
type PhysicalDeviceStatus struct {
	// nodeName is the satellite that owns this device.
	// +optional
	NodeName string `json:"nodeName,omitempty"`

	// stableID is the stable identifier the satellite picked from
	// `/dev/disk/by-id/...` (preferring `wwn-*` → `scsi-SATA_*`
	// → `nvme-*` → `by-path/*`). The CRD's metadata.name is
	// derived from this so it survives `/dev/sdN` re-lettering.
	// +optional
	StableID string `json:"stableId,omitempty"`

	// devicePath is the canonical /dev/disk/by-id symlink the
	// satellite uses internally. Stable across reboots (unless the
	// device is physically replaced).
	// +optional
	DevicePath string `json:"devicePath,omitempty"`

	// currentDevPath is the volatile `/dev/sdN` or `/dev/nvmeNnN`
	// path from the most recent discovery tick. Refreshed on
	// every discovery — operators MUST NOT rely on this being
	// stable across reboots.
	// +optional
	CurrentDevPath string `json:"currentDevPath,omitempty"`

	// sizeBytes is the device size as reported by `lsblk -o SIZE`.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// model carries the manufacturer model string (lsblk MODEL).
	// +optional
	Model string `json:"model,omitempty"`

	// serial carries the device serial number when reported by
	// the firmware. Some virtualised setups (virtio without
	// serial passthrough) leave this empty.
	// +optional
	Serial string `json:"serial,omitempty"`

	// rotational reflects the kernel's view of the spindle —
	// false for SSD/NVMe, true for HDD. Used by the autoplacer
	// to bias placement when ResourceGroup says so.
	// +optional
	Rotational *bool `json:"rotational,omitempty"`

	// transport is the bus type ("sata", "nvme", "scsi", "virtio").
	// +optional
	Transport string `json:"transport,omitempty"`

	// phase is the discovery + attach state.
	// +kubebuilder:validation:Enum=Available;Attaching;Failed
	// +optional
	Phase string `json:"phase,omitempty"`

	// conditions carry detailed state (e.g. `WipeRequired` when
	// the device has on-disk signatures and `Spec.AttachTo.Wipe`
	// is false).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PhysicalDevice phase constants — string values for
// Status.Phase. Use the typed accessor when reading from Go.
const (
	PhysicalDevicePhaseAvailable = "Available"
	PhysicalDevicePhaseAttaching = "Attaching"
	PhysicalDevicePhaseFailed    = "Failed"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// PhysicalDevice is one raw block device discovered on a satellite.
// Phase 10.7 — replaces the upstream `linstor physical-storage`
// PropsContainers / gRPC pattern.
type PhysicalDevice struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata. The expected name
	// shape is `<nodeName>.<stable-id-slug>` (matching every
	// other composite-key CRD in the project — Resource,
	// StoragePool, Snapshot — so operators can grep across
	// kinds with a single `<node>.` pattern); the metadata.labels
	// must carry `blockstor.io/node=<nodeName>` so per-node
	// filters work via label selectors.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PhysicalDevice
	// +required
	Spec PhysicalDeviceSpec `json:"spec"`

	// status defines the observed state of PhysicalDevice
	// +optional
	Status PhysicalDeviceStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PhysicalDeviceList contains a list of PhysicalDevice.
type PhysicalDeviceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PhysicalDevice `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PhysicalDevice{}, &PhysicalDeviceList{})
}
