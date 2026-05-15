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

package v1

// PhysicalDevice is the wire shape blockstor uses internally for one
// raw block device discovered on a satellite. Distinct from the CRD
// (`api/v1alpha1.PhysicalDevice`) — this is the format the
// `pkg/store.PhysicalDeviceStore` interface returns and the
// REST shim translates to upstream LINSTOR's `PhysicalStorage*`
// envelopes for golinstor compatibility.
//
// The store-internal shape stays stable; the REST transcoder
// shapes it into golinstor's `PhysicalStorage` (cluster-wide,
// grouped by attributes) or `PhysicalStorageDevice` (per-node)
// at the boundary.
type PhysicalDevice struct {
	// Name is the CRD metadata.name (`<node>-<stable-id-slug>`).
	// Stable across reboots; survives /dev/sdN re-lettering.
	Name string `json:"name"`

	// NodeName is the satellite that owns this device. Mirrors
	// the CRD's `metadata.labels["blockstor.io/node"]` for fast
	// per-node listing.
	NodeName string `json:"node_name"`

	// StableID is the upstream identifier (preferring WWN →
	// scsi-SATA → nvme → by-path). The Name field above
	// embeds a slug of this for k8s name validation.
	StableID string `json:"stable_id,omitempty"`

	// DevicePath is the canonical /dev/disk/by-id symlink. Stable
	// across reboots unless physically replaced.
	DevicePath string `json:"device_path,omitempty"`

	// CurrentDevPath is the volatile /dev/sdN path. Refreshed on
	// every discovery tick — operators MUST NOT rely on it being
	// stable.
	CurrentDevPath string `json:"current_dev_path,omitempty"`

	// SizeBytes is the device size from `lsblk -o SIZE`.
	SizeBytes int64 `json:"size,omitempty"`

	// Model carries the manufacturer model string (lsblk MODEL).
	Model string `json:"model,omitempty"`

	// Serial carries the device serial number when the firmware
	// reports one. Some virtualised setups leave this empty.
	Serial string `json:"serial,omitempty"`

	// Rotational reflects the kernel's view of the spindle —
	// false for SSD/NVMe, true for HDD. Used by the autoplacer
	// to bias placement when ResourceGroup says so.
	Rotational *bool `json:"rotational,omitempty"`

	// Transport is the bus type ("sata", "nvme", "scsi", "virtio").
	Transport string `json:"transport,omitempty"`

	// Phase is the discovery + attach state ("Available" /
	// "Attaching" / "Failed"). The CRD is deleted when an
	// attach succeeds — readers never see "Ready".
	Phase string `json:"phase,omitempty"`

	// AttachTo is the controller-side request that this device
	// be folded into a pool. nil when the device is published
	// as available.
	AttachTo *PhysicalDeviceAttachTo `json:"attach_to,omitempty"`

	// Free reflects the satellite's most-recent assessment of
	// whether the device carries any blocking signature (lsblk
	// FSType / mountpoint / LVM PV / ZFS / DRBD / wipefs match).
	// Sourced from Status.Conditions[Type=Free].Status on the CRD
	// — nil means the discovery loop hasn't published a verdict
	// yet (mid-bootstrap). Bug 89: the REST `ps cdp` path checks
	// this before accepting an attach; the `ps l` endpoint
	// already filters non-free devices out of its bucket list,
	// so the two paths must agree.
	Free *bool `json:"free,omitempty"`

	// FreeReason carries the human-readable explanation the
	// discovery loop stamped alongside Free — e.g.
	// "SignatureFound" / "FreeBlockDevice" — so the REST handler
	// can surface a `cause` line on `ps cdp` rejection. Bug 89.
	FreeReason string `json:"free_reason,omitempty"`

	// FreeMessage is the discovery loop's longer explanation
	// (Status.Conditions[Type=Free].Message). Quoted verbatim
	// in the `cause` field of the 409 envelope so the operator
	// sees the same wording the CRD carries. Bug 89.
	FreeMessage string `json:"free_message,omitempty"`
}

// PhysicalDeviceAttachTo mirrors the CRD's Spec.AttachTo at the
// wire layer.
type PhysicalDeviceAttachTo struct {
	StoragePoolName string `json:"storage_pool_name"`
	ProviderKind    string `json:"provider_kind"`
	VGName          string `json:"vg_name,omitempty"`
	ThinPoolName    string `json:"thin_pool_name,omitempty"`
	ZPoolName       string `json:"z_pool_name,omitempty"`
	Directory       string `json:"directory,omitempty"`
	Wipe            bool   `json:"wipe,omitempty"`
}
