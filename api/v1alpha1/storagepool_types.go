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

// StoragePoolSpec is the desired state of a LINSTOR storage pool.
//
// LINSTOR storage pools are keyed by (node_name, pool_name); a single CRD
// instance represents one pool on one satellite. The metadata.name is the
// LINSTOR pool name, and spec.nodeName names the satellite hosting it.
type StoragePoolSpec struct {
	// nodeName is the name of the satellite hosting this pool. Same as the
	// LINSTOR Node CRD's metadata.name.
	// +required
	NodeName string `json:"nodeName"`

	// poolName is the LINSTOR pool name (LVM VG, ZFS dataset, etc.). When
	// empty, defaults to metadata.name.
	// +optional
	PoolName string `json:"poolName,omitempty"`

	// providerKind is the storage backend.
	// +kubebuilder:validation:Enum=LVM;LVM_THIN;ZFS;ZFS_THIN;FILE;FILE_THIN;DISKLESS
	ProviderKind string `json:"providerKind"`

	// sharedSpaceId groups pools that physically share the same backing
	// LUN (e.g. an EXOS / NetApp / Ceph-RBD-as-shared-disk slice attached
	// to multiple satellites). Pools in the same group contribute their
	// free capacity once to cluster totals instead of summing, and the
	// autoplacer treats them as anti-affine for replica placement so a
	// 2-replica RD never lands twice on the same physical LUN.
	// Empty string = local pool (default).
	// +optional
	SharedSpaceID string `json:"sharedSpaceId,omitempty"`

	// props is the LINSTOR property map for this pool.
	// +optional
	Props map[string]string `json:"props,omitempty"`
}

// StoragePoolStatus is the observed state of a storage pool, populated by
// the satellite once it brings up the backing volume group.
type StoragePoolStatus struct {
	// freeCapacity is the free capacity in KiB reported by the satellite.
	// Matches the wire shape (gRPC SatellitePool.FreeCapacityKib, REST
	// `free_capacity_kib`) so /v1/view/storage-pools and the autoplacer
	// can read this field directly without a unit conversion.
	// +optional
	FreeCapacity int64 `json:"freeCapacity,omitempty"`

	// totalCapacity is the total capacity in KiB reported by the satellite.
	// +optional
	TotalCapacity int64 `json:"totalCapacity,omitempty"`

	// supportsSnapshots is whether the backing provider can create snapshots.
	// +optional
	SupportsSnapshots bool `json:"supportsSnapshots,omitempty"`

	// staticTraits exposes provider-static attributes (kind, etc.).
	// +optional
	StaticTraits map[string]string `json:"staticTraits,omitempty"`

	// poolMissing indicates the satellite's last PoolStatus probe failed —
	// typically the backing pool (zpool / VG / FILE_THIN dir) was destroyed
	// out-of-band. While true, the placer skips this pool and the REST
	// wire view emits state="Faulty" instead of "Ok".
	// +optional
	PoolMissing bool `json:"poolMissing,omitempty"`

	// conditions represent the current state of the StoragePool resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// StoragePool is the Schema for the storagepools API
type StoragePool struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of StoragePool
	// +required
	Spec StoragePoolSpec `json:"spec"`

	// status defines the observed state of StoragePool
	// +optional
	Status StoragePoolStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// StoragePoolList contains a list of StoragePool
type StoragePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []StoragePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StoragePool{}, &StoragePoolList{})
}
