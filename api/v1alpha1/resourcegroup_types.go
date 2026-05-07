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

// ResourceGroupSpec is the desired state of a LINSTOR ResourceGroup — the
// template linstor-csi creates per StorageClass and Spawns ResourceDefinitions
// from. Mirrors the upstream `ResourceGroup` shape (modulo k8s naming).
type ResourceGroupSpec struct {
	// description is human-readable.
	// +optional
	Description string `json:"description,omitempty"`

	// props is the LINSTOR property map for this resource group.
	// +optional
	Props map[string]string `json:"props,omitempty"`

	// peerSlots is the DRBD peer-slot count baked into spawned resources.
	// +optional
	PeerSlots int32 `json:"peerSlots,omitempty"`

	// selectFilter constrains autoplacer when spawning resources from this
	// group.
	// +optional
	SelectFilter ResourceGroupSelectFilter `json:"selectFilter,omitzero"`

	// volumeGroups templates the per-volume props/flags for spawned resources.
	// +optional
	VolumeGroups []ResourceGroupVolumeGroup `json:"volumeGroups,omitempty"`
}

// ResourceGroupSelectFilter is the in-CRD shape of LINSTOR's AutoSelectFilter.
type ResourceGroupSelectFilter struct {
	// +optional
	PlaceCount int32 `json:"placeCount,omitempty"`
	// +optional
	StoragePool string `json:"storagePool,omitempty"`
	// +optional
	StoragePoolList []string `json:"storagePoolList,omitempty"`
	// +optional
	StoragePoolDisklessList []string `json:"storagePoolDisklessList,omitempty"`
	// +optional
	NodeNameList []string `json:"nodeNameList,omitempty"`
	// +optional
	ReplicasOnSame []string `json:"replicasOnSame,omitempty"`
	// +optional
	ReplicasOnDifferent []string `json:"replicasOnDifferent,omitempty"`
	// +optional
	NotPlaceWithRsc []string `json:"notPlaceWithRsc,omitempty"`
	// +optional
	NotPlaceWithRscRegex string `json:"notPlaceWithRscRegex,omitempty"`
	// +optional
	LayerStack []string `json:"layerStack,omitempty"`
	// +optional
	ProviderList []string `json:"providerList,omitempty"`
	// +optional
	DisklessOnRemaining bool `json:"disklessOnRemaining,omitempty"`
}

// ResourceGroupVolumeGroup is one volume's template inside a ResourceGroup.
type ResourceGroupVolumeGroup struct {
	VolumeNumber int32 `json:"volumeNumber"`
	// +optional
	Props map[string]string `json:"props,omitempty"`
	// +optional
	Flags []string `json:"flags,omitempty"`
}

// ResourceGroupStatus is the observed state.
type ResourceGroupStatus struct {
	// conditions represent the current state of the ResourceGroup.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// ResourceGroup is the Schema for the resourcegroups API
type ResourceGroup struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ResourceGroup
	// +required
	Spec ResourceGroupSpec `json:"spec"`

	// status defines the observed state of ResourceGroup
	// +optional
	Status ResourceGroupStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ResourceGroupList contains a list of ResourceGroup
type ResourceGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ResourceGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ResourceGroup{}, &ResourceGroupList{})
}
