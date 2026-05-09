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

// ResourceDefinitionSpec is the desired state of a LINSTOR
// ResourceDefinition — the named entity from which Resource (replica)
// instances are spawned. linstor-csi creates one per PVC.
type ResourceDefinitionSpec struct {
	// externalName is the user-facing name surfaced by csi (CSI volume id).
	// Empty means the same as metadata.name.
	// +optional
	ExternalName string `json:"externalName,omitempty"`

	// resourceGroupName references the ResourceGroup template this RD was
	// spawned from (or empty if directly created).
	// +optional
	ResourceGroupName string `json:"resourceGroupName,omitempty"`

	// props is the LINSTOR property map.
	// +optional
	Props map[string]string `json:"props,omitempty"`

	// flags carries user-controlled RD flags (DELETE, INACTIVE, ...).
	// +optional
	Flags []string `json:"flags,omitempty"`

	// volumeDefinitions are the volume slots inside this RD.
	// +optional
	VolumeDefinitions []ResourceDefinitionVolume `json:"volumeDefinitions,omitempty"`

	// layerStack is the LINSTOR layer composition for this RD's
	// satellite-side render — `["DRBD","STORAGE"]` (default) renders a
	// .res file and runs drbdadm; `["LUKS","STORAGE"]` layers
	// cryptsetup over the storage device with no DRBD; `["STORAGE"]`
	// is single-replica local mode (no replication, no encryption).
	// Order is top-down: the first layer's device is what the
	// consumer Pod mounts, the last is the raw block device the
	// storage provider creates.
	// Empty = inherits from the parent ResourceGroup; both empty =
	// `["DRBD","STORAGE"]`.
	// +optional
	LayerStack []string `json:"layerStack,omitempty"`
}

// ResourceDefinitionVolume is one volume slot inside an RD.
type ResourceDefinitionVolume struct {
	VolumeNumber int32 `json:"volumeNumber"`
	SizeKib      int64 `json:"sizeKib"`
	// +optional
	Props map[string]string `json:"props,omitempty"`
	// +optional
	Flags []string `json:"flags,omitempty"`
}

// ResourceDefinitionStatus is the observed state.
type ResourceDefinitionStatus struct {
	// conditions represent the current state of the ResourceDefinition.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// ResourceDefinition is the Schema for the resourcedefinitions API
type ResourceDefinition struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ResourceDefinition
	// +required
	Spec ResourceDefinitionSpec `json:"spec"`

	// status defines the observed state of ResourceDefinition
	// +optional
	Status ResourceDefinitionStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ResourceDefinitionList contains a list of ResourceDefinition
type ResourceDefinitionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ResourceDefinition `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ResourceDefinition{}, &ResourceDefinitionList{})
}
