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

// ResourceSpec is the desired state of one replica of a ResourceDefinition
// placed on a node. The composite key is (resourceDefinitionName, nodeName);
// metadata.name encodes that as `<rd>.<node>`.
type ResourceSpec struct {
	// resourceDefinitionName is the parent ResourceDefinition.
	// +required
	ResourceDefinitionName string `json:"resourceDefinitionName"`

	// nodeName is the satellite hosting this replica.
	// +required
	NodeName string `json:"nodeName"`

	// props is the LINSTOR per-resource property map.
	// +optional
	Props map[string]string `json:"props,omitempty"`

	// flags carries the user-controlled placement flags.
	// +optional
	Flags []string `json:"flags,omitempty"`
}

// ResourceStatus is the observed state of a placed resource.
type ResourceStatus struct {
	// inUse is whether DRBD reports the resource as primary anywhere.
	// +optional
	InUse bool `json:"inUse,omitempty"`

	// drbdNodeID is the DRBD-9 node-id assigned to this replica.
	// Allocated once when the Resource first reconciles and never
	// changes for the lifetime of the Resource — re-numbering live
	// replicas would re-map their DRBD bitmaps and corrupt data on
	// peer-to-peer resync. Range 0..15 (drbd-9 max-peers). nil means
	// the controller has not yet allocated.
	// +optional
	DRBDNodeID *int32 `json:"drbdNodeId,omitempty"`

	// drbdPort is the TCP port the controller allocated for this RD's
	// replication mesh. All replicas of the same RD share the port;
	// the controller stamps the same value on every sibling Resource.
	// nil means not yet allocated.
	// +optional
	DRBDPort *int32 `json:"drbdPort,omitempty"`

	// drbdMinor is the local /dev/drbd<N> minor number. Same scoping
	// rule as drbdPort.
	// +optional
	DRBDMinor *int32 `json:"drbdMinor,omitempty"`

	// volumes is the per-volume runtime state reported by the satellite.
	// +optional
	Volumes []ResourceVolumeStatus `json:"volumes,omitempty"`

	// conditions represent the current state of the Resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ResourceVolumeStatus is the runtime state of one volume of a Resource.
type ResourceVolumeStatus struct {
	VolumeNumber int32 `json:"volumeNumber"`
	// +optional
	StoragePool string `json:"storagePool,omitempty"`
	// +optional
	DevicePath string `json:"devicePath,omitempty"`
	// +optional
	AllocatedKib int64 `json:"allocatedKib,omitempty"`
	// +optional
	UsableKib int64 `json:"usableKib,omitempty"`
	// +optional
	DiskState string `json:"diskState,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// Resource is the Schema for the resources API
type Resource struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Resource
	// +required
	Spec ResourceSpec `json:"spec"`

	// status defines the observed state of Resource
	// +optional
	Status ResourceStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ResourceList contains a list of Resource
type ResourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Resource `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Resource{}, &ResourceList{})
}
