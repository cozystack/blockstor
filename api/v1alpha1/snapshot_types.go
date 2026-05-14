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

// SnapshotSpec is the desired state of a LINSTOR Snapshot. The composite
// key is (resource definition, snapshot name); metadata.name encodes that
// as `<rd>.<snap>`.
type SnapshotSpec struct {
	// resourceDefinitionName is the parent ResourceDefinition.
	// +required
	ResourceDefinitionName string `json:"resourceDefinitionName"`

	// snapshotName is the user-facing snapshot identifier.
	// +required
	SnapshotName string `json:"snapshotName"`

	// nodes are the satellites the snapshot should live on. Empty means
	// "every node currently hosting the parent resource".
	// +optional
	Nodes []string `json:"nodes,omitempty"`

	// props is the LINSTOR property map for the snapshot.
	// +optional
	Props map[string]string `json:"props,omitempty"`

	// volumeDefinitions records the size of each volume captured.
	// +optional
	VolumeDefinitions []SnapshotVolumeRef `json:"volumeDefinitions,omitempty"`
}

// SnapshotVolumeRef is one volume slot inside a Snapshot.
type SnapshotVolumeRef struct {
	VolumeNumber int32 `json:"volumeNumber"`
	SizeKib      int64 `json:"sizeKib"`
}

// SnapshotStatusFlagFailed is stamped on Status.Flags by the
// satellite reconciler when CreateSnapshot returned a terminal
// error (e.g. parent volume missing, unknown resource, source
// pool absent). Surfaces through crdToWireSnapshot as
// `flags: ["FAILED"]` on the wire, which the Python CLI maps
// to the `State="Failed"` column in `linstor s l`. Matches
// upstream LINSTOR's `FAILED_DEPLOYMENT` SnapshotDefinition
// flag — same semantic ("the satellite tried and gave up"),
// shorter name. Once stamped, the reconciler does NOT requeue:
// a terminal failure is a dead-letter that an operator must
// either delete or recreate.
const SnapshotStatusFlagFailed = "FAILED"

// SnapshotStatus is the observed state of a Snapshot.
type SnapshotStatus struct {
	// nodeStatus reports per-node readiness from the satellites.
	// +optional
	NodeStatus []SnapshotPerNodeStatus `json:"nodeStatus,omitempty"`

	// flags carries terminal-state markers. Currently only
	// "FAILED" is meaningful — stamped by the satellite when
	// CreateSnapshot returns a non-retryable error class.
	// +optional
	Flags []string `json:"flags,omitempty"`

	// conditions represent the current state of the Snapshot.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// SnapshotPerNodeStatus is the satellite-reported state of one
// materialisation of the snapshot.
type SnapshotPerNodeStatus struct {
	NodeName string `json:"nodeName"`
	// +optional
	CreateTimestamp int64 `json:"createTimestamp,omitempty"`
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// Snapshot is the Schema for the snapshots API
type Snapshot struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Snapshot
	// +required
	Spec SnapshotSpec `json:"spec"`

	// status defines the observed state of Snapshot
	// +optional
	Status SnapshotStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SnapshotList contains a list of Snapshot
type SnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Snapshot `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Snapshot{}, &SnapshotList{})
}
