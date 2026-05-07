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

// KVEntrySpec is one entry in the LINSTOR-style key/value store.
//
// Upstream LINSTOR groups entries by an "instance" (e.g. "csi-volumes")
// and addresses them by a free-form key (e.g. "Aux/csi-volume-annotations").
// We store one CRD per (instance, key) pair so neither half of the key
// runs into k8s naming restrictions, and so the per-write blast radius is
// a single object instead of an entire ConfigMap.
type KVEntrySpec struct {
	// instance is the LINSTOR KV instance name. Free-form string.
	// +required
	Instance string `json:"instance"`

	// key is the free-form key inside the instance. May contain '/', '.',
	// uppercase, etc. — anything golinstor sends.
	// +required
	Key string `json:"key"`

	// value is the opaque payload.
	// +optional
	Value string `json:"value,omitempty"`
}

// KVEntryStatus is currently empty: KVEntries are pure config and have no
// observed state.
type KVEntryStatus struct {
	// conditions represent the current state of the KVEntry. Reserved for
	// future use; populated by reconcilers if any are introduced.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// KVEntry is the Schema for the kventries API
type KVEntry struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of KVEntry
	// +required
	Spec KVEntrySpec `json:"spec"`

	// status defines the observed state of KVEntry
	// +optional
	Status KVEntryStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// KVEntryList contains a list of KVEntry
type KVEntryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []KVEntry `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KVEntry{}, &KVEntryList{})
}
