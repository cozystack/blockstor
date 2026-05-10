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

// NodeSpec is the desired state of a LINSTOR satellite node as seen by
// blockstor. It mirrors the configurable subset of the upstream LINSTOR
// `Node` object — the parts a client (linstor-csi, piraeus-operator, the
// `linstor` CLI) sets via the REST API.
//
// Runtime fields (ConnectionStatus, NetInterfaces advertised by the
// satellite, runtime flags) live on NodeStatus.
type NodeSpec struct {
	// type is the LINSTOR node role. Common values: SATELLITE, COMBINED,
	// CONTROLLER, AUXILIARY.
	// +kubebuilder:validation:Enum=CONTROLLER;SATELLITE;COMBINED;AUXILIARY;REMOTE_SPDK;OPENFLEX_TARGET;EBS_TARGET;EBS_INIT
	Type string `json:"type"`

	// props is the LINSTOR property map for this node. Keys are LINSTOR
	// namespace paths (e.g. `Aux/foo`, `DrbdOptions/Net/...`).
	// +optional
	Props map[string]string `json:"props,omitempty"`

	// flags is the desired set of LINSTOR flags. Most node flags are
	// observed (computed by the satellite); set via spec only those that
	// are user-controlled.
	// +optional
	Flags []string `json:"flags,omitempty"`

	// netInterfaces is the desired list of advertised network interfaces.
	// The first interface is treated as the satellite endpoint.
	// +optional
	NetInterfaces []NodeNetInterface `json:"netInterfaces,omitempty"`

	// satelliteEndpoint is the controller→satellite gRPC endpoint
	// (`host:port`). Phase 10.3: replaces `Props["SatelliteEndpoint"]`
	// — the dispatcher reads this typed field first, falling back to
	// the props bag for forward-compat with pre-migration data. The
	// field becomes irrelevant once Phase 10.6 lands and the gRPC
	// path is gone.
	// +optional
	SatelliteEndpoint string `json:"satelliteEndpoint,omitempty"`

	// drbdPortRange is the inclusive [min, max] TCP port range the
	// allocator picks DRBD listen ports from for replicas placed on
	// this node. Replaces `Props["DrbdOptions/TcpPortRange"]`. nil
	// inherits the controller-wide default (7000–7999). Phase 10.3.
	// +optional
	DRBDPortRange *PortRange `json:"drbdPortRange,omitempty"`

	// drbdMinorRange is the inclusive [min, max] /dev/drbd<N> minor
	// range the allocator picks from. Replaces
	// `Props["DrbdOptions/MinorNrRange"]`. nil inherits the
	// controller-wide default (1000–1099). Phase 10.3.
	// +optional
	DRBDMinorRange *PortRange `json:"drbdMinorRange,omitempty"`
}

// PortRange is an inclusive [Min, Max] integer range. Used for
// DRBD TCP port ranges and /dev/drbd<N> minor ranges. Empty (nil)
// means "inherit cluster-wide default".
type PortRange struct {
	// min is the lower bound (inclusive).
	// +kubebuilder:validation:Minimum=0
	// +required
	Min int32 `json:"min"`

	// max is the upper bound (inclusive). Must be ≥ Min.
	// +kubebuilder:validation:Minimum=0
	// +required
	Max int32 `json:"max"`
}

// NodeNetInterface is one advertised endpoint of a satellite.
type NodeNetInterface struct {
	// name is the user-facing identifier ("default", "drbd-net", etc.).
	Name string `json:"name"`

	// address is the IP address of the endpoint.
	Address string `json:"address"`

	// satellitePort is the port the satellite listens on; 0 means default.
	// +optional
	SatellitePort int32 `json:"satellitePort,omitempty"`

	// satelliteEncryptionType is "PLAIN" or "SSL".
	// +kubebuilder:validation:Enum=PLAIN;SSL
	// +optional
	SatelliteEncryptionType string `json:"satelliteEncryptionType,omitempty"`
}

// NodeStatus is the observed state of a node.
type NodeStatus struct {
	// connectionStatus is the satellite connection state reported by the
	// controller (CONNECTED, OFFLINE, …).
	// +optional
	ConnectionStatus string `json:"connectionStatus,omitempty"`

	// flags computed by the satellite (e.g. EVICTED, EVACUATING).
	// +optional
	Flags []string `json:"flags,omitempty"`

	// conditions represents the current state of the Node resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// Node is the Schema for the nodes API
type Node struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Node
	// +required
	Spec NodeSpec `json:"spec"`

	// status defines the observed state of Node
	// +optional
	Status NodeStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// NodeList contains a list of Node
type NodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Node `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Node{}, &NodeList{})
}
