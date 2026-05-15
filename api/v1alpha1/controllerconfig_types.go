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

// ControllerConfigName is the canonical singleton name of the
// cluster-wide ControllerConfig instance. Operators install
// blockstor with one ControllerConfig at this name; the reconciler
// looks up `default` and refuses to read any other instance, so a
// stale or duplicated ControllerConfig can't quietly take effect.
const ControllerConfigName = "default"

// ControllerConfigSpec is the desired state of the cluster-wide
// blockstor controller configuration. Phase 10.4: replaces the
// upstream-LINSTOR-shaped `ControllerProps` KVEntry instance with a
// typed singleton CRD so admission validation enforces enum
// constraints and structured tooling (kubectl edit, GitOps) can
// operate on it directly.
//
// Cluster-wide DRBD overrides live under Spec.DRBDOptions and feed
// into the same hierarchy resolver used at RG/RD/Resource scopes
// (controller → RG → RD → Resource); lower scopes still win
// per non-nil field.
type ControllerConfigSpec struct {
	// drbdOptions is the cluster-wide DRBD configuration. Acts as
	// the controller-scope input to the typed hierarchy resolver
	// (`pkg/drbd.ResolveDRBDOptions`).
	// +optional
	DRBDOptions *DRBDOptions `json:"drbdOptions,omitempty"`

	// passphraseSecretRef points at the Secret carrying the
	// cluster-wide LUKS passphrase under the key `passphrase`. The
	// satellite reads it via the apiserver during reconcile;
	// passphrase never lands on Spec in plaintext. Replaces
	// `Props["DrbdOptions/Encryption/passphrase"]` on the legacy
	// ControllerProps instance.
	// +optional
	PassphraseSecretRef *PassphraseSecretRef `json:"passphraseSecretRef,omitempty"`

	// extraProps carries upstream-LINSTOR-shaped Props keys we have
	// not yet typed into structured fields. Forward-compat shim
	// populated only by the REST shim when golinstor sends an
	// unknown key. Drained as we type more fields here.
	// +optional
	ExtraProps map[string]string `json:"extraProps,omitempty"`

	// nodeConnections is the per-(nodeA,nodeB) property bag exposed
	// at `/v1/node-connections/{nodeA}/{nodeB}` — backs upstream
	// LINSTOR's `linstor node-connection set-property / list` CLI
	// (Bug 101). Keys are the canonical pair-id `<lo>::<hi>` where
	// `<lo>` and `<hi>` are the lexicographically lower / higher of
	// the two node names — so a write against (A,B) and a read
	// against (B,A) hit the same record without the caller having
	// to remember any ordering convention.
	//
	// Stored on the cluster-wide ControllerConfig singleton rather
	// than a per-pair CRD: upstream LINSTOR persists these on a
	// flat in-memory matrix attached to the Controller, and the
	// cozystack node-connection use case is overwhelmingly empty
	// or one-off ("flag this pair as cross-site"). A dedicated CRD
	// per pair would add admission/RBAC surface for a feature that
	// in practice carries ≤ a handful of rows on any given cluster.
	// +optional
	NodeConnections map[string]map[string]string `json:"nodeConnections,omitempty"`
}

// PassphraseSecretRef is the cluster-wide passphrase Secret pointer.
// Wrapper rather than a bare LocalObjectReference so we can attach
// a Namespace field later without breaking the API (the secret
// must live in the controller's own namespace today; once Phase 10.6
// lands and the satellite reads via the apiserver it may move).
type PassphraseSecretRef struct {
	// name is the Secret name. Required.
	// +required
	Name string `json:"name"`
}

// ControllerConfigStatus reports observed state.
type ControllerConfigStatus struct {
	// conditions track the current state of the ControllerConfig
	// (e.g. PassphraseSecretFound, DRBDOptionsValid).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// ControllerConfig is the cluster-wide singleton config for the
// blockstor controller. Exactly one instance with name=`default`
// is recognised; any other instance is ignored. Phase 10.4.
type ControllerConfig struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ControllerConfig
	// +required
	Spec ControllerConfigSpec `json:"spec"`

	// status defines the observed state of ControllerConfig
	// +optional
	Status ControllerConfigStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ControllerConfigList contains a list of ControllerConfig.
type ControllerConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ControllerConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ControllerConfig{}, &ControllerConfigList{})
}
