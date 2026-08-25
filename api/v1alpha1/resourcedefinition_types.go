// SPDX-License-Identifier: Apache-2.0

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

	// drbdOptions is the typed DRBD configuration applied to this
	// RD. Overrides the parent ResourceGroup's drbdOptions
	// field-by-field; in turn overridden by per-Resource drbdOptions
	// at the Resource scope. Phase 10.3.
	// +optional
	DRBDOptions *DRBDOptions `json:"drbdOptions,omitempty"`

	// encryption configures LUKS encryption for the volumes in this
	// RD. The passphrase is held in a referenced Secret rather than
	// inline in the spec. Phase 10.3.
	// +optional
	Encryption *EncryptionConfig `json:"encryption,omitempty"`

	// extraProps carries upstream-LINSTOR property keys we have not
	// yet typed into structured fields. Forward-compat shim populated
	// only by the REST shim when golinstor sends an unknown key.
	// Phase 10.3.
	// +optional
	ExtraProps map[string]string `json:"extraProps,omitempty"`

	// drbdPort is an OPTIONAL preferred TCP port seed for this RD,
	// mirroring upstream LINSTOR's `RscDfn.port` preferred value. The
	// per-node port allocator (Resource.Spec.DRBDPort) tries this
	// value first on each hosting node and falls back to a per-node
	// free port on collision. nil means "no preference — allocate
	// per-node freely". This is a seed, NOT the authoritative
	// listen-port: the authoritative per-replica value lives on
	// Resource.Spec.DRBDPort. clusterIP-style settable-once so an
	// accidental edit can't perturb the per-node allocation.
	// +optional
	// +kubebuilder:validation:XValidation:rule="!oldSelf.hasValue() || self == oldSelf.value()",optionalOldSelf=true,message="drbdPort is settable-once"
	DRBDPort *int32 `json:"drbdPort,omitempty"`

	// initialized is the durable "this RD has completed its initial
	// replica-set establishment and now holds (or has held) committed
	// data" latch. The controller flips it true ONCE, append-only, the
	// first time any diskful replica of the RD reports a real
	// data-bearing disk state that has advanced past the deterministic
	// day0 GI (a genuine write, or an adoption of pre-existing data) —
	// see ResourceReconciler.ensureRDInitialized. Once true it is NEVER
	// cleared or mutated.
	//
	// It is the OFFLINE-SAFE, BACKUP-SAFE source of truth for the
	// skip-initial-sync decision (replaces the live-kernel
	// AnyConnectedPeerHasData probe as the authoritative signal):
	//
	//   - A Resource created while initialized is nil/false is part of
	//     the genuinely-fresh initial set — every such replica is
	//     stamped Spec.SkipInitialSync=true and skips the initial sync
	//     (instant UpToDate, no SyncTarget). Invariant 1.
	//   - A Resource created AFTER initialized latched true (relocate /
	//     migrate-disk / extra replica) is stamped
	//     Spec.SkipInitialSync=false and must SyncTarget the real data —
	//     EVEN IF the sole data-holder is offline at seed time, because
	//     this latch was set while the data was being written (holder
	//     online) and PERSISTS in Spec across the holder going offline.
	//     Invariant 2 + offline-safety.
	//
	// Lives in Spec (backed up by `kubectl get -o yaml`, restored by
	// `kubectl apply`), NOT Status — a restored, already-initialized RD
	// must keep forcing new replicas to sync rather than skip. Invariant
	// 3 (backup/restore-safe).
	//
	// clusterIP-style append-only: nil means "not yet initialized" (the
	// conservative pre-upgrade / fresh default → fresh replicas may skip,
	// the controller flips it on first real data). The controller writes
	// it only when transitioning nil/false→true and never otherwise, so
	// a user `kubectl apply` / golinstor REST that omits it never fights
	// the controller (3-way-merge-safe — invariant 4).
	// +optional
	// +kubebuilder:validation:XValidation:rule="!oldSelf.hasValue() || self == oldSelf.value() || (oldSelf.value() == false && self == true)",optionalOldSelf=true,message="initialized is append-only: it may go unset→value or false→true but never back to false"
	Initialized *bool `json:"initialized,omitempty"`
}

// ResourceDefinitionVolume is one volume slot inside an RD.
type ResourceDefinitionVolume struct {
	VolumeNumber int32 `json:"volumeNumber"`

	// sizeKib is the volume size. The bounds are enforced HERE, by the
	// API server, rather than in whichever client happens to be writing:
	// the CLI talks to these CRDs directly, so a check that lives in one
	// client is not a check the data is subject to.
	//
	// The floor is DRBD's own per-device minimum once metadata is
	// reserved. Below it the satellite does not fail — it loops on
	// `drbdadm create-md` forever, which is why zero is the one value
	// that must never reach it, and why an overflowing size parse that
	// lands on zero has to be refused before it is stored.
	//
	// +kubebuilder:validation:Minimum=4096
	// +kubebuilder:validation:Maximum=17179869184
	SizeKib int64 `json:"sizeKib"`
	// +optional
	Props map[string]string `json:"props,omitempty"`
	// +optional
	Flags []string `json:"flags,omitempty"`

	// drbdMinor is the /dev/drbd<N> device minor for THIS volume,
	// identical on every node that hosts a replica (the minor is the
	// device identity, not a per-replica value). Mirrors upstream
	// LINSTOR's per-volume-definition `VLM_MINOR_NR`.
	//
	// clusterIP-style allocation (Service.spec.clusterIP model): nil
	// means "controller, allocate one for me"; a non-nil value is
	// authoritative and is NEVER overwritten — that is what makes a
	// plain `kubectl get -o yaml` backup + `kubectl apply` restore
	// preserve the device identity with no resync/flap. The pointer
	// (rather than a plain int32) distinguishes "unset" from a valid
	// minor 0.
	//
	// Replaces the legacy single `RD.Status.DRBDMinor` base +
	// `base+volumeNumber` derivation: each volume carries its own
	// minor, so adoption can preserve arbitrary (possibly
	// non-contiguous) LINSTOR per-volume minors verbatim.
	//
	// NOTE: no CEL settable-once rule here. volumeDefinitions is a
	// plain (uncorrelatable) array — CEL `oldSelf` cannot track an
	// element across an update, so a transition rule on a nested
	// array field is rejected by the apiserver. Immutability is
	// instead enforced by the controller's allocate-if-nil /
	// respect-preset pass (it NEVER overwrites a non-nil minor) plus
	// the store-side VolumeDefinitions carry-across that preserves the
	// value through a REST modify. The settable-once CEL is retained
	// on the (correlatable scalar) Resource.Spec.DRBDPort / DRBDNodeID
	// and RD.Spec.DRBDPort fields.
	// +optional
	DRBDMinor *int32 `json:"drbdMinor,omitempty"`
}

// ResourceDefinitionStatus is the observed state.
type ResourceDefinitionStatus struct {
	// conditions represent the current state of the ResourceDefinition.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// drbdPort is a LEGACY read-only mirror of the (historically
	// cluster-scope) DRBD port. DEPRECATED: the authoritative DRBD
	// listen-port now lives per-replica on Resource.Spec.DRBDPort,
	// and the optional preferred seed on RD.Spec.DRBDPort. Retained
	// so the one-time status→spec backfill migration can read the
	// pre-upgrade value, and so observers/REST that still read it
	// don't break in the same change. Not written by the allocator
	// any more. Bug 266.
	// +optional
	DRBDPort *int32 `json:"drbdPort,omitempty"`

	// drbdMinor is a LEGACY read-only mirror of the (historically
	// cluster-scope, base+k) DRBD minor. DEPRECATED: the authoritative
	// per-volume minor now lives on
	// ResourceDefinition.Spec.VolumeDefinitions[].DRBDMinor. Retained
	// so the one-time status→spec backfill migration can read the
	// pre-upgrade base value (expanded base+k per volume). Not written
	// by the allocator any more. Bug 268.
	// +optional
	DRBDMinor *int32 `json:"drbdMinor,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Group",type=string,JSONPath=`.spec.resourceGroupName`
// +kubebuilder:printcolumn:name="Port",type=integer,JSONPath=`.spec.drbdPort`
// +kubebuilder:printcolumn:name="Layers",type=string,JSONPath=`.spec.layerStack`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

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
