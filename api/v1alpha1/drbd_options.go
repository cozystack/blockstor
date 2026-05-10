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
	corev1 "k8s.io/api/core/v1"
)

// DRBDOptions is the typed-fields replacement for upstream LINSTOR's
// `Spec.Props["DrbdOptions/<Section>/<Key>"]` bag. Embedded on
// ResourceGroup, ResourceDefinition, Resource (and ControllerConfig
// once Phase 10.4 lands). The resolver merges field-by-field along
// the override hierarchy ControllerConfig → RG → RD → Resource, lowest
// scope wins per non-nil field.
//
// `*int32` and `*bool` use the nil-vs-set discipline: nil means "not
// overridden at this scope, inherit from parent"; any non-nil value
// (including the zero value) means "explicitly set, do not inherit".
type DRBDOptions struct {
	// net configures the `net { ... }` section of the rendered .res
	// file (replication protocol, secret, dual-primary, after-sb).
	// +optional
	Net *DRBDNetOptions `json:"net,omitempty"`

	// disk configures the `disk { ... }` section (on-io-error,
	// activity-log size, resync hints).
	// +optional
	Disk *DRBDDiskOptions `json:"disk,omitempty"`

	// peerDevice configures the `peer-device { ... }` block (per-peer
	// resync rate caps and similar).
	// +optional
	PeerDevice *DRBDPeerDeviceOptions `json:"peerDevice,omitempty"`

	// resource configures resource-level `options { ... }` (auto-
	// promote, quorum mode, on-no-quorum action).
	// +optional
	Resource *DRBDResourceOptions `json:"resource,omitempty"`

	// handlers maps event names to script paths (e.g. fence-peer,
	// before-resync-target). Free-form for now; the rendered .res
	// emits one line per pair under `handlers { }`.
	// +optional
	Handlers map[string]string `json:"handlers,omitempty"`
}

// DRBDNetOptions is the typed equivalent of `DrbdOptions/Net/*` keys.
type DRBDNetOptions struct {
	// protocol picks the DRBD replication protocol. A=async (writer
	// returns once data hits local disk), B=memory-sync (returns on
	// peer ACK after kernel buffer), C=sync (returns on peer disk
	// fsync). DRBD-9 default is C.
	// +kubebuilder:validation:Enum=A;B;C
	// +optional
	Protocol string `json:"protocol,omitempty"`

	// sharedSecretRef references a Secret containing the DRBD network
	// authentication shared secret under the key `shared-secret`.
	// +optional
	SharedSecretRef *corev1.LocalObjectReference `json:"sharedSecretRef,omitempty"`

	// allowTwoPrimaries enables dual-primary mode (live-migration,
	// shared-export NFS Ganesha). Must be paired with consumer-side
	// fencing — DRBD does NOT serialise concurrent writes from two
	// primaries.
	// +optional
	AllowTwoPrimaries *bool `json:"allowTwoPrimaries,omitempty"`

	// maxBuffers is the maximum number of in-flight network buffers.
	// +optional
	MaxBuffers *int32 `json:"maxBuffers,omitempty"`

	// afterSb0Pri is the recovery action when DRBD detects split-
	// brain and both sides were Secondary at the time.
	// +kubebuilder:validation:Enum=disconnect;discard-younger-primary;discard-older-primary;discard-zero-changes;discard-least-changes;discard-local;discard-remote
	// +optional
	AfterSb0Pri string `json:"afterSb0Pri,omitempty"`

	// afterSb1Pri is the recovery action when one side was Primary.
	// +kubebuilder:validation:Enum=disconnect;consensus;violently-as0p;discard-secondary;call-pri-lost-after-sb
	// +optional
	AfterSb1Pri string `json:"afterSb1Pri,omitempty"`

	// afterSb2Pri is the recovery action when both sides were Primary
	// (only possible if allow-two-primaries was enabled).
	// +kubebuilder:validation:Enum=disconnect;violently-as0p;call-pri-lost-after-sb
	// +optional
	AfterSb2Pri string `json:"afterSb2Pri,omitempty"`
}

// DRBDDiskOptions is the typed equivalent of `DrbdOptions/Disk/*` keys.
type DRBDDiskOptions struct {
	// onIOError is the action when the local backing device returns
	// an I/O error. `detach` is the production-safe default — the
	// satellite drops the local replica to Diskless and consumers
	// keep going via the network path.
	// +kubebuilder:validation:Enum=detach;pass-on;call-local-io-error
	// +optional
	OnIOError string `json:"onIoError,omitempty"`

	// alExtents is the activity-log size (in extents). Larger values
	// reduce metadata write amplification at the cost of longer
	// resync windows.
	// +optional
	ALExtents *int32 `json:"alExtents,omitempty"`
}

// DRBDPeerDeviceOptions is the typed equivalent of
// `DrbdOptions/PeerDevice/*` keys.
type DRBDPeerDeviceOptions struct {
	// cMaxRate caps the per-peer resync transfer rate. Format
	// matches DRBD: integer + suffix (`100M`, `1G`).
	// +optional
	CMaxRate string `json:"cMaxRate,omitempty"`
}

// DRBDResourceOptions is the typed equivalent of
// `DrbdOptions/Resource/*` keys (resource-level `options { }` block).
type DRBDResourceOptions struct {
	// autoPromote causes DRBD to promote the resource to Primary
	// automatically when a consumer opens the device, provided no
	// other peer is already Primary. Cluster-wide default true.
	// +optional
	AutoPromote *bool `json:"autoPromote,omitempty"`

	// quorum picks the quorum policy. `majority` requires more than
	// half of all replicas (including diskless witnesses) to be
	// connected before allowing writes. `off` allows writes from any
	// replica regardless of partition state — split-brain risk.
	// `all` requires every replica to be reachable, useful only for
	// strict 2-replica configurations under deliberate operator
	// control.
	// +kubebuilder:validation:Enum=off;majority;all
	// +optional
	Quorum string `json:"quorum,omitempty"`

	// onNoQuorum is the action when quorum is lost. `io-error`
	// returns EIO to the consumer (drbd-reactor's typical trigger
	// for failover). `suspend-io` blocks I/O until quorum returns
	// (useful for deliberate maintenance windows).
	// +kubebuilder:validation:Enum=io-error;suspend-io;freeze-io
	// +optional
	OnNoQuorum string `json:"onNoQuorum,omitempty"`

	// autoTieBreaker gates auto-creation of a DISKLESS+TIE_BREAKER
	// witness when a parent RD has an even number of diskful
	// replicas and no operator-placed diskless replica. Replaces
	// `Props["DrbdOptions/AutoAddQuorumTiebreaker"]`. nil inherits
	// the cluster-wide default (true). Phase 10.3.
	// +optional
	AutoTieBreaker *bool `json:"autoTieBreaker,omitempty"`
}

// EncryptionConfig configures LUKS encryption for a ResourceDefinition.
// Replaces the `Props["DrbdOptions/Encryption/passphrase"]` plaintext-
// in-spec antipattern with a Secret reference. Cluster-wide encryption
// (single passphrase per cluster) is configured via ControllerConfig.
type EncryptionConfig struct {
	// passphraseSecretRef references the Secret carrying the LUKS
	// passphrase under the key `passphrase`. The satellite reads it
	// via the apiserver during reconcile; the passphrase never lands
	// on Spec in plaintext.
	// +optional
	PassphraseSecretRef *corev1.LocalObjectReference `json:"passphraseSecretRef,omitempty"`
}
