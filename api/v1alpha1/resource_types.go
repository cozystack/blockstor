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

// ConditionMetadataCreated is a Status Condition stamped by the
// satellite after `drbdmeta create-md` succeeds on this node. It
// caches the on-disk metadata state so the reconciler's
// `firstActivation` predicate and the FSM shadow's
// `Observation.MetadataExists` can be derived from the apiserver
// view without round-tripping through the kernel.
//
// Authoritative source of truth remains kernel-side `drbdmeta
// dump-md` (via `Adm.HasMD`) — this Condition is a cache. On
// satellite startup, every Resource on this node whose Condition
// is absent but whose kernel probe returns true gets the Condition
// backfilled, closing the upgrade gap for clusters that pre-date
// Phase 11.3 Stage 1.
//
// Migrates the legacy `<rd>.md-created` file marker
// (`StateDir/<rd>.md-created`). The marker file is still written
// for one release as belt-and-braces — both readers consult the
// Condition first and fall back to the file presence when the
// Condition is absent.
const ConditionMetadataCreated = "MetadataCreated"

// ConditionFilesystemFormatted is a Status Condition stamped by the
// satellite after the RG-driven auto-mkfs path (scenario 9.W14)
// reports every diskful volume of this Resource as carrying a
// filesystem — either freshly mkfs'd or adopted via the per-volume
// `blkid -o export` probe. Caches the "we already finished mkfs"
// state so the reconciler's auto-mkfs fast path and the Bug 311
// retry gate (`needsAutoMkfsRetry`) can short-circuit from the
// apiserver view without a stat on the on-disk
// `<rd>.mkfs.done` marker.
//
// SAFETY: the Condition is a hot-path optimisation, NOT the
// double-mkfs safety net. The per-volume `blkid` probe inside
// `runAutoMkfs` stays as the authoritative guard — re-running
// mkfs on a populated filesystem silently destroys data, and a
// stale / lost Condition must never let that happen. The blkid
// probe sees an empty device only when it really is empty.
//
// Migrates the legacy `<rd>.mkfs.done` file marker
// (`StateDir/<rd>.mkfs.done`). The marker file is still written
// for one release as belt-and-braces — both readers consult the
// Condition first and fall back to the file presence when the
// Condition is absent. Phase 11.3 Stage 2.
const ConditionFilesystemFormatted = "FilesystemFormatted"

// ConditionKernelLoaded is True when the DRBD kernel slot for this
// Resource is loaded (events2 `exists resource` has fired) and not
// yet destroyed. Cached from the observer's events2 stream so the
// reconciler's hot path can skip the `drbdsetup status` round-trip.
//
// SAFETY: like other Phase 11.3 Conditions, this is a hot-path
// optimisation. A stale True value can only cause an extra adjust
// (idempotent); a stale False causes the legacy probe path which
// is correct but slow. The authoritative source remains the
// kernel-direct probe (drbdsetup status / drbdmeta dump-md).
const ConditionKernelLoaded = "KernelLoaded"

// ResourceAnnotationVolumeNumbers is the metadata.annotation key the
// satellite reconciler stamps on a Resource CRD after every successful
// apply pass. The value is a comma-separated list of int32 volume
// numbers sourced from the parent ResourceDefinition's
// `spec.volumeDefinitions[].volumeNumber` at apply time.
//
// Bug 107: when `linstor rd delete` cascades the parent RD CRD via
// owner refs, the per-Resource satellite finalizer runs `handleDelete`
// AFTER the RD CRD is already gone. The pre-fix `lookupVolumeNumbers`
// read the volume-number list straight from `rd.Spec.VolumeDefinitions`
// — with the RD gone it returned empty, the per-volume DeleteVolume
// loop iterated over zero items, and the backing `.img` / ZVOL / LV
// stayed on disk forever. This annotation is the surviving record that
// lets handleDelete fall back to the last-known volume-number set when
// the RD lookup hits NotFound.
//
// Stamped only on successful apply so the annotation never claims
// volumes that haven't been materialised yet. Comma-separated rather
// than JSON so a human operator can `kubectl get resource <r> -o
// yaml | grep volume-numbers` and read it without parsing.
const ResourceAnnotationVolumeNumbers = "blockstor.io/volume-numbers"

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

	// drbdOptions is the typed DRBD configuration applied to this
	// specific replica. Overrides the parent RD's drbdOptions
	// field-by-field. Rare — most config lives at RD or RG scope.
	// Phase 10.3.
	// +optional
	DRBDOptions *DRBDOptions `json:"drbdOptions,omitempty"`

	// extraProps carries upstream-LINSTOR property keys we have not
	// yet typed into structured fields. Forward-compat shim populated
	// only by the REST shim when golinstor sends an unknown key.
	// Phase 10.3.
	// +optional
	ExtraProps map[string]string `json:"extraProps,omitempty"`

	// storagePool is the LINSTOR storage pool name this replica
	// allocates from. Replaces `Spec.Props["StorPoolName"]` (which
	// the dispatcher's buildVolumes already prefers when set).
	// Phase 10.3 step.
	// +optional
	StoragePool string `json:"storagePool,omitempty"`

	// volumes carries per-volume seed configuration that the
	// satellite applies once on first activation of this replica.
	// Today the only field is SeedFromGi (Phase 8.1) — when set,
	// the satellite stamps the new replica's DRBD metadata with
	// this generation identifier before `drbdadm up` so the GI
	// handshake sees the new device as already-in-sync with that
	// peer, skipping the full initial-sync.
	// +listType=map
	// +listMapKey=volumeNumber
	// +optional
	Volumes []ResourceVolumeSpec `json:"volumes,omitempty"`

	// toggleDiskCancel asks the satellite reconciler to abort an
	// in-flight diskless→diskful conversion and roll back to the
	// pre-toggle Diskless state. Set by the REST shim when the
	// operator passes `?cancel=true` to PUT toggle-disk (upstream
	// LINSTOR's `linstor r td --cancel` shape). Bug 40.
	//
	// The reconciler observes this flag in conjunction with a
	// "partial diskful" state (storage carved, DRBD not yet Up or
	// not yet UpToDate) and unwinds by:
	//
	//   1. drbdadm down (idempotent if the kernel resource is
	//      already gone),
	//   2. DeleteVolume on the storage provider,
	//   3. re-stamping the DISKLESS flag on Spec.Flags,
	//   4. clearing this flag and Status.ToggleDiskRetries.
	//
	// Once cleared the satellite resumes normal reconcile loops
	// against the (now diskless) Spec. A cancel issued against a
	// Resource that's already past the partial-state window (DRBD
	// is Up and Volumes report UpToDate) is a no-op — the operator
	// must issue a fresh toggle-disk to demote.
	// +optional
	ToggleDiskCancel bool `json:"toggleDiskCancel,omitempty"`
}

// ResourceVolumeSpec is one volume's per-replica configuration knobs.
// Distinct from ResourceVolumeStatus: this carries one-shot seeding
// hints the satellite consumes during first activation, the Status
// counterpart carries observed runtime state.
type ResourceVolumeSpec struct {
	// volumeNumber matches the corresponding ResourceDefinition
	// VolumeDefinition. +required when this struct is populated.
	VolumeNumber int32 `json:"volumeNumber"`

	// seedFromGi pre-seeds the DRBD-9 generation identifier of this
	// replica's metadata block before the first `drbdadm up`. When
	// set to the CurrentGi of an existing UpToDate peer, DRBD's GI
	// handshake on first connect sees the match and skips the full
	// initial-sync — turning hours of resync on multi-TiB volumes
	// into instant. Phase 8.1.
	//
	// Consumed once on first activation; subsequent reconciles
	// ignore it (the satellite checks `drbdmeta show-gi` before
	// re-stamping).
	// +optional
	SeedFromGi string `json:"seedFromGi,omitempty"`
}

// ResourceStatus is the observed state of a placed resource.
type ResourceStatus struct {
	// inUse is whether DRBD reports the resource as primary anywhere.
	// +optional
	InUse bool `json:"inUse,omitempty"`

	// drbdState is the current resource-level DRBD state reported by
	// `drbdsetup events2` — `UpToDate`, `Outdated`, `Connected`,
	// `Failed`, etc. Phase 10.2: written by the satellite via the
	// Status subresource so a concurrent Spec mutation
	// (auto-diskful, resize) can't clobber it.
	// +optional
	DrbdState string `json:"drbdState,omitempty"`

	// drbdNodeID is the DRBD-9 node-id assigned to this replica.
	// Allocated once when the Resource first reconciles and never
	// changes for the lifetime of the Resource — re-numbering live
	// replicas would re-map their DRBD bitmaps and corrupt data on
	// peer-to-peer resync. Range 0..15 (drbd-9 max-peers). nil means
	// the controller has not yet allocated.
	// +optional
	DRBDNodeID *int32 `json:"drbdNodeId,omitempty"`

	// drbdPort is the TCP port this replica listens on. Allocated
	// from the hosting node's TCP-port range — different replicas
	// of the same RD can use different ports because each lives on
	// a different node and the port is local to that node. Matches
	// upstream LINSTOR's per-resource (not per-RD) port model.
	// nil means not yet allocated.
	// +optional
	DRBDPort *int32 `json:"drbdPort,omitempty"`

	// drbdMinor is the local /dev/drbd<N> minor number on the
	// hosting node. Like drbdPort, allocated per-replica from the
	// node's minor-range — minors are local device numbers, so two
	// replicas on different nodes can have unrelated minors.
	// +optional
	DRBDMinor *int32 `json:"drbdMinor,omitempty"`

	// volumes is the per-volume runtime state reported by the satellite.
	// +listType=map
	// +listMapKey=volumeNumber
	// +optional
	Volumes []ResourceVolumeStatus `json:"volumes,omitempty"`

	// connections is the per-peer DRBD connection state observed by the
	// satellite (from `drbdsetup events2` / `drbdsetup status`). The
	// Python `linstor r list --faulty` reads this through the REST
	// `layer_object.drbd.connections` map — without it, the CLI cannot
	// distinguish a healthy three-peer mesh from one with a broken
	// connection. Keyed by peer node name.
	// +listType=map
	// +listMapKey=peerNodeName
	// +optional
	Connections []ResourceConnectionStatus `json:"connections,omitempty"`

	// conditions represent the current state of the Resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// role is the kernel-reported DRBD-9 role on this node, parsed
	// from the `role:` token in `drbdsetup events2` resource frames.
	// Values mirror the DRBD-9 enum: `Primary` (open for write),
	// `Secondary` (replication-only), `Unknown` (transient — kernel
	// just attached, has not yet picked a role). Per-replica, in
	// contrast to the cluster-wide `inUse` bool: a 3-replica RD has
	// up to one Primary and two Secondaries, all on different nodes.
	// ~15 e2e tests `grep role:` from `drbdsetup status`; surfacing
	// it on Status lets them switch to a k8s-native read.
	// +optional
	Role string `json:"role,omitempty"`

	// suspended is the kernel-reported reason for I/O suspension on
	// this replica, parsed from the `suspended:` token in
	// `drbdsetup events2` resource frames. Values mirror the DRBD-9
	// enum: `No` (I/O serves normally), `Quorum` (quorum lost,
	// `on-no-quorum=suspend-io` blocked the I/O queue), `User`
	// (`drbdadm suspend-io` issued by operator), `NoData` (no
	// UpToDate replica reachable, kernel is waiting for one),
	// `Fencing` (resource-and-stonith fencing handler running).
	// Paired with the per-volume `quorum` field for quorum-loss
	// recovery tests — 3 e2e tests need this to distinguish a
	// recoverable quorum suspend from a user/fencing suspend.
	// +optional
	Suspended string `json:"suspended,omitempty"`

	// toggleDiskRetries counts how many times the satellite
	// reconciler has failed mid-conversion while running the
	// diskless→diskful chain (storage carve → drbdmeta create-md
	// → drbdadm up → initial-sync). Upstream LINSTOR exposes the
	// same counter under `Resource.toggle_disk_retries` so
	// operators can spot a permanently-stuck conversion via
	// `linstor r l`. Bug 39.
	//
	// Reset to 0 when the conversion succeeds (Resource reaches
	// the diskful steady-state) or when a cancel completes via
	// `Spec.ToggleDiskCancel`. The reconciler does NOT impose a
	// hard cap — the bound is policy that lives in the controller
	// layer (today the legacy backoff cap of 10 attempts) and is
	// surfaced via a `ToggleDiskFailed` condition once exceeded.
	// +optional
	ToggleDiskRetries int32 `json:"toggleDiskRetries,omitempty"`
}

// ResourceConnectionStatus is the state of one DRBD peer connection
// from this replica's point of view. Maps 1:1 to upstream LINSTOR's
// `DrbdConnection` REST shape — the Python CLI walks the `connected`
// flag to color the Conns column red on `linstor r list`.
type ResourceConnectionStatus struct {
	// peerNodeName is the name of the peer Resource (and the node
	// it lives on — Resource names are RD.node).
	PeerNodeName string `json:"peerNodeName"`
	// connected is true iff DRBD reports the connection as
	// `Connected` (handshake complete, replication active).
	// +optional
	Connected bool `json:"connected,omitempty"`
	// message is the human-readable DRBD connection state
	// (`Connected`, `StandAlone`, `BrokenPipe`, `NetworkFailure`,
	// `Connecting`, `Timeout`, etc.) as reported by `drbdsetup
	// events2`. Populated even when Connected — useful for
	// surfacing transitional states.
	// +optional
	Message string `json:"message,omitempty"`

	// replicationState is the DRBD-9 replication state for this
	// peer reported by `drbdsetup events2 --statistics` peer-
	// device frames: Established / SyncSource / SyncTarget /
	// PausedSync* / VerifyS / VerifyT / Ahead / Behind / Off /
	// WFBitMap* / WFSyncUUID / StartingSyncS / StartingSyncT. The
	// Python CLI's `linstor v l` Repl column reads this.
	// +optional
	ReplicationState string `json:"replicationState,omitempty"`

	// peerDrbdNodeId is the DRBD-9 node-id assigned to the peer in
	// this resource's connection table. Read from `drbdsetup
	// events2` connection-frame `peer-node-id` field. Six e2e
	// tests today parse this value out of `drbdsetup status
	// --verbose` adjacent to the peer-name — surfacing it on
	// Status lets them switch to a k8s-native read. The peer's
	// own Resource CRD also carries this number under
	// `Status.DRBDNodeID`, so callers with the atomic-view luxury
	// can cross-check; the field is retained here for ergonomics
	// when only the local Resource is in hand.
	// +optional
	PeerDRBDNodeID *int32 `json:"peerDrbdNodeId,omitempty"`

	// peerVolumes is the peer's view of each volume's disk state
	// on this connection — read from `drbdsetup events2
	// --statistics` peer-device frames' `peer-disk:<state>` token.
	// Important under network-partition: the local replica's
	// `Status.Volumes[i].DiskState` (UpToDate from this node's
	// kernel) and the peer's view of the same volume
	// (`DUnknown` from the peer's kernel) diverge. The
	// state-standalone-partition.sh and network-partition.sh
	// e2e tests assert against this divergence; without
	// peerVolumes they shell into the satellite and grep
	// drbdsetup. Keyed by volumeNumber.
	// +optional
	// +listType=map
	// +listMapKey=volumeNumber
	PeerVolumes []PeerVolumeStatus `json:"peerVolumes,omitempty" patchMergeKey:"volumeNumber" patchStrategy:"merge"`
}

// PeerVolumeStatus is the per-volume slice of a peer's view of one
// connection: what the peer's kernel reports its replica's disk_state
// to be. Distinct from the local-side `ResourceVolumeStatus.DiskState`
// because under network-partition the two views diverge (local says
// UpToDate, peer says DUnknown / Outdated). 8 e2e partition tests
// parse this token out of `drbdsetup status --verbose`; the field
// gives them a kubectl-native substitute.
type PeerVolumeStatus struct {
	// volumeNumber is the DRBD-9 volume number this peer-view entry
	// describes. Matches the parent
	// `ResourceDefinition.Spec.VolumeDefinitions[].VolumeNumber`.
	VolumeNumber int32 `json:"volumeNumber"`

	// peerDiskState is the peer's DRBD-9 disk_state for this volume
	// — `UpToDate`, `Outdated`, `Inconsistent`, `Diskless`, or
	// `DUnknown` (the local kernel cannot determine the peer's
	// disk state, typical under partition). Mirrors the
	// `peer-disk:<state>` token in `drbdsetup events2
	// --statistics` peer-device frames.
	// +optional
	PeerDiskState string `json:"peerDiskState,omitempty"`
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

	// currentGi is the DRBD-9 current generation identifier reported
	// by `drbdsetup events2` for this replica's local volume. The
	// controller reads it when adding a new replica to skip the full
	// initial-sync: pre-seeding the new metadata block with this GI
	// makes DRBD's GI handshake see the new peer as already-in-sync,
	// turning hours of resync on multi-TiB volumes into instant.
	// Updated by the satellite-side observer; never set in Spec.
	// +optional
	CurrentGi string `json:"currentGi,omitempty"`

	// historyGi carries the historical GI chain (DRBD keeps 3-4
	// previous generations). Useful for split-brain forensics and
	// cluster-state UI; may be elided when we run tight on Status
	// budget. Order is newest-first.
	// +optional
	HistoryGi []string `json:"historyGi,omitempty"`

	// outOfSyncKib is the worst-case "how many KiB this volume is
	// behind any peer" reported by `drbdsetup events2 --statistics`
	// peer-device frames. Combined with RD.Spec.VolumeDefinitions
	// SizeKib, callers compute a sync-progress percentage:
	//   progress = (1 - outOfSyncKib / sizeKib) * 100
	//
	// 0 means fully in sync; sizeKib (or close to it) means fresh
	// resync hasn't started yet. Surfaced via the REST view layer
	// so `linstor r l` / piraeus dashboards can render a CDI-style
	// progress bar without polling drbdsetup themselves.
	// +optional
	OutOfSyncKib int64 `json:"outOfSyncKib,omitempty"`

	// quorum reports the DRBD-9 kernel's per-volume quorum view
	// from the `quorum:yes|no` field in events2 `device` frames.
	// true means the local replica has quorum and may serve I/O;
	// false means the kernel has marked the volume quorum-lost
	// (writes block / fail-fast depending on `on-no-quorum`
	// policy). Per-volume is finer-grained than the node-wide
	// `drbd.linbit.com/lost-quorum` Kubernetes taint: a node with
	// quorum on one RD and no-quorum on another surfaces both
	// states correctly. The CSI plugin's quorum-aware mount path
	// (refusing to mount a no-quorum volume) reads this signal.
	// +optional
	Quorum bool `json:"quorum,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || self.metadata.name == self.spec.resourceDefinitionName + '.' + self.spec.nodeName",message="metadata.name must equal <spec.resourceDefinitionName>.<spec.nodeName>",optionalOldSelf=true

// Resource is the Schema for the resources API.
//
// The CEL rule above enforces the cluster-wide naming convention every
// node-bound CRD in the project follows: `metadata.name == <rd>.<node>`.
// Keeping the composite key encoded in the name lets operators grep for
// `<node>.` across kinds (Resource, Snapshot, StoragePool) and find every
// resource bound to one satellite at once.
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
