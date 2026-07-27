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

	// suspendIO, when true, signals every satellite that hosts a
	// diskful replica of the parent ResourceDefinition to call
	// `drbdsetup suspend-io <rd>` before taking the local backing
	// snapshot. Bug 351: two diskful replicas snapshotting
	// independently would otherwise capture divergent bytes while
	// the application writer's traffic is still streaming through
	// DRBD; suspending I/O on every peer first freezes the
	// replicated block stream at a single, common point so the
	// per-node LV / zvol snapshots all reflect the same
	// point-in-time bytes.
	//
	// Lifecycle (driven by the controller-side SnapshotReconciler):
	//   1. apiserver creates Snapshot CRD with SuspendIO=true,
	//      TakeSnapshot=false. Satellites stamp
	//      Status.NodeStatus[].SuspendIOAcked once their suspend-io
	//      call returns.
	//   2. Once every targeted node has acked, the controller stamps
	//      Spec.TakeSnapshot=true. Satellites then dispatch
	//      provider.CreateSnapshot and stamp Status.NodeStatus[].Ready.
	//   3. Once every targeted node is Ready (or any has Failed=true),
	//      the controller flips Spec.SuspendIO=false and satellites
	//      issue `drbdsetup resume-io <rd>`.
	//
	// Resume-on-failure is mandatory — a partially-acked suspend
	// followed by an abort MUST still resume I/O on the nodes that
	// did ack, otherwise application I/O hangs forever. The
	// controller's abort path clears SuspendIO unconditionally.
	// +optional
	SuspendIO bool `json:"suspendIO,omitempty"`

	// takeSnapshot, when true, signals each satellite that has
	// already acked the suspend-io barrier (see SuspendIO above) to
	// dispatch the local provider.CreateSnapshot. The flag is
	// stamped by the controller-side SnapshotReconciler once every
	// targeted node's Status.NodeStatus[].SuspendIOAcked is true —
	// the two-step `suspend → take → resume` shape mirrors upstream
	// LINSTOR's CtrlSnapshotCrtApiCallHandler 3-phase flow so the
	// per-node backing snapshots all reflect the same point-in-time
	// bytes.
	// +optional
	TakeSnapshot bool `json:"takeSnapshot,omitempty"`

	// groupID, when non-empty, marks this Snapshot as a member of a
	// transactional multi-RD batch — every Snapshot CRD stamped with
	// the same GroupID participates in a SINGLE suspend-io broadcast
	// across the UNION of every sibling's targeted nodes. Bug 353:
	// `linstor s create-multiple` (POST /v1/actions/snapshot/multi)
	// previously looped per-RD through the Bug-351 orchestrator and
	// each Snapshot ran its own independent 3-phase suspend/take/
	// resume — so per-RD suspend windows did not overlap and the
	// cross-RD point-in-time consistency operators expect from a
	// "group snapshot" (DB + WAL on separate RDs) was lost.
	//
	// Lifecycle (driven by the controller-side SnapshotReconciler):
	//   1. apiserver stamps every batched Snapshot with the same
	//      crypto/rand-generated GroupID + SuspendIO=true.
	//   2. Controller gates phase advancement on the FULL group:
	//      Phase 2 only fires when every sibling's every targeted
	//      node has acked the suspend.
	//   3. Phase 3 only fires when every sibling's every targeted
	//      node is Ready — or any sibling node Failed=true, in
	//      which case the abort cascade fires SuspendIO=false on
	//      every sibling immediately.
	//
	// Empty GroupID is the single-snap path (Bug 351 behaviour
	// preserved verbatim — siblings denominator collapses to self).
	// +optional
	GroupID string `json:"groupID,omitempty"`

	// groupSize is the number of Snapshot CRDs the apiserver fanned
	// out for this transactional batch (len of the
	// `snapshot create-multiple` entry list). It is the denominator the
	// controller-side SnapshotReconciler uses to decide when the group
	// is fully ASSEMBLED before it opens the suspend-io barrier.
	//
	// Bug 046 / Bug-353 atomicity fix: the apiserver creates the
	// sibling CRDs SEQUENTIALLY (one Store.Create per entry, with
	// hydration + an offline pre-check between each), so on a busy
	// stand the last sibling's CRD can land ~15s after the first. If
	// each satellite started `drbdsetup suspend-io` the instant its
	// own sibling appeared (the old behaviour, with SuspendIO stamped
	// at Create time), the group's volumes would freeze ~15s APART —
	// well over the ≤5s consistency budget — so a DB's data volume and
	// WAL volume on separate RDs would be captured at different
	// instants and a group restore could be corrupt.
	//
	// With GroupSize the controller holds the WHOLE group at Phase 0
	// (no SuspendIO) until it observes every member, then flips
	// SuspendIO=true on every sibling in a single reconcile pass so
	// they all enter suspend within one controller cycle (sub-second),
	// bounding the suspend-entry slip far under budget.
	//
	// Zero means "not a coordinated batch" (the single-snap Bug-351
	// path, or a legacy grouped Snapshot created before this field
	// existed) — in that case the controller falls back to the
	// observed-siblings count so a GroupID with no GroupSize still
	// makes progress instead of hanging on a missing denominator.
	// +optional
	GroupSize int32 `json:"groupSize,omitempty"`
}

// SnapshotVolumeRef is one volume slot inside a Snapshot.
type SnapshotVolumeRef struct {
	VolumeNumber int32 `json:"volumeNumber"`
	SizeKib      int64 `json:"sizeKib"`
}

// AnnotationSnapshotAdopted marks a Snapshot whose on-disk
// materialisation ALREADY EXISTS on every targeted node — it was
// created by a previous storage controller (LINSTOR) and imported by
// a migration tool, not taken by blockstor. The controller-side
// SnapshotReconciler treats such a Snapshot as terminally complete:
// it backfills Status.NodeStatus[*].Ready=true for every Spec.Nodes
// entry and NEVER runs the suspend→take→resume orchestration —
// re-driving Phase 1 against an adopted Snapshot would freeze
// production I/O (drbdsetup suspend-io on every diskful peer) just to
// re-take a snapshot that is already on disk.
//
// Set this annotation ONLY when the backing snapshot verifiably
// exists on the listed nodes; stamping it on a Snapshot with no
// on-disk backing yields an object that lists as Successful but whose
// restore will fail with the provider's not-found error.
//
// The annotation is meant to be stamped at CREATION. The controller
// deliberately IGNORES it on a Snapshot that is already mid-flight
// (Spec.SuspendIO=true) and completes the normal orchestration
// instead: short-circuiting there would leave every diskful peer
// holding `drbdsetup suspend-io` with nothing left to resume it —
// frozen production I/O with no error surfaced.
const AnnotationSnapshotAdopted = "blockstor.io/adopted"

// AnnotationSnapshotAdoptedCreatedAt optionally carries the original
// creation time of an adopted snapshot as milliseconds since the Unix
// epoch (the shape LINSTOR's RESOURCES.create_timestamp column uses).
// The SnapshotReconciler copies it into the backfilled per-node
// CreateTimestamp entries so `linstor s l` keeps showing the REAL
// snapshot age instead of the adoption time.
const AnnotationSnapshotAdoptedCreatedAt = "blockstor.io/adopted-created-at"

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

// SnapshotStatusFlagFailedDisconnect is stamped on Status.Flags by
// the controller-side SnapshotReconciler when it aborts a snapshot
// that hung in the suspend/take phase past the deadline — a
// satellite never reported back (silently unreachable, satellite
// pod stuck, backend snapshot hang) so no per-node Failed=true
// stamp ever arrived. Mirrors upstream LINSTOR's `FAILED_DISCONNECT`
// SnapshotDefinition flag: the take did not complete because a
// satellite stopped responding rather than because the satellite
// tried and gave up (`FAILED_DEPLOYMENT`/`FAILED`). The Python CLI
// renders this as the `State="Satellite disconnected"` column in
// `linstor s l`. Distinguishing this from the plain FAILED stamp is
// the "distinguish failure reasons" half of the upstream safety
// contract — operators see *why* the snapshot aborted.
const SnapshotStatusFlagFailedDisconnect = "FAILED_DISCONNECT"

// SnapshotStatusConditionType is the well-known Status.Conditions[]
// type the controller-side SnapshotReconciler stamps to record a
// human-readable abort reason (timeout vs non-UpToDate replica vs
// satellite-unreachable) alongside the terminal Flags marker.
const SnapshotStatusConditionType = "SnapshotComplete"

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

	// suspendIOAcked is stamped true by the local satellite once
	// `drbdsetup suspend-io <rd>` has returned for the parent
	// ResourceDefinition. The controller-side SnapshotReconciler
	// gates the Phase-2 `TakeSnapshot` transition on every targeted
	// node having acked. Cleared back to false when the controller
	// flips Spec.SuspendIO=false and the satellite issues
	// `drbdsetup resume-io <rd>`. Bug 351.
	// +optional
	SuspendIOAcked bool `json:"suspendIOAcked,omitempty"`

	// failed is stamped true when the local satellite hit a
	// terminal failure while either suspending I/O, taking the
	// per-node snapshot, or resuming I/O. The controller-side
	// SnapshotReconciler treats any Failed=true entry as an abort
	// signal: it flips Spec.SuspendIO=false immediately so the
	// already-suspended siblings resume rather than wait
	// indefinitely on the doomed node, and stamps the parent
	// Snapshot's Status.Flags with FAILED. Bug 351.
	// +optional
	Failed bool `json:"failed,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || self.metadata.name.lowerAscii() == (self.spec.resourceDefinitionName + '.' + self.spec.snapshotName).lowerAscii()",message="metadata.name must equal <spec.resourceDefinitionName>.<spec.snapshotName> (case-insensitive)",optionalOldSelf=true

// +kubebuilder:printcolumn:name="Definition",type=string,JSONPath=`.spec.resourceDefinitionName`
// +kubebuilder:printcolumn:name="Snapshot",type=string,JSONPath=`.spec.snapshotName`
// +kubebuilder:printcolumn:name="Nodes",type=string,JSONPath=`.spec.nodes`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// Snapshot is the Schema for the snapshots API.
//
// The CEL rule above enforces the cluster-wide naming convention every
// composite-keyed CRD in the project follows: `metadata.name == <rd>.<snap>`.
// Keeping the composite key encoded in the name lets the store's
// `snapshotCRDName` helper round-trip the (rd, snap) pair through k8s
// metadata without a sidecar index, and lets operators grep for
// `<rd>.` across kinds (Resource, Snapshot, StoragePool) to find every
// object bound to one parent. The `optionalOldSelf` escape makes the
// rule create-only — finalizer-strip on a stale-named Snapshot
// (e.g. one created before this marker existed) is never blocked.
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
