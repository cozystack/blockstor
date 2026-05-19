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

	// suspendIo, when true, signals every satellite that hosts a
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
	//   1. apiserver creates Snapshot CRD with SuspendIo=true,
	//      TakeSnapshot=false. Satellites stamp
	//      Status.NodeStatus[].SuspendIoAcked once their suspend-io
	//      call returns.
	//   2. Once every targeted node has acked, the controller stamps
	//      Spec.TakeSnapshot=true. Satellites then dispatch
	//      provider.CreateSnapshot and stamp Status.NodeStatus[].Ready.
	//   3. Once every targeted node is Ready (or any has Failed=true),
	//      the controller flips Spec.SuspendIo=false and satellites
	//      issue `drbdsetup resume-io <rd>`.
	//
	// Resume-on-failure is mandatory — a partially-acked suspend
	// followed by an abort MUST still resume I/O on the nodes that
	// did ack, otherwise application I/O hangs forever. The
	// controller's abort path clears SuspendIo unconditionally.
	// +optional
	SuspendIo bool `json:"suspendIo,omitempty"`

	// takeSnapshot, when true, signals each satellite that has
	// already acked the suspend-io barrier (see SuspendIo above) to
	// dispatch the local provider.CreateSnapshot. The flag is
	// stamped by the controller-side SnapshotReconciler once every
	// targeted node's Status.NodeStatus[].SuspendIoAcked is true —
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
	//      crypto/rand-generated GroupID + SuspendIo=true.
	//   2. Controller gates phase advancement on the FULL group:
	//      Phase 2 only fires when every sibling's every targeted
	//      node has acked the suspend.
	//   3. Phase 3 only fires when every sibling's every targeted
	//      node is Ready — or any sibling node Failed=true, in
	//      which case the abort cascade fires SuspendIo=false on
	//      every sibling immediately.
	//
	// Empty GroupID is the single-snap path (Bug 351 behaviour
	// preserved verbatim — siblings denominator collapses to self).
	// +optional
	GroupID string `json:"groupId,omitempty"`
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

	// suspendIoAcked is stamped true by the local satellite once
	// `drbdsetup suspend-io <rd>` has returned for the parent
	// ResourceDefinition. The controller-side SnapshotReconciler
	// gates the Phase-2 `TakeSnapshot` transition on every targeted
	// node having acked. Cleared back to false when the controller
	// flips Spec.SuspendIo=false and the satellite issues
	// `drbdsetup resume-io <rd>`. Bug 351.
	// +optional
	SuspendIoAcked bool `json:"suspendIoAcked,omitempty"`

	// failed is stamped true when the local satellite hit a
	// terminal failure while either suspending I/O, taking the
	// per-node snapshot, or resuming I/O. The controller-side
	// SnapshotReconciler treats any Failed=true entry as an abort
	// signal: it flips Spec.SuspendIo=false immediately so the
	// already-suspended siblings resume rather than wait
	// indefinitely on the doomed node, and stamps the parent
	// Snapshot's Status.Flags with FAILED. Bug 351.
	// +optional
	Failed bool `json:"failed,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || self.metadata.name == self.spec.resourceDefinitionName + '.' + self.spec.snapshotName",message="metadata.name must equal <spec.resourceDefinitionName>.<spec.snapshotName>",optionalOldSelf=true

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
