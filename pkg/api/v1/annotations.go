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

package v1

// AutoTiebreakerSuppressedUntilAnnotation is stamped on an RD when an
// operator (or an internal cleanup path) deletes a TIE_BREAKER replica.
// While the annotation timestamp is in the future, the RD-level
// reconciler skips its auto-witness branch. Without the suppression
// window, `linstor r d <tiebreaker-node> <rd>` returns success and
// then the reconciler re-stamps a fresh witness within milliseconds,
// silently undoing operator intent.
//
// Defined here (rather than in pkg/rest or internal/controller) so
// both the REST writer (`stampTiebreakerSuppression`) and the
// controller reader (`isTiebreakerSuppressed`) share a single source
// of truth without either package importing the other — pkg/api/v1
// is the neutral, dependency-free shared layer both already import.
const AutoTiebreakerSuppressedUntilAnnotation = "blockstor.io/auto-tiebreaker-suppressed-until"

// AutoDiskfulDeadlineAnnotation is stamped on an RD when the
// AutoDiskfulReconciler first observes a diskful-replica deficit
// (count < SelectFilter.PlaceCount) and the effective
// `DrbdOptions/auto-diskful` prop (minutes) is positive. The value is
// the RFC3339 wall-clock deadline at which the reconciler will promote
// a diskless replica to diskful (toggle-disk shape: drop DISKLESS
// flag, stamp StorPoolName) to refill the deficit.
//
// The annotation is stripped on the next reconcile that observes the
// deficit gone — either the operator added a replica manually, the
// reconciler promoted one, or the operator dropped place_count. This
// keeps the "deadline in the past" check from getting stuck firing
// repeatedly after the cluster is back at full health.
//
// Scenario 7.W03 — UG9 §"Auto-diskful and related options" (lines
// 4349-4425). The prop hierarchy is Controller → RG → RD: lower scope
// wins. AutoDiskfulPropKey + this annotation are defined here so the
// REST writer and controller reader share a single source of truth.
const AutoDiskfulDeadlineAnnotation = "blockstor.io/auto-diskful-deadline"

// AutoDiskfulPropKey is the upstream-LINSTOR property name for the
// auto-diskful timer (minutes). Set on ControllerProps for cluster-
// wide default, or on RG / RD props to override per-template /
// per-RD. A non-positive / unparseable value disables the feature
// at that scope; the resolver falls back to the next layer.
const AutoDiskfulPropKey = "DrbdOptions/auto-diskful"

// AutoDiskfulAllowCleanupPropKey gates the post-promotion cleanup of
// excess Secondary replicas. Default (unset / "true") cleans up;
// "false" leaves the extra diskless in place. Scenario 7.W03 wires
// only the promotion half — cleanup is a follow-up, but the key is
// reserved here so both consumers stay in sync.
const AutoDiskfulAllowCleanupPropKey = "DrbdOptions/auto-diskful-allow-cleanup"

// PeerForgetAckAnnotationPrefix is the per-peer ACK annotation key the
// satellite stamps on its OWN Resource CRD after `tearDownRemovedPeers`
// has issued `drbdadm del-peer` + `drbdmeta forget-peer` for a peer
// node that just departed from the desired peer set. Full annotation
// key shape: `blockstor.io/peer-forget-acked.<peerNodeName>`; the
// value is the RFC3339Nano wall-clock timestamp of the ACK.
//
// The REST handler's `waitForPeerDeletionAcks` polls every surviving
// sibling Resource for this key as the cluster-wide signal that the
// del-peer + forget-peer cleanup has finished against the doomed peer.
// Without this gate, a fresh `linstor rd ap` autoplacing the same RD
// right after `linstor r d <node>` can race the cleanup: the new
// replica's `node-id` may be re-allocated to the same integer the
// departed peer held, and DRBD-9 refuses to handshake a fresh peer
// over a metadata slot that still carries the previous peer's
// bitmap (the classic "node_id mismatch" / stuck-Connecting symptom).
//
// Spec-tracked: §4.2 "Two-phase delete" and §6 "Three-replica `n lost`
// + autoplace edge case" of the DRBD node-id allocation behavioural
// specification. The ACK is the controller-visible half of the
// two-phase delete: phase 1 is "mark and broadcast", phase 2 is the
// satellite's del-peer/forget-peer + ACK, after which the controller
// is free to physically drop the doomed Resource CRD and let the
// allocator reuse its `node-id`.
//
// Lives in pkg/api/v1 so both the REST handler (`pkg/rest/
// peer_delete_sync.go`) and the satellite stamper (`pkg/satellite/
// controllers/peer_forget_ack_stamper.go`) share a single source of
// truth without either package importing the other.
const PeerForgetAckAnnotationPrefix = "blockstor.io/peer-forget-acked."

// PeerChangedAnnotation is stamped by the REST `handleResourceDelete`
// handler on every SURVIVING sibling Resource CRD when one peer of an
// RD is dropped. The annotation value is an RFC3339Nano timestamp the
// REST writer updates to the current wall-clock time on every bump, so
// repeated peer-drops produce monotonically advancing values.
//
// Bug 67: removing a peer replica via `linstor r d <node> <rd>` left
// the surviving satellites stuck on `Connecting(<deleted-peer>)`. The
// satellite reconciler watches its OWN Resource CRDs via a node-name
// predicate, so a peer-Resource Delete event landing on a different
// node never triggered the survivor's Reconcile — the dispatcher would
// have happily rebuilt the DesiredResource with a shrunk peer set, but
// nothing woke the loop. Bumping a metadata annotation on each
// survivor is the cheapest event the local watch DOES see: it forces a
// Reconcile, the survivor's Reconcile re-derives the peer list from
// remaining Resources, the dispatcher emits a peer-less DesiredResource,
// and the already-working satellite teardown path runs `drbdadm
// disconnect + del-peer` against the gone replica.
//
// Wire-side `apiv1.Resource.Annotations` carries this value through to
// the K8s store, which surfaces it on the CRD's `metadata.annotations`
// so the controller-runtime watch fires. The annotation is harmless to
// the dispatcher / Python CLI (they ignore unknown keys).
const PeerChangedAnnotation = "blockstor.io/peer-changed"

// RDSpawnShortfallAnnotation is stamped on a ResourceDefinition when
// `rg spawn` placed strictly fewer replicas than the parent RG's
// PlaceCount asked for — i.e. the partial-fail path where 2 of 3
// nodes accepted a replica and the 3rd hit a transient pool error.
// The value is the RFC3339 wall-clock timestamp of the spawn event so
// the rebalance reconciler can age the marker against the configured
// GracePeriod before retrying.
//
// Scenario 2.20 (UG9 §"Automatically maintaining resource group
// placement count"): the rebalance reconciler scans for this marker on
// every periodic tick and, once GracePeriod has elapsed since the
// stamp, re-runs the additive placer for that RD. On success the
// annotation is stripped so the next scheduled tick is a clean no-op;
// on continued failure the marker survives and a later tick retries.
//
// Defined here (rather than in pkg/rest or internal/controller) so the
// REST writer (the spawn handler in 9.W05's partial-fail path) and the
// controller reader (RGRebalanceReconciler) share a single source of
// truth without either package importing the other — pkg/api/v1 is
// the neutral, dependency-free shared layer both already import.
const RDSpawnShortfallAnnotation = "blockstor.io/spawn-shortfall"
