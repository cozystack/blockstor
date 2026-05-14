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
