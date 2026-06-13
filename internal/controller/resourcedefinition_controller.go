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

package controller

import (
	"context"
	stderrors "errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug148ResizePendingAnnotationPrefix is the per-volume annotation
// key the controller stamps on every Resource of an RD whose
// `spec.volumeDefinitions[].sizeKib` differs from the Resource's
// observed `status.volumes[n].usableKib`. Mirrors the production
// constant in pkg/rest/volume_definitions.go (`resizePending
// AnnotationPrefix`) — Bug 136's REST handler stamps the same key
// on the PUT path; Bug 148 extends coverage to the kubectl-edit
// path that bypasses REST.
//
// Per-volume key suffix so multi-volume RDs (rare today but on the
// roadmap) keep concurrent grow-shrink decisions distinguishable.
const Bug148ResizePendingAnnotationPrefix = "bug136.blockstor.cozystack.io/resize-pending-size-kib-vol-"

// ResourceDefinitionReconciler watches RD CRDs and maintains the
// tiebreaker invariant: an RD with exactly 2 diskful replicas in a
// cluster with 3+ satellite nodes auto-gains a 3rd DISKLESS replica
// on a remaining node so DRBD-9's `quorum: majority` always has a
// majority to compare against on a peer split.
//
// Without the tiebreaker, a 2-replica RD survives a single-node
// failure but freezes on quorum loss in a network partition — the
// surviving replica can't tell whether it's the majority or the
// outvoted minority. The diskless witness fixes that for free
// (no extra storage, just network presence).
type ResourceDefinitionReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Store is the shared blockstor store. Same instance the
	// NodeReconciler and REST server use.
	Store store.Store

	// APIReader is a direct apiserver reader used to enumerate
	// Resources for the witness-decision. Bypasses the informer
	// cache, which trails the apiserver during the first 100ms
	// after a `kubectl apply` of multiple Resources — a stale read
	// would see only 1 diskful replica, skip witness creation, and
	// wait for the next watch event to re-enqueue. Wired from
	// `mgr.GetAPIReader()` in SetupWithManager; tests construct the
	// reconciler directly and skip this — the field is nil-safe
	// and falls back to the cached client below.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resourcedefinitions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resourcedefinitions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resourcedefinitions/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resources,verbs=get;list;watch;create;update;patch;delete

// Reconcile ensures the tiebreaker for a 2-replica RD. Idempotent:
// re-running on an RD that already has its tiebreaker is a no-op.
func (r *ResourceDefinitionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.Store == nil {
		return ctrl.Result{}, nil
	}

	var rd blockstoriov1alpha1.ResourceDefinition

	// Use APIReader for the initial fetch when available — the
	// cached client trails the apiserver by tens to hundreds of ms
	// after a `kubectl apply`, and a Reconcile fired by an early
	// watch event would see NotFound through the cache and exit
	// before the witness gets created. APIReader bypasses the cache.
	reader := r.directOrCached()

	err := reader.Get(ctx, req.NamespacedName, &rd)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !rd.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	err = r.ensureTiebreaker(ctx, &rd)
	if err != nil {
		log.Error(err, "ensure tiebreaker", "rd", rd.Name)

		return ctrl.Result{}, err
	}

	// Bug 148: stamp the per-volume resize-pending annotation on
	// every Resource whose `status.volumes[n].usableKib` lags the
	// RD's `spec.volumeDefinitions[].sizeKib`. The REST handler
	// (Bug 136) already stamps on the PUT path; this branch covers
	// the kubectl-edit path where `spec.volumeDefinitions[].sizeKib`
	// is mutated directly and the REST handler never runs. Without
	// this, a kubectl-edit'd grow would change the spec but leave
	// the satellite with no resize-pending breadcrumb — the on-disk
	// block device stays at the old size forever.
	err = r.stampResizePending(ctx, &rd)
	if err != nil {
		log.Error(err, "stamp resize-pending", "rd", rd.Name)

		return ctrl.Result{}, err
	}

	// Belt-and-braces re-enqueue: the witness-decision read in
	// ensureTiebreaker goes through the cached client, and an RD
	// reconciled right when the second Resource arrives may see a
	// stale 1-diskful view and skip witness creation. Watches on
	// Resource events re-enqueue the RD as the cache fills, but if
	// only one Resource event lands before the reconciler drains
	// the queue we'd wait for the next periodic re-sync (minutes)
	// before the witness appears. A short requeue closes that
	// window without changing the steady-state behaviour: once the
	// witness exists, the next ensureTiebreaker is a no-op.
	return ctrl.Result{RequeueAfter: rdReconcileRequeue}, nil
}

// rdReconcileRequeue is the cache-warmup safety net for the RD
// reconciler. See the comment in Reconcile for why it exists.
const rdReconcileRequeue = 5 * time.Second

// ensureTiebreaker keeps both invariants upstream LINSTOR maintains:
//
//  1. shouldTieBreakerExist (CtrlRscAutoTieBreakerHelper.java#L468):
//     create a TIE_BREAKER witness iff diskful ≥ 2 AND diskful is
//     even AND there are no eligible diskless replicas already.
//     Drop the witness when the condition no longer holds (e.g. user
//     scaled to 3 replicas, or added a manual diskless that already
//     breaks the tie).
//
//  2. isQuorumFeasible (CtrlRscAutoQuorumHelper.java#L265):
//     `quorum:majority` is feasible when:
//     (diskful == 2 AND diskless ≥ 1)  -- 2 + witness
//     OR diskful ≥ 3
//     otherwise we set `quorum:off` because a partition would freeze
//     both halves with no clear winner.
//
//     Diskful here = NOT-diskless. Diskless = any DRBD_DISKLESS,
//     counting both user-added and TIE_BREAKER witnesses.
//
// We mirror that logic exactly so a cluster running blockstor sees
// the same tiebreaker / quorum decisions as one running upstream
// LINSTOR — important for the cozystack migration story.
//
// Bug 130 guard: if the RD is being deleted (CRD DeletionTimestamp
// set, or the Store row has already vanished under a concurrent
// rd-delete cascade), short-circuit BEFORE creating any new Resource.
// Without this, a Reconcile fired by a Resource watch event that
// landed milliseconds before the rd-delete REST handler would race
// the cascade's snapshot-then-delete sequence and stamp a fresh
// TIE_BREAKER witness on a third node — which the cascade then
// misses, leaving a phantom Resource CRD that blocks reuse of the
// RD name. The cascade-side retry loop (pkg/rest) catches whatever
// slips past this guard; together they make the rd-delete fan-out
// race-free.
func (r *ResourceDefinitionReconciler) ensureTiebreaker(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) error {
	if r.rdIsDeleting(ctx, rd) {
		return nil
	}

	// Bug 334: TIE_BREAKER is a DRBD-9 quorum primitive (1 diskless
	// peer acting as a tie-breaker arbiter for `quorum: majority`
	// decisions in 2-replica setups). Without DRBD in the effective
	// LayerStack there is no quorum machinery to arbitrate — the
	// witness would be a meaningless extra Resource CRD on a third
	// node that surprises operators with phantom rows in
	// `linstor r l` output. Skip the witness invariant outright.
	//
	// The check uses the resolved RD-or-parent-RG LayerStack so an RD
	// that inherits `-l STORAGE` from its parent RG also skips the
	// witness. Empty everywhere falls through to DefaultLayerStack()
	// which contains DRBD, so the legacy "no LayerStack set" RDs that
	// the rest of the codebase treats as DRBD-by-default keep their
	// witness.
	effectiveStack := r.resolveRDLayerStack(ctx, rd)
	if !apiv1.ContainsReplicationLayer(effectiveStack) {
		return nil
	}

	replicas, err := r.listReplicasDirect(ctx, rd.Name)
	if err != nil {
		return err
	}

	// Bug 385: a replica hosted on an EVICTED / LOST node is a draining
	// placement, not a live one. Counting it would (a) let a stranded
	// diskful keep the witness invariant "satisfied" so the reconciler
	// never relocates the witness off the drained node, and (b) leave a
	// TIE_BREAKER pinned to the very node the operator just evicted —
	// the exact "evict doesn't take effect" symptom. Mirror the placer's
	// documented semantic ("replicas on EVICTED/LOST nodes are NOT
	// counted") so the witness / quorum decision runs over live replicas
	// only. Stranded witnesses are reaped explicitly below so a fresh one
	// can land on a healthy spare (or quorum falls to off when none
	// remains) — never by demoting a healthy diskful to TIE_BREAKER.
	// Bug-024 extends the same treatment to replicas whose node row is
	// gone entirely (ghost witnesses left behind by `n lost`).
	live, err := r.dropStrandedReplicas(ctx, rd.Name, replicas)
	if err != nil {
		return err
	}

	// Bug 387: an INACTIVE replica is `drbdadm down` (operator
	// deactivation) — its DRBD device is not up, so it is NOT a voting
	// peer in the quorum the auto-tiebreaker invariant defends. Counting
	// it as a diskful replica corrupts every downstream witness decision:
	// e.g. an RD with 2 active diskful + 1 INACTIVE diskful, after an
	// `r d` of one active diskful, would otherwise look like "2 diskful,
	// nonWitnessDiskless==0, even" and spuriously grow a TIE_BREAKER —
	// upstream LINSTOR just deletes the replica with no witness. Drop
	// INACTIVE replicas from the (already disabled-node-filtered) voting
	// set before the split so neither disabled-node nor deactivated
	// replicas influence the diskful or diskless/witness count.
	// Bug 393: an INACTIVE diskful replica means active redundancy is
	// degraded and a backfill is expected (the placer's `r c --auto-place`
	// gap-fills a replacement active diskful on a healthy spare). While
	// that is true, an EXISTING witness on a spare node is the backfill
	// target the placer is about to promote in place — reaping it
	// collapses the very node redundancy is being restored onto, and the
	// reconciler oscillates 1↔2 active diskful forever (the
	// inactive-return-backfills-redundancy hang). Detect the INACTIVE
	// replica BEFORE filterActiveReplicas drops it from the voting set, so
	// shouldTieBreakerExist can hold the witness through the backfill
	// window. This never grows a NEW witness (the create branch is
	// unaffected) — it only preserves one that already exists, which is
	// also the forward-correct shape: when the INACTIVE replica returns
	// (`r activate`) the cluster is immediately at the 2-diskful +
	// 1-witness three-voter quorum with no recreate churn.
	hasInactive := containsInactiveReplica(live)

	live = filterActiveReplicas(live)

	diskful, diskless := splitByDiskless(live)
	witness := filterTieBreaker(diskless)

	wantWitness := shouldTieBreakerExist(rd, diskful, diskless, witness, hasInactive)

	// Bug 338: the orphan-witness shape (1 diskful + 1 TIE_BREAKER +
	// 0 user-diskless) collapses on this reconcile — no grace timer.
	// The race the earlier grace gate guarded (in-flight relocate
	// `r c <tiebreaker-node>` promoting the witness in-place via
	// promoteDisklessReplica while the controller deletes the same
	// row) is now closed at the node-id-allocator layer by Bug 342's
	// kernel-confirmed PeerDRBDNodeID union (see
	// resource_controller.go:collectTakenNodeIDs). If the relocate
	// `r c` lands after the witness is gone, promoteDisklessReplica
	// falls through to a fresh Create with a freshly-allocated DRBD
	// node-id that the allocator guarantees does not collide with
	// the kernel slot the just-freed witness held — so the relocate
	// converges via the create-fresh path instead of the in-place
	// promote-fast-path. One extra Resource CRD cycle in the narrow
	// `r d → r c` overlap, no oscillation, no kernel-slot reuse.
	// removeWitnesses still re-checks each row's live Flags before
	// Delete so a witness that already promoted to the relocate
	// target (TIE_BREAKER stripped) is skipped — the same layered
	// guard the relocate-onto-tiebreaker integration test exercises.

	willCreate := wantWitness && len(witness) == 0
	willRemove := !wantWitness && len(witness) > 0

	if willCreate || willRemove {
		logf.FromContext(ctx).Info("ensureTiebreaker",
			"rd", rd.Name,
			"replicas", len(replicas),
			"diskful", len(diskful),
			"witness", len(witness),
			"willCreate", willCreate,
			"willRemove", willRemove,
		)
	}

	disklessAfter, err := r.applyWitnessDecision(ctx, rd, replicas, diskless, witness, wantWitness)
	if err != nil {
		return err
	}

	// Scenario 7.W01 / UG9 §"Auto-quorum policies": when the
	// effective `DrbdOptions/AutoQuorum` is `disabled`, leave the
	// per-RD `DrbdOptions/Resource/quorum` prop alone. The operator
	// owns the policy in that mode and writes `majority` / `off`
	// (plus the companion `on-no-quorum=suspend-io|io-error`)
	// explicitly. Overwriting here would silently revert manual
	// settings on every reconcile, which is the exact failure mode
	// the scenario calls out.
	//
	// We read the RD's Spec.Props directly — the REST POST
	// /v1/resource-definitions handler folds parent-RG +
	// ControllerProps onto the RD at create time (see existing
	// isAutoTieBreakerEnabled comment), so cluster / RG-scope
	// `auto-quorum=disabled` reaches us here.
	if isAutoQuorumDisabled(rd) {
		return nil
	}

	return r.setQuorum(ctx, rd, quorumPolicy(len(diskful), len(disklessAfter)))
}

// shouldKeepExistingWitness implements the keep-branch (Bug 104)
// with the Bug 338 carve-out:
//
//   - diskful == 2: keep the witness — it's the third voter the
//     auto-quorum invariant promises and the upstream LINSTOR
//     shouldTieBreakerExist contract creates on its own.
//
//   - diskful == 1 (any diskless count): drop the witness. With a
//     single diskful copy there is no even-diskful tie to break and
//     `quorumPolicy` already returns quorum=off (it only arms
//     majority at diskful == 2 or diskful >= 3), so a witness is a
//     pure divergence from upstream LINSTOR's shouldTieBreakerExist —
//     which never manages a witness below 2 diskful — and on a
//     steady "1 diskful + 1 diskless" shape (e.g. a diskless client
//     mount) it would needlessly occupy a node. Earlier Bug 104/108
//     kept it here on the premise that "1 diskful + 1 diskless
//     freezes quorum:majority"; that premise is false because quorum
//     is off in this shape. This also subsumes the former Bug 338
//     carve-out (1 diskful + 0 diskless → collapse).
//
//   - diskful >= 3: the cluster has a clear majority on its own; the
//     witness is dead weight.
func shouldKeepExistingWitness(diskful, _, witnessUnnecessaryDiskfulCount int) bool {
	if diskful >= witnessUnnecessaryDiskfulCount {
		return false
	}

	// Keep the auto-witness only at the even-diskful tie it defends.
	return diskful == 2
}

// shouldTieBreakerExist decides whether the RD should carry an
// auto-managed TIE_BREAKER witness. Splits into two complementary
// branches, both gated on DrbdOptions/AutoAddQuorumTiebreaker
// (upstream LINSTOR's auto-tiebreaker prop):
//
//  1. Create branch (mirrors upstream shouldTieBreakerExist exactly):
//     diskful ≥ 2, parity is even, and no user-added diskless already
//     breaks the tie. Suppression also gates this branch — the REST
//     per-resource-delete handler stamps a short-lived annotation
//     right before dropping the witness so the next reconcile
//     doesn't put it back milliseconds later.
//
//  2. Keep branch (Bug 104): preserve an already-stamped TIE_BREAKER
//     only at the even-diskful tie it defends (diskful == 2). Below
//     that — at diskful <= 1, e.g. after `r td --diskless` drops a
//     2-diskful RD to 1 diskful + 1 user-diskless — the witness is
//     reaped: `quorumPolicy` returns quorum=off for a lone diskful,
//     so there is no majority to freeze, and upstream LINSTOR never
//     manages a witness below 2 diskful. Earlier Bug 104/108 kept or
//     re-created the witness in the 1-diskful window on the (false)
//     premise that it freezes quorum:majority. Suppression is not
//     consulted here — that annotation is stamped at delete-time and
//     is only meaningful when the witness has already been removed.
func shouldTieBreakerExist(
	rd *blockstoriov1alpha1.ResourceDefinition,
	diskful, diskless, witness []apiv1.Resource,
	hasInactive bool,
) bool {
	if !isAutoTieBreakerEnabled(rd) {
		return false
	}

	nonWitnessDiskless := len(diskless) - len(witness)

	wantNewWitness := !isTiebreakerSuppressed(rd) &&
		len(diskful) >= 2 && len(diskful)%2 == 0 && nonWitnessDiskless == 0

	const witnessUnnecessaryDiskfulCount = 3

	// Bug 393: hold an EXISTING witness through the backfill window when
	// an INACTIVE diskful replica is present (active redundancy degraded,
	// a replacement active diskful is being backfilled onto the witness's
	// node). Without this the orphan-collapse branch (1 active diskful +
	// 1 witness) reaps the very spare the placer is promoting, and the
	// reconciler flaps 1↔2 active diskful forever. Gated on an existing
	// witness, so it never grows a new one.
	if hasInactive && len(witness) > 0 && len(diskful) >= 1 {
		return true
	}

	keepExistingWitness := keepExistingWitnessFor(rd, diskful, witness, nonWitnessDiskless,
		witnessUnnecessaryDiskfulCount)

	return wantNewWitness || keepExistingWitness
}

// containsInactiveReplica reports whether any replica in the slice
// carries the INACTIVE flag (`drbdadm down`). Used by the tiebreaker
// decision to recognise the degraded-redundancy / backfill-in-progress
// state — see the Bug 393 keep branch in shouldTieBreakerExist. Probed
// on the pre-filter slice, before filterActiveReplicas drops INACTIVE
// replicas from the voting set.
func containsInactiveReplica(replicas []apiv1.Resource) bool {
	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagInactive) {
			return true
		}
	}

	return false
}

// keepExistingWitnessFor folds the two "preserve an existing
// TIE_BREAKER witness" branches into a single helper so
// shouldTieBreakerExist stays under the gocyclo threshold. Both
// branches gate on `len(witness) > 0` — we never "keep" what isn't
// there.
//
// Branches:
//
//  1. Operator-intent override (Bug B.1, hunt-v3):
//     `linstor r d --keep-tiebreaker <diskful>` stamps the
//     KeepTiebreakerUntilAnnotation on the parent RD. While the
//     annotation deadline is in the future AND at least one diskful
//     survives, retain the witness regardless of the Bug-338 carve-
//     out. The diskful floor matters because lone TIE_BREAKER + 0
//     diskful = 1 voter, strictly worse than no witness.
//
//  2. Bug-104 / Bug-338 steady-state keep branch:
//     shouldKeepExistingWitness encodes the existing "preserve the
//     witness across diskful→diskless toggle" + "collapse the
//     orphaned witness when diskful=1 and no non-witness diskless
//     co-resident" rules.
func keepExistingWitnessFor(
	rd *blockstoriov1alpha1.ResourceDefinition,
	diskful, witness []apiv1.Resource,
	nonWitnessDiskless, witnessUnnecessaryDiskfulCount int,
) bool {
	if len(witness) == 0 {
		return false
	}

	if len(diskful) >= 1 && isKeepTiebreakerActive(rd) {
		return true
	}

	return shouldKeepExistingWitness(len(diskful), nonWitnessDiskless, witnessUnnecessaryDiskfulCount)
}

// isAutoQuorumDisabled reports whether the RD opted out of the
// auto-quorum reconciler. Upstream LINSTOR (UG9 §"Auto-quorum
// policies", lines 4233-4279) accepts `disabled`, `suspend-io`,
// `io-error` for `DrbdOptions/auto-quorum`; only `disabled` stops
// the reconciler — the other two are "auto-set on-no-quorum to this
// value" instructions that we don't honour yet (P2 — tracked in
// scenario 7.W01 as a wave-2 follow-up).
//
// Case-insensitive match: operators sometimes paste `Disabled` from
// the manual.
func isAutoQuorumDisabled(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	if rd == nil {
		return false
	}

	// Corner-case B1/B4: the canonical upstream key is the kebab-case
	// `DrbdOptions/auto-quorum` — that is what the `linstor (rg|rd)
	// set-property … DrbdOptions/auto-quorum disabled` CLI writes, what
	// `seedAutoQuorumDefaults` stamps on RD create, and what the
	// PropsInfo catalogue advertises. The earlier camelCase spelling
	// (`DrbdOptions/AutoQuorum`) was never written by any production
	// path, so the gate silently never fired against a real cluster:
	// auto-quorum=disabled set via the CLI was ignored and the
	// reconciler kept overwriting the operator's manual quorum policy
	// on every pass. Read the kebab key first; keep the camelCase
	// spelling as a forward-compat fallback so any hand-stamped legacy
	// value still opts out.
	const (
		propKey       = "DrbdOptions/auto-quorum"
		legacyPropKey = "DrbdOptions/AutoQuorum"
	)

	// Corner-case B2: the CRD-backed store (wireToCRDRDSpec →
	// propsToTyped / stripDRBDProps) routes EVERY `DrbdOptions/*` key
	// OUT of Spec.Props. Recognised keys become typed Spec.DRBDOptions;
	// the section-less ones we don't type yet — and
	// `DrbdOptions/auto-quorum` is exactly such a key, only
	// `DrbdOptions/AutoAddQuorumTiebreaker` is typed — land in
	// Spec.ExtraProps. The B1 fix corrected the key spelling but still
	// read only Spec.Props, so on a real (CRD-backed) cluster the
	// operator's `set-property DrbdOptions/auto-quorum disabled` lived
	// in Spec.ExtraProps where this gate never looked: it never fired
	// and setQuorum kept re-stamping quorum=majority over the
	// operator's manual `quorum off`. Consult both bags — Spec.Props
	// first (the in-memory / fake-client tests populate it directly),
	// then Spec.ExtraProps (the production CRD shape).
	value := rdPropFromBags(rd, propKey)
	if value == "" {
		value = rdPropFromBags(rd, legacyPropKey)
	}

	return strings.EqualFold(value, "disabled")
}

// rdPropFromBags reads a single property key off an RD, consulting
// Spec.Props first and falling back to Spec.ExtraProps. The CRD-backed
// store splits the upstream flat `DrbdOptions/*` prop bag across two
// CRD fields (Spec.Props keeps non-DRBD keys; recognised DRBD keys
// become typed Spec.DRBDOptions; unrecognised DRBD keys land in
// Spec.ExtraProps — see pkg/store/k8s.wireToCRDRDSpec), so any
// reconciler helper that wants the operator-visible value of a
// section-less DRBD key MUST look in both. Returns "" when neither bag
// carries the key.
func rdPropFromBags(rd *blockstoriov1alpha1.ResourceDefinition, key string) string {
	if rd.Spec.Props != nil {
		if v, ok := rd.Spec.Props[key]; ok {
			return v
		}
	}

	if rd.Spec.ExtraProps != nil {
		if v, ok := rd.Spec.ExtraProps[key]; ok {
			return v
		}
	}

	return ""
}

// isTiebreakerSuppressed reports whether the operator recently
// dropped a TIE_BREAKER replica via the REST per-resource-delete
// handler. The handler stamps an RFC3339 deadline onto the RD; this
// helper returns true while the deadline is in the future.
//
// Bad / unparseable values are treated as "not suppressed" so a
// hand-edited annotation can't accidentally freeze the auto-witness
// invariant forever. An expired stamp also returns false — the
// auto-quorum invariant resumes its normal behaviour without any
// manual cleanup.
func isTiebreakerSuppressed(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	if rd.Annotations == nil {
		return false
	}

	raw, ok := rd.Annotations[apiv1.AutoTiebreakerSuppressedUntilAnnotation]
	if !ok || raw == "" {
		return false
	}

	deadline, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return false
	}

	return time.Now().Before(deadline)
}

// isKeepTiebreakerActive reports whether an operator recently ran
// `linstor r d --keep-tiebreaker <diskful> <rd>` against this RD.
// The REST per-resource-delete handler stamps an RFC3339 deadline on
// the parent RD when the `?keep_tiebreaker=true` query parameter is
// set; this helper returns true while the deadline is in the future.
//
// Used by `shouldTieBreakerExist` to short-circuit the Bug-338
// orphan-witness collapse: with this annotation fresh, an existing
// TIE_BREAKER witness must survive a diskful→single-replica
// transition the carve-out would otherwise reap. Without the
// override, the CLI flag silently no-ops because the controller's
// reconciler can't distinguish operator intent ("keep this witness")
// from steady-state ("orphaned witness, prune").
//
// Bad / unparseable values are treated as "no override" so a hand-
// edited annotation can't accidentally freeze the witness invariant
// forever. An expired stamp also returns false — the auto-quorum
// invariant resumes its normal Bug-338 behaviour with no manual
// cleanup needed.
func isKeepTiebreakerActive(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	if rd == nil || rd.Annotations == nil {
		return false
	}

	raw, ok := rd.Annotations[apiv1.KeepTiebreakerUntilAnnotation]
	if !ok || raw == "" {
		return false
	}

	deadline, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return false
	}

	return time.Now().Before(deadline)
}

// rdIsDeleting reports whether the RD is mid-delete from the
// controller's perspective: the CRD has a DeletionTimestamp
// stamped. The CRD-level DeletionTimestamp is the canonical
// "k8s is finalising this object" signal — the rd-delete REST
// handler stamps it before walking the cascade, and the controller
// must not stamp new Resources on a rd that's being torn down.
// Any witness created in that window is the Bug 130 phantom.
//
// We deliberately do NOT probe the Store for RD presence here.
// Several existing tests (Bug 104, Bug 108) construct an RD in the
// fake client only — the in-memory Store has no parallel row — and
// rely on EnsureTiebreaker creating the witness anyway. Reading the
// Store would mis-classify those legitimate witness creations as
// "rd mid-delete" and break the auto-quorum invariant on every
// reconcile. The CRD-DeletionTimestamp probe stays narrow: it
// catches the case Bug 130 documents (controller fires AFTER
// `kubectl delete rd` stamps the timestamp but BEFORE the cascade
// finishes) while leaving the long-standing happy path alone.
//
// The cascade-side multi-pass list-then-delete (Bug 130 fix in
// pkg/rest/resource_definitions.go) is the second half of the
// invariant: it reaps any witness that slipped past this guard.
func (r *ResourceDefinitionReconciler) rdIsDeleting(_ context.Context, rd *blockstoriov1alpha1.ResourceDefinition) bool {
	if rd == nil {
		return true
	}

	return !rd.DeletionTimestamp.IsZero()
}

// directOrCached returns the APIReader-direct reader when available
// (production path via SetupWithManager) and falls back to the
// embedded cached client otherwise (unit-test path).
func (r *ResourceDefinitionReconciler) directOrCached() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}

	return r.Client
}

// listReplicasDirect enumerates the Resource children of an RD by
// reading apiserver-direct via APIReader, bypassing the informer
// cache. The cache trails the apiserver by tens to hundreds of
// milliseconds, which means a Reconcile triggered by the FIRST
// Resource Create event sees only 1 diskful replica when the test
// just `kubectl apply`-d two. The cache-based read would miss the
// witness-creation window until the next sync. Tests that
// construct the reconciler directly leave APIReader nil — fall
// back to the Store path so unit tests don't need an apiserver.
func (r *ResourceDefinitionReconciler) listReplicasDirect(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	if r.APIReader == nil {
		return r.Store.Resources().ListByDefinition(ctx, rdName)
	}

	var crdList blockstoriov1alpha1.ResourceList
	if err := r.APIReader.List(ctx, &crdList); err != nil {
		return nil, err
	}

	out := make([]apiv1.Resource, 0, len(crdList.Items))

	for i := range crdList.Items {
		if crdList.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		out = append(out, apiv1.Resource{
			Name:     crdList.Items[i].Spec.ResourceDefinitionName,
			NodeName: crdList.Items[i].Spec.NodeName,
			Flags:    crdList.Items[i].Spec.Flags,
		})
	}

	return out, nil
}

// isAutoTieBreakerEnabled gates witness auto-creation. Default is
// enabled (matches the effective cozystack / piraeus-operator
// behaviour where ControllerProps seeds it true). Operators who
// explicitly place a manual DISKLESS replica disable the auto path
// per-RD.
//
// Phase 10.3: typed `Spec.DRBDOptions.Resource.AutoTieBreaker` wins;
// legacy `Spec.Props["DrbdOptions/AutoAddQuorumTiebreaker"]` is the
// forward-compat fallback. We only check the RD here; the resolver
// (controller → RG → RD → Resource hierarchy) doesn't run on the
// RD reconciler path because that path doesn't dispatch to the
// satellite. A cluster-wide ControllerProps default still propagates
// because the REST POST /v1/resource-definitions handler folds
// parent-RG + ControllerProps into the RD on create.
func isAutoTieBreakerEnabled(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	if rd.Spec.DRBDOptions != nil && rd.Spec.DRBDOptions.Resource != nil &&
		rd.Spec.DRBDOptions.Resource.AutoTieBreaker != nil {
		return *rd.Spec.DRBDOptions.Resource.AutoTieBreaker
	}

	const propKey = "DrbdOptions/AutoAddQuorumTiebreaker"

	if rd.Spec.Props == nil {
		return true
	}

	value, ok := rd.Spec.Props[propKey]
	if !ok {
		return true
	}

	return !strings.EqualFold(value, "false")
}

// applyWitnessDecision creates or removes the witness and returns
// the diskless slice as it should look after the decision (so the
// caller's quorum computation reflects the post-write state).
func (r *ResourceDefinitionReconciler) applyWitnessDecision(
	ctx context.Context,
	rd *blockstoriov1alpha1.ResourceDefinition,
	replicas, diskless, witness []apiv1.Resource,
	wantWitness bool,
) ([]apiv1.Resource, error) {
	switch {
	case wantWitness && len(witness) == 0:
		err := r.createWitness(ctx, rd, replicas)
		if err != nil {
			return nil, err
		}

		return append(diskless, apiv1.Resource{
			Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		}), nil

	case !wantWitness && len(witness) > 0:
		err := r.removeWitnesses(ctx, rd.Name, witness)
		if err != nil {
			return nil, err
		}

		// Drop witnesses from the diskless slice for the quorum
		// computation.
		out := make([]apiv1.Resource, 0, len(diskless))

		for i := range diskless {
			if !slices.Contains(diskless[i].Flags, apiv1.ResourceFlagTieBreaker) {
				out = append(out, diskless[i])
			}
		}

		return out, nil
	}

	return diskless, nil
}

// quorumPolicy implements upstream LINSTOR's isQuorumFeasible.
// QuorumPolicyMajority / QuorumPolicyOff are the two values
// `quorumPolicy` returns; exposed as constants so test files
// elsewhere in the package can reference them by name (and so
// goconst stops flagging the literals).
const (
	QuorumPolicyMajority = "majority"
	QuorumPolicyOff      = "off"
)

// 2 diskful + ≥1 diskless OR ≥3 diskful → majority; else off.
func quorumPolicy(diskful, diskless int) string {
	const minDiskfulForMajority = 3

	if (diskful == 2 && diskless >= 1) || diskful >= minDiskfulForMajority {
		return QuorumPolicyMajority
	}

	return QuorumPolicyOff
}

// createWitness picks a healthy non-replica node and creates a
// DISKLESS+TIE_BREAKER Resource on it.
//
// Bug 153 burst-aware rollback: after the witness Create succeeds,
// re-probe the parent RD via the APIReader (direct apiserver path,
// no informer cache). If the CRD is now NotFound — i.e. the
// cascade has dropped it during the window between
// `rdIsDeleting` and the Create — delete the just-created witness
// so it doesn't outlive its parent and become a phantom. Combined
// with the cascade-side retry-until-empty (Bug 130 fix in
// pkg/rest/resource_definitions.go::cascadeDeleteResources) and
// the existing DeletionTimestamp guard, this closes the burst
// race from a third side without requiring K8s owner-reference
// GC (which the in-memory Store doesn't model and which the
// satellite finalizer chain interacts with in non-trivial ways).
//
// The probe only runs when APIReader is non-nil — unit-test setups
// that construct the reconciler directly and rely on the
// Bug 104/108 fake-client-only fixtures stay unaffected.
func (r *ResourceDefinitionReconciler) createWitness(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition, existing []apiv1.Resource) error {
	hostingReplica := map[string]bool{}
	for i := range existing {
		hostingReplica[existing[i].NodeName] = true
	}

	// Bug 261: route through the RD-aware selector so a stale
	// `existing` snapshot can't slip a diskful node into the
	// witness-candidate set. The selector re-probes the store for
	// diskful Resources of the RD and hard-excludes them — defense-
	// in-depth against caller-snapshot staleness.
	tiebreakerNode, err := r.pickTiebreakerNodeForRD(ctx, rd.Name, hostingReplica)
	if err != nil {
		return err
	}

	if tiebreakerNode == "" {
		// No spare healthy node; the witness can't be created
		// today. Quorum will fall back to off below.
		return nil
	}

	// Bug-024 placement guard: the candidate list above comes from
	// Store.Nodes().List, which in production reads the manager's
	// informer cache — after `n lost` deletes a node, the lagging
	// cache re-offers it for tens to hundreds of ms and the Create
	// below would stamp a `[DISKLESS TIE_BREAKER]` ghost on the
	// just-deleted node (nothing reaps it: no DeletionTimestamp
	// event, no finalizer). Re-validate the pick against the
	// authoritative reader right before the write — the same shape
	// as the REST layer's Bug 174 node-deleted-race guard. On a miss
	// (or a freshly-stamped drain flag) skip this pass; the standing
	// rdReconcileRequeue retries with a fresh candidate list.
	probe, err := r.probeNodeDirect(ctx, tiebreakerNode)
	if err != nil {
		return err
	}

	if !probe.found || probe.drained {
		logf.FromContext(ctx).Info("witness candidate vanished or drained; deferring witness create",
			"rd", rd.Name, "node", tiebreakerNode, "found", probe.found)

		return nil
	}

	newWitness := apiv1.Resource{
		Name:     rd.Name,
		NodeName: tiebreakerNode,
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}

	err = r.Store.Resources().Create(ctx, &newWitness)
	if err != nil && !stderrors.Is(err, store.ErrAlreadyExists) && !alreadyExists(err) {
		return err
	}

	r.rollbackWitnessIfRDGone(ctx, rd.Name, tiebreakerNode)

	// Bug-024, post-write half (mirrors Bug 174's two-sided close):
	// the node can vanish between the pre-write probe and the Create.
	// Re-probe and roll the witness back if the node is gone — the
	// next reconcile re-picks from a fresh list. Best-effort like
	// rollbackWitnessIfRDGone: a failed rollback is caught by the
	// repair leg (dropStrandedReplicas) on the next pass.
	r.rollbackWitnessIfNodeGone(ctx, rd.Name, tiebreakerNode)

	return nil
}

// rollbackWitnessIfNodeGone deletes the just-created witness when its
// host node no longer exists per the authoritative reader. Swallows
// every error: the witness Create already succeeded, and the ghost-
// repair leg in dropStrandedReplicas reaps whatever slips through on
// the next reconcile.
func (r *ResourceDefinitionReconciler) rollbackWitnessIfNodeGone(ctx context.Context, rdName, witnessNode string) {
	probe, err := r.probeNodeDirect(ctx, witnessNode)
	if err != nil || probe.found {
		return
	}

	_ = r.Store.Resources().Delete(ctx, rdName, witnessNode)
}

// rollbackWitnessIfRDGone probes the parent RD via the APIReader
// after a successful witness Create and rolls back if the CRD is
// now absent or has gained a DeletionTimestamp. Best-effort —
// any error in the rollback path is logged via the reconciler's
// usual error surface (returning here keeps the original Create
// successful; the next reconcile will catch the orphan via the
// cascade's retry loop).
func (r *ResourceDefinitionReconciler) rollbackWitnessIfRDGone(ctx context.Context, rdName, witnessNode string) {
	if r.APIReader == nil {
		return
	}

	var fresh blockstoriov1alpha1.ResourceDefinition

	err := r.APIReader.Get(ctx, client.ObjectKey{Name: rdName}, &fresh)
	if err == nil && fresh.DeletionTimestamp.IsZero() {
		// RD is still live and not mid-delete; witness is valid.
		return
	}

	// Either NotFound (cascade dropped the CRD) or DeletionTimestamp
	// is set (cascade in progress). Either way the witness must not
	// outlive its parent — roll it back. Swallow ErrNotFound: a
	// concurrent reconcile may have already done the cleanup.
	_ = r.Store.Resources().Delete(ctx, rdName, witnessNode)
}

// resolveRDLayerStack returns the effective layer composition for
// the RD by walking RD → RG → default. Mirrors the (unexported)
// ResourceReconciler.resolveLayerStack but lives on the RD
// reconciler so the witness gate (Bug 334) doesn't have to depend on
// the resource-reconciler instance.
//
// Read order:
//
//  1. RD.Spec.LayerStack — the operator-set / REST-stamped value.
//  2. Parent RG.Spec.SelectFilter.LayerStack — when the RD itself
//     leaves the field empty.
//  3. apiv1.DefaultLayerStack() — the upstream LINSTOR default,
//     `["DRBD","STORAGE"]`. This is the load-bearing fallback for
//     legacy RDs (and the entire pre-Phase-9 test suite) that never
//     stamped a LayerStack: the witness invariant must continue to
//     apply to them.
//
// Soft-fail on the RG lookup: if the parent RG can't be fetched (RG
// vanished mid-cascade, transient apiserver hiccup), fall through to
// the default rather than blocking the reconcile. The witness is
// quorum-correctness, not quorum-safety — a brief over-creation
// during an RG outage is cheaper than refusing to converge.
func (r *ResourceDefinitionReconciler) resolveRDLayerStack(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) []string {
	if rd == nil {
		return apiv1.DefaultLayerStack()
	}

	if len(rd.Spec.LayerStack) > 0 {
		return rd.Spec.LayerStack
	}

	if rd.Spec.ResourceGroupName == "" {
		return apiv1.DefaultLayerStack()
	}

	reader := r.directOrCached()

	var rg blockstoriov1alpha1.ResourceGroup
	if err := reader.Get(ctx, client.ObjectKey{Name: rd.Spec.ResourceGroupName}, &rg); err != nil {
		return apiv1.DefaultLayerStack()
	}

	if len(rg.Spec.SelectFilter.LayerStack) > 0 {
		return rg.Spec.SelectFilter.LayerStack
	}

	return apiv1.DefaultLayerStack()
}

// filterTieBreaker returns the subset of diskless replicas that
// carry the TIE_BREAKER flag.
func filterTieBreaker(diskless []apiv1.Resource) []apiv1.Resource {
	out := make([]apiv1.Resource, 0, len(diskless))

	for i := range diskless {
		if slices.Contains(diskless[i].Flags, apiv1.ResourceFlagTieBreaker) {
			out = append(out, diskless[i])
		}
	}

	return out
}

// setQuorum stamps DrbdOptions/Resource/quorum on the RD's prop bag
// and, when quorum is `majority`, seeds the companion
// `DrbdOptions/Resource/on-no-quorum=suspend-io` if the operator
// hasn't pinned it. Idempotent: returns early if both props already
// carry the values we want. The satellite picks up the change on
// next dispatch and re-renders the .res file.
//
// Bug 297 (P1, data-loss class): without `on-no-quorum=suspend-io`,
// DRBD-9 falls back to its built-in `io-error` policy. On quorum
// loss the minority replica returns ENODATA / EIO from open(2) and
// the kernel slot freezes in a state that survives partition heal —
// `drbdadm primary` then fails on auto-promote and dd opens with
// "No data available". `suspend-io` instead blocks I/O until quorum
// returns, then the slot resumes cleanly with the freshly synced
// data. The REST POST handler's `seedAutoQuorumDefaults` already
// stamps this on POST-created RDs, but kubectl-apply on the CRD
// directly (e2e tests, GitOps flows that bypass the REST surface)
// never hit that path — so the seeding has to live on every code
// path that produces an `quorum=majority` RD. The controller is
// the right level: it sees every RD regardless of create path.
//
// Operator-supplied `on-no-quorum` wins — silently overriding an
// explicit `io-error` would undo the same operator control that
// `seedAutoQuorumDefaults` documents preserving (and the same
// scenario 7.W01 the auto-quorum-disabled gate respects).
//
// Retries on conflict because the RD reconciler races against the
// resource reconciler — both can write the RD spec under heavy
// reconcile pressure (e.g. fan-out from a Watches event), and a
// stale local copy hits "object has been modified" on Update.
func (r *ResourceDefinitionReconciler) setQuorum(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition, value string) error {
	const (
		quorumKey      = "DrbdOptions/Resource/quorum"
		onNoQuorumKey  = "DrbdOptions/Resource/on-no-quorum"
		onNoQuorumSeed = "suspend-io"
	)

	for range 3 {
		if quorumPropsAlreadySet(rd, value, quorumKey, onNoQuorumKey) {
			return nil
		}

		if rd.Spec.Props == nil {
			rd.Spec.Props = map[string]string{}
		}

		rd.Spec.Props[quorumKey] = value

		// Bug 309: also stamp the typed slot. `effectiveprops.
		// Resolve` copies `TypedDRBDOptionsToProps(Spec.DRBDOptions)`
		// on top of `Spec.Props`, so the typed field wins on the
		// dispatch path. Without this mirror write the CSI-initial
		// `Spec.DRBDOptions.Resource.Quorum="off"` (stamped by
		// `wireToCRDRDSpec` from the REST POST's pre-witness prop
		// bag) overrides the reconciler's `Spec.Props[quorum]=
		// majority`, the satellite renders `.res` with `quorum off`,
		// and drbd-reactor's promoter refuses the resource with
		// `quorum is 'off', but also fencing is 'dont-care'` —
		// rwx-ganesha NFS sidecar can't promote, RWX Pods hang in
		// ContainerCreating. Keeping the prop write so existing
		// downstream readers (tests, golinstor wire) still see it.
		if rd.Spec.DRBDOptions == nil {
			rd.Spec.DRBDOptions = &blockstoriov1alpha1.DRBDOptions{}
		}

		if rd.Spec.DRBDOptions.Resource == nil {
			rd.Spec.DRBDOptions.Resource = &blockstoriov1alpha1.DRBDResourceOptions{}
		}

		rd.Spec.DRBDOptions.Resource.Quorum = value

		// Companion seeding only when quorum is enabled — the
		// `quorum=off` path doesn't consult `on-no-quorum` and
		// stamping it would create churn for no benefit.
		if value == QuorumPolicyMajority {
			if _, present := rd.Spec.Props[onNoQuorumKey]; !present {
				rd.Spec.Props[onNoQuorumKey] = onNoQuorumSeed
			}

			if rd.Spec.DRBDOptions.Resource.OnNoQuorum == "" {
				rd.Spec.DRBDOptions.Resource.OnNoQuorum = onNoQuorumSeed
			}
		}

		err := r.Update(ctx, rd)
		if err == nil {
			return nil
		}

		if !apierrors.IsConflict(err) {
			return err
		}

		// Refetch and retry.
		err = r.Get(ctx, client.ObjectKey{Name: rd.Name}, rd)
		if err != nil {
			return err
		}
	}

	return apierrors.NewConflict(
		blockstoriov1alpha1.GroupVersion.WithResource("resourcedefinitions").GroupResource(),
		rd.Name, nil)
}

// quorumPropsAlreadySet reports whether the RD's prop bag AND the
// typed `Spec.DRBDOptions.Resource` slot both already reflect the
// desired quorum value AND (for the `majority` branch) either carry
// an operator-pinned `on-no-quorum` or have the seed value we'd
// stamp. Used by setQuorum to short-circuit the Update when nothing
// would change — keeps ResourceVersion stable and avoids the
// conflict-retry storm a write-on-every-reconcile would trigger
// under fan-out load.
//
// Bug 309: must consult the typed slot too — `effectiveprops.
// Resolve` lets typed override `Spec.Props`, so a stale typed value
// (initial CSI POST) would mask the desired prop write and the
// short-circuit would lie. Mirror writes happen in setQuorum.
func quorumPropsAlreadySet(rd *blockstoriov1alpha1.ResourceDefinition, value, quorumKey, onNoQuorumKey string) bool {
	if rd.Spec.Props == nil {
		return false
	}

	if rd.Spec.Props[quorumKey] != value {
		return false
	}

	if rd.Spec.DRBDOptions == nil ||
		rd.Spec.DRBDOptions.Resource == nil ||
		rd.Spec.DRBDOptions.Resource.Quorum != value {
		return false
	}

	if value != QuorumPolicyMajority {
		// `quorum=off` doesn't consult `on-no-quorum` — desired
		// state is purely the quorum value.
		return true
	}

	// quorum is correct; companion seed is desired-state iff the
	// operator hasn't already pinned an `on-no-quorum` value. Check
	// both the prop bag and the typed slot — either one being unset
	// triggers a stamp.
	if _, present := rd.Spec.Props[onNoQuorumKey]; !present {
		return false
	}

	return rd.Spec.DRBDOptions.Resource.OnNoQuorum != ""
}

// removeWitnesses deletes every TIE_BREAKER replica of the named RD.
// Best-effort: a witness that was already reaped, or has been promoted
// to a diskful backfill/relocate target, is silently skipped so
// concurrent reconciles converge.
//
// Why the delete must be a version-guarded conditional, not a plain
// Get-then-Delete: the `witnesses` slice is a snapshot taken at the top
// of ensureTiebreaker, and two distinct concurrent transitions promote a
// witness row IN PLACE on the same (rd, node) key:
//
//   - Phase-3 relocate-onto-the-tiebreaker (`r-full-lifecycle.sh`:
//     `r d <other-diskful>` leaves 1 diskful + 1 orphan witness, then
//     `r c <tiebreaker-node>` promotes that SAME witness via
//     promoteDisklessReplica — TIE_BREAKER+DISKLESS stripped,
//     StorPoolName stamped).
//   - Bug 393 redundancy backfill: after an inactive-replica return /
//     node event, `r c --auto-place` promotes the witness to a diskful
//     backfill replica via pkg/placer.promoteWitness (corner-D2b).
//
// A non-atomic Get-then-Delete narrows but does NOT close the window: the
// promotion can land in the gap between our re-Get and the Delete, and an
// unconditional Delete-by-name then clobbers the freshly-promoted diskful
// replica — redundancy is silently never restored (fewer diskful replicas
// than place_count). DeleteIfTieBreaker carries the re-check and the
// delete across as one optimistic-concurrency operation (ResourceVersion
// + UID precondition on the k8s store; lock-held on the in-memory store),
// so a racing promotion aborts the delete instead of clobbering it.
func (r *ResourceDefinitionReconciler) removeWitnesses(ctx context.Context, rdName string, witnesses []apiv1.Resource) error {
	for i := range witnesses {
		_, err := r.Store.Resources().DeleteIfTieBreaker(ctx, rdName, witnesses[i].NodeName)
		if err != nil {
			return err
		}
	}

	return nil
}

// filterActiveReplicas drops INACTIVE replicas from the slice. An
// INACTIVE replica is one the operator deactivated with `drbdadm down`
// (Bug 387): its DRBD device is not running, so it casts no vote in the
// `quorum: majority` decision the auto-tiebreaker invariant defends.
// The tiebreaker policy must reason only over the live voting peers, so
// INACTIVE replicas are excluded before the diskful/diskless split —
// they count as neither a voting diskful nor a diskless witness/peer.
func filterActiveReplicas(replicas []apiv1.Resource) []apiv1.Resource {
	active := make([]apiv1.Resource, 0, len(replicas))

	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagInactive) {
			continue
		}

		active = append(active, replicas[i])
	}

	return active
}

// splitByDiskless partitions replicas into (diskful, diskless) lists.
// DRBD treats DISKLESS replicas as connection-mesh participants only
// — they don't allocate storage but they vote in the quorum.
func splitByDiskless(replicas []apiv1.Resource) ([]apiv1.Resource, []apiv1.Resource) {
	var diskful, diskless []apiv1.Resource

	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDiskless) {
			diskless = append(diskless, replicas[i])
		} else {
			diskful = append(diskful, replicas[i])
		}
	}

	return diskful, diskless
}

// pickTiebreakerNode chooses any healthy satellite that is not
// already hosting a replica of this RD. Picks deterministically
// (lowest name first) so two reconcile races converge on the same
// answer instead of both creating a tiebreaker.
//
// Bug 261 (P1, data-loss class): the per-call `hostingReplica` map
// is a snapshot built by the caller from `listReplicasDirect` at
// the top of `ensureTiebreaker`. A stale snapshot (Resource watch
// race, REST cache lag on a sibling apiserver replica) could miss a
// diskful node — and the downstream `Store.Resources().Create` of
// a `[DISKLESS, TIE_BREAKER]` Resource on that node would land
// inside the partition-vulnerable window of an `r d <witness>`
// operator flow, leaving the cluster one race away from a silent
// `r td --diskless` against the diskful Resource (data-loss class).
//
// `pickTiebreakerNodeForRD` re-probes the store for diskful
// Resources of the RD and excludes them unconditionally — defense-
// in-depth against any caller-snapshot staleness. The legacy
// `pickTiebreakerNode` shim stays as a back-compat surface (only
// the pick_tiebreaker_test.go callers exercise it without an RD
// name); production wiring goes through the RD-aware variant.
func (r *ResourceDefinitionReconciler) pickTiebreakerNode(ctx context.Context, hostingReplica map[string]bool) (string, error) {
	return r.pickTiebreakerNodeForRD(ctx, "", hostingReplica)
}

// pickTiebreakerNodeForRD is the Bug-261-defended selector: same
// contract as pickTiebreakerNode plus an unconditional hard-exclude
// of every node currently hosting a diskful Resource of `rdName`,
// re-probed against the store on every call. When `rdName==""` the
// store re-probe is skipped (legacy caller surface).
func (r *ResourceDefinitionReconciler) pickTiebreakerNodeForRD(
	ctx context.Context,
	rdName string,
	hostingReplica map[string]bool,
) (string, error) {
	excluded := make(map[string]bool, len(hostingReplica))
	maps.Copy(excluded, hostingReplica)

	if rdName != "" {
		// Defense-in-depth: re-probe the store for diskful Resources
		// of this RD and exclude them. Caller's hostingReplica may be
		// stale, but the store snapshot read inline here is the
		// freshest signal the controller-side can get without a full
		// reconcile fan-out.
		live, err := r.Store.Resources().ListByDefinition(ctx, rdName)
		if err != nil {
			return "", err
		}

		for i := range live {
			if slices.Contains(live[i].Flags, apiv1.ResourceFlagDiskless) {
				continue
			}

			excluded[live[i].NodeName] = true
		}
	}

	nodes, err := r.Store.Nodes().List(ctx)
	if err != nil {
		return "", err
	}

	candidates := make([]string, 0, len(nodes))

	for i := range nodes {
		if excluded[nodes[i].Name] {
			continue
		}

		if isDisabledNode(&nodes[i]) {
			continue
		}

		if nodes[i].Type != "" && nodes[i].Type != apiv1.NodeTypeSatellite && nodes[i].Type != apiv1.NodeTypeCombined {
			continue
		}

		candidates = append(candidates, nodes[i].Name)
	}

	if len(candidates) == 0 {
		return "", nil
	}

	slices.Sort(candidates)

	return candidates[0], nil
}

// isDisabledNode mirrors placer.disabledNodes for the RD-level
// tiebreaker path so we don't pin an EVICTED/LOST node as the witness.
func isDisabledNode(node *apiv1.Node) bool {
	for _, f := range node.Flags {
		if f == apiv1.NodeFlagEvicted || f == apiv1.NodeFlagLost {
			return true
		}
	}

	return false
}

// dropStrandedReplicas reduces the RD's replica snapshot to the live
// voting set and reaps stranded witnesses along the way. Two classes
// of replica are excluded:
//
//   - Bug 385: replicas hosted on EVICTED / LOST nodes — draining
//     placements, not live voters.
//   - Bug-024: replicas whose Node row no longer exists at all (e.g.
//     a TIE_BREAKER ghost stamped on a node that `n lost` just
//     deleted — nothing else ever reaps it: the node object is gone,
//     so there is no DeletionTimestamp event and no finalizer pass).
//     Absence is confirmed against the authoritative reader (direct
//     read, not the informer cache) so a momentarily-lagging cache
//     can't trigger a wrongful reap.
//
// TIE_BREAKER rows in either class are deleted via
// removeStrandedWitnesses so a fresh witness can land on a healthy
// spare; stranded diskful replicas are left alone (relocating those
// is the NodeReconciler's job) but still excluded from the count.
func (r *ResourceDefinitionReconciler) dropStrandedReplicas(
	ctx context.Context,
	rdName string,
	replicas []apiv1.Resource,
) ([]apiv1.Resource, error) {
	disabled, known, err := r.nodeSets(ctx)
	if err != nil {
		return nil, err
	}

	live, stranded := splitByDisabledNode(replicas, disabled)

	live, ghosts, err := r.confirmGhostReplicas(ctx, live, known)
	if err != nil {
		return nil, err
	}

	stranded = append(stranded, ghosts...)

	err = r.removeStrandedWitnesses(ctx, rdName, stranded)
	if err != nil {
		return nil, err
	}

	return live, nil
}

// nodeSets returns (disabled, known): the set of node names flagged
// EVICTED / LOST and the set of ALL node names the store currently
// lists. Bug 385: the RD-level witness / quorum decision must treat
// replicas on disabled nodes as draining placements, not live ones —
// mirroring the placer's `disabledNodes` semantic. Bug-024 extends
// the same idea to nodes that are gone entirely: a replica whose
// node is absent from `known` is a ghost candidate (confirmed
// against the authoritative reader in confirmGhostReplicas before
// any reap). Re-probed from the store on every reconcile so a
// freshly-stamped flag — or a freshly-deleted node — takes effect on
// the next witness pass.
func (r *ResourceDefinitionReconciler) nodeSets(ctx context.Context) (map[string]struct{}, map[string]struct{}, error) {
	nodes, err := r.Store.Nodes().List(ctx)
	if err != nil {
		return nil, nil, err
	}

	disabled := make(map[string]struct{})
	known := make(map[string]struct{}, len(nodes))

	for i := range nodes {
		known[nodes[i].Name] = struct{}{}

		if isDisabledNode(&nodes[i]) {
			disabled[nodes[i].Name] = struct{}{}
		}
	}

	return disabled, known, nil
}

// confirmGhostReplicas partitions `replicas` into (live, ghosts):
// a ghost is a replica whose node is missing from the cached `known`
// set AND confirmed absent by the authoritative reader. The double
// check matters in both directions — the informer cache can lag the
// apiserver, so `known` alone could briefly miss a real node (which
// must NOT get its witness reaped), while the cache can also still
// serve a node `n lost` already deleted (the Bug-024 ghost-create
// source, handled separately in createWitness).
func (r *ResourceDefinitionReconciler) confirmGhostReplicas(
	ctx context.Context,
	replicas []apiv1.Resource,
	known map[string]struct{},
) ([]apiv1.Resource, []apiv1.Resource, error) {
	var live, ghosts []apiv1.Resource

	for i := range replicas {
		if _, ok := known[replicas[i].NodeName]; ok {
			live = append(live, replicas[i])

			continue
		}

		probe, err := r.probeNodeDirect(ctx, replicas[i].NodeName)
		if err != nil {
			return nil, nil, err
		}

		if probe.found {
			// Cached list lagged a just-created node; the replica is
			// real. Never reap on a stale negative.
			live = append(live, replicas[i])

			continue
		}

		ghosts = append(ghosts, replicas[i])
	}

	return live, ghosts, nil
}

// linstorNameAnnotation mirrors pkg/store/k8s.AnnotationLinstorName.
// The literal is duplicated (same pattern as snapshotGroupIDLabel in
// snapshot_controller.go) to avoid a controller→store/k8s dependency.
// Node CRDs whose LINSTOR name needed slugifying carry the original
// wire name here; probeNodeDirect matches against both spellings.
const linstorNameAnnotation = "blockstor.io/linstor-name"

// nodeProbe is the answer probeNodeDirect returns about a node's
// authoritative state.
type nodeProbe struct {
	// found reports the node row exists.
	found bool
	// drained reports an EVICTED / LOST drain flag on the found node.
	drained bool
}

// probeNodeDirect answers "does this node exist right NOW, and is it
// usable as a witness host?" against the authoritative source rather
// than the manager's informer cache. Bug-024: the witness-placement
// candidate list comes from Store.Nodes().List, which in production
// is backed by the cached client and trails the apiserver — after
// `n lost` deletes a node, the lagging cache happily re-offers it
// and the controller would stamp a `[DISKLESS TIE_BREAKER]` ghost on
// the just-deleted node. Mirrors the REST layer's Bug 174 node-
// deleted-race guard (pkg/rest/autoplace.go::
// refuseResourceCreateOnNodeDeletedRace), which re-validates the
// pinned node against the store right after the write.
//
// Production path: APIReader (direct apiserver reads, no cache).
// LINSTOR node names are matched case-insensitively against both the
// CRD name and the preserved original-name annotation, the same
// resolution pkg/store/k8s applies. Unit tests construct the
// reconciler without APIReader — the Store fallback is authoritative
// there (InMemory has no cache to lag).
func (r *ResourceDefinitionReconciler) probeNodeDirect(ctx context.Context, wireName string) (nodeProbe, error) {
	if r.APIReader == nil {
		node, err := r.Store.Nodes().Get(ctx, wireName)
		if stderrors.Is(err, store.ErrNotFound) {
			return nodeProbe{}, nil
		}

		if err != nil {
			return nodeProbe{}, err
		}

		return nodeProbe{found: true, drained: isDisabledNode(&node)}, nil
	}

	var list blockstoriov1alpha1.NodeList
	if err := r.APIReader.List(ctx, &list); err != nil {
		return nodeProbe{}, err
	}

	for i := range list.Items {
		if !nodeCRDMatchesWireName(&list.Items[i], wireName) {
			continue
		}

		return nodeProbe{found: true, drained: nodeHasDrainFlag(&list.Items[i])}, nil
	}

	return nodeProbe{}, nil
}

// nodeCRDMatchesWireName reports whether a Node CRD addresses the
// given LINSTOR wire name: either directly via metadata.name or via
// the preserved original-name annotation (slugified names). LINSTOR
// identifiers are case-insensitive, so both comparisons fold case.
func nodeCRDMatchesWireName(node *blockstoriov1alpha1.Node, wireName string) bool {
	if strings.EqualFold(node.Name, wireName) {
		return true
	}

	original, ok := node.Annotations[linstorNameAnnotation]

	return ok && strings.EqualFold(original, wireName)
}

// splitByDisabledNode partitions replicas into (live, stranded) by the
// disabled-node set. `live` is hosted on healthy (non-EVICTED/-LOST)
// nodes and drives the witness / quorum decision; `stranded` sits on a
// draining node and is handled separately (Bug 385).
func splitByDisabledNode(replicas []apiv1.Resource, disabled map[string]struct{}) ([]apiv1.Resource, []apiv1.Resource) {
	var live, stranded []apiv1.Resource

	for i := range replicas {
		if _, off := disabled[replicas[i].NodeName]; off {
			stranded = append(stranded, replicas[i])

			continue
		}

		live = append(live, replicas[i])
	}

	return live, stranded
}

// removeStrandedWitnesses deletes every TIE_BREAKER witness that sits on
// an EVICTED / LOST node (Bug 385). A diskless witness has no business
// remaining on a drained node — leaving it there pins the witness to the
// very node the operator evicted and blocks a fresh witness from landing
// on a healthy spare. Diskful replicas on disabled nodes are intentionally
// left alone here: relocating / deleting those is the NodeReconciler's job
// (placer gap-fill for EVICTED, source-delete for LOST), and dropping a
// diskful's only data copy from this path would be a data-availability
// hazard. Per-row Delete tolerates NotFound so a concurrent
// node-migration cascade can't fail the reconcile.
func (r *ResourceDefinitionReconciler) removeStrandedWitnesses(ctx context.Context, rdName string, stranded []apiv1.Resource) error {
	for i := range stranded {
		if !slices.Contains(stranded[i].Flags, apiv1.ResourceFlagTieBreaker) {
			continue
		}

		err := r.Store.Resources().Delete(ctx, rdName, stranded[i].NodeName)
		if err != nil && !stderrors.Is(err, store.ErrNotFound) {
			return err
		}
	}

	return nil
}

// alreadyExists is a string-based check for the wrapped errors the
// k8s store returns. The k8s store wraps errAlreadyExists from
// kube-apiserver in a cockroachdb/errors.Wrap — Is() doesn't tunnel
// through that, so we keyword-match on the message.
func alreadyExists(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "already exists")
}

// stampResizePending walks the RD's volume-definitions and for each
// child Resource whose observed `Status.Volumes[n].UsableKib` lags
// the RD spec's `SizeKib`, stamps the per-volume resize-pending
// annotation so operators (and downstream watchers) can see that
// the satellite still owes the resize. Bug 148.
//
// Mirrors Bug 136's REST-handler stamp but fires on EVERY reconcile,
// so kubectl-edit-driven grows (which bypass the REST surface)
// gain the same operator-visible signal. Idempotent: a Resource
// whose UsableKib already matches the target size doesn't get
// re-stamped, avoiding apiserver write thrash on every periodic
// reconcile.
//
// Best-effort on per-Resource Update errors — the RD spec change
// is the load-bearing mutation; an annotation-stamp failure on
// one Resource doesn't roll back the others, and the next
// reconcile re-tries the failed entries.
//
// Operates on the K8s Resource CRDs (not the in-memory Store)
// because the annotation is what `kubectl get resource -o yaml`
// surfaces and what the satellite reconciler's RD-watch hook
// keys off of when it re-renders.
func (r *ResourceDefinitionReconciler) stampResizePending(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) error {
	if len(rd.Spec.VolumeDefinitions) == 0 {
		return nil
	}

	var resList blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &resList)
	if err != nil {
		return err
	}

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName != rd.Name {
			continue
		}

		err = r.stampResizePendingOnResource(ctx, rd, &resList.Items[i])
		if err != nil && !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Info("stampResizePending: update skipped",
				"resource", resList.Items[i].Name, "err", err.Error())
		}
	}

	return nil
}

// stampResizePendingOnResource stamps the per-volume annotation on
// one Resource whose Status.Volumes[n].UsableKib lags the RD spec.
// Returns nil when the Resource needs no update (idempotent path).
func (r *ResourceDefinitionReconciler) stampResizePendingOnResource(
	ctx context.Context,
	rd *blockstoriov1alpha1.ResourceDefinition,
	res *blockstoriov1alpha1.Resource,
) error {
	updated := false

	for _, vd := range rd.Spec.VolumeDefinitions {
		observed := observedUsableKib(res, vd.VolumeNumber)
		if observed == vd.SizeKib {
			continue
		}

		key := Bug148ResizePendingAnnotationPrefix + strconv.FormatInt(int64(vd.VolumeNumber), 10)
		value := strconv.FormatInt(vd.SizeKib, 10)

		if res.Annotations != nil && res.Annotations[key] == value {
			continue
		}

		if res.Annotations == nil {
			res.Annotations = map[string]string{}
		}

		res.Annotations[key] = value
		updated = true
	}

	if !updated {
		return nil
	}

	return r.Update(ctx, res)
}

// observedUsableKib returns the satellite-reported UsableKib for the
// given volume number on this Resource, or 0 when the satellite has
// not yet reported volumes for that slot. Treating 0 as "lags the
// target" is correct: a freshly-spawned Resource with no observed
// volumes yet should get the resize-pending stamp so the satellite's
// first apply pass sees the target size as the desired state.
func observedUsableKib(res *blockstoriov1alpha1.Resource, vn int32) int64 {
	for i := range res.Status.Volumes {
		if res.Status.Volumes[i].VolumeNumber == vn {
			return res.Status.Volumes[i].UsableKib
		}
	}

	return 0
}

// SetupWithManager sets up the controller with the Manager.
//
// We Watch Resources too — the tiebreaker logic needs to fire when
// child Resources land, not just on the RD's own creation. Without
// the watch, an `apply RD + 2 Resources` race never re-runs the RD
// reconciler after the Resources finish, and a 2-replica RD sits
// without its DISKLESS witness until the next periodic re-sync.
func (r *ResourceDefinitionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}

	// Bug 342: filter Resource watch events down to Spec / Create /
	// Delete changes. Without this, every satellite-side Status patch
	// (Conn state, replication state, peer info, per-volume disk
	// state — dozens per Resource per second when DRBD is settling)
	// triggers a full RD reconcile that runs ensureTiebreaker. Diag
	// evidence on bug342 stand shows 20-50 reconciles per second
	// during Phase 3 r d/r c churn, starving the Resource
	// reconciler's workqueue and producing a stale APIReader view
	// (`replicas=2 diskful=1 witness=1` while apiserver actually has
	// 3 Resources). The tiebreaker only cares about Spec.Flags + the
	// set of Resources (which Create/Delete already cover), so
	// suppressing Status-only updates eliminates the hot loop
	// without losing correctness.
	resourceEventFilter := predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return true },
		DeleteFunc: func(_ event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Generation bump = Spec change. Status-only patches
			// keep Generation steady.
			return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.ResourceDefinition{}).
		Watches(&blockstoriov1alpha1.Resource{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueRDForResource),
			builder.WithPredicates(resourceEventFilter)).
		// Bug 386: re-run the tiebreaker invariant when a node's
		// EVICTED / LOST flag set changes. `linstor n rst` clears
		// EVICTED on a recovered node; `linstor n evacuate` sets it.
		// `ensureTiebreaker` (and pickTiebreakerNode) treat an
		// EVICTED/LOST node as an unusable witness candidate, so a
		// 2-diskful RD whose witness collapsed while a node was drained
		// must re-place the TIE_BREAKER once the node returns. Without
		// this watch nothing enqueues the RD on a node-flag toggle and
		// the witness is only (re)placed on the next periodic re-sync —
		// leaving two diskful UpToDate replicas with no quorum witness
		// in between (split-brain risk on a subsequent failure). The
		// nodeDrainFlagChanged predicate filters to the flag transition
		// so unrelated Node Spec edits (props, net-interfaces) don't
		// fan out to every RD.
		Watches(&blockstoriov1alpha1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueRDsForNode),
			builder.WithPredicates(nodeDrainFlagChanged())).
		Named("resourcedefinition").
		Complete(r)
}

// nodeDrainFlagChanged fires only when a Node's drain-signal flag set
// (EVICTED / LOST) transitions. The REST `n rst` / `n evacuate` /
// `n lost` handlers stamp these onto `Spec.Flags`; the satellite also
// re-applies Node Spec on every reconcile with the flag set unchanged,
// so a bare generation/Spec-equality watch would fan out to every RD
// on every heartbeat. Restricting to the EVICTED/LOST membership delta
// keeps the Bug 386 re-enqueue precise.
//
// Create fires when the node already carries a drain flag (controller
// restart re-reads existing state) so the witness invariant runs at
// least once against the recovered topology.
func nodeDrainFlagChanged() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			n, ok := e.Object.(*blockstoriov1alpha1.Node)
			if !ok {
				return false
			}

			return nodeHasDrainFlag(n)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, ok := e.ObjectOld.(*blockstoriov1alpha1.Node)
			if !ok {
				return false
			}

			newNode, ok := e.ObjectNew.(*blockstoriov1alpha1.Node)
			if !ok {
				return false
			}

			return nodeHasDrainFlag(oldNode) != nodeHasDrainFlag(newNode)
		},
		// Bug-024: a node DELETE is the strongest drain signal there
		// is — `n lost` removes the Node row outright. Re-running the
		// witness invariant on it lets the ghost-repair leg
		// (dropStrandedReplicas) reap a TIE_BREAKER stranded on the
		// deleted node as soon as the event lands instead of waiting
		// for the periodic requeue. Node deletion is rare, so the
		// all-RD fan-out stays cheap.
		DeleteFunc:  func(_ event.DeleteEvent) bool { return true },
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// nodeHasDrainFlag reports whether a Node CRD carries an EVICTED or
// LOST flag on either Spec (operator intent, set by the REST handlers)
// or Status (satellite-observed). Mirrors isDisabledNode's flag set so
// the watch predicate and the witness-candidate filter agree on what
// "drained" means.
func nodeHasDrainFlag(n *blockstoriov1alpha1.Node) bool {
	for _, f := range n.Spec.Flags {
		if f == apiv1.NodeFlagEvicted || f == apiv1.NodeFlagLost {
			return true
		}
	}

	for _, f := range n.Status.Flags {
		if f == apiv1.NodeFlagEvicted || f == apiv1.NodeFlagLost {
			return true
		}
	}

	return false
}

// enqueueRDsForNode maps a Node flag-change event to every
// ResourceDefinition in the cluster. The tiebreaker invariant is a
// cluster-wide candidate-set decision (any RD's witness might have
// collapsed onto, or now want to re-land on, the toggled node), so a
// per-node restore re-evaluates all RDs. The set of RDs is small
// relative to Resources, and the nodeDrainFlagChanged predicate gates
// the fan-out to genuine EVICTED/LOST transitions, so this stays cheap.
func (r *ResourceDefinitionReconciler) enqueueRDsForNode(ctx context.Context, _ client.Object) []reconcile.Request {
	var rdList blockstoriov1alpha1.ResourceDefinitionList

	err := r.List(ctx, &rdList)
	if err != nil {
		return nil
	}

	out := make([]reconcile.Request, 0, len(rdList.Items))
	for i := range rdList.Items {
		out = append(out, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: rdList.Items[i].Name},
		})
	}

	return out
}

// enqueueRDForResource maps a Resource event to its parent RD.
// Resource.Spec.ResourceDefinitionName is the canonical link.
func (r *ResourceDefinitionReconciler) enqueueRDForResource(_ context.Context, obj client.Object) []reconcile.Request {
	res, ok := obj.(*blockstoriov1alpha1.Resource)
	if !ok || res.Spec.ResourceDefinitionName == "" {
		return nil
	}

	return []reconcile.Request{
		{NamespacedName: types.NamespacedName{Name: res.Spec.ResourceDefinitionName}},
	}
}
