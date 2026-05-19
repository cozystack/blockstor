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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch;create;update;patch;delete

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

	diskful, diskless := splitByDiskless(replicas)
	witness := filterTieBreaker(diskless)

	wantWitness := shouldTieBreakerExist(rd, diskful, diskless, witness)

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
//   - diskful == 1 AND a non-witness diskless is present: this is
//     the post-`r td --diskless` shape Bug 104 protects. The witness
//     IS still the third voter (1 diskful + 1 user-diskless + 1
//     witness = 3 voters); dropping it would freeze the volume on
//     the next partition.
//
//   - diskful == 1 AND NO non-witness diskless: Bug 338. The user
//     ran `linstor r d <one-of-diskful>` and the witness is now
//     orphaned. 1 diskful + 1 witness is a 2-voter quorum with no
//     real majority — collapse the witness so the lone diskful
//     runs cleanly under quorum=off.
//
//   - diskful >= 3: the cluster has a clear majority on its own; the
//     witness is dead weight.
func shouldKeepExistingWitness(diskful, nonWitnessDiskless, witnessUnnecessaryDiskfulCount int) bool {
	if diskful >= witnessUnnecessaryDiskfulCount {
		return false
	}

	if diskful == 2 {
		return true
	}

	// diskful == 1 — keep only if a non-witness diskless co-exists.
	return diskful == 1 && nonWitnessDiskless >= 1
}

// shouldTieBreakerExist decides whether the RD should carry an
// auto-managed TIE_BREAKER witness. Splits into three complementary
// branches, all gated on DrbdOptions/AutoAddQuorumTiebreaker
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
//     across toggle-disk diskful→diskless transitions. Without this,
//     `r td --diskless <one-of-diskful>` on a 2-diskful + 1-witness
//     RD would see diskful drop to 1 and a user-added diskless climb
//     to 1, flip wantWitness to false, and remove the witness —
//     leaving 1 diskful + 1 diskless with no third voter, which
//     freezes quorum:majority the moment those two lose comms. Once
//     a witness exists, only drop it when diskful ≥ 3 (clear
//     majority without help) or there's no diskful at all (nothing
//     to defend). Suppression is not consulted here — that
//     annotation is stamped at delete-time and is only meaningful
//     when the witness has already been removed.
//
//  3. Post-toggle race branch (Bug 108): the v2 bug-hunt agent
//     reproduced (3/3) a regression where `rd ap --place-count 2`
//     followed IMMEDIATELY by `r td --diskless <one-of-diskful>`
//     leaves the RD at 1 diskful + 1 user-diskless + ZERO witness.
//     Bug 104's keep branch can't help — no witness was ever
//     stamped because the toggle landed inside the cache-trail
//     window between the auto-place's two-Resource fan-out and
//     the RD-reconciler's first witness-creation pass. The keep
//     branch only preserves what's there. This branch closes the
//     race: when the post-toggle topology matches the steady
//     state the keep branch defends (1 diskful + 1 user-diskless,
//     no witness), MAKE a witness so the next toggle-back returns
//     us to the canonical 2-diskful + 1-witness shape. Cap at
//     diskful < 3 so a 3-replica RD with a transient diskless
//     replica (e.g. node-evacuate in flight) doesn't grow a
//     fourth peer. Suppression is honoured because the same
//     race-after-tiebreaker-delete pattern would otherwise
//     re-stamp the witness an operator just dropped.
func shouldTieBreakerExist(
	rd *blockstoriov1alpha1.ResourceDefinition,
	diskful, diskless, witness []apiv1.Resource,
) bool {
	if !isAutoTieBreakerEnabled(rd) {
		return false
	}

	nonWitnessDiskless := len(diskless) - len(witness)

	wantNewWitness := !isTiebreakerSuppressed(rd) &&
		len(diskful) >= 2 && len(diskful)%2 == 0 && nonWitnessDiskless == 0

	const witnessUnnecessaryDiskfulCount = 3

	keepExistingWitness := len(witness) > 0 &&
		shouldKeepExistingWitness(len(diskful), nonWitnessDiskless, witnessUnnecessaryDiskfulCount)

	// Bug 108: post-toggle race repair. The keep branch above
	// preserves an existing witness across diskful→diskless; this
	// branch creates one when the race ate the witness-creation
	// pass. Scoped to total-non-witness-replicas == 2 (i.e. 1
	// diskful + 1 user-diskless after a `r td --diskless` on one
	// of two place_count=2 replicas) so we don't grow a witness
	// for the upstream-LINSTOR steady state "2 diskful + 1
	// user-diskless" (3 voters, parity odd) which TestTiebreaker-
	// EvenWithDiskless pins as "no witness". The 2-voter window
	// is the one the v2 bug-hunt agent reported: without a
	// witness, the next toggle-back to diskful would return us
	// to 2 diskful + 1 user-diskless (3 voters, fine) — but
	// during the partition-vulnerable window of "1 diskful + 1
	// user-diskless" we're at 2 voters with no majority. The
	// witness restores the third voter that the operator's
	// place_count=2 + auto-witness contract promised.
	repairAfterToggleRace := !isTiebreakerSuppressed(rd) &&
		len(witness) == 0 &&
		len(diskful) >= 1 && len(diskful) < witnessUnnecessaryDiskfulCount &&
		nonWitnessDiskless >= 1 &&
		(len(diskful)+nonWitnessDiskless) == 2

	return wantNewWitness || keepExistingWitness || repairAfterToggleRace
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
	if rd == nil || rd.Spec.Props == nil {
		return false
	}

	const propKey = "DrbdOptions/AutoQuorum"

	return strings.EqualFold(rd.Spec.Props[propKey], "disabled")
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

	return nil
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
// Best-effort: ErrNotFound is swallowed so concurrent reconciles
// converge.
func (r *ResourceDefinitionReconciler) removeWitnesses(ctx context.Context, rdName string, witnesses []apiv1.Resource) error {
	for i := range witnesses {
		err := r.Store.Resources().Delete(ctx, rdName, witnesses[i].NodeName)
		if err != nil && !stderrors.Is(err, store.ErrNotFound) {
			return err
		}
	}

	return nil
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

	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.ResourceDefinition{}).
		Watches(&blockstoriov1alpha1.Resource{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueRDForResource)).
		Named("resourcedefinition").
		Complete(r)
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
