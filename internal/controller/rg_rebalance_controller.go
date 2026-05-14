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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// RGRebalanceReconciler is the controller-side half of the Bug 60
// fix + scenarios 2.15 / 2.20 periodic auto-rebalance. It complements
// the existing ResourceGroupReconciler with both an explicit
// REST-driven trigger and a periodic background tick:
//
//  1. REST `linstor rg modify` stamps the
//     `blockstor.io/rebalance-pending` annotation on the RG CRD when
//     PlaceCount strictly increases or a placement-affecting
//     SelectFilter changes (see pkg/rest/resource_groups.go).
//  2. The controller-runtime watch on RG CRDs delivers a reconcile
//     request. The annotation-driven path runs the additive placer
//     for every child RD, then strips the marker so the next event
//     is a clean no-op.
//  3. Scenario 2.15: every Reconcile returns
//     `Result.RequeueAfter = BalanceResourcesInterval` so the
//     reconciler re-wakes on a fixed cadence even without an RG / RD
//     change. The scheduled tick covers the case where a satellite
//     was evicted between RG events — without it, a vacated slot
//     would sit unfilled until an unrelated REST modify fired.
//  4. Scenario 2.15 grace gate: while at least one Node has been
//     observed `ConnectionStatus=OFFLINE` for less than
//     `BalanceResourcesGracePeriod` minutes, the scheduled pass
//     short-circuits. A flapping / rebooting node thus gets a chance
//     to return before healthy peers churn replacement replicas on
//     its behalf. The annotation-driven path is NOT gated — an
//     operator-driven REST modify still flows through.
//  5. Scenario 2.20: every Reconcile also scans for RDs stamped with
//     `blockstor.io/spawn-shortfall=<RFC3339>` by the 9.W05
//     partial-fail spawn path; once GracePeriod has elapsed since the
//     stamp, the placer re-runs against that RD and the annotation is
//     stripped on success.
//
// Strictly additive: the placer's Place method only spawns replicas,
// never removes them. Matches upstream LINSTOR's contract that
// scale-down requires explicit `linstor r d`.
type RGRebalanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Store is the shared blockstor store. Required for placer
	// invocation and for the deferred annotation strip.
	Store store.Store

	// Now is the wall-clock provider. Production wires `time.Now`;
	// tests override it so the Interval / GracePeriod transitions
	// can be exercised without a real sleep. Nil defaults to
	// time.Now.
	Now func() time.Time

	// nodeOfflineSinceMu guards nodeOfflineSince. The reconciler
	// observes Node ConnectionStatus on every tick and tracks the
	// first wall-clock instant each currently-OFFLINE node was seen
	// in that state; reads / writes are coalesced through this
	// mutex so a concurrent controller-runtime worker pool doesn't
	// race on the map.
	nodeOfflineSinceMu sync.Mutex
	nodeOfflineSince   map[string]time.Time
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcegroups,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=nodes,verbs=get;list;watch

// Reconcile is the explicit + periodic rebalance pass. See the type
// comment for the five-step lifecycle. Returns `RequeueAfter =
// BalanceResourcesInterval` on every path (annotation-driven,
// scheduled-tick, kill-switched, grace-gated) so the cadence is
// stable regardless of branch taken.
func (r *RGRebalanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Store == nil {
		return ctrl.Result{}, nil
	}

	interval, gracePeriod, err := r.resolveTuning(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	requeue := ctrl.Result{RequeueAfter: interval}

	rg, err := r.Store.ResourceGroups().Get(ctx, req.Name)
	if err != nil {
		// RG already gone; the scheduled cadence still applies to
		// surviving RGs, but for THIS request there is nothing to do.
		//nolint:nilerr // missing RG is a benign "nothing to rebalance" state.
		return ctrl.Result{}, nil
	}

	if balanceResourcesDisabled(ctx, r.Store) {
		return requeue, r.handleDisabledPass(ctx, &rg)
	}

	// Refresh the offline-since tracker against the current Node list
	// BEFORE consulting the grace gate so a newly-recovered node is
	// not counted against the window.
	r.refreshOfflineTracker(ctx)

	if perr := r.runRebalancePass(ctx, &rg, gracePeriod); perr != nil {
		return requeue, perr
	}

	// Scenario 2.20 spawn-shortfall replay. Independent of the
	// rebalance pass — an RD that already carries the shortfall
	// marker gets retried once the grace window since its stamp has
	// elapsed, regardless of whether the parent RG has the
	// rebalance-pending annotation. Annotation-stripped on success.
	return requeue, r.replaySpawnShortfalls(ctx, &rg, gracePeriod)
}

// handleDisabledPass implements scenario 2.W02's "kill-switched" branch.
// Both the annotation path AND the scheduled-tick path are suppressed;
// a stamped annotation is stripped so a stale marker doesn't loop
// forever against the disabled reconciler.
func (r *RGRebalanceReconciler) handleDisabledPass(ctx context.Context, rg *apiv1.ResourceGroup) error {
	if _, hasAnnotation := rg.Annotations[apiv1.AnnotationRGRebalancePending]; !hasAnnotation {
		return nil
	}

	logf.FromContext(ctx).WithValues("rg", rg.Name).
		Info("rebalance disabled by BalanceResourcesEnabled=false; stripping stale annotation")

	return r.stripRebalanceAnnotation(ctx, rg)
}

// runRebalancePass dispatches the additive placer for one of two
// trigger paths: the operator-driven REST annotation, or the scheduled
// tick (gated by the per-node grace window). The two paths share the
// same placer call but differ in pre/post-conditions — operator intent
// is logged at INFO and strips the annotation; the scheduled cadence
// stays quiet on a clean no-op pass and respects the per-RD shortfall
// grace window (so 2.20 retries don't churn faster than configured).
func (r *RGRebalanceReconciler) runRebalancePass(ctx context.Context, rg *apiv1.ResourceGroup, gracePeriod time.Duration) error {
	log := logf.FromContext(ctx).WithValues("rg", rg.Name)

	if _, hasAnnotation := rg.Annotations[apiv1.AnnotationRGRebalancePending]; hasAnnotation {
		// Operator intent: ignore per-RD shortfall grace and the
		// per-node offline grace — a REST modify means "act now".
		count, perr := r.rebalanceChildRDs(ctx, rg, 0)
		if perr != nil {
			return perr
		}

		log.Info("rebalance pass complete (annotation-driven)", "rds_processed", count)

		return r.stripRebalanceAnnotation(ctx, rg)
	}

	// Scenario 2.15 scheduled tick — only fires when no Node is
	// inside its grace window. The placer's idempotent already-placed
	// accounting keeps a clean cluster a no-op.
	if r.graceGateActive(gracePeriod) {
		return nil
	}

	count, perr := r.rebalanceChildRDs(ctx, rg, gracePeriod)
	if perr != nil {
		return perr
	}

	if count > 0 {
		log.V(1).Info("rebalance pass complete (scheduled)", "rds_processed", count)
	}

	return nil
}

// rebalanceChildRDs walks every RD that points at this RG and runs
// the additive placer with the RG's current SelectFilter. Errors on
// individual RDs are logged and accumulated as a soft skip — the
// reconciler still strips the annotation so a permanently-broken RD
// doesn't block the others. (The dedicated RD-level reconcilers
// surface that breakage on their own paths.)
//
// `shortfallGrace` is the per-RD scenario-2.20 window: an RD whose
// `blockstor.io/spawn-shortfall` stamp is younger than this duration
// is skipped (the dedicated replay path owns the retry, gated by the
// same window). Pass 0 to disable the gate — the annotation-driven
// path does this so an operator's `linstor rg modify` acts on every
// child RD immediately.
func (r *RGRebalanceReconciler) rebalanceChildRDs(ctx context.Context, rg *apiv1.ResourceGroup, shortfallGrace time.Duration) (int, error) {
	log := logf.FromContext(ctx)

	rds, err := r.Store.ResourceDefinitions().List(ctx)
	if err != nil {
		return 0, err
	}

	p := placer.New(r.Store)
	processed := 0
	now := r.now()

	for i := range rds {
		if rds[i].ResourceGroupName != rg.Name {
			continue
		}

		if shortfallGrace > 0 && r.shortfallWithinGrace(&rds[i], now, shortfallGrace) {
			log.V(1).Info("scheduled rebalance skipped (shortfall grace active)",
				"rg", rg.Name, "rd", rds[i].Name)

			continue
		}

		filter := rg.SelectFilter
		if filter.PlaceCount == 0 {
			filter.PlaceCount = 1
		}

		_, _, perr := p.Place(ctx, rds[i].Name, &filter)
		if perr != nil {
			log.Error(perr, "placer call failed during RG rebalance",
				"rg", rg.Name, "rd", rds[i].Name)
		}

		processed++
	}

	return processed, nil
}

// shortfallWithinGrace reports whether an RD carries a still-fresh
// spawn-shortfall annotation. A malformed / missing stamp falls
// through to "no grace active" so the rebalance pass continues to
// behave like the pre-2.20 path on RDs that never touched the new
// annotation.
func (r *RGRebalanceReconciler) shortfallWithinGrace(rd *apiv1.ResourceDefinition, now time.Time, grace time.Duration) bool {
	stamp, ok := rd.Annotations[apiv1.RDSpawnShortfallAnnotation]
	if !ok {
		return false
	}

	stampedAt, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		return false
	}

	return now.Sub(stampedAt) < grace
}

// replaySpawnShortfalls handles scenario 2.20. Walks every RD in the
// parent RG, looks for the `blockstor.io/spawn-shortfall` annotation,
// and once the stamped wall-clock + grace window has elapsed re-runs
// the additive placer. The annotation is stripped on success so a
// subsequent tick is a clean no-op; on continued failure the marker
// survives and a later tick retries.
func (r *RGRebalanceReconciler) replaySpawnShortfalls(ctx context.Context, rg *apiv1.ResourceGroup, grace time.Duration) error {
	rds, err := r.Store.ResourceDefinitions().List(ctx)
	if err != nil {
		return err
	}

	p := placer.New(r.Store)

	for i := range rds {
		if rds[i].ResourceGroupName != rg.Name {
			continue
		}

		if stripErr := r.replayOneShortfall(ctx, p, rg, &rds[i], grace); stripErr != nil {
			return stripErr
		}
	}

	return nil
}

// replayOneShortfall processes a single shortfall-stamped RD. Returns
// a non-nil error only when the strip-annotation write fails — the
// "still under-placed" / "grace window not elapsed" branches are
// soft-skips that retain the annotation for the next tick.
func (r *RGRebalanceReconciler) replayOneShortfall(ctx context.Context, p *placer.Placer, rg *apiv1.ResourceGroup, rd *apiv1.ResourceDefinition, grace time.Duration) error {
	log := logf.FromContext(ctx)

	stamp, ok := rd.Annotations[apiv1.RDSpawnShortfallAnnotation]
	if !ok {
		return nil
	}

	stampedAt, parseErr := time.Parse(time.RFC3339, stamp)
	if parseErr != nil {
		// Unparseable stamp = treat as "stamped infinitely far in
		// the past" so the retry fires immediately rather than
		// jamming on a malformed annotation forever.
		stampedAt = time.Time{}
	}

	now := r.now()

	if !stampedAt.IsZero() && now.Sub(stampedAt) < grace {
		log.V(1).Info("spawn-shortfall replay deferred (grace window)",
			"rd", rd.Name,
			"age", now.Sub(stampedAt))

		return nil
	}

	filter := rg.SelectFilter
	if filter.PlaceCount == 0 {
		filter.PlaceCount = 1
	}

	placed, want, perr := p.Place(ctx, rd.Name, &filter)
	if perr != nil {
		log.Error(perr, "spawn-shortfall replay failed",
			"rd", rd.Name, "placed", placed, "want", want)

		return nil
	}

	if placed < want {
		log.V(1).Info("spawn-shortfall still under-placed; retaining marker",
			"rd", rd.Name, "placed", placed, "want", want)

		return nil
	}

	log.Info("spawn-shortfall replay succeeded", "rd", rd.Name, "placed", placed)

	return r.stripShortfallAnnotation(ctx, rd)
}

// stripShortfallAnnotation removes the spawn-shortfall marker from an
// RD and persists the change. Mirrors stripRebalanceAnnotation in
// shape so both annotation lifecycles round-trip identically through
// the store path.
func (r *RGRebalanceReconciler) stripShortfallAnnotation(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	if _, ok := rd.Annotations[apiv1.RDSpawnShortfallAnnotation]; !ok {
		return nil
	}

	delete(rd.Annotations, apiv1.RDSpawnShortfallAnnotation)

	if len(rd.Annotations) == 0 {
		rd.Annotations = nil
	}

	return r.Store.ResourceDefinitions().Update(ctx, rd)
}

// stripRebalanceAnnotation removes the rebalance-pending marker from
// the RG and persists the change. Idempotent: a re-run after a
// successful strip is a no-op because the annotation is already
// gone.
func (r *RGRebalanceReconciler) stripRebalanceAnnotation(ctx context.Context, rg *apiv1.ResourceGroup) error {
	if _, ok := rg.Annotations[apiv1.AnnotationRGRebalancePending]; !ok {
		return nil
	}

	delete(rg.Annotations, apiv1.AnnotationRGRebalancePending)

	// Nil-out an empty map so the wire envelope round-trips as the
	// pre-Bug-60 shape — round-trip stability matters for the JSON
	// goldens that pin the RG payload.
	if len(rg.Annotations) == 0 {
		rg.Annotations = nil
	}

	return r.Store.ResourceGroups().Update(ctx, rg)
}

// resolveTuning resolves the BalanceResources Interval + GracePeriod
// against the controller-scope props bag, falling back to package
// defaults. Per-RG / per-RD overrides land in a follow-up — the
// controller-scope read is enough to wire the scheduled cadence + the
// node-flap grace window, and matches the resolution stub used by the
// 2.W02 kill-switch.
func (r *RGRebalanceReconciler) resolveTuning(ctx context.Context) (time.Duration, time.Duration, error) {
	interval := time.Duration(apiv1.DefaultBalanceResourcesIntervalMinutes) * time.Minute
	grace := time.Duration(apiv1.DefaultBalanceResourcesGracePeriodMinutes) * time.Minute

	props, err := r.Store.ControllerProps().Get(ctx)
	if err != nil {
		return interval, grace, err
	}

	if m, ok := parsePositiveMinutes(props[apiv1.PropBalanceResourcesInterval]); ok {
		interval = time.Duration(m) * time.Minute
	}

	if m, ok := parsePositiveMinutes(props[apiv1.PropBalanceResourcesGracePeriod]); ok {
		grace = time.Duration(m) * time.Minute
	}

	return interval, grace, nil
}

// refreshOfflineTracker reconciles the in-memory offline-since map
// against the current Node list. Newly-OFFLINE nodes get stamped with
// `now`; nodes that came back ONLINE are dropped. The tracker is
// process-local — a controller restart clears every entry, which is
// the conservative behaviour (the freshly-started reconciler waits an
// additional GracePeriod before churning anything).
func (r *RGRebalanceReconciler) refreshOfflineTracker(ctx context.Context) {
	nodes, err := r.Store.Nodes().List(ctx)
	if err != nil {
		// Soft-fail: a single failed list shouldn't suppress every
		// future scheduled tick. The grace gate stays open until the
		// next successful refresh, at which point any genuine
		// offline-since stamp is re-applied.
		return
	}

	now := r.now()

	r.nodeOfflineSinceMu.Lock()
	defer r.nodeOfflineSinceMu.Unlock()

	if r.nodeOfflineSince == nil {
		r.nodeOfflineSince = map[string]time.Time{}
	}

	seen := map[string]bool{}

	for i := range nodes {
		seen[nodes[i].Name] = true

		offline := nodes[i].ConnectionStatus == apiv1.NodeTypeOffline
		if !offline {
			delete(r.nodeOfflineSince, nodes[i].Name)

			continue
		}

		if _, already := r.nodeOfflineSince[nodes[i].Name]; !already {
			r.nodeOfflineSince[nodes[i].Name] = now
		}
	}

	// Drop entries for nodes that have been removed from the store
	// entirely — otherwise a deleted-and-re-created node would
	// inherit a stale stamp from before its delete.
	for name := range r.nodeOfflineSince {
		if !seen[name] {
			delete(r.nodeOfflineSince, name)
		}
	}
}

// graceGateActive returns true when at least one tracked node has
// been offline for strictly less than `grace`. The first-tick stamp
// (in refreshOfflineTracker) is `now`, so on the same tick a freshly
// observed offline node has age 0 — well inside any positive window.
func (r *RGRebalanceReconciler) graceGateActive(grace time.Duration) bool {
	if grace <= 0 {
		return false
	}

	now := r.now()

	r.nodeOfflineSinceMu.Lock()
	defer r.nodeOfflineSinceMu.Unlock()

	for _, since := range r.nodeOfflineSince {
		if now.Sub(since) < grace {
			return true
		}
	}

	return false
}

// now defers to the injected clock or wall time. The injection point
// is the test seam — production never sets Now.
func (r *RGRebalanceReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

// balanceResourcesDisabled returns true when the controller-scope
// `BalanceResourcesEnabled` prop is explicitly set to "false". Any
// other value (including missing, empty, or a typo) keeps the
// rebalance reconciler armed — matches upstream LINSTOR's "default
// = enabled" semantics so an unconfigured cluster behaves the same
// way it did before scenario 2.W02 landed.
func balanceResourcesDisabled(ctx context.Context, st store.Store) bool {
	if st == nil {
		return false
	}

	props, err := st.ControllerProps().Get(ctx)
	if err != nil || props == nil {
		// A read error must not silently disable the reconciler — we
		// fail OPEN, the operator still has the annotation gate + the
		// RG-level CRD as throttling layers. The error itself will be
		// surfaced on the next placer call when it hits the same row.
		return false
	}

	return props[apiv1.PropBalanceResourcesEnabled] == "false"
}

// SetupWithManager wires the reconciler into the manager. We share
// the RG CRD watch with ResourceGroupReconciler — controller-runtime
// supports multiple controllers on the same `For` resource as long
// as each has a distinct Named ID.
func (r *RGRebalanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.ResourceGroup{}).
		Named("rg-rebalance").
		Complete(r)
}
