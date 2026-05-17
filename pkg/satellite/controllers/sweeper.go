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

package controllers

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// SweeperPeriod is the default cadence at which the orphan sweeper
// reconciles kernel-resident DRBD resources against Resource CRDs
// on this node.
//
// Bug 290 (P1): the previous 5-minute cadence proved far too long
// once a single orphan kernel slot reliably blocks every new RD
// that would re-use its minor. With `drbdsetup new-minor` failing
// on a reconcile-tight feedback loop ("Minor or volume exists
// already" / "Device '<minor>' is configured!"), the kubelet-side
// PVC binding and the e2e harness's `wait_uptodate` both time out
// long before the 5-minute window elapses. 30 s keeps
// drbdsetup-status traffic negligible on a healthy cluster
// (~one call/min per satellite, plus the immediate-on-start
// sweep) while shrinking post-strip recovery latency to a window
// an e2e run or an interactive operator can tolerate. The
// SweeperRDGrace window (60s) still protects against
// create/delete-fanout races (Bug 291) — the grace check fires
// against the RD timestamp, not the tick cadence.
const SweeperPeriod = 30 * time.Second

// SweeperSetupDownTimeout bounds the per-resource `drbdsetup down`
// call the sweeper issues against an orphan.
//
// Bug 290 (P1): without this bound, `sweepOnce` passed its whole
// tick context — `context.WithTimeout(parent, SweeperPeriod)` —
// straight into `Adm.SetupDown`, so a SINGLE wedged kernel slot
// (the DRBD-stuck-state pattern from
// `blockstor_drbd_stuck_state`, where `drbdsetup down` hangs
// forever on a netlink op against a gone peer) consumed the
// entire tick budget. The sweeper then made zero forward progress
// on any OTHER orphan, and on the next tick the same wedged slot
// re-burned the next tick's budget the same way. Observed on
// e2e7-worker-1 Run 7 (pv-test minor 1000 leaked + drbd_w_pv-test
// kernel thread stuck on dead peer) where the 5-min ctx kill
// fired every cycle and the slot blocked every fresh 2-replica
// RD from reaching UpToDate.
//
// 10 s is generous for a healthy `drbdsetup down` (finishes in
// well under a second) and short enough that the sweeper still
// scans many orphans within one tick + the per-cycle rate-limit.
// The per-call ctx is derived from the tick ctx so a tick
// cancellation still aborts the in-flight call promptly.
//
// Note: this does NOT recover the kernel-stuck state itself —
// that requires a node reboot (the kernel thread is stuck inside
// netlink, beyond userspace's reach). The bound exists so the
// sweeper doesn't compound the kernel-stuck state by ALSO making
// the userspace cleanup loop unresponsive to other orphans.
const SweeperSetupDownTimeout = 10 * time.Second

// SweeperMaxDownPerCycle bounds the number of `drbdadm down` calls
// the sweeper issues per tick. The whole point of the sweeper is
// catching the slow-trickle aftermath of a bad delete; if a bug
// upstream of us starts producing 50 orphans per tick the sweeper
// should NOT panic and tear them all down at once — that would
// mask the real bug and could lose in-flight I/O on resources that
// only LOOK orphaned because the apiserver cache was stale.
//
// Three per cycle = ~36/hour with the default 5-min cadence. That
// is plenty for the steady-state cleanup load this is designed for
// (one or two stuck resources after an operator force-deleted a
// Resource CRD without waiting for the finalizer) and far under
// any plausible production resource count, so a true orphan storm
// stays visible in logs without a self-inflicted outage.
const SweeperMaxDownPerCycle = 3

// SweeperRDGrace bounds how recently a matching ResourceDefinition
// may have been created or marked for deletion before the sweeper
// will tear down its kernel slot.
//
// Bug 291 (P1): the sweeper's "no local Resource CRD ⇒ orphan"
// rule races the controller's create / delete propagation. When
// `linstor rd create` lands, the controller writes the
// ResourceDefinition first, then the per-node Resource CRDs in a
// follow-up reconcile pass; the satellite reconciler may have
// already issued `drbdadm up` on this node (because the .res file
// was written eagerly) before the matching Resource CRD lands in
// the cache the sweeper consults. A sweep tick that fires in that
// window sees a kernel slot with no local CRD and tears it down —
// the next reconciler pass then has to bootstrap the slot from
// scratch, and on a busy stand the slot never converges before
// the e2e `wait_uptodate` budget elapses. Mirror-image race on
// delete: kubectl deletes the RD, kernel slot survives by design
// (CRD finalizers gate teardown), but the cached Resource CRDs
// drop out of the list before the satellite's DeleteResource
// finishes — the sweeper sees "orphan" and tears down the slot
// the satellite was still cleaning up, leaking partial state.
//
// 60s covers both: the controller's create-fanout reliably
// completes within ~5s on a healthy apiserver, and the
// satellite's DeleteResource bounds itself to ~30s per resource
// even on the slowest path. Anything still orphaned after 60s is
// the genuine force-strip aftermath this code exists for.
const SweeperRDGrace = 60 * time.Second

// SweeperSkipAnnotation is the annotation key the sweeper honours
// on the LOCAL Node CRD (NOT the Resource CRD — orphans by
// definition have no CRD). Setting it to "true" disables the
// sweeper on this node entirely, which is the documented escape
// hatch for the Bug 4 scenario where an operator needs the
// kernel-side `drbdsetup down` to NOT run while they are doing
// manual recovery (e.g. exporting GI / bitmap state out of a
// "stuck" resource that has no CRD but is intentionally being
// preserved). Without this opt-out, an operator who deleted the
// CRD as part of recovery would race the sweeper and lose the
// kernel-state evidence they wanted to preserve.
const SweeperSkipAnnotation = "blockstor.io/skip-orphan-sweeper"

// sweeperSkipValue is the annotation value that turns the skip
// switch on. Kept as a named constant rather than the bare
// "true" literal so the test fixture and the production guard
// share one source of truth (goconst).
const sweeperSkipValue = "true"

// OrphanSweeperRunnable is a controller-runtime Runnable that
// periodically reconciles kernel-resident DRBD resources against
// Resource CRDs placed on this satellite's node and tears down
// the strays.
//
// Closes the "force-strip aftermath" loop documented in
// `blockstor_drbd_stuck_state`: when an operator force-deletes a
// Resource CRD (bypassing the satellite finalizer), the
// satellite's `handleDelete` never fires and `drbdadm down`
// never runs, leaving the kernel resource in a half-alive state
// that survives satellite restarts. The sweeper picks up the
// pieces.
type OrphanSweeperRunnable struct {
	Client   client.Client
	Adm      *drbd.Adm
	NodeName string

	// Period overrides SweeperPeriod (test-only — production uses
	// the default constant). A zero Period falls back to
	// SweeperPeriod.
	Period time.Duration

	// MaxDownPerCycle overrides SweeperMaxDownPerCycle (test-only).
	// A zero value falls back to SweeperMaxDownPerCycle. A negative
	// value disables the rate-limit entirely (useful in tests that
	// want to assert the full-list behaviour without juggling the
	// bound).
	MaxDownPerCycle int

	// RDGrace overrides SweeperRDGrace (test-only). A zero value
	// falls back to SweeperRDGrace; a negative value disables the
	// grace window entirely (legacy behaviour, useful in tests
	// that assert the immediate-tear-down path).
	RDGrace time.Duration

	// SetupDownTimeout overrides SweeperSetupDownTimeout (test-only).
	// A zero value falls back to SweeperSetupDownTimeout; a negative
	// value disables the per-resource bound entirely (only used by
	// tests that want to assert the pre-Bug-290 unbounded behaviour
	// from a regression-guard angle).
	SetupDownTimeout time.Duration

	// now returns the current time; pluggable for tests so the
	// grace-window assertions don't have to juggle real-time
	// sleeps. Defaults to time.Now when unset.
	now func() time.Time

	// setupDownFn is a test-only hook for the per-orphan
	// `drbdsetup down` call. Defaults to s.Adm.SetupDown when
	// unset. Tests use it to capture the per-resource ctx deadline
	// (asserting the SweeperSetupDownTimeout bound is applied) and
	// to simulate the DRBD-stuck-state hang without needing a real
	// drbdsetup process. Production callers must leave this nil.
	setupDownFn func(ctx context.Context, resource string) error
}

// NeedLeaderElection returns false. Every satellite must run its
// own sweeper — leader election would pick one pod to sweep every
// node, which is wrong (one node's kernel state is opaque to
// another node's satellite).
func (*OrphanSweeperRunnable) NeedLeaderElection() bool { return false }

// Start runs the sweep loop until ctx cancels. Surface errors are
// logged but never abort the loop — a transient drbdsetup hiccup
// or apiserver blip must not take the satellite out of service.
//
// Bug 290 (P1): the first sweep fires IMMEDIATELY rather than
// waiting one full period. controller-runtime starts the non-
// leader-election Runnables (which we are) only after the cache
// has completed its initial sync (see
// `runnables.Caches.Start` → `runnables.Others.Start` in c-r
// 0.23.3 internal.go), so the previous "wait for cache to warm"
// rationale no longer applies. On a satellite restart that left a
// leaked kernel slot behind (force-strip pattern from
// `blockstor_drbd_stuck_state`), every extra second the slot
// lingers is a second the next RD wanting its minor stays
// Inconsistent / fails create-md. Sweeping once on Start clears
// that within the first second of pod readiness instead of after
// one tick.
func (s *OrphanSweeperRunnable) Start(ctx context.Context) error {
	period := s.Period
	if period == 0 {
		period = SweeperPeriod
	}

	logger := log.FromContext(ctx).WithName("orphan-sweeper").WithValues("node", s.NodeName)

	// Bug 290: immediate sweep on Start. controller-runtime has
	// already waited for cache sync before calling us, so the
	// orphan classification cannot mis-fire against a half-warm
	// cache. ctx propagation in sweepOnce ensures a shutdown still
	// aborts any in-flight drbdsetup call.
	err := s.sweepOnce(ctx, logger)
	if err != nil {
		logger.Error(err, "initial sweep")
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := s.sweepOnce(ctx, logger)
			if err != nil {
				logger.Error(err, "sweep cycle")
			}
		}
	}
}

// RegisterWithManager adds the sweeper to mgr alongside the
// per-CRD reconcilers + observer + heartbeat.
func (s *OrphanSweeperRunnable) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(s)
	if err != nil {
		return errors.Wrap(err, "add OrphanSweeperRunnable")
	}

	return nil
}

// sweepOnce performs exactly one reconcile cycle: list kernel
// resources, list Resource CRDs on this node, diff, and `drbdadm
// down` the strays subject to the per-cycle bound.
//
// Exposed (lowercase, package-internal) for the unit tests so a
// table-driven run can pin one tick's behaviour against canned
// kernel + CRD state without juggling a ticker.
//
// Bug 275 (P1): per-tick deadline so the sweeper that's supposed to
// recover from DRBD-stuck-state isn't itself stuck by it. The budget
// is the sweep period — a tick that exceeds its own period would
// pile up behind the next ticker fire anyway, so cancelling at the
// period boundary keeps the loop in lock-step with the schedule.
func (s *OrphanSweeperRunnable) sweepOnce(ctx context.Context, logger logr.Logger) error {
	period := s.Period
	if period == 0 {
		period = SweeperPeriod
	}

	ctx, cancel := context.WithTimeout(ctx, period)
	defer cancel()

	skip, err := s.shouldSkip(ctx)
	if err != nil {
		// Read failures on the Node CRD shouldn't block the sweep —
		// fall back to "not skipped" so a transient apiserver blip
		// doesn't silently disable the safety net. Log so operators
		// notice if it's persistent.
		logger.Error(err, "check skip-sweeper annotation; proceeding without skip")
	} else if skip {
		logger.V(1).Info("sweep skipped by Node annotation", "annotation", SweeperSkipAnnotation)

		return nil
	}

	kernel, err := s.Adm.StatusResources(ctx)
	if err != nil {
		return errors.Wrap(err, "list kernel resources")
	}

	if len(kernel) == 0 {
		return nil
	}

	owned, err := s.listOwnedResourceNames(ctx)
	if err != nil {
		return errors.Wrap(err, "list local Resource CRDs")
	}

	rdAges, err := s.listResourceDefinitionAges(ctx)
	if err != nil {
		// RD-read failures shouldn't abort the sweep — fall back to
		// "no grace window" so a transient apiserver blip doesn't
		// silently disable the safety net. The pre-grace behaviour
		// is the safe-side default: at worst the sweeper tears down
		// a slot that the reconciler then rebuilds, which is a
		// recoverable loss.
		logger.Error(err, "list ResourceDefinitions for grace window; proceeding without grace")

		rdAges = nil
	}

	s.tearDownOrphans(ctx, logger, kernel, owned, rdAges)

	return nil
}

// sweepDecision is the per-orphan-candidate outcome from
// classifyOrphan: whether to skip (matching CRD), defer (inside
// RD grace window), or tear it down.
type sweepDecision int

const (
	sweepKeep sweepDecision = iota
	sweepDefer
	sweepTearDown
)

// classifyOrphan decides what to do with one kernel resource: keep
// (matching local Resource CRD), defer (no CRD but the RD was
// touched inside the grace window), or tear it down. Pulled out of
// sweepOnce to bring the orchestration function back under the
// gocyclo budget after the Bug 291 grace-window addition.
func classifyOrphan(rsc string, owned map[string]struct{}, rdAges map[string]time.Time, now time.Time, grace time.Duration) (sweepDecision, time.Duration) {
	if _, ok := owned[rsc]; ok {
		return sweepKeep, 0
	}

	if grace <= 0 {
		return sweepTearDown, 0
	}

	anchor, ok := rdAges[rsc]
	if !ok {
		return sweepTearDown, 0
	}

	age := now.Sub(anchor)
	if age < grace {
		return sweepDefer, age
	}

	return sweepTearDown, age
}

// tearDownOrphans iterates the kernel-resource list and applies the
// classifyOrphan decision to each, subject to the per-cycle
// rate-limit. Pulled out of sweepOnce for gocyclo budget reasons.
func (s *OrphanSweeperRunnable) tearDownOrphans(ctx context.Context, logger logr.Logger, kernel []string, owned map[string]struct{}, rdAges map[string]time.Time) {
	limit := s.MaxDownPerCycle
	if limit == 0 {
		limit = SweeperMaxDownPerCycle
	}

	grace := s.RDGrace
	if grace == 0 {
		grace = SweeperRDGrace
	}

	clock := s.now
	if clock == nil {
		clock = time.Now
	}

	now := clock()

	var torn int

	for _, rsc := range kernel {
		decision, age := classifyOrphan(rsc, owned, rdAges, now, grace)

		switch decision {
		case sweepKeep:
			continue
		case sweepDefer:
			logger.V(1).Info("orphan candidate within RD grace window; deferring",
				"resource", rsc,
				"age", age,
				"grace", grace)

			continue
		case sweepTearDown:
			// fall through to teardown below
		}

		if limit >= 0 && torn >= limit {
			logger.Info("orphan sweep rate-limit hit; deferring remainder to next cycle",
				"limit", limit, "kernel_total", len(kernel))

			return
		}

		// Use `drbdsetup down` (kernel-direct) rather than
		// `drbdadm down` (which needs the .res file to enumerate
		// the resource). See issue 288 for the full rationale.
		// Bug 290: bound the per-call to SweeperSetupDownTimeout
		// so a single wedged slot can't burn the whole tick.
		logger.Info("orphan DRBD resource detected; running drbdsetup down",
			"resource", rsc)

		downErr := s.callSetupDown(ctx, rsc)
		if downErr != nil {
			// Per-resource failure shouldn't abort the cycle —
			// move on to the next orphan and let the next tick
			// retry the failure. We don't increment `torn` so the
			// rate-limit budget still reflects successful tear-downs.
			logger.Error(downErr, "drbdsetup down on orphan", "resource", rsc)

			continue
		}

		torn++
	}
}

// callSetupDown wraps the per-orphan `drbdsetup down` invocation
// with the per-resource bound (SweeperSetupDownTimeout) and the
// test-only setupDownFn hook.
//
// Bug 290 (P1): a single wedged kernel slot used to consume the
// whole sweep tick because the per-orphan call inherited the
// tick's 5-minute ctx. The bound limits each call to
// SweeperSetupDownTimeout (10s by default) so the sweeper keeps
// scanning even when one slot is in the stuck-state pattern from
// `blockstor_drbd_stuck_state`.
//
// A negative SetupDownTimeout disables the bound (test-only — see
// the field doc on OrphanSweeperRunnable).
func (s *OrphanSweeperRunnable) callSetupDown(ctx context.Context, resource string) error {
	timeout := s.SetupDownTimeout
	if timeout == 0 {
		timeout = SweeperSetupDownTimeout
	}

	setupDown := s.setupDownFn
	if setupDown == nil {
		setupDown = s.Adm.SetupDown
	}

	if timeout < 0 {
		return setupDown(ctx, resource)
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return setupDown(callCtx, resource)
}

// shouldSkip checks the local Node CRD for the SweeperSkipAnnotation.
// Returns false if the Node CRD is missing (operator hasn't bootstrapped
// it yet) — we sweep by default on unconfigured nodes too, since
// an unbootstrapped node with kernel-resident DRBD resources is
// itself a bug-state we want cleaned up.
func (s *OrphanSweeperRunnable) shouldSkip(ctx context.Context) (bool, error) {
	var node blockstoriov1alpha1.Node

	err := s.Client.Get(ctx, client.ObjectKey{Name: s.NodeName}, &node)
	if err != nil {
		return false, errors.Wrap(err, "get Node")
	}

	v, ok := node.Annotations[SweeperSkipAnnotation]
	if !ok {
		return false, nil
	}

	return v == sweeperSkipValue, nil
}

// listOwnedResourceNames returns the set of ResourceDefinition
// names this satellite's node owns a Resource for. The kernel
// names DRBD resources by ResourceDefinition (not by Resource
// CRD name which is `<rd>.<node>`), so the set membership check
// in sweepOnce uses RD names.
func (s *OrphanSweeperRunnable) listOwnedResourceNames(ctx context.Context) (map[string]struct{}, error) {
	var list blockstoriov1alpha1.ResourceList

	err := s.Client.List(ctx, &list)
	if err != nil {
		return nil, errors.Wrap(err, "list Resources")
	}

	out := map[string]struct{}{}

	for i := range list.Items {
		r := &list.Items[i]
		if r.Spec.NodeName != s.NodeName {
			continue
		}

		out[r.Spec.ResourceDefinitionName] = struct{}{}
	}

	return out, nil
}

// listResourceDefinitionAges returns a per-RD-name "freshness
// anchor" used by the grace window in sweepOnce. The anchor is
// the most recent of CreationTimestamp and DeletionTimestamp:
// either one means the controller / satellite is mid-fanout and
// the sweeper should defer.
//
// An RD whose status has been stable for longer than the grace
// window is NOT included — those are the steady-state cases
// where any orphan kernel slot is the genuine force-strip
// aftermath the sweeper exists to clean up.
//
// Returning a nil map (no RDs / read failure handled by caller)
// is a valid zero result: callers MUST treat "name absent" as
// "no grace window applies".
func (s *OrphanSweeperRunnable) listResourceDefinitionAges(ctx context.Context) (map[string]time.Time, error) {
	var list blockstoriov1alpha1.ResourceDefinitionList

	err := s.Client.List(ctx, &list)
	if err != nil {
		return nil, errors.Wrap(err, "list ResourceDefinitions")
	}

	out := map[string]time.Time{}

	for i := range list.Items {
		rd := &list.Items[i]

		anchor := rd.CreationTimestamp.Time

		if rd.DeletionTimestamp != nil && rd.DeletionTimestamp.After(anchor) {
			anchor = rd.DeletionTimestamp.Time
		}

		out[rd.Name] = anchor
	}

	return out, nil
}

// Compile-time check that the runnable satisfies the contract.
var _ manager.Runnable = (*OrphanSweeperRunnable)(nil)
