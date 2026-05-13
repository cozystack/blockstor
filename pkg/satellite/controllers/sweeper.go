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
// on this node. Five minutes is the same ballpark as kubelet's
// volume-GC cycle: long enough to avoid hammering drbdsetup on a
// healthy cluster, short enough that a force-strip-leftover (the
// failure mode this exists to catch — see the
// `blockstor_drbd_stuck_state` recovery skill) clears within one
// HPA / human-attention window.
const SweeperPeriod = 5 * time.Minute

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
}

// NeedLeaderElection returns false. Every satellite must run its
// own sweeper — leader election would pick one pod to sweep every
// node, which is wrong (one node's kernel state is opaque to
// another node's satellite).
func (*OrphanSweeperRunnable) NeedLeaderElection() bool { return false }

// Start runs the sweep loop until ctx cancels. Surface errors are
// logged but never abort the loop — a transient drbdsetup hiccup
// or apiserver blip must not take the satellite out of service.
// The first sweep waits one period before firing: on satellite
// startup the c-r cache hasn't yet warmed (the four reconcilers
// are still flushing initial CRD events) and a sweep against a
// half-populated cache would mistake every legitimate resource
// for an orphan and tear them all down.
func (s *OrphanSweeperRunnable) Start(ctx context.Context) error {
	period := s.Period
	if period == 0 {
		period = SweeperPeriod
	}

	logger := log.FromContext(ctx).WithName("orphan-sweeper").WithValues("node", s.NodeName)

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
func (s *OrphanSweeperRunnable) sweepOnce(ctx context.Context, logger logr.Logger) error {
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

	limit := s.MaxDownPerCycle
	if limit == 0 {
		limit = SweeperMaxDownPerCycle
	}

	var torn int

	for _, rsc := range kernel {
		if _, ok := owned[rsc]; ok {
			continue
		}

		if limit >= 0 && torn >= limit {
			logger.Info("orphan sweep rate-limit hit; deferring remainder to next cycle",
				"limit", limit, "kernel_total", len(kernel))

			return nil
		}

		logger.Info("orphan DRBD resource detected; running drbdadm down",
			"resource", rsc)

		downErr := s.Adm.Down(ctx, rsc)
		if downErr != nil {
			// Per-resource failure shouldn't abort the cycle —
			// move on to the next orphan and let the next tick
			// retry the failure. We don't increment `torn` so the
			// rate-limit budget still reflects successful tear-downs.
			logger.Error(downErr, "drbdadm down on orphan", "resource", rsc)

			continue
		}

		torn++
	}

	return nil
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

// Compile-time check that the runnable satisfies the contract.
var _ manager.Runnable = (*OrphanSweeperRunnable)(nil)
