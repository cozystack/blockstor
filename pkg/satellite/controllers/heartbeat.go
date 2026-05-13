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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// HeartbeatPeriod is the cadence at which the satellite stamps
// LastHeartbeatTime on its Node CRD. Mirrors kubelet's
// `--node-status-update-frequency=10s` default — short enough that
// the controller's watchdog can flip a dead node within one grace
// period, long enough that the apiserver doesn't sink under the
// write load on a real cluster.
const HeartbeatPeriod = 10 * time.Second

// HeartbeatFieldOwner is the SSA field-manager the heartbeat
// runnable uses. Distinct from the satellite-reconciler owner so
// `kubectl get node.blockstor -o yaml` cleanly attributes Status
// writes between "heartbeat" and "reconcile" managers.
const HeartbeatFieldOwner = "blockstor-satellite-heartbeat"

// UnregisteredLogEvery throttles how often the satellite re-logs
// the "Node CRD missing" actionable error. The first observation
// after a successful (or fresh-start) heartbeat is always logged
// at ERROR; subsequent consecutive misses fall through to a
// re-log every UnregisteredLogEvery ticks so the operator sees a
// breadcrumb without log-spamming a satellite that's been
// orphaned for hours.
const UnregisteredLogEvery = 60

// nodeMissingHint is the actionable message emitted to the
// satellite log when the Node CRD does not exist. Operators who
// previously ran `linstor node lost <name>` (or whose teardown
// dropped the Node CRD) must explicitly re-register the node —
// blockstor deliberately mirrors upstream LINSTOR's "lost is
// final" semantic and does NOT auto-resurrect the Node CRD just
// because a satellite pod restarted on the same host.
//
// Exported so tests pin the exact text the operator sees.
const nodeMissingHint = "Node CRD missing for this satellite; " +
	"the controller treats this as 'node lost' and will not auto-create it. " +
	"Re-register the node via `linstor node create <name> <ip>` " +
	"(or apply the matching Node CRD) to bring the satellite back online."

// HeartbeatRunnable is a controller-runtime Runnable that keeps
// the local satellite's Node CRD Status fresh:
//
//   - On every tick (HeartbeatPeriod) it SSA-Applies
//     Node.Status.LastHeartbeatTime = now() AND a `Ready=True`
//     Condition with LastTransitionTime preserved across same-status
//     updates (k8s convention — the apiserver does the preserve).
//   - The controller-side watchdog (internal/controller.NodeHeartbeatReconciler)
//     reads LastHeartbeatTime and flips Ready to Unknown when stale.
//
// One Runnable per satellite pod, NodeName is the local node it
// owns. Picks up on satellite startup, exits with the manager
// context.
//
// Design — "Node CRD missing" handling (Bug 20):
//
// When the Node CRD this satellite stamps against does not exist
// (e.g. an operator ran `linstor node lost <name>` and the
// DaemonSet then respawned the satellite pod), the runnable does
// NOT auto-create the Node CRD. Mirrors upstream LINSTOR's
// "lost is final" semantic — auto-resurrecting on a satellite
// restart would silently undo a deliberate operator action and
// surprise everyone relying on `node lost` to actually evict a
// node. Instead the runnable logs an actionable ERROR (see
// nodeMissingHint) on the first observation and re-logs at
// UnregisteredLogEvery cadence so the situation cannot be
// silently lost. The satellite keeps ticking — the operator may
// re-register the node at any time and the next stamp will
// succeed without restarting the pod.
type HeartbeatRunnable struct {
	Client   client.Client
	NodeName string
	Period   time.Duration

	// missCount tracks consecutive NotFound observations. Reset
	// on the first successful stamp. Internal — exposed only so
	// the controller-package tests can pin the rate-limit behaviour
	// without needing to assert on log output.
	missCount int
}

// NeedLeaderElection returns false — satellites are per-node and
// stamp THEIR OWN Node CRD; leader election would pick one pod to
// stamp every node which is wrong.
func (*HeartbeatRunnable) NeedLeaderElection() bool { return false }

// Start runs the heartbeat loop until ctx cancels. Errors during
// individual stamps are logged but never abort the loop — a
// transient apiserver hiccup must not take the satellite out of
// service. The first stamp fires immediately (without waiting a
// full period) so a satellite that just started is visible to the
// controller within seconds rather than HeartbeatPeriod.
func (h *HeartbeatRunnable) Start(ctx context.Context) error {
	period := h.Period
	if period == 0 {
		period = HeartbeatPeriod
	}

	logger := log.FromContext(ctx).WithValues("node", h.NodeName)

	err := h.stamp(ctx, logger)
	if err != nil {
		logger.Error(err, "initial heartbeat stamp")
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err = h.stamp(ctx, logger)
			if err != nil {
				logger.Error(err, "heartbeat stamp")
			}
		}
	}
}

// RegisterWithManager adds the runnable to mgr. Used by NewManager
// to wire the heartbeat alongside the per-CRD reconcilers + the
// events2 observer.
func (h *HeartbeatRunnable) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(h)
	if err != nil {
		return errors.Wrap(err, "add HeartbeatRunnable")
	}

	return nil
}

// stamp SSA-applies the current heartbeat onto the satellite's Node CRD.
//
// The Node CRD must already exist — it is the operator's job to
// create one Node per cluster member (the dev stand's
// install-blockstor.sh bootstraps them from k8s worker nodes).
//
// Missing-Node is NOT fatal and is NOT auto-resurrected: it is
// the documented signal that an operator ran `linstor node lost`
// (or the equivalent CRD delete). The satellite logs an
// actionable ERROR on the first observation, throttles re-logs
// at UnregisteredLogEvery, and resets the counter once the Node
// CRD reappears. The loop keeps ticking so re-registering the
// node never requires a satellite pod restart.
func (h *HeartbeatRunnable) stamp(ctx context.Context, logger logr.Logger) error {
	now := metav1.NewTime(time.Now())

	apply := &blockstoriov1alpha1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Node",
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: h.NodeName},
		Status: blockstoriov1alpha1.NodeStatus{
			LastHeartbeatTime: &now,
			Conditions: []metav1.Condition{
				{
					Type:               blockstoriov1alpha1.NodeConditionReady,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
					Reason:             "SatelliteHeartbeat",
					Message:            "satellite is reconciling normally",
				},
			},
		},
	}

	err := h.Client.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		client.FieldOwner(HeartbeatFieldOwner),
		client.ForceOwnership)
	if err != nil {
		if apierrors.IsNotFound(err) {
			h.logNodeMissing(logger)

			return nil
		}

		// Reset on any other error so a transient apiserver
		// hiccup doesn't burn through the throttle window.
		h.missCount = 0

		return errors.Wrapf(err, "ssa heartbeat for node %q", h.NodeName)
	}

	// Successful stamp — if we just came back from a missing-Node
	// window, surface the recovery so the operator's log story
	// matches what they saw on the way in.
	if h.missCount > 0 {
		logger.Info("Node CRD reappeared; heartbeat resumed",
			"prior_misses", h.missCount)

		h.missCount = 0
	}

	return nil
}

// logNodeMissing emits the actionable "Node CRD missing" error
// the first time it's observed after a healthy stamp, then
// throttles re-logs at UnregisteredLogEvery so a long outage
// doesn't drown the log. Always increments missCount so the
// recovery log on the way out can report how long the window
// lasted.
func (h *HeartbeatRunnable) logNodeMissing(logger logr.Logger) {
	if h.missCount == 0 || h.missCount%UnregisteredLogEvery == 0 {
		logger.Error(errors.New(nodeMissingHint),
			"satellite refusing to auto-register; operator action required",
			"node", h.NodeName,
			"consecutive_misses", h.missCount+1)
	}

	h.missCount++
}
