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
type HeartbeatRunnable struct {
	Client   client.Client
	NodeName string
	Period   time.Duration
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

	err := h.stamp(ctx)
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
			err = h.stamp(ctx)
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
// Missing-Node is logged but not an error: re-runs once it lands.
func (h *HeartbeatRunnable) stamp(ctx context.Context) error {
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
			// Operator hasn't registered the Node CRD yet. Not fatal —
			// keep ticking, we'll succeed once it lands.
			return nil
		}

		return errors.Wrapf(err, "ssa heartbeat for node %q", h.NodeName)
	}

	return nil
}
