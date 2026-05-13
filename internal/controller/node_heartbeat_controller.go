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
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// Node-monitor knobs — mirror the kube-controller-manager defaults
// so blockstor's satellite-liveness story behaves like kubelet's.
const (
	// NodeMonitorPeriod is how often we re-evaluate every Node's
	// LastHeartbeatTime. Matches `--node-monitor-period=5s`.
	NodeMonitorPeriod = 5 * time.Second

	// NodeMonitorGracePeriod is the staleness window past which a
	// Node's Ready Condition flips to Unknown. Matches
	// `--node-monitor-grace-period=40s`.
	NodeMonitorGracePeriod = 40 * time.Second

	// HeartbeatWatchdogFieldOwner is the SSA manager the watchdog
	// uses when it owns the Ready Condition + ConnectionStatus.
	// Distinct from the satellite-side heartbeat manager so the
	// apiserver merges cleanly: the satellite owns the
	// `Status=True` happy path, the watchdog owns the Unknown
	// flip.
	HeartbeatWatchdogFieldOwner = "blockstor-controller-node-watchdog"
)

// NodeHeartbeatReconciler implements the kube-controller-manager
// `node-monitor` algorithm against blockstor Node CRDs.
//
// On every reconcile it looks at `Status.LastHeartbeatTime` (the
// satellite-side heartbeat runnable stamps it every ~10s):
//
//   - fresh (within NodeMonitorGracePeriod) → ensure Ready=True
//     and `ConnectionStatus=ONLINE`
//   - stale → flip Ready=Unknown, `ConnectionStatus=OFFLINE`
//
// Requeues every NodeMonitorPeriod regardless of state — without
// a periodic re-check we'd never notice a node going stale (the
// satellite stops stamping when the pod dies, so there's no
// apiserver event to drive a reconcile).
type NodeHeartbeatReconciler struct {
	client.Client

	// Now is the time source; tests inject a deterministic clock.
	// Production passes time.Now.
	Now func() time.Time
}

// Reconcile evaluates one Node CRD and reconciles its Ready
// condition / ConnectionStatus against LastHeartbeatTime. Always
// requeues after NodeMonitorPeriod.
func (r *NodeHeartbeatReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("node", req.Name)

	var node blockstoriov1alpha1.Node

	err := r.Get(ctx, req.NamespacedName, &node)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get Node")
	}

	now := r.Now()
	stale := isHeartbeatStale(node.Status.LastHeartbeatTime, now)

	desiredStatus, desiredConn, reason, message := r.desiredState(stale)

	err = r.applyState(ctx, &node, desiredStatus, desiredConn, reason, message, now)
	if err != nil {
		return ctrl.Result{}, err
	}

	if stale {
		logger.Info("satellite heartbeat stale",
			"connectionStatus", desiredConn,
			"lastHeartbeatTime", node.Status.LastHeartbeatTime)
	}

	return ctrl.Result{RequeueAfter: NodeMonitorPeriod}, nil
}

// SetupWithManager wires the watchdog to watch Node CRDs. We also
// suppress the "Status-only update" event noise — a satellite that
// keeps stamping LastHeartbeatTime every 10s would otherwise cause
// a reconcile per stamp. Status-only changes still requeue via
// `RequeueAfter`; we don't need apiserver events to drive freshness
// checks.
func (r *NodeHeartbeatReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}

	err := ctrl.NewControllerManagedBy(mgr).
		Named("node-heartbeat").
		For(&blockstoriov1alpha1.Node{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc: func(_ event.CreateEvent) bool { return true },
				DeleteFunc: func(_ event.DeleteEvent) bool { return true },
				UpdateFunc: func(e event.UpdateEvent) bool {
					return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
				},
			})).
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register NodeHeartbeatReconciler")
	}

	return nil
}

// isHeartbeatStale returns true when the satellite hasn't stamped
// LastHeartbeatTime within NodeMonitorGracePeriod. Nil heartbeat
// (Node just created, satellite never reported) is treated as
// stale immediately — there is no satellite yet to call ONLINE.
func isHeartbeatStale(lastHeartbeat *metav1.Time, now time.Time) bool {
	if lastHeartbeat == nil {
		return true
	}

	return now.Sub(lastHeartbeat.Time) > NodeMonitorGracePeriod
}

// desiredState picks the Condition status + ConnectionStatus
// projection + a human-readable Reason/Message for the watchdog's
// SSA write.
func (*NodeHeartbeatReconciler) desiredState(stale bool) (metav1.ConditionStatus, string, string, string) {
	if stale {
		return metav1.ConditionUnknown,
			blockstoriov1alpha1.NodeConnectionStatusOffline,
			"NodeStatusNeverUpdated",
			"satellite heartbeat is stale beyond node-monitor grace period"
	}

	return metav1.ConditionTrue,
		blockstoriov1alpha1.NodeConnectionStatusOnline,
		"SatelliteHeartbeat",
		"satellite heartbeat is fresh"
}

// applyState SSA-writes the desired Ready condition +
// ConnectionStatus on the Node CRD. Skips the write if the
// observed state already matches the desired one — apiserver
// load on a 100-node cluster ticking every 5s would otherwise be
// 1200 writes/minute for no behaviour change.
//
// When we DO write, we use a distinct field manager from the
// satellite's heartbeat owner: the satellite owns the True path,
// the watchdog owns the Unknown flip. ForceOwnership lets us take
// the field back when state diverges.
func (r *NodeHeartbeatReconciler) applyState(
	ctx context.Context,
	node *blockstoriov1alpha1.Node,
	desiredStatus metav1.ConditionStatus,
	desiredConn string,
	reason string,
	message string,
	now time.Time,
) error {
	// Skip no-op writes — the satellite's heartbeat owner keeps
	// the Ready=True condition fresh on its own cadence, and a
	// stale node only needs one Unknown flip until it recovers.
	if conditionMatches(node, desiredStatus) && node.Status.ConnectionStatus == desiredConn {
		return nil
	}

	transition := metav1.NewTime(now)

	apply := &blockstoriov1alpha1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind:       nodeKind,
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: node.Name},
		Status: blockstoriov1alpha1.NodeStatus{
			ConnectionStatus: desiredConn,
			Conditions: []metav1.Condition{
				{
					Type:               blockstoriov1alpha1.NodeConditionReady,
					Status:             desiredStatus,
					LastTransitionTime: transition,
					Reason:             reason,
					Message:            message,
				},
			},
		},
	}

	err := r.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		client.FieldOwner(HeartbeatWatchdogFieldOwner),
		client.ForceOwnership)
	if err != nil {
		return errors.Wrapf(err, "watchdog ssa for node %q", node.Name)
	}

	return nil
}

// conditionMatches reports whether the Node's current Ready
// condition status matches the desired one.
func conditionMatches(node *blockstoriov1alpha1.Node, desired metav1.ConditionStatus) bool {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == blockstoriov1alpha1.NodeConditionReady {
			return node.Status.Conditions[i].Status == desired
		}
	}

	return false
}
