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
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// AutoEvict prop keys mirror upstream LINSTOR's
// `controller/src/main/java/com/linbit/linstor/tasks/AutoEvictTask`
// algorithm (the inline auto-evict path in `ReconnectorTask.java`'s
// `getFailedPeers`). Operators tune them via `linstor c set-property
// DrbdOptions/AutoEvictAfterTime 120` (controller-wide) or `linstor
// n set-property <node> DrbdOptions/AutoEvictAllowEviction false`
// (per-node opt-out).
//
// Scope hierarchy mirrors `pkg/effectiveprops`: per-Node prop wins
// over Controller-prop. Anything outside those two scopes is N/A —
// AutoEvict has no RG/RD layer in upstream.
const (
	// PropAutoEvictAfterTime is the minutes-long window after which a
	// continuously-offline satellite becomes eligible for eviction.
	// Upstream default: 60 (one hour).
	PropAutoEvictAfterTime = "DrbdOptions/AutoEvictAfterTime"

	// PropAutoEvictAllowEviction is the per-node / cluster-wide
	// kill-switch. `false` short-circuits the eviction check with no
	// log spam — the canonical "drain this node out of band, keep it
	// offline as long as you like" workflow. Default: true.
	PropAutoEvictAllowEviction = "DrbdOptions/AutoEvictAllowEviction"

	// PropAutoEvictMaxDisconnectedNodes caps how many satellites may
	// be in the offline-but-not-yet-evicted state before AutoEvict
	// stops acting. Protects against controller-side connectivity
	// blips that would otherwise mass-evict the entire cluster.
	// Upstream stores a *percentage* (default 34); we store the raw
	// node count (simpler to reason about in a 6-node homelab; an
	// operator who wants "34%" can set the integer to 2 on a 6-node
	// cluster). Default: 2.
	PropAutoEvictMaxDisconnectedNodes = "DrbdOptions/AutoEvictMaxDisconnectedNodes"

	// DefaultAutoEvictAfterTimeMinutes matches the upstream
	// `DrbdOptions/AutoEvictAfterTime` default of 60 minutes.
	DefaultAutoEvictAfterTimeMinutes = 60

	// DefaultAutoEvictAllowEviction matches upstream's
	// `DrbdOptions/AutoEvictAllowEviction` default (true since
	// Migration_2020_12_03_Enable_AutoEvictAllowEviction).
	DefaultAutoEvictAllowEviction = true

	// DefaultAutoEvictMaxDisconnectedNodes is the blockstor default
	// (raw count, not percentage). Two simultaneous disconnects is
	// the "controller might be the problem" threshold for the
	// typical 3-replica cozystack deployment.
	DefaultAutoEvictMaxDisconnectedNodes = 2
)

// AutoEvictPeriod is the default poll cadence. Upstream LINSTOR
// piggybacks AutoEvict on `ReconnectorTask`'s ~10s loop; we go a
// little slower (60s) because the AfterTime resolution is minutes
// anyway — sub-minute polling just burns API server load without
// changing eviction latency.
const AutoEvictPeriod = 1 * time.Minute

// AutoEvictReconciler is the cluster-wide cron that watches Node
// CRDs and stamps `Spec.Flags += EVICTED` on satellites that have
// been continuously OFFLINE for longer than the resolved
// `AutoEvictAfterTime`, subject to the `AllowEviction` and
// `MaxDisconnectedNodes` gates.
//
// Algorithm (lifted from upstream
// `controller/src/main/java/com/linbit/linstor/tasks/ReconnectorTask.java`'s
// `getFailedPeers` AutoEvict branch):
//
//  1. List every Node.
//  2. For each Node, resolve `AfterTime`, `AllowEviction`,
//     `MaxDisconnectedNodes` from per-Node Spec.Props with fallback
//     to ControllerConfig.Spec.ExtraProps.
//  3. If the Node is already EVICTED → skip (idempotent).
//  4. If ConnectionStatus != OFFLINE → skip (not a candidate).
//  5. If AllowEviction=false → silent skip (no log; the operator
//     deliberately opted out).
//  6. If `now - LastHeartbeatTime < AfterTime` → skip (not stale
//     enough yet).
//  7. If currently-disconnected count >= MaxDisconnectedNodes →
//     skip + log once (the "controller might be the problem"
//     short-circuit).
//  8. Stamp Spec.Flags += EVICTED. The existing NodeReconciler
//     reacts to the flag and cascades replica migration via the
//     placer — same path 4.W04 already proved out.
//
// Cascade-delete of resources is intentionally NOT done here: the
// W02 invariant is "AutoEvict is a *trigger*, not a *broker*",
// matching upstream's clean separation between
// `Node.setEvictionTimestamp` + `Spec.Flags=EVICTED` (this
// runnable) and `ctrlNodeApiCallHandler.declareEvicted` (the
// NodeReconciler, downstream of the flag).
//
// Implements manager.Runnable. Leader-elected (NeedLeaderElection
// returns true) — without it, a multi-replica controller
// Deployment would race-evict the same node N times.
type AutoEvictReconciler struct {
	// Client is the controller-runtime client used for both reads
	// (Node + ControllerConfig CRDs) and writes (Node.Spec.Flags
	// update). Production wires `mgr.GetClient()`; tests inject
	// fakes via `controller-runtime/pkg/client/fake`.
	Client client.Client

	// Clock is the time source. Defaults to RealClock when nil.
	// Tests inject a deterministic clock so the AfterTime window
	// can be advanced without sleeping.
	Clock Clock

	// Period overrides AutoEvictPeriod (test-only). A zero Period
	// falls back to the default.
	Period time.Duration
}

// NeedLeaderElection returns true: this runnable mutates Node CRDs
// cluster-wide. Without leader election a multi-replica controller
// Deployment would race-evict the same node and produce duplicate
// flag-set events.
func (*AutoEvictReconciler) NeedLeaderElection() bool { return true }

// Start runs the auto-evict loop until ctx cancels. First tick
// fires one period after Start so the controller-runtime cache has
// time to warm — an immediate first tick against a cold cache
// would log "no nodes found" on every controller restart.
func (r *AutoEvictReconciler) Start(ctx context.Context) error {
	period := r.Period
	if period == 0 {
		period = AutoEvictPeriod
	}

	logger := log.FromContext(ctx).WithName("auto-evict")

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := r.Tick(ctx)
			if err != nil {
				logger.Error(err, "auto-evict tick")
			}
		}
	}
}

// Tick performs exactly one reconcile cycle. Exported so unit
// tests can drive the cycle deterministically without running the
// ticker loop — same pattern AutoSnapshotRunnable uses.
func (r *AutoEvictReconciler) Tick(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("auto-evict")

	clk := r.Clock
	if clk == nil {
		clk = RealClock{}
	}

	ctrlExtras, err := r.controllerExtras(ctx)
	if err != nil {
		return errors.Wrap(err, "read controller props")
	}

	var nodeList blockstoriov1alpha1.NodeList

	err = r.Client.List(ctx, &nodeList)
	if err != nil {
		return errors.Wrap(err, "list Nodes")
	}

	now := clk.Now()

	// Count currently-disconnected satellites BEFORE we mutate any
	// of them. Mirrors upstream's `reconnectorConfigSet.size()` —
	// the cap is against the offline-but-not-yet-evicted set, so a
	// healthy + already-EVICTED node both don't count.
	disconnected := countDisconnected(nodeList.Items)

	for i := range nodeList.Items {
		node := &nodeList.Items[i]

		err := r.processNode(ctx, node, ctrlExtras, disconnected, now)
		if err != nil {
			// Don't bail on one Node — the next tick retries the
			// survivors. Same defensive pattern as
			// AutoSnapshotRunnable.Tick.
			logger.Error(err, "auto-evict node", "node", node.Name)
		}
	}

	return nil
}

// processNode evaluates one Node against the AutoEvict policy and
// stamps the EVICTED flag if every gate passes. Returns the first
// gate that vetoed (for tests + logs); nil means "evicted or
// idempotent no-op, nothing went wrong".
func (r *AutoEvictReconciler) processNode(
	ctx context.Context,
	node *blockstoriov1alpha1.Node,
	ctrlExtras map[string]string,
	disconnected int,
	now time.Time,
) error {
	logger := log.FromContext(ctx).WithName("auto-evict").WithValues("node", node.Name)

	// Already EVICTED → idempotent skip. The migration cascade is
	// owned by NodeReconciler; re-stamping the flag would just
	// produce a no-op Update.
	if slices.Contains(node.Spec.Flags, apiv1.NodeFlagEvicted) {
		return nil
	}

	allowEviction := resolveBoolProp(node.Spec.Props, ctrlExtras,
		PropAutoEvictAllowEviction, DefaultAutoEvictAllowEviction)

	if !allowEviction {
		// Per the W02 invariant: short-circuit with no log spam.
		// The operator deliberately turned this off; logging on
		// every tick would just noise up the controller logs for
		// the (intentional) keep-offline workflow.
		return nil
	}

	if !isNodeOffline(node) {
		return nil
	}

	afterTime := resolveDurationMinutesProp(node.Spec.Props, ctrlExtras,
		PropAutoEvictAfterTime, DefaultAutoEvictAfterTimeMinutes)

	if !hasBeenOfflineLongEnough(node, now, afterTime) {
		return nil
	}

	maxDisconnected := resolveIntProp(node.Spec.Props, ctrlExtras,
		PropAutoEvictMaxDisconnectedNodes, DefaultAutoEvictMaxDisconnectedNodes)

	if disconnected > maxDisconnected {
		// The "controller might be the problem" short-circuit.
		// Mirrors upstream's "Currently more than X% nodes are
		// not connected to the controller. The controller might
		// have a problem with it's connections, therefore no
		// nodes will be declared as EVICTED" log.
		logger.Info(
			"auto-evict suppressed: too many disconnected nodes",
			"disconnected", disconnected,
			"max", maxDisconnected,
		)

		return nil
	}

	logger.Info(
		"satellite offline beyond AutoEvictAfterTime; flagging EVICTED",
		"afterTime", afterTime,
		"disconnected", disconnected,
	)

	return r.flagEvicted(ctx, node)
}

// flagEvicted writes `Spec.Flags += EVICTED` on the Node. The
// existing NodeReconciler observes the flag and drives the replica
// migration via the placer — same path 4.W04 already covers. We
// don't touch Status here; the heartbeat watchdog still owns it.
func (r *AutoEvictReconciler) flagEvicted(ctx context.Context, node *blockstoriov1alpha1.Node) error {
	node.Spec.Flags = append(node.Spec.Flags, apiv1.NodeFlagEvicted)

	err := r.Client.Update(ctx, node)
	if err != nil {
		return errors.Wrapf(err, "flag node %q EVICTED", node.Name)
	}

	return nil
}

// controllerExtras returns ControllerConfig.Spec.ExtraProps —
// the cluster-wide fallback bag for the AutoEvict knobs.
// Missing ControllerConfig is fine; an empty map just means
// per-Node props (or defaults) decide everything.
func (r *AutoEvictReconciler) controllerExtras(ctx context.Context) (map[string]string, error) {
	var cfg blockstoriov1alpha1.ControllerConfig

	err := r.Client.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &cfg)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return map[string]string{}, nil
		}

		return nil, errors.Wrap(err, "get ControllerConfig")
	}

	return cfg.Spec.ExtraProps, nil
}

// RegisterWithManager wires the runnable into the controller-runtime
// manager. Symmetrical to AutoSnapshotRunnable.RegisterWithManager.
func (r *AutoEvictReconciler) RegisterWithManager(mgr manager.Manager) error {
	err := mgr.Add(r)
	if err != nil {
		return errors.Wrap(err, "add AutoEvictReconciler")
	}

	return nil
}

// resolveBoolProp walks the AutoEvict scope hierarchy (per-Node
// wins over Controller-wide) and parses the resulting raw value as
// a bool. An unparseable value at either scope falls through to
// the next scope — this matches the upstream
// `Boolean.parseBoolean` behaviour where a typo silently returns
// false, but blockstor surfaces it as "use the default" rather
// than treating typo == false.
func resolveBoolProp(nodeProps, ctrlExtras map[string]string, key string, fallback bool) bool {
	if raw, ok := nodeProps[key]; ok {
		if v, err := strconv.ParseBool(raw); err == nil {
			return v
		}
	}

	if raw, ok := ctrlExtras[key]; ok {
		if v, err := strconv.ParseBool(raw); err == nil {
			return v
		}
	}

	return fallback
}

// resolveIntProp walks the same scope hierarchy and parses the
// raw value as a non-negative integer. An unparseable / negative
// value at either scope falls through to the next scope.
func resolveIntProp(nodeProps, ctrlExtras map[string]string, key string, fallback int) int {
	if raw, ok := nodeProps[key]; ok {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			return v
		}
	}

	if raw, ok := ctrlExtras[key]; ok {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			return v
		}
	}

	return fallback
}

// resolveDurationMinutesProp parses the prop as minutes (the
// upstream wire convention) and returns the corresponding
// time.Duration. <=0 falls through to the next scope.
func resolveDurationMinutesProp(
	nodeProps, ctrlExtras map[string]string,
	key string,
	fallbackMinutes int,
) time.Duration {
	mins := resolveIntProp(nodeProps, ctrlExtras, key, fallbackMinutes)

	return time.Duration(mins) * time.Minute
}

// isNodeOffline reports whether the Node's heartbeat watchdog has
// marked it OFFLINE. We consult ConnectionStatus rather than
// scanning Conditions because the heartbeat watchdog is the
// authoritative source — Conditions may lag while the watchdog
// is mid-SSA.
func isNodeOffline(node *blockstoriov1alpha1.Node) bool {
	return node.Status.ConnectionStatus == blockstoriov1alpha1.NodeConnectionStatusOffline
}

// hasBeenOfflineLongEnough reports whether the Node's last
// heartbeat is older than `afterTime`.
//
// Bug 285: a nil LastHeartbeatTime means the satellite has never
// reported in to the controller yet. Two realistic causes:
//
//  1. Brand-new install — Node CRD was applied but the satellite
//     daemonset pod hasn't come up / connected yet. AfterTime is
//     60 min by default; on a slow control-plane bootstrap the
//     pod can easily lag the Node CRD by that much, and the
//     pre-Bug-285 fallback to CreationTimestamp would silently
//     EVICT the node before its satellite ever shook hands. The
//     EVICTED flag is sticky (no auto-recovery on reconnect),
//     so the cluster ends up one node short of its place-count
//     for every RD until an operator notices and `linstor node
//     restore`s — the exact pre-condition that broke 5 of Run 5's
//     e2e scenarios on stand e2e7 (worker-3 EVICTED → tiebreaker
//     witness has no candidate → 2-replica RDs never get a
//     witness → recovery-port-collision / resize-luks / tiebreaker
//     / snapshot-restore-cross-node / disk-replace all fail their
//     wait_uptodate gates).
//
//  2. Controller restart — the heartbeat controller stamps
//     Status.LastHeartbeatTime on the K8s Node CRD on every tick.
//     If the controller pod was restarted after the heartbeat
//     status was stamped, the field persists. If it was restarted
//     before the field was ever populated (sub-second window), the
//     same nil-LHT shape appears on a perfectly healthy cluster.
//
// In both cases the safe behaviour is "wait until we actually see
// a heartbeat before we start counting offline time" — eviction
// without a single ever-observed heartbeat is a false positive.
// Real "node truly gone forever" is caught by NodeFlagLost; that's
// already an operator gesture, not an auto path.
func hasBeenOfflineLongEnough(node *blockstoriov1alpha1.Node, now time.Time, afterTime time.Duration) bool {
	if node.Status.LastHeartbeatTime == nil {
		// Never heard from the satellite. Refuse to evict until
		// a real heartbeat lands and the AfterTime window starts
		// counting from a known-good baseline.
		return false
	}

	return now.Sub(node.Status.LastHeartbeatTime.Time) >= afterTime
}

// countDisconnected returns the count of Nodes that are
// OFFLINE-but-not-yet-EVICTED. Matches upstream's
// `reconnectorConfigSet.size()` semantic: an already-EVICTED node
// is no longer "disconnected from the controller" (it's been
// formally taken out of rotation), so it doesn't count against
// the MaxDisconnectedNodes cap.
func countDisconnected(nodes []blockstoriov1alpha1.Node) int {
	count := 0

	for i := range nodes {
		n := &nodes[i]
		if !isNodeOffline(n) {
			continue
		}

		if slices.Contains(n.Spec.Flags, apiv1.NodeFlagEvicted) {
			continue
		}

		count++
	}

	return count
}
