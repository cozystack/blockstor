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

package controller_test

import (
	"context"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// autoEvictClock is a deterministic clock for the auto-evict
// tests. Distinct from autosnapshot_controller_test.go's stubClock
// to avoid cross-file coupling; both end up identical in shape but
// the package convention is one clock per controller.
type autoEvictClock struct {
	t time.Time
}

func (c *autoEvictClock) Now() time.Time { return c.t }

// makeOfflineNode is the common fixture builder — a Node CRD with
// `Status.ConnectionStatus=OFFLINE` and a LastHeartbeatTime set to
// `lastSeen`. The heartbeat watchdog normally writes both fields
// in lockstep; the tests bypass the watchdog and stamp them
// directly so the AutoEvict gate logic is what's under test.
func makeOfflineNode(name string, lastSeen time.Time) *blockstoriov1alpha1.Node {
	stamp := metav1.NewTime(lastSeen)

	return &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: stamp,
		},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type: apiv1.NodeTypeSatellite,
		},
		Status: blockstoriov1alpha1.NodeStatus{
			ConnectionStatus:  blockstoriov1alpha1.NodeConnectionStatusOffline,
			LastHeartbeatTime: &stamp,
		},
	}
}

// makeOnlineNode builds a satellite Node in the steady-state happy
// path: ConnectionStatus=ONLINE, heartbeat fresh.
func makeOnlineNode(name string, now time.Time) *blockstoriov1alpha1.Node {
	stamp := metav1.NewTime(now)

	return &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: stamp,
		},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type: apiv1.NodeTypeSatellite,
		},
		Status: blockstoriov1alpha1.NodeStatus{
			ConnectionStatus:  blockstoriov1alpha1.NodeConnectionStatusOnline,
			LastHeartbeatTime: &stamp,
		},
	}
}

// getNodeFlags reads back the live flag list for a Node — used by
// every test to assert whether the runnable stamped EVICTED.
func getNodeFlags(t *testing.T, cli client.Client, name string) []string {
	t.Helper()

	var got blockstoriov1alpha1.Node

	err := cli.Get(context.Background(), types.NamespacedName{Name: name}, &got)
	if err != nil {
		t.Fatalf("get node %q: %v", name, err)
	}

	return got.Spec.Flags
}

// TestAutoEvict_HappyEvictionAfterTimeout pins the canonical
// scenario: a satellite has been OFFLINE for > AfterTime, the
// AllowEviction gate is on (default), and the disconnected-count
// is under MaxDisconnectedNodes. AutoEvict stamps EVICTED.
func TestAutoEvict_HappyEvictionAfterTimeout(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	// AfterTime is 60min by default; place the offline timestamp
	// 90min in the past so we're comfortably past the window.
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	lastSeen := now.Add(-90 * time.Minute)

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			makeOfflineNode("sat-1", lastSeen),
			makeOnlineNode("sat-2", now),
			makeOnlineNode("sat-3", now),
		).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	flags := getNodeFlags(t, cli, "sat-1")
	if !slices.Contains(flags, apiv1.NodeFlagEvicted) {
		t.Errorf("expected sat-1 to be flagged EVICTED after AfterTime; got %v", flags)
	}

	// Online peers must NOT be touched.
	if got := getNodeFlags(t, cli, "sat-2"); slices.Contains(got, apiv1.NodeFlagEvicted) {
		t.Errorf("sat-2 wrongly evicted; flags=%v", got)
	}

	if got := getNodeFlags(t, cli, "sat-3"); slices.Contains(got, apiv1.NodeFlagEvicted) {
		t.Errorf("sat-3 wrongly evicted; flags=%v", got)
	}
}

// TestAutoEvict_NotYetStaleSkipped: an offline satellite whose
// disconnect predates AfterTime must not be touched yet. Pins the
// timing edge — without this, AutoEvict would mass-evict on the
// next tick after a transient blip.
func TestAutoEvict_NotYetStaleSkipped(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	// AfterTime is 60min by default; 30min stale is below the
	// gate.
	lastSeen := now.Add(-30 * time.Minute)

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(makeOfflineNode("sat-1", lastSeen)).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	flags := getNodeFlags(t, cli, "sat-1")
	if slices.Contains(flags, apiv1.NodeFlagEvicted) {
		t.Errorf("sat-1 wrongly evicted before AfterTime elapsed; flags=%v", flags)
	}
}

// TestAutoEvict_AllowEvictionFalseShortCircuits pins the per-Node
// kill-switch invariant: `DrbdOptions/AutoEvictAllowEviction=false`
// on the Node's Spec.Props overrides the controller default and
// prevents the EVICTED stamp even past AfterTime. Scenario doc:
// "AutoEvictAllowEviction=false → silent skip, no log spam".
func TestAutoEvict_AllowEvictionFalseShortCircuits(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	lastSeen := now.Add(-90 * time.Minute) // well past AfterTime

	node := makeOfflineNode("sat-optout", lastSeen)
	node.Spec.Props = map[string]string{
		controllerpkg.PropAutoEvictAllowEviction: "false",
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(node).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	flags := getNodeFlags(t, cli, "sat-optout")
	if slices.Contains(flags, apiv1.NodeFlagEvicted) {
		t.Errorf("AllowEviction=false must short-circuit AutoEvict; flags=%v", flags)
	}
}

// TestAutoEvict_AllowEvictionFalseControllerWide pins that the
// kill-switch also works at the ControllerConfig.ExtraProps scope
// (cluster-wide opt-out). Distinct from the per-Node path because
// the ExtraProps lookup goes through a separate code branch.
func TestAutoEvict_AllowEvictionFalseControllerWide(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	lastSeen := now.Add(-90 * time.Minute)

	ctrlCfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				controllerpkg.PropAutoEvictAllowEviction: "false",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			ctrlCfg,
			makeOfflineNode("sat-1", lastSeen),
			makeOfflineNode("sat-2", lastSeen),
		).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	for _, name := range []string{"sat-1", "sat-2"} {
		if slices.Contains(getNodeFlags(t, cli, name), apiv1.NodeFlagEvicted) {
			t.Errorf("%s wrongly evicted under cluster-wide AllowEviction=false", name)
		}
	}
}

// TestAutoEvict_MaxDisconnectedNodesCap pins the
// "controller-might-be-the-problem" backstop: when more nodes are
// OFFLINE than `MaxDisconnectedNodes`, AutoEvict refuses to act on
// any of them. Mirrors upstream's `numDiscon > maxDiscon` branch.
//
// Setup: 4 nodes offline, MaxDisconnectedNodes=2. Cap is exceeded
// (4 > 2), so NONE of the four get the EVICTED flag this tick.
func TestAutoEvict_MaxDisconnectedNodesCap(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	lastSeen := now.Add(-90 * time.Minute)

	// Cluster-wide cap = 2, but four nodes are offline. The cap
	// is exceeded, so AutoEvict must short-circuit on all four.
	ctrlCfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				controllerpkg.PropAutoEvictMaxDisconnectedNodes: "2",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			ctrlCfg,
			makeOfflineNode("sat-1", lastSeen),
			makeOfflineNode("sat-2", lastSeen),
			makeOfflineNode("sat-3", lastSeen),
			makeOfflineNode("sat-4", lastSeen),
		).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	for _, name := range []string{"sat-1", "sat-2", "sat-3", "sat-4"} {
		if slices.Contains(getNodeFlags(t, cli, name), apiv1.NodeFlagEvicted) {
			t.Errorf("%s wrongly evicted past MaxDisconnectedNodes cap", name)
		}
	}
}

// TestAutoEvict_BelowCapEvicts is the symmetric companion of the
// MaxDisconnectedNodes test: with the same cap=2 but only 2
// offline nodes, the cap is NOT exceeded (2 not > 2) and both
// nodes are evicted. Pins that the comparison is strictly-greater
// (matching upstream's `numDiscon <= maxDiscon`).
func TestAutoEvict_BelowCapEvicts(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	lastSeen := now.Add(-90 * time.Minute)

	ctrlCfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				controllerpkg.PropAutoEvictMaxDisconnectedNodes: "2",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			ctrlCfg,
			makeOfflineNode("sat-1", lastSeen),
			makeOfflineNode("sat-2", lastSeen),
			makeOnlineNode("sat-3", now),
		).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	for _, name := range []string{"sat-1", "sat-2"} {
		if !slices.Contains(getNodeFlags(t, cli, name), apiv1.NodeFlagEvicted) {
			t.Errorf("%s should have been evicted at cap=2; flags=%v",
				name, getNodeFlags(t, cli, name))
		}
	}
}

// TestAutoEvict_IdempotentReEvictionIsNoOp: a Node already
// flagged EVICTED must not be touched on subsequent ticks. The
// runnable should leave Spec.Flags untouched (no duplicate
// EVICTED stamp, no flag re-ordering) so the NodeReconciler's
// migration cascade doesn't get re-triggered every minute.
func TestAutoEvict_IdempotentReEvictionIsNoOp(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	lastSeen := now.Add(-90 * time.Minute)

	node := makeOfflineNode("sat-already-evicted", lastSeen)
	node.Spec.Flags = []string{apiv1.NodeFlagEvicted}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(node).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	// Two consecutive ticks — neither should produce a write.
	for range 2 {
		err := r.Tick(context.Background())
		if err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}

	flags := getNodeFlags(t, cli, "sat-already-evicted")
	// Exactly one EVICTED entry; no duplicates from re-stamps.
	count := 0

	for _, f := range flags {
		if f == apiv1.NodeFlagEvicted {
			count++
		}
	}

	if count != 1 {
		t.Errorf("idempotency broken: expected exactly 1 EVICTED flag, got %d (flags=%v)", count, flags)
	}
}

// TestAutoEvict_PerNodeOverridesController pins the upstream
// hierarchy: per-Node prop wins over Controller-wide. Setup:
// controller-wide `AllowEviction=false` but the offline node has
// `AllowEviction=true` on its own Spec.Props — the per-Node value
// wins, so the node IS evicted past AfterTime.
func TestAutoEvict_PerNodeOverridesController(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	lastSeen := now.Add(-90 * time.Minute)

	ctrlCfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				controllerpkg.PropAutoEvictAllowEviction: "false",
			},
		},
	}

	node := makeOfflineNode("sat-override", lastSeen)
	node.Spec.Props = map[string]string{
		controllerpkg.PropAutoEvictAllowEviction: "true",
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(ctrlCfg, node).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	flags := getNodeFlags(t, cli, "sat-override")
	if !slices.Contains(flags, apiv1.NodeFlagEvicted) {
		t.Errorf("per-Node AllowEviction=true must override controller-wide false; flags=%v", flags)
	}
}

// TestAutoEvict_OnlineNodeNotEvicted: a satellite reporting
// ONLINE must never be evicted even if its LastHeartbeatTime
// somehow looks old. The ConnectionStatus gate is the
// authoritative source — the watchdog already encodes the
// heartbeat-age semantics into the ONLINE/OFFLINE projection.
func TestAutoEvict_OnlineNodeNotEvicted(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	// Build an ONLINE node but with a stale LastHeartbeatTime —
	// this should never happen in practice (the watchdog flips
	// to OFFLINE first), but pinning the gate ordering matters.
	stale := metav1.NewTime(now.Add(-120 * time.Minute))
	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sat-online-stale",
			CreationTimestamp: stale,
		},
		Spec: blockstoriov1alpha1.NodeSpec{Type: apiv1.NodeTypeSatellite},
		Status: blockstoriov1alpha1.NodeStatus{
			ConnectionStatus:  blockstoriov1alpha1.NodeConnectionStatusOnline,
			LastHeartbeatTime: &stale,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(node).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	flags := getNodeFlags(t, cli, "sat-online-stale")
	if slices.Contains(flags, apiv1.NodeFlagEvicted) {
		t.Errorf("ONLINE node must never be evicted; flags=%v", flags)
	}
}

// TestAutoEvict_NeverReportedHeartbeatNotEvicted (Bug 285): a Node
// that has been OFFLINE since creation and has never stamped a
// LastHeartbeatTime MUST NOT be auto-evicted — even when its CRD's
// creationTimestamp is well past AfterTime.
//
// Pre-Bug-285, the missing-heartbeat fallback was the CRD's
// CreationTimestamp, which conflates "just-installed cluster
// waiting for the satellite daemonset to come up" with "node that
// the controller has been talking to for hours and lost contact
// with". Both look identical at the gate: OFFLINE +
// LastHeartbeatTime=nil + creationTimestamp old. The first case is
// a false positive that mass-evicts every-not-yet-online node on a
// slow bootstrap; the EVICTED flag is sticky (no auto-recovery on
// reconnect; only manual `linstor node restore` clears it), so the
// cluster ends up one node short of place_count for every RD until
// an operator notices and runs the restore by hand.
//
// Repro caught on stand e2e7 during Run 5: worker-3 was EVICTED by
// an auto-evict tick that fired before the satellite ever reported
// in (Node CRD created 16:14, satellite pods didn't connect until
// later). That left subsequent 2-replica e2e scenarios with no
// candidate node for the tiebreaker witness — root cause of 5 of
// the 7 scenarios in Run 5's batch failing their wait_uptodate
// gates (recovery-port-collision, resize-luks, tiebreaker,
// snapshot-restore-cross-node, disk-replace-internal-metadata).
//
// Fixed by refusing eviction until a real LastHeartbeatTime is
// observed (hasBeenOfflineLongEnough returns false on nil-LHT).
// Real "node truly gone forever" still works because the node will
// eventually stamp a heartbeat once, then go offline and trip the
// gate from a known-good baseline.
func TestAutoEvict_NeverReportedHeartbeatNotEvicted(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	// Node was created 2 hours ago — well past the 60-min AfterTime
	// default. Status.ConnectionStatus=OFFLINE (the heartbeat
	// watchdog flips it that way for nodes whose LHT is nil or
	// stale). The pre-fix code would have CreationTimestamp-
	// fallbacked to "2h offline" and stamped EVICTED on this tick.
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	createdAt := metav1.NewTime(now.Add(-2 * time.Hour))

	node := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sat-never-reported",
			CreationTimestamp: createdAt,
		},
		Spec: blockstoriov1alpha1.NodeSpec{Type: apiv1.NodeTypeSatellite},
		Status: blockstoriov1alpha1.NodeStatus{
			ConnectionStatus: blockstoriov1alpha1.NodeConnectionStatusOffline,
			// LastHeartbeatTime intentionally nil — never reported.
			LastHeartbeatTime: nil,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(node).
		Build()

	r := &controllerpkg.AutoEvictReconciler{
		Client: cli,
		Clock:  &autoEvictClock{t: now},
	}

	err := r.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}

	flags := getNodeFlags(t, cli, "sat-never-reported")
	if slices.Contains(flags, apiv1.NodeFlagEvicted) {
		t.Errorf("Bug 285: node with nil LastHeartbeatTime must NOT be auto-evicted regardless of CRD age; flags=%v", flags)
	}
}
