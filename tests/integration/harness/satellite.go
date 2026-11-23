// SPDX-License-Identifier: Apache-2.0

//go:build integration

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

package harness

import (
	"context"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

const (
	// satelliteTickInterval governs how often the satellite mock
	// scans CRDs. Short enough that Eventually(30s) loops never
	// wait long; long enough not to thrash the apiserver in slow
	// CI.
	satelliteTickInterval = 200 * time.Millisecond

	// defaultPoolTotalKiB is the fixture's stand-in pool capacity
	// when the test didn't pre-stamp Status.TotalCapacity. 10 GiB
	// is generous enough that placer logic that rejects undersized
	// pools (Bug 35 guards) doesn't trip on the smoke.
	defaultPoolTotalKiB int64 = 10 * 1024 * 1024 // 10 GiB

	// drbdStateUpToDate is the steady-state DRBD state for a
	// healthy diskful replica.
	drbdStateUpToDate = "UpToDate"

	// providerLVMThinUpper / providerZFSThinUpper are the upstream
	// LINSTOR enum values for thin pools. Hoisted to consts so
	// goconst doesn't flag the same string in the helpers and in
	// fixtures.go.
	providerLVMThinUpper = "LVM_THIN"
	providerZFSThinUpper = "ZFS_THIN"
)

// Satellite is the in-process mock of the per-node satellite
// reconcilers. It tail-polls the apiserver and writes Status fields
// onto Node / StoragePool / Resource / Snapshot CRDs the way a
// healthy 3-node cluster would: nodes go Ready, pools report
// FreeCapacity, resources reach DrbdState=UpToDate after a tick.
//
// The full satellite stack (pkg/satellite/controllers) drives a
// real DRBD state machine through FakeExec — far more than Phase 0
// needs. We deliberately re-implement only the externally observable
// "healthy steady state" projection here. Phase 1 tests can extend
// via the simulation knobs (SimulatePoolMissing, SimulateDRBDState,
// FailNext) without touching the rest of the harness.
//
// Implements manager.Runnable so it shuts down with the manager.
type Satellite struct {
	client client.Client

	mu sync.Mutex

	// poolMissing[node][pool] true → satellite stamps Status.PoolMissing.
	poolMissing map[string]map[string]bool

	// drbdState[rdName][node] → forced DrbdState for that replica.
	drbdState map[string]map[string]string

	// nodeOffline[node] true → satellite stamps ConnectionStatus
	// OFFLINE instead of ONLINE — models a dead satellite so `linstor
	// n lost` flows can pass the Bug 111 online-refusal gate the way
	// they do in production (the documented `n lost` precondition is
	// a satellite that no longer heartbeats).
	nodeOffline map[string]bool

	// failNext is the queue of pending FakeExec-style failures the
	// satellite mock is asked to inject the next time it sees a
	// matching command. Phase 0 stores them but does not consult
	// them — the simulator never shells out. Phase 1 tests will
	// plumb this through their own group-specific helpers.
	failNext []string

	// tickInterval governs how often the satellite scans CRDs.
	tickInterval time.Duration
}

// NewSatellite returns a Satellite ready to be Add'd to a manager.
func NewSatellite(cli client.Client) *Satellite {
	return &Satellite{
		client:       cli,
		poolMissing:  map[string]map[string]bool{},
		drbdState:    map[string]map[string]string{},
		nodeOffline:  map[string]bool{},
		tickInterval: satelliteTickInterval,
	}
}

// NeedLeaderElection: tests don't elect leaders, but interface
// compliance keeps the manager happy if we ever flip the flag.
func (*Satellite) NeedLeaderElection() bool { return false }

// Start is the manager.Runnable entrypoint. Returns when ctx is
// cancelled.
func (s *Satellite) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.tickOnce(ctx)
		}
	}
}

// SimulatePoolMissing toggles PoolMissing=true on the next tick for
// the named (node, pool) tuple — surfaces the `Faulty` shape on
// `linstor sp l` (Bug 83 guard).
func (s *Satellite) SimulatePoolMissing(node, pool string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.poolMissing[node] == nil {
		s.poolMissing[node] = map[string]bool{}
	}

	s.poolMissing[node][pool] = true
}

// SimulateNodeOffline makes the mock stamp ConnectionStatus OFFLINE
// for the named node from the next tick on (instead of the default
// ONLINE heartbeat). Models a dead/unreachable satellite — the
// documented precondition for `linstor n lost` (Bug 111 gate: the
// REST handler refuses `n lost` against an ONLINE satellite).
func (s *Satellite) SimulateNodeOffline(node string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nodeOffline[node] = true
}

// SimulateDRBDState pins the DrbdState the satellite will stamp on
// (rdName, node). Use to force `SyncTarget`, `Outdated`, `Failed`,
// etc. for tests that need a non-healthy resource.
func (s *Satellite) SimulateDRBDState(rdName, node, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.drbdState[rdName] == nil {
		s.drbdState[rdName] = map[string]string{}
	}

	s.drbdState[rdName][node] = state
}

// FailNext enqueues a FakeExec-style command match that the next
// satellite-side shell-out should fail with. Phase 0 stores it but
// does not enforce — the mock never shells out. The slot exists so
// Phase 1 Group F tests (e.g. Bug 83 reproduction) can declare the
// intent now and the harness can be extended without an API churn.
func (s *Satellite) FailNext(cmdline string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failNext = append(s.failNext, cmdline)
}

// tickOnce reconciles each CRD kind the mock cares about. Errors
// are swallowed (the test will time out via Eventually if the mock
// can't make progress) — we deliberately don't log per-tick errors
// because envtest's apiserver routinely returns "object has been
// modified" during reconcile contention and the noise drowns real
// failures.
func (s *Satellite) tickOnce(ctx context.Context) {
	s.reconcileNodes(ctx)
	s.reconcileStoragePools(ctx)
	s.reconcileResources(ctx)
}

// reconcileNodes stamps Conditions[Ready] and ConnectionStatus on
// every Node — the steady-state shape a satellite produces after its
// first heartbeat. Idempotent: only writes when the value actually
// changes.
//
// A node under SimulateNodeOffline gets the whole offline shape, not
// just the status field: a stale heartbeat and Ready=Unknown, which is
// what a satellite that stopped reporting leaves behind. Stamping a
// FRESH heartbeat beside an OFFLINE status is a state the real system
// cannot produce, and NodeHeartbeatReconciler decides on the timestamp
// rather than on the status — it would find the beat fresh, write
// ONLINE straight back, and the two writers would then trade the field
// between them. A test that takes a node offline, waits for OFFLINE and
// then acts on it read whichever of the two wrote last: that is what
// made `n lost` refuse against a node the test had just watched go
// offline, with a message naming a satellite nothing was pretending was
// alive.
func (s *Satellite) reconcileNodes(ctx context.Context) {
	var nodes blockstoriov1alpha1.NodeList

	err := s.client.List(ctx, &nodes)
	if err != nil {
		return
	}

	for i := range nodes.Items {
		node := &nodes.Items[i]

		desiredStatus := blockstoriov1alpha1.NodeConnectionStatusOnline
		desiredReady := metav1.ConditionTrue
		reason := "SatelliteMockHealthy"
		message := "harness/satellite.go stamped Ready"
		heartbeat := ptrNow()

		if s.isNodeOffline(node.Name) {
			desiredStatus = blockstoriov1alpha1.NodeConnectionStatusOffline
			desiredReady = metav1.ConditionUnknown
			reason = "SatelliteMockOffline"
			message = "harness/satellite.go stopped reporting for this node"
			heartbeat = ptrStaleHeartbeat()
		}

		if node.Status.ConnectionStatus == desiredStatus &&
			hasReadyStatus(node.Status.Conditions, desiredReady) {
			continue
		}

		patched := node.DeepCopy()
		patched.Status.ConnectionStatus = desiredStatus
		patched.Status.LastHeartbeatTime = heartbeat
		patched.Status.Conditions = upsertCondition(patched.Status.Conditions, &metav1.Condition{
			Type:               blockstoriov1alpha1.NodeConditionReady,
			Status:             desiredReady,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
		})

		_ = s.client.Status().Update(ctx, patched)
	}
}

// reconcileStoragePools writes FreeCapacity from the SP's
// TotalCapacity (or the fixture default) and toggles PoolMissing
// based on the simulation map. Mirrors what a real satellite emits
// once `lvm vgs` / `zfs list` succeed.
func (s *Satellite) reconcileStoragePools(ctx context.Context) {
	var pools blockstoriov1alpha1.StoragePoolList

	err := s.client.List(ctx, &pools)
	if err != nil {
		return
	}

	for i := range pools.Items {
		pool := &pools.Items[i]
		want := s.desiredPoolStatus(pool)

		if poolStatusEqual(pool.Status, want) {
			continue
		}

		patched := pool.DeepCopy()
		patched.Status = want
		_ = s.client.Status().Update(ctx, patched)
	}
}

// reconcileResources advances Resource.Status.DrbdState to UpToDate
// (or the simulator-overridden value) and stamps per-volume DiskState.
// Connections are left empty in Phase 0 — Phase 1 group I will fill
// them in as needed.
func (s *Satellite) reconcileResources(ctx context.Context) {
	var resources blockstoriov1alpha1.ResourceList

	err := s.client.List(ctx, &resources)
	if err != nil {
		return
	}

	for i := range resources.Items {
		resource := &resources.Items[i]

		want := s.desiredDrbdState(resource)
		if resource.Status.DrbdState == want {
			continue
		}

		patched := resource.DeepCopy()
		patched.Status.DrbdState = want

		// Volumes follow the resource: the satellite reports
		// UpToDate / Diskless on a per-volume DiskState. We
		// project the resource-level value uniformly — sufficient
		// for the smoke test and Phase 0.
		for j := range patched.Status.Volumes {
			patched.Status.Volumes[j].DiskState = want
		}

		_ = s.client.Status().Update(ctx, patched)
	}
}

// desiredPoolStatus computes the status we want stamped on a
// StoragePool. FreeCapacity defaults to defaultPoolTotalKiB if the
// fixture didn't set TotalCapacity.
func (s *Satellite) desiredPoolStatus(pool *blockstoriov1alpha1.StoragePool) blockstoriov1alpha1.StoragePoolStatus {
	total := pool.Status.TotalCapacity
	if total == 0 {
		total = defaultPoolTotalKiB
	}

	free := pool.Status.FreeCapacity
	if free == 0 {
		free = total
	}

	missing := s.isPoolMissing(pool.Spec.NodeName, pool.Spec.PoolName)

	return blockstoriov1alpha1.StoragePoolStatus{
		FreeCapacity:      free,
		TotalCapacity:     total,
		SupportsSnapshots: providerSupportsSnapshots(pool.Spec.ProviderKind),
		PoolMissing:       missing,
		StaticTraits:      map[string]string{"kind": pool.Spec.ProviderKind},
	}
}

// desiredDrbdState returns the configured override for this
// (rd, node) pair or UpToDate as the healthy default.
func (s *Satellite) desiredDrbdState(resource *blockstoriov1alpha1.Resource) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	perNode, ok := s.drbdState[resource.Spec.ResourceDefinitionName]
	if ok {
		state, ok := perNode[resource.Spec.NodeName]
		if ok {
			return state
		}
	}

	return drbdStateUpToDate
}

func (s *Satellite) isPoolMissing(node, pool string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.poolMissing[node][pool]
}

func (s *Satellite) isNodeOffline(node string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nodeOffline[node]
}

// hasReadyStatus is a tiny helper kept off the global namespace so
// internal callers don't accidentally use it as a public assertion.
func hasReadyStatus(conds []metav1.Condition, want metav1.ConditionStatus) bool {
	for i := range conds {
		if conds[i].Type == blockstoriov1alpha1.NodeConditionReady {
			return conds[i].Status == want
		}
	}

	return false
}

func upsertCondition(conds []metav1.Condition, cond *metav1.Condition) []metav1.Condition {
	for i := range conds {
		if conds[i].Type == cond.Type {
			if conds[i].Status != cond.Status {
				conds[i] = *cond
			}

			return conds
		}
	}

	return append(conds, *cond)
}

func ptrNow() *metav1.Time {
	now := metav1.Now()

	return &now
}

// ptrStaleHeartbeat is a heartbeat old enough that the watchdog reads
// the node as unreachable. Comfortably past NodeMonitorGracePeriod, so
// a slow tick or a busy runner cannot land it back inside the window.
func ptrStaleHeartbeat() *metav1.Time {
	stale := metav1.NewTime(time.Now().Add(-4 * controller.NodeMonitorGracePeriod))

	return &stale
}

func providerSupportsSnapshots(kind string) bool {
	switch strings.ToUpper(kind) {
	case "ZFS", providerZFSThinUpper, providerLVMThinUpper, "FILE_THIN":
		return true
	default:
		return false
	}
}

func poolStatusEqual(actual, want blockstoriov1alpha1.StoragePoolStatus) bool {
	return actual.FreeCapacity == want.FreeCapacity &&
		actual.TotalCapacity == want.TotalCapacity &&
		actual.PoolMissing == want.PoolMissing &&
		actual.SupportsSnapshots == want.SupportsSnapshots
}
