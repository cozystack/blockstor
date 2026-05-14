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

package rest

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// haControllerViewSLA is the upper bound the HA-controller scenario
// (wave2 11.W02) gives blockstor for surfacing a per-node DRBD-state
// change on /v1/view/resources. The piraeus/cozystack HA controller
// uses this view to decide when to apply
// `linstor.linbit.com/lost-quorum` taints so pods evict before
// kubelet's default 5-minute `tolerationSeconds`. 30s gives the HA
// controller a healthy budget on top of blockstor's surface time
// (the upstream HA-controller defaults to applying the taint within
// 30s of detecting quorum loss).
//
// In-process this is sub-millisecond; the explicit budget guards
// against future regressions (e.g. a synchronous full-cluster
// recompute slipping into the GET path) that would push wall time
// past the SLA.
const haControllerViewSLA = 30 * time.Second

// TestHAControllerView_PerNodeDRBDStateSurfacesWithin30s is the
// scenario 11.W02 contract for blockstor's role in the fast-failover
// pipeline: when a satellite writes an observed DRBD state change for
// one replica, the /v1/view/resources aggregate must reflect that
// change keyed by NodeName within the HA-controller SLA. The HA
// controller (deployed separately by piraeus/cozystack) watches this
// view to identify nodes whose DRBD replicas have lost quorum / gone
// to Outdated, so it can stamp the `linstor.linbit.com/lost-quorum`
// taint and let the scheduler re-home workloads before kubelet's
// default 5-minute eviction kicks in.
//
// The test seeds two replicas of the same RD on two nodes, flips the
// DRBD state on one of them via the SetState path the satellite
// observer uses in production, and asserts:
//
//  1. /v1/view/resources returns one entry per (RD, node) replica.
//  2. Each entry surfaces NodeName + State.DrbdState so the HA
//     controller can attribute the unhealthy state to a specific
//     node (without this, the controller cannot decide WHICH node
//     to taint).
//  3. The updated DRBD state propagates within haControllerViewSLA
//     of the satellite write — the HA controller's 30s decision
//     budget covers the whole stack, so blockstor's contribution
//     must be a small fraction of that.
func TestHAControllerView_PerNodeDRBDStateSurfacesWithin30s(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Seed: two replicas of `pvc-ha` on worker-1 (Primary, UpToDate)
	// and worker-2 (Secondary, UpToDate). This is the steady state
	// the HA controller sees while everything is healthy — no taint
	// stamped.
	primary := true
	secondary := false

	for _, r := range []apiv1.Resource{
		{
			Name:     "pvc-ha",
			NodeName: "worker-1",
			State: apiv1.ResourceState{
				InUse:     &primary,
				DrbdState: "UpToDate",
			},
			Volumes: []apiv1.Volume{{
				VolumeNumber: 0,
				State:        apiv1.VolumeState{DiskState: "UpToDate"},
			}},
		},
		{
			Name:     "pvc-ha",
			NodeName: "worker-2",
			State: apiv1.ResourceState{
				InUse:     &secondary,
				DrbdState: "UpToDate",
			},
			Volumes: []apiv1.Volume{{
				VolumeNumber: 0,
				State:        apiv1.VolumeState{DiskState: "UpToDate"},
			}},
		},
	} {
		if err := st.Resources().Create(ctx, &r); err != nil {
			t.Fatalf("seed %s/%s: %v", r.Name, r.NodeName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Simulate the satellite observing worker-2's local DRBD link
	// failing — replica flips from UpToDate to Outdated, peer-device
	// state goes Inconsistent. SetState is the same path the
	// production events2 observer uses, so this exercises the same
	// wire shape the HA controller sees in a real outage.
	flipStart := time.Now()

	err := st.Resources().SetState(ctx, "pvc-ha", "worker-2",
		apiv1.ResourceState{
			InUse:     &secondary,
			DrbdState: "Outdated",
		},
		[]apiv1.VolumeObservation{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "Inconsistent"},
		}},
	)
	if err != nil {
		t.Fatalf("SetState worker-2: %v", err)
	}

	// Fetch the aggregate view the HA controller polls. Wall-time is
	// the contract — see haControllerViewSLA — so we time the GET
	// from the flip onwards. In-process this is sub-millisecond; the
	// budget exists to catch future regressions, not pass today.
	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	elapsed := time.Since(flipStart)
	if elapsed > haControllerViewSLA {
		t.Errorf("flip→GET elapsed %v > SLA %v: HA controller cannot taint "+
			"within scenario 11.W02's 30s budget", elapsed, haControllerViewSLA)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2 (one entry per replica)", len(got))
	}

	// Index by node name — the view's deterministic Name+NodeName
	// sort gives worker-1 first, but key on NodeName so the test
	// stays robust to a future tiebreak change.
	byNode := map[string]apiv1.ResourceWithVolumes{}
	for i := range got {
		if got[i].NodeName == "" {
			t.Errorf("entry %d: empty NodeName — HA controller cannot "+
				"attribute DRBD state to a specific node", i)
		}

		byNode[got[i].NodeName] = got[i]
	}

	healthy, ok := byNode["worker-1"]
	if !ok {
		t.Fatalf("worker-1 missing from view; got nodes=%v", nodeNamesFromView(got))
	}

	if healthy.State.DrbdState != "UpToDate" {
		t.Errorf("worker-1 State.DrbdState: got %q, want UpToDate "+
			"(replica was not touched by SetState)", healthy.State.DrbdState)
	}

	unhealthy, ok := byNode["worker-2"]
	if !ok {
		t.Fatalf("worker-2 missing from view; got nodes=%v", nodeNamesFromView(got))
	}

	if unhealthy.State.DrbdState != "Outdated" {
		t.Errorf("worker-2 State.DrbdState: got %q, want Outdated "+
			"(post-SetState, HA controller must see the flip to decide "+
			"whether to stamp the lost-quorum taint)", unhealthy.State.DrbdState)
	}

	// Resource-level State.DrbdState is the wire field the HA
	// controller reads to decide whether to stamp the lost-quorum
	// taint — `Outdated`, `StandAlone`, `Failed`, `Connecting` are
	// the failure tokens that warrant a taint; `UpToDate` /
	// `Connected` are healthy. Per-volume DiskState is a related
	// signal exposed via the `?faulty=true` aggregate filter (see
	// TestHAControllerView_FaultyFilterIsolatesUnhealthyNode); the
	// HA controller doesn't have to choose, both arrive in the same
	// poll cycle.
	if unhealthy.NodeName != "worker-2" {
		t.Errorf("unhealthy entry NodeName: got %q, want worker-2", unhealthy.NodeName)
	}
}

// TestHAControllerView_FaultyFilterIsolatesUnhealthyNode is the
// recovery-copilot's complement to the per-node test above: the HA
// controller wants the cheapest possible "is anything broken right
// now?" probe so it can short-circuit when the cluster is healthy.
// `/v1/view/resources?faulty=true` returns only replicas with
// non-UpToDate disk state — the controller can poll this on its tick
// and skip the per-node correlation step when the response is empty.
//
// Scenario 11.W02 doesn't mandate this filter but the wave2 test plan
// pins it as the operator-friendly fast path; without it the HA
// controller has to fetch the full view and filter client-side, which
// is wasteful on large clusters (one entry per replica, not per RD).
func TestHAControllerView_FaultyFilterIsolatesUnhealthyNode(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, r := range []apiv1.Resource{
		{
			Name:     "pvc-ha",
			NodeName: "worker-1",
			Volumes: []apiv1.Volume{{
				VolumeNumber: 0,
				State:        apiv1.VolumeState{DiskState: "UpToDate"},
			}},
		},
		{
			Name:     "pvc-ha",
			NodeName: "worker-2",
			Volumes: []apiv1.Volume{{
				VolumeNumber: 0,
				State:        apiv1.VolumeState{DiskState: "Outdated"},
			}},
		},
	} {
		if err := st.Resources().Create(ctx, &r); err != nil {
			t.Fatalf("seed %s/%s: %v", r.Name, r.NodeName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources?faulty=true")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// `?faulty=true` aggregates at RD level — one bad replica taints
	// the whole RD as faulty, so both replicas of `pvc-ha` come back.
	// The HA controller reads NodeName off each entry to find the
	// candidates: every entry where the per-volume DiskState is
	// non-UpToDate is a node that needs the lost-quorum taint.
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2 (RD-level faulty aggregation)", len(got))
	}

	var taintCandidates []string

	for i := range got {
		if got[i].NodeName == "" {
			t.Errorf("entry %d: empty NodeName", i)
		}

		for j := range got[i].Volumes {
			if got[i].Volumes[j].State.DiskState != "UpToDate" {
				taintCandidates = append(taintCandidates, got[i].NodeName)

				break
			}
		}
	}

	if len(taintCandidates) != 1 || taintCandidates[0] != "worker-2" {
		t.Errorf("taint candidates: got %v, want [worker-2] — the HA "+
			"controller must derive a single unambiguous node to taint "+
			"from the faulty-filtered view", taintCandidates)
	}
}

// nodeNamesFromView returns the node names a /v1/view/resources
// response carries, in their wire-emitted order, for diagnostic
// messages that need to show which nodes were actually returned.
func nodeNamesFromView(in []apiv1.ResourceWithVolumes) []string {
	out := make([]string, 0, len(in))
	for i := range in {
		out = append(out, in[i].NodeName)
	}

	return out
}
