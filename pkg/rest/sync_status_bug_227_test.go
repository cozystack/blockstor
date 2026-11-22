// SPDX-License-Identifier: Apache-2.0

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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 227 (P3) — `GET /v1/resource-definitions/{rd}/sync-status` was
// missing. Upstream LINSTOR exposes the endpoint to answer
// "is every replica of <rd> done resyncing?" with a single boolean
// (`synced_on_all`) — Java
// `controller/.../ResourceDefinitions.java:getSyncStatus` and
// `JsonGenTypes.ResourceDefinitionSyncStatus`. Driven by the python
// CLI's `linstor rd sync-status` and used by the snapshot-shipping
// flow to gate on "every peer is UpToDate before we take the snap".
//
// blockstor answers by the rule of LINSTOR's isResourceSynced:
// diskless replicas (tie-breakers included) are skipped, a replica
// with no known state counts as synced, and every other replica must
// report UpToDate on every volume.

// TestBug227SyncStatusAllUpToDate: every replica's DrbdState is
// `UpToDate` → `synced_on_all` is true. Pre-fix 404s.
func TestBug227SyncStatusAllUpToDate(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-227"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, node := range []string{"n1", "n2", "n3"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "rd-227",
			NodeName: node,
			State:    apiv1.ResourceState{DrbdState: "UpToDate"},
		}); err != nil {
			t.Fatalf("seed resource %s: %v", node, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd-227/sync-status")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got struct {
		SyncedOnAll bool `json:"synced_on_all"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if !got.SyncedOnAll {
		t.Errorf("synced_on_all: got false, want true (every replica is UpToDate)")
	}
}

// TestBug227SyncStatusOneSyncTarget: one replica still in
// `SyncTarget` → `synced_on_all` must be false. The python CLI's
// snapshot pre-check relies on this to refuse a snapshot of a
// resync-in-progress RD.
func TestBug227SyncStatusOneSyncTarget(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-227"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd-227", NodeName: "n1",
		State: apiv1.ResourceState{DrbdState: "UpToDate"},
	}); err != nil {
		t.Fatalf("seed n1: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd-227", NodeName: "n2",
		State: apiv1.ResourceState{DrbdState: "SyncTarget"},
	}); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd-227/sync-status")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got struct {
		SyncedOnAll bool `json:"synced_on_all"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if got.SyncedOnAll {
		t.Errorf("synced_on_all: got true, want false (n2 is SyncTarget)")
	}
}

// TestBug227SyncStatusUnknownRD: an unknown RD must 404.
func TestBug227SyncStatusUnknownRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/ghost-rd/sync-status")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

func getSyncedOnAll(t *testing.T, url string) bool {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got struct {
		SyncedOnAll bool `json:"synced_on_all"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	return got.SyncedOnAll
}

// restartSatellite brings n2 back ONLINE under a new satellite process,
// leaving its replica stamped by the given incarnation.
func restartSatellite(t *testing.T, backend store.Store, replicaIncarnation string) {
	t.Helper()

	ctx := t.Context()

	node, err := backend.Nodes().Get(ctx, "n2")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	node.ConnectionStatus = apiv1.NodeTypeOnline
	node.SatelliteIncarnation = "inc-2"

	if err := backend.Nodes().Update(ctx, &node); err != nil {
		t.Fatalf("bring node back: %v", err)
	}

	rsc, err := backend.Resources().Get(ctx, "pvc-x", "n2")
	if err != nil {
		t.Fatalf("get replica: %v", err)
	}

	rsc.SatelliteIncarnation = replicaIncarnation

	if err := backend.Resources().Update(ctx, &rsc); err != nil {
		t.Fatalf("stamp incarnation: %v", err)
	}
}

// markInconsistent stores Inconsistent as the last state n2's
// satellite reported for its replica.
func markInconsistent(t *testing.T, backend store.Store) {
	t.Helper()

	ctx := t.Context()

	rsc, err := backend.Resources().Get(ctx, "pvc-x", "n2")
	if err != nil {
		t.Fatalf("get replica: %v", err)
	}

	rsc.State.DrbdState = "Inconsistent"
	rsc.Volumes = []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "Inconsistent"}}}

	if err := backend.Resources().Update(ctx, &rsc); err != nil {
		t.Fatalf("store Inconsistent: %v", err)
	}
}

// LINSTOR answers an OFFLINE node's replica with no state, and
// isResourceSynced counts a replica with no state as synced. The
// Inconsistent the dead satellite last reported must not decide the
// answer, or evacuation of that node waits forever.
func TestBug227SyncStatusOfflineReplicaIsSynced(t *testing.T) {
	backend := seedOfflinePeer(t)
	markInconsistent(t, backend)

	base, stop := startServerWithStore(t, backend)
	defer stop()

	if !getSyncedOnAll(t, base+"/v1/resource-definitions/pvc-x/sync-status") {
		t.Errorf("synced_on_all: got false, want true (n2 is OFFLINE, its state is unknown)")
	}
}

// After a satellite restart the stored state comes from the previous
// process until the new one reports the replica. Until then the state
// is unknown and counts as synced; once the new process has reported
// Inconsistent, the replica is not synced.
func TestBug227SyncStatusFollowsSatelliteIncarnation(t *testing.T) {
	cases := []struct {
		name       string
		reportedBy string
		want       bool
	}{
		{name: "reported by the previous satellite", reportedBy: "inc-1", want: true},
		{name: "reported by the running satellite", reportedBy: "inc-2", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := seedOfflinePeer(t)
			markInconsistent(t, backend)
			restartSatellite(t, backend, tc.reportedBy)

			base, stop := startServerWithStore(t, backend)
			defer stop()

			if got := getSyncedOnAll(t, base+"/v1/resource-definitions/pvc-x/sync-status"); got != tc.want {
				t.Errorf("synced_on_all: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestBug227SyncStatusReplicaRule pins replicaSynced, LINSTOR's
// allVolumesUpToDate(defaultIfUnknown=true) over the diskful
// replicas, one replica shape per case next to an UpToDate peer.
func TestBug227SyncStatusReplicaRule(t *testing.T) {
	upToDate := apiv1.VolumeState{DiskState: "UpToDate"}

	cases := []struct {
		name    string
		replica apiv1.Resource
		want    bool
	}{
		{
			name:    "never reported",
			replica: apiv1.Resource{},
			want:    true,
		},
		{
			name: "diskless tie-breaker with no state",
			replica: apiv1.Resource{
				Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
			},
			want: true,
		},
		{
			name: "diskless reporting Diskless",
			replica: apiv1.Resource{
				Flags:   []string{apiv1.ResourceFlagDiskless},
				State:   apiv1.ResourceState{DrbdState: "Diskless"},
				Volumes: []apiv1.Volume{{State: apiv1.VolumeState{DiskState: "Diskless"}}},
			},
			want: true,
		},
		{
			name: "tie-breaker flag alone",
			replica: apiv1.Resource{
				Flags: []string{apiv1.ResourceFlagTieBreaker},
				State: apiv1.ResourceState{DrbdState: "Diskless"},
			},
			want: true,
		},
		{
			name: "diskful with a detached disk",
			replica: apiv1.Resource{
				State:   apiv1.ResourceState{DrbdState: "Diskless"},
				Volumes: []apiv1.Volume{{State: apiv1.VolumeState{DiskState: "Diskless"}}},
			},
			want: false,
		},
		{
			name: "one volume of two not UpToDate",
			replica: apiv1.Resource{
				State: apiv1.ResourceState{DrbdState: "UpToDate"},
				Volumes: []apiv1.Volume{
					{VolumeNumber: 0, State: upToDate},
					{VolumeNumber: 1, State: apiv1.VolumeState{DiskState: "Inconsistent"}},
				},
			},
			want: false,
		},
		{
			name: "one volume reported, one not yet",
			replica: apiv1.Resource{
				Volumes: []apiv1.Volume{
					{VolumeNumber: 0, State: upToDate},
					{VolumeNumber: 1},
				},
			},
			want: true,
		},
		{
			name: "disk state compared without case",
			replica: apiv1.Resource{
				Volumes: []apiv1.Volume{{State: apiv1.VolumeState{DiskState: "uptodate"}}},
			},
			want: true,
		},
		{
			name: "only the resource-level state reported",
			replica: apiv1.Resource{
				State: apiv1.ResourceState{DrbdState: "Outdated"},
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-227"}); err != nil {
				t.Fatalf("seed RD: %v", err)
			}

			peer := apiv1.Resource{
				Name: "rd-227", NodeName: "n1",
				State:   apiv1.ResourceState{DrbdState: "UpToDate"},
				Volumes: []apiv1.Volume{{State: upToDate}},
			}
			if err := st.Resources().Create(ctx, &peer); err != nil {
				t.Fatalf("seed n1: %v", err)
			}

			replica := tc.replica
			replica.Name = "rd-227"
			replica.NodeName = "n2"

			if err := st.Resources().Create(ctx, &replica); err != nil {
				t.Fatalf("seed n2: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			if got := getSyncedOnAll(t, base+"/v1/resource-definitions/rd-227/sync-status"); got != tc.want {
				t.Errorf("synced_on_all: got %v, want %v", got, tc.want)
			}
		})
	}
}
