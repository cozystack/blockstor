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
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// The wire spells a replica's node as it was registered, Node-1, while an
// operator, a URL or a pool may spell it node-1. Every path that picks a
// node's own replica out of a listing reads the two as one node.
func nodeSpellingStore(t *testing.T) store.Store {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, name := range []string{"Node-1", "Node-2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: name}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}
	}

	for _, node := range []string{"Node-1", "Node-2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "pvc-mix",
			NodeName: node,
			Props:    map[string]string{"StorPoolName": "pool-" + node},
			Volumes:  []apiv1.Volume{{VolumeNumber: 0, StoragePool: "data"}},
		}); err != nil {
			t.Fatalf("seed replica on %s: %v", node, err)
		}
	}

	return st
}

func TestPoolReferencesSeeAReplicaInAnotherNodeSpelling(t *testing.T) {
	t.Parallel()

	refs, err := (&Server{Store: nodeSpellingStore(t)}).referencingResources(t.Context(), "node-1", "data")
	if err != nil || len(refs) != 1 {
		t.Fatalf("references of data on node-1 = %v, %v; want the replica on Node-1", refs, err)
	}
}

func TestFindStoragePoolByNameMatchesAnotherNodeSpelling(t *testing.T) {
	t.Parallel()

	pools := &crdv1alpha1.StoragePoolList{Items: []crdv1alpha1.StoragePool{{
		Spec: crdv1alpha1.StoragePoolSpec{NodeName: "node-1", PoolName: "data"},
	}}}

	if findStoragePoolByName(pools, "Node-1", "data") == nil {
		t.Error("pool data created on node-1 not found for Node-1")
	}
}

func TestTakeoverPoolSkipsTheTakeoverNodeInAnotherSpelling(t *testing.T) {
	t.Parallel()

	pool, err := (&Server{Store: nodeSpellingStore(t)}).resolveTakeoverStorPool(t.Context(), "pvc-mix", "node-1")
	if err != nil || pool != "pool-Node-2" {
		t.Errorf("takeover pool for node-1 = %q, %v; want the sibling's pool-Node-2", pool, err)
	}
}

func TestPeerBumpSkipsTheRemovedNodeInAnotherSpelling(t *testing.T) {
	t.Parallel()

	st := nodeSpellingStore(t)
	(&Server{Store: st}).bumpPeerChangedOnSiblings(t.Context(), "pvc-mix", "node-1")

	removed, err := st.Resources().Get(t.Context(), "pvc-mix", "Node-1")
	if err != nil {
		t.Fatalf("get the removed row: %v", err)
	}

	if _, bumped := removed.Annotations[apiv1.PeerChangedAnnotation]; bumped {
		t.Error("the row of the removed node-1 was bumped as a sibling")
	}
}

// The removed replica itself is never waited on for an acknowledgment: with
// only that replica left the wait returns at once.
func TestPeerDeleteWaitSkipsTheRemovedNodeInAnotherSpelling(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "Node-1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-mix", NodeName: "Node-1"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	start := time.Now()
	(&Server{Store: st}).waitForPeerDeletionAcks(waitCtx, "pvc-mix", "node-1")

	if took := time.Since(start); took > time.Second {
		t.Errorf("waited %v for the removed replica's own acknowledgment", took)
	}
}

// A single-node create names the node twice, in the URL and in the body;
// two spellings of one node are not a conflict.
func TestSingleNodeCreateTakesTheBodyNodeInAnotherSpelling(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "Node-1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-mix"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-mix/resources/node-1",
		[]byte(`[{"resource":{"node_name":"Node-1","flags":["DISKLESS"]}}]`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("status 400 for body node Node-1 on URL node node-1")
	}
}

// A satellite reports its snapshot entry under its own node name, node-1,
// while the snapshot lists the node as registered, Node-1, and the replica
// listing does too. The snapshot is successful.
func TestSnapshotIsSuccessfulWithAnEntryInAnotherNodeSpelling(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-mix", NodeName: "Node-1"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	diskful, err := (&Server{Store: st}).diskfulPeerSet(ctx, "pvc-mix")
	if err != nil {
		t.Fatalf("diskfulPeerSet: %v", err)
	}

	for _, reporter := range []string{"node-1", "NODE-1"} {
		snap := &apiv1.Snapshot{
			Nodes:     []string{"Node-1"},
			Snapshots: []apiv1.SnapshotPerNode{{NodeName: reporter, CreateTimestamp: 1}},
		}

		if !isSnapshotSuccessful(snap, diskful) {
			t.Errorf("the snapshot taken on %s for Node-1 reads as not successful", reporter)
		}
	}
}

// A restore onto node-1 takes the pool of the source's replica on Node-1.
func TestRestorePoolIsTheSourceReplicasOnAnotherNodeSpelling(t *testing.T) {
	t.Parallel()

	byNode, _ := storPoolsByNodeFromSourceRD(t.Context(), nodeSpellingStore(t), "pvc-mix")
	if pool := byNode[naming.Key("node-1")]; pool != "pool-Node-1" {
		t.Errorf("pool for node-1 = %q, want pool-Node-1", pool)
	}
}

// A multi-node evacuate names the nodes as the operator spells them,
// node-1, while the store lists them, and their replicas, as registered,
// Node-1. Both guards read the two as one node.
func TestEvacuateGuardsSeeTheNodesInAnotherSpelling(t *testing.T) {
	t.Parallel()

	st := nodeSpellingStore(t)
	s := &Server{Store: st}
	evacuating := evacuatingSet([]string{"node-1", "node-2"})

	rec := httptest.NewRecorder()
	if s.checkEvacuateCandidate(t.Context(), rec, []string{"node-1", "node-2"}, evacuating) {
		t.Errorf("evacuating node-1 and node-2 found a candidate among Node-1 and Node-2")
	}

	inUse := true

	res, err := st.Resources().Get(t.Context(), "pvc-mix", "Node-1")
	if err != nil {
		t.Fatalf("get replica: %v", err)
	}

	res.State.InUse = &inUse
	if err := st.Resources().Update(t.Context(), &res); err != nil {
		t.Fatalf("mark replica in use: %v", err)
	}

	rec = httptest.NewRecorder()
	if s.checkEvacuateInUse(t.Context(), rec, evacuatingSet([]string{"NODE-1"})) {
		t.Errorf("evacuating NODE-1 passed over the in-use replica on Node-1")
	}
}

// The disabled set and the pools it filters come from two spellings, the
// node as registered, Node-1, and a pool created as node-1: advise and
// query-size-info leave out the pool of an EVICTED node either way.
func TestPoolPickersLeaveOutAnEvictedNodeInAnotherSpelling(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "Node-1", Flags: []string{apiv1.NodeFlagEvicted}}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	disabled, err := (&Server{Store: st}).disabledNodes(ctx)
	if err != nil {
		t.Fatalf("disabledNodes: %v", err)
	}

	for _, spelling := range []string{"node-1", "Node-1", "NODE-1"} {
		pools := []apiv1.StoragePool{{NodeName: spelling, StoragePoolName: "data", FreeCapacity: 100, TotalCapacity: 100}}

		if picks := pickAdvicePools(pools, &apiv1.AutoSelectFilter{}, disabled, 1); len(picks) != 0 {
			t.Errorf("advise picked the pool on %s of the EVICTED Node-1: %+v", spelling, picks)
		}

		if candidates, _, _ := collectCandidates(pools, disabled, nil); len(candidates) != 0 {
			t.Errorf("query-size-info counted the pool on %s of the EVICTED Node-1", spelling)
		}
	}
}

// A restore pins its autoplace to the snapshot's nodes, which the snapshot
// lists as registered, Node-3, while the node's pool was created as node-3.
// The restore still places there.
func TestARestorePlacesOnTheSnapshotNodeInAnotherSpelling(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	for i, name := range []string{"Node-1", "Node-2", "Node-3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: strings.ToLower(name), StoragePoolName: "data", ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: int64(3-i) * 100, TotalCapacity: 1000,
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", name, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src"}); err != nil {
		t.Fatalf("seed src: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{ResourceName: "src", Name: "snap", Nodes: []string{"Node-3"}}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	rd := &apiv1.ResourceDefinition{Name: "dst", Props: map[string]string{"BlockstorRestoreFromSnapshot": "src:snap"}}
	filter := &apiv1.AutoSelectFilter{PlaceCount: 1}
	constrainAutoplaceToSnapshotNodes(ctx, st, rd, filter)

	if _, _, err := placer.New(st).Place(ctx, "dst", filter); err != nil {
		t.Fatalf("Place: %v", err)
	}

	got, err := st.Resources().ListByDefinition(ctx, "dst")
	if err != nil || len(got) != 1 || !strings.EqualFold(got[0].NodeName, "node-3") {
		t.Fatalf("restore placed %+v, %v; want one replica on Node-3", got, err)
	}
}

// A snapshot's target set lists its nodes as registered, Node-1, and a
// replica may spell its node node-1: the non-snapshot pool under it is still
// in scope and refuses the snapshot.
func TestNonSnapshotPoolCheckScopesTheTargetsInAnotherSpelling(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "Node-1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName: "node-1", StoragePoolName: "thick", ProviderKind: apiv1.StoragePoolKindLVM,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	replicas := []apiv1.Resource{{Name: "pvc-mix", NodeName: "node-1", Props: map[string]string{storPoolPropKey: "thick"}}}

	locs := (&Server{Store: st}).nonSnapshotPoolLocations(ctx, replicas, []string{"Node-1"})
	if len(locs) != 1 {
		t.Fatalf("offenders = %v, want the thick pool on node-1", locs)
	}
}
