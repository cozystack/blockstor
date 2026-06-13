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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 038 (CI regression): the clone / restore data plane reworked the
// placement path to materialise restore-marked replicas synchronously.
// An earlier iteration ALSO gated the REST handler on per-node snapshot
// readiness (a non-zero satellite-set CreateTimestamp) and BLOCKED inside
// the handler until it landed. That deadlocks envtest — there is no
// satellite to stamp the timestamp, so the handler blocked until the
// client deadline and `TestGroupG/SnapRestoreSnapshotHolderOnly` failed
// with `POST .../snapshot-restore-resource: context deadline exceeded`.
//
// It is also a real-world hazard: a failed / never-Ready snapshot would
// hang every restore POST until the operator's timeout. The correct
// race fix lives on the satellite (materializeVolume requeues on a
// not-yet-local @snap before conceding to a blank create), so the
// handler MUST return promptly without waiting on satellite-set status.
//
// These tests pin that contract: with a snapshot whose per-node
// CreateTimestamp is ZERO (never materialised — exactly the satellite-
// less envtest shape), both the restore POST and the clone POST must
// return their success envelope within a tight deadline. A reintroduced
// blocking wait on snapshot readiness would blow the context deadline
// and fail these tests, just as it failed CI.

// restorePromptDeadline bounds how long the handler may take. The
// handler does only synchronous Store writes, so a healthy path is
// sub-second; a generous-but-finite budget catches a reintroduced
// blocking wait (the removed gate's production default was 90s) without
// flaking on a loaded CI box.
const restorePromptDeadline = 5 * time.Second

// httpPostWithDeadline POSTs body to addr under a context bounded by
// restorePromptDeadline and fails the test if the request does not
// complete in time (the deadline-exceeded surfaces as a transport error
// on Do, which is precisely the "handler hung" failure mode CI saw).
func httpPostWithDeadline(t *testing.T, addr string, body []byte) *http.Response {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), restorePromptDeadline)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("restore POST did not return within %v: %v — the handler must "+
			"NOT block on satellite-set snapshot readiness (Bug 038 CI hang)",
			restorePromptDeadline, err)
	}

	return resp
}

// seedRestoreSourceWithNeverReadySnapshot stamps a 2-node VD-bearing
// source RD, its diskful replicas, and a snapshot whose per-node entries
// carry a ZERO CreateTimestamp — i.e. the satellite has NOT (and in a
// satellite-less store never will) stamp the readiness timestamp the
// removed gate waited on.
func seedRestoreSourceWithNeverReadySnapshot(t *testing.T, st store.Store, srcRD, snapName string) {
	t.Helper()

	ctx := t.Context()
	nodes := []string{"n1", "n2"}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: srcRD}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, srcRD, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      64 * 1024,
	}); err != nil {
		t.Fatalf("seed source VD: %v", err)
	}

	for _, n := range nodes {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     srcRD,
			NodeName: n,
			Props:    map[string]string{"StorPoolName": "stand"},
		}); err != nil {
			t.Fatalf("seed source replica on %s: %v", n, err)
		}
	}

	perNode := make([]apiv1.SnapshotPerNode, 0, len(nodes))
	for _, n := range nodes {
		perNode = append(perNode, apiv1.SnapshotPerNode{
			SnapshotName:    snapName,
			NodeName:        n,
			CreateTimestamp: 0, // satellite never stamps it here
		})
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         snapName,
		ResourceName: srcRD,
		Nodes:        nodes,
		Snapshots:    perNode,
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
}

// TestBug038RestoreHandlerReturnsPromptlyWithoutSatellite is the L1
// guard for the CI regression: an explicit holder-node restore against a
// snapshot whose per-node CreateTimestamp never materialises must return
// 201 within restorePromptDeadline. A blocking readiness wait would hang
// the POST until the deadline and fail here (the same way CI failed).
func TestBug038RestoreHandlerReturnsPromptlyWithoutSatellite(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRestoreSourceWithNeverReadySnapshot(t, st, "src-prompt", "snap-prompt")

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"to_resource":   "dst-prompt",
		"snapshot_name": "snap-prompt",
		"node_names":    []string{"n1"}, // n1 holds the snapshot
	})

	resp := httpPostWithDeadline(t, base+"/v1/resource-definitions/src-prompt/snapshot-restore-resource", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("restore status: got %d, want 201 (handler must stamp the "+
			"replica synchronously and return — the satellite requeue carries "+
			"the not-yet-ready snapshot race)", resp.StatusCode)
	}

	// The replica must have been stamped synchronously by the handler —
	// the satellite's job is only to materialise the on-disk clone later.
	got, err := st.Resources().ListByDefinition(t.Context(), "dst-prompt")
	if err != nil {
		t.Fatalf("list restored replicas: %v", err)
	}

	if len(got) != 1 || got[0].NodeName != "n1" {
		t.Fatalf("restored replicas: got %+v, want exactly one on n1", got)
	}
}

// TestBug038CloneHandlerReturnsPromptlyWithoutSatellite is the clone-path
// twin: `rd clone` routes through the same eager-place path
// (materializeRestoredRD → placeRestoredResources) and must likewise
// return its CloneStarted envelope promptly with a never-Ready snapshot.
func TestBug038CloneHandlerReturnsPromptlyWithoutSatellite(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Online nodes + a snapshot-capable pool so the clone's internal
	// snapshot preconditions pass; the clone then takes its own snapshot
	// (CreateTimestamp zero on the in-memory store) and restores from it.
	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, ConnectionStatus: "ONLINE"}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName:  "stand",
			NodeName:         n,
			ProviderKind:     apiv1.StoragePoolKindZFSThin,
			SupportsSnapshot: true,
			FreeCapacity:     100000,
			TotalCapacity:    1000000,
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src-clone"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "src-clone", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      64 * 1024,
	}); err != nil {
		t.Fatalf("seed source VD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "src-clone",
			NodeName: n,
			Props:    map[string]string{"StorPoolName": "stand"},
		}); err != nil {
			t.Fatalf("seed source replica on %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"name":          "dst-clone",
		"use_zfs_clone": true,
	})

	resp := httpPostWithDeadline(t, base+"/v1/resource-definitions/src-clone/clone", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("clone status: got %d, want 201 (clone POST must stamp eagerly "+
			"and return — no blocking wait on snapshot readiness)", resp.StatusCode)
	}

	// The clone stamps one replica per snapshot node synchronously.
	got, err := st.Resources().ListByDefinition(t.Context(), "dst-clone")
	if err != nil {
		t.Fatalf("list clone replicas: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("clone replicas: got %d, want 2 (one per snapshot node)", len(got))
	}
}
