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
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedRDWithReplicas seeds an RD plus one diskful Resource replica per
// node, and a Node CRD per node with the given connection status. Used
// by the offline-precheck tests to control which targets are reachable.
func seedRDWithReplicas(t *testing.T, st store.Store, rd string, nodeStatus map[string]string) {
	t.Helper()

	ctx := context.Background()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("seed RD %s: %v", rd, err)
	}

	for node, status := range nodeStatus {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:             node,
			Type:             apiv1.NodeTypeSatellite,
			ConnectionStatus: status,
		}); err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: rd, NodeName: node}); err != nil {
			t.Fatalf("seed resource %s/%s: %v", rd, node, err)
		}
	}
}

// TestSnapshotCreateRefusesOfflineTarget pins the fail-fast offline
// pre-check on the per-RD create path: when a targeted diskful node is
// OFFLINE the handler refuses with 503 and does NOT persist the
// Snapshot — so SuspendIo is never stamped and the reachable replicas
// are never frozen for a snapshot that cannot complete. Mirrors
// upstream getOfflineNodes.
func TestSnapshotCreateRefusesOfflineTarget(t *testing.T) {
	st := store.NewInMemory()
	seedRDWithReplicas(t, st, "pvc-1", map[string]string{
		"n1": apiv1.NodeTypeOnline,
		"n2": apiv1.NodeTypeOffline,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"name":"snap-1"}`)
	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", resp.StatusCode)
	}

	bodyBytes := readBody(t, resp)
	if !strings.Contains(bodyBytes, "n2") {
		t.Errorf("refusal message should name the offline node n2, got: %s", bodyBytes)
	}

	// CRITICAL: the Snapshot must NOT have been persisted — no freeze
	// was started.
	snaps, err := st.Snapshots().ListByDefinition(context.Background(), "pvc-1")
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}

	if len(snaps) != 0 {
		t.Errorf("offline-refused snapshot was persisted (SuspendIo would freeze peers): %d rows", len(snaps))
	}
}

// TestSnapshotCreateAllowsAllOnlineTargets pins the negative case: when
// every targeted node is online the create proceeds and persists,
// confirming the offline gate does not regress the happy path.
func TestSnapshotCreateAllowsAllOnlineTargets(t *testing.T) {
	st := store.NewInMemory()
	seedRDWithReplicas(t, st, "pvc-1", map[string]string{
		"n1": apiv1.NodeTypeOnline,
		"n2": apiv1.NodeTypeOnline,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"name":"snap-1"}`)
	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshots", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	snaps, err := st.Snapshots().ListByDefinition(context.Background(), "pvc-1")
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}

	if len(snaps) != 1 {
		t.Errorf("all-online snapshot not persisted: %d rows", len(snaps))
	}
}

// TestSnapshotCreateMultiRefusesOfflineEntry pins the offline pre-check
// on the multi-create batch path: the entry whose target is offline
// gets an error ApiCallRc and is NOT persisted, while a sibling entry
// whose targets are all online still succeeds (best-effort batch).
func TestSnapshotCreateMultiRefusesOfflineEntry(t *testing.T) {
	st := store.NewInMemory()
	// pvc-a: target offline; pvc-b: target online.
	seedRDWithReplicas(t, st, "pvc-a", map[string]string{"n1": apiv1.NodeTypeOffline})
	seedRDWithReplicas(t, st, "pvc-b", map[string]string{"n2": apiv1.NodeTypeOnline})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"snapshots":[
		{"resource_name":"pvc-a","name":"snap-a"},
		{"resource_name":"pvc-b","name":"snap-b"}
	]}`)

	resp := httpPost(t, base+"/v1/actions/snapshot/multi", body)
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(rcs) != 2 {
		t.Fatalf("ApiCallRc count: got %d, want 2", len(rcs))
	}

	// Entry 0 (pvc-a, offline target) must be an error naming n1.
	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("pvc-a entry should carry an error rc, got %#v", rcs[0])
	}

	if !strings.Contains(rcs[0].Message, "n1") {
		t.Errorf("pvc-a error should name offline node n1, got %q", rcs[0].Message)
	}

	ctx := context.Background()

	aSnaps, _ := st.Snapshots().ListByDefinition(ctx, "pvc-a")
	if len(aSnaps) != 0 {
		t.Errorf("offline-refused multi entry pvc-a was persisted: %d rows", len(aSnaps))
	}

	bSnaps, _ := st.Snapshots().ListByDefinition(ctx, "pvc-b")
	if len(bSnaps) != 1 {
		t.Errorf("online multi entry pvc-b not persisted: %d rows", len(bSnaps))
	}
}

// readBody slurps a response body to a string for substring assertions.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()

	var sb strings.Builder

	buf := make([]byte, 4096)

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}

		if err != nil {
			break
		}
	}

	return sb.String()
}
