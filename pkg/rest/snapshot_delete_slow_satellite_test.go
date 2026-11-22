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
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The budget the tests below give the snapshot delete: short, so a drain
// that outlasts it does not make the suite wait 10s.
const slowSatelliteBudget = time.Second

// startSlowSatelliteServer seeds a definition with one snapshot on two
// nodes, the second in the given connection state, and serves it with
// the short budget.
func startSlowSatelliteServer(t *testing.T, clearAfter time.Duration, secondNode string) string {
	t.Helper()

	return startSnapshotDeleteServer(t, clearAfter, []apiv1.Node{
		{Name: "n1", Type: "SATELLITE", ConnectionStatus: apiv1.NodeTypeOnline},
		{Name: "n2", Type: "SATELLITE", ConnectionStatus: secondNode},
	}, apiv1.Snapshot{Nodes: []string{"n1", "n2"}})
}

// startSnapshotDeleteServer seeds the given nodes and a definition rd1
// with the snapshot snap1 shaped by snap, and serves them with the
// short budget.
func startSnapshotDeleteServer(t *testing.T, clearAfter time.Duration, nodes []apiv1.Node, snap apiv1.Snapshot) string {
	t.Helper()

	st := newStuckSnapshotStore(clearAfter)

	for _, node := range nodes {
		err := st.inner.Nodes().Create(t.Context(), &node)
		if err != nil {
			t.Fatalf("seed node %s: %v", node.Name, err)
		}
	}

	err := st.inner.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "rd1"})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	snap.Name = "snap1"
	snap.ResourceName = "rd1"

	err = st.inner.Snapshots().Create(t.Context(), &snap)
	if err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	base, stop := startServerCustom(t, &Server{
		Addr:                 pickFreeAddr(t),
		Store:                st,
		Client:               newFakeRESTClient(t),
		Namespace:            testRESTNamespace,
		snapshotDeleteBudget: slowSatelliteBudget,
	})
	t.Cleanup(stop)

	return base
}

// deleteSnap1 sends the snapshot delete with a client deadline well past
// the budget, so a delete that keeps waiting fails the test instead of
// hanging the suite, and reports how long the answer took. The caller
// closes the body.
func deleteSnap1(t *testing.T, base string) (*http.Response, time.Duration) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete,
		base+"/v1/resource-definitions/rd1/snapshots/snap1", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	start := time.Now()

	resp, err := (&http.Client{Timeout: 5 * slowSatelliteBudget}).Do(req)
	if err != nil {
		t.Fatalf("delete did not answer within %v: %v", 5*slowSatelliteBudget, err)
	}

	return resp, time.Since(start)
}

// TestSnapshotDeleteWaitsOutASlowOnlineSatellite: the satellites are
// online but take longer than the budget to delete their copies, as
// they did under a burst of deletes on CPU-limited pods. LINSTOR answers
// the delete once they are done; a 504 here made linstor-csi skip the
// removal of the deleted volume's definition for good, because its
// retry found the snapshot being deleted and never sent the delete
// again.
func TestSnapshotDeleteWaitsOutASlowOnlineSatellite(t *testing.T) {
	base := startSlowSatelliteServer(t, 3*slowSatelliteBudget, apiv1.NodeTypeOnline)

	resp := httpDelete(t, base+"/v1/resource-definitions/rd1/snapshots/snap1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200 once the satellites are done (body=%s)", resp.StatusCode, body)
	}

	rcs, raw := readEnvelope(t, resp)
	if len(rcs) == 0 || rcs[0].RetCode != maskInfo {
		t.Fatalf("envelope: got %s, want the snapshot deleted success", raw)
	}

	get, err := http.Get(base + "/v1/resource-definitions/rd1/snapshots/snap1") //nolint:noctx // test
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	_ = get.Body.Close()

	if get.StatusCode != http.StatusNotFound {
		t.Errorf("snapshot after the answer: got %d, want 404", get.StatusCode)
	}
}

// TestSnapshotDeleteGivesUpOnAnOfflineNode keeps Bug 193's answer: a
// node the snapshot lists is offline, so its copy will not go, and the
// delete answers 504 once the budget is spent instead of hanging.
func TestSnapshotDeleteGivesUpOnAnOfflineNode(t *testing.T) {
	base := startSlowSatelliteServer(t, 0, apiv1.NodeTypeOffline)

	resp, elapsed := deleteSnap1(t, base)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusGatewayTimeout {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 504 (body=%s)", resp.StatusCode, body)
	}

	if elapsed > slowSatelliteBudget+2*time.Second {
		t.Errorf("DELETE returned in %v; want about the %v budget", elapsed, slowSatelliteBudget)
	}
}

// TestSnapshotDeleteGivesUpOnAnUnknownNode: the snapshot lists a node
// the store no longer has (deleted or lost without a cascade), so no
// satellite of that name will take the finalizer off. The delete must
// answer 504 once the budget is spent, as for an offline node, instead
// of counting the missing node as online and waiting for good.
func TestSnapshotDeleteGivesUpOnAnUnknownNode(t *testing.T) {
	base := startSnapshotDeleteServer(t, 0, []apiv1.Node{
		{Name: "n1", Type: "SATELLITE", ConnectionStatus: apiv1.NodeTypeOnline},
	}, apiv1.Snapshot{Nodes: []string{"n1", "gone"}})

	resp, elapsed := deleteSnap1(t, base)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusGatewayTimeout {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 504 (body=%s)", resp.StatusCode, body)
	}

	if elapsed > slowSatelliteBudget+2*time.Second {
		t.Errorf("DELETE returned in %v; want about the %v budget", elapsed, slowSatelliteBudget)
	}
}

// TestSnapshotDeleteAnswersASatelliteFailure: the satellites are online,
// but one of them failed to delete its copy (lvremove refused, pool in
// error). LINSTOR answers the delete with the error its satellite
// returned; waiting here, with the heartbeat alive, hung the call until
// the client gave up and told it nothing about why.
func TestSnapshotDeleteAnswersASatelliteFailure(t *testing.T) {
	base := startSnapshotDeleteServer(t, 0, []apiv1.Node{
		{Name: "n1", Type: "SATELLITE", ConnectionStatus: apiv1.NodeTypeOnline},
		{Name: "n2", Type: "SATELLITE", ConnectionStatus: apiv1.NodeTypeOnline},
	}, apiv1.Snapshot{
		Nodes: []string{"n1", "n2"},
		DeleteFailures: []apiv1.SnapshotDeleteFailure{
			{NodeName: "n2", Message: "logical volume vg/rd1_snap1_00000 in use"},
		},
	})

	resp, elapsed := deleteSnap1(t, base)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 500 with the satellite's error (body=%s)", resp.StatusCode, body)
	}

	if elapsed > slowSatelliteBudget {
		t.Errorf("DELETE returned in %v; want the failure answered before the %v budget", elapsed, slowSatelliteBudget)
	}

	rcs, raw := readEnvelope(t, resp)
	if len(rcs) != 1 || rcs[0].RetCode&apiCallRcError != apiCallRcError ||
		rcs[0].ObjRefs[objRefNode] != "n2" || !strings.Contains(rcs[0].Cause, "in use") {
		t.Errorf("envelope: got %s, want one error for n2 carrying its message", raw)
	}
}

// errSnapshotStoreUnreachable stands for a read that cannot reach the store.
var errSnapshotStoreUnreachable = errors.New("apiserver unreachable")

// failingSnapshots answers every Get with err.
type failingSnapshots struct {
	store.SnapshotStore

	err error
}

func (f failingSnapshots) Get(context.Context, string, string) (apiv1.Snapshot, error) {
	return apiv1.Snapshot{}, f.err
}

type failingSnapshotStore struct {
	store.Store

	snapshots failingSnapshots
}

func (f failingSnapshotStore) Snapshots() store.SnapshotStore { return f.snapshots }

// TestSnapshotDrainIsNotProvedByAFailedRead: only a NotFound shows the
// snapshot went. A read cut short by the request ending (server shutdown
// while the client is still connected) or one that keeps failing must
// not let the delete answer 200 "snapshot deleted" with the snapshot
// still there.
func TestSnapshotDrainIsNotProvedByAFailedRead(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	for name, tc := range map[string]struct {
		ctx context.Context //nolint:containedctx // table input
		err error
	}{
		"canceled request": {ctx: canceled, err: context.Canceled},
		"failing read":     {ctx: t.Context(), err: errSnapshotStoreUnreachable},
	} {
		t.Run(name, func(t *testing.T) {
			srv := &Server{
				Store:                failingSnapshotStore{Store: store.NewInMemory(), snapshots: failingSnapshots{err: tc.err}},
				snapshotDeleteBudget: 100 * time.Millisecond,
			}

			outcome, _ := srv.waitForSnapshotDrained(tc.ctx, "rd1", "snap1")
			if outcome == snapshotWaitGone {
				t.Errorf("a %s was taken for the snapshot being gone", name)
			}
		})
	}
}
