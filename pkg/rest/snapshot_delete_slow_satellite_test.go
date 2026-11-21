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
	"io"
	"net/http"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// The budget the tests below give the snapshot delete: short, so a drain
// that outlasts it does not make the suite wait 10s.
const slowSatelliteBudget = time.Second

// startSlowSatelliteServer seeds a definition with one snapshot on two
// nodes in the given connection state and serves it with the short
// budget.
func startSlowSatelliteServer(t *testing.T, clearAfter time.Duration, secondNode string) string {
	t.Helper()

	st := newStuckSnapshotStore(clearAfter)

	for _, node := range []apiv1.Node{
		{Name: "n1", Type: "SATELLITE", ConnectionStatus: apiv1.NodeTypeOnline},
		{Name: "n2", Type: "SATELLITE", ConnectionStatus: secondNode},
	} {
		err := st.inner.Nodes().Create(t.Context(), &node)
		if err != nil {
			t.Fatalf("seed node %s: %v", node.Name, err)
		}
	}

	err := st.inner.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "rd1"})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	err = st.inner.Snapshots().Create(t.Context(), &apiv1.Snapshot{
		Name:         "snap1",
		ResourceName: "rd1",
		Nodes:        []string{"n1", "n2"},
	})
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

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete,
		base+"/v1/resource-definitions/rd1/snapshots/snap1", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	start := time.Now()

	// A client deadline well past the budget, so a delete that keeps
	// waiting on the offline node fails here instead of hanging the suite.
	resp, err := (&http.Client{Timeout: 5 * slowSatelliteBudget}).Do(req)
	if err != nil {
		t.Fatalf("delete did not answer within %v: %v", 5*slowSatelliteBudget, err)
	}
	defer func() { _ = resp.Body.Close() }()

	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusGatewayTimeout {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 504 (body=%s)", resp.StatusCode, body)
	}

	if elapsed > slowSatelliteBudget+2*time.Second {
		t.Errorf("DELETE returned in %v; want about the %v budget", elapsed, slowSatelliteBudget)
	}
}
