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
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestREST128UnsupportedAnswer501: the REST 1.28 endpoints blockstor does
// not serve answer 501 with a LINSTOR error envelope, which python-linstor
// prints, rather than a 404 once their client-side gates are open.
func TestREST128UnsupportedAnswer501(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/controller/database/export", `{"backup_name":"x"}`},
		{http.MethodPost, "/v1/controller/auth/initialize-token-auth", `{"only_satellites":false}`},
		{http.MethodGet, "/v1/controller/auth/token", ""},
		{http.MethodPost, "/v1/controller/auth/token", `{"description":"x"}`},
		{http.MethodPut, "/v1/controller/auth/token/1", `{"is_active":false}`},
		{http.MethodDelete, "/v1/controller/auth/token/1", ""},
		{http.MethodPost, "/v1/nodes/exec/drbd-reactorctl/status", `{"nodes":["n1"]}`},
		{http.MethodPost, "/v1/nodes/exec/drbd-reactorctl/evict", `{"nodes":["n1"],"resource":"r"}`},
	} {
		req, err := http.NewRequestWithContext(t.Context(), tc.method, base+tc.path, bytes.NewBufferString(tc.body))
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}

		if resp.StatusCode != http.StatusNotImplemented {
			t.Errorf("%s %s: status %d, want 501", tc.method, tc.path, resp.StatusCode)
		}

		if rcs := decodeRCs(t, resp); rcs[0].RetCode != apiCallRcError {
			t.Errorf("%s %s: ret_code %#x, want an error", tc.method, tc.path, rcs[0].RetCode)
		}

		_ = resp.Body.Close()
	}
}

// TestDrbdClientIsRefusedAndFalseAccepted: drbd_client (REST 1.28) on
// resource create and modify. True asks for a replica blockstor cannot
// render; false is what every replica here already is.
func TestDrbdClientIsRefusedAndFalseAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedOnlineNodeWithPool(t, st, "n1")
	seedOnlineNodeWithPool(t, st, "n2")
	seedRDWithVD(t, st, "pvc-client", 1024)

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{Name: "pvc-client", NodeName: "n1"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-client/resources",
		[]byte(`[{"resource":{"node_name":"n2","flags":["DRBD_DISKLESS"]},"drbd_client":true}]`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("create with drbd_client=true: status %d, want 501", resp.StatusCode)
	}

	for _, tc := range []struct {
		body   string
		status int
	}{
		{`{"drbd_client":true}`, http.StatusNotImplemented},
		{`{"drbd_tiebreaker":true}`, http.StatusNotImplemented},
		{`{"drbd_client":false,"override_props":{"Aux/x":"y"}}`, http.StatusOK},
	} {
		resp := httpPut(t, base+"/v1/resource-definitions/pvc-client/resources/n1", []byte(tc.body))
		_ = resp.Body.Close()

		if resp.StatusCode != tc.status {
			t.Errorf("modify %s: status %d, want %d", tc.body, resp.StatusCode, tc.status)
		}
	}
}
