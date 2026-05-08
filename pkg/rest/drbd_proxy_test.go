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
	"net/http"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// TestDRBDProxyEndpointsRespond: every DRBD-proxy endpoint we expose
// returns 501 Not Implemented. Cozystack does not run DRBD proxy
// (everything is on the same flat L2), but the linstor CLI calls
// these and falling through to 404 is worse than an explicit 501.
func TestDRBDProxyEndpointsRespond(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/resource-definitions/pvc-1/drbd-proxy/enable/n1/n2"},
		{http.MethodPost, "/v1/resource-definitions/pvc-1/drbd-proxy/disable/n1/n2"},
		{http.MethodPut, "/v1/resource-definitions/pvc-1/drbd-proxy"},
	}

	for _, tc := range cases {
		req, _ := http.NewRequestWithContext(t.Context(), tc.method, base+tc.path, nil)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusNotImplemented {
			t.Errorf("%s %s: status %d, want 501", tc.method, tc.path, resp.StatusCode)
		}
	}
}
