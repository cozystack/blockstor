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

	"github.com/cozystack/blockstor/pkg/store"
)

// TestNodeConnectionsReturnEmpty pins the contract: list and per-pair
// node-connections both 200 with `[]`. golinstor's polling loop logs an
// error for any non-200, so the empty-but-present shape is what keeps
// the controller log clean for cozystack's flat-L2 setup where there
// are no inter-satellite tunnels to report.
func TestNodeConnectionsReturnEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	for _, path := range []string{
		"/v1/node-connections",
		"/v1/node-connections/n1/n2",
	} {
		resp := httpGet(t, base+path)

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			t.Errorf("%s status: got %d, want 200", path, resp.StatusCode)

			continue
		}

		var got []map[string]any

		err := json.NewDecoder(resp.Body).Decode(&got)
		_ = resp.Body.Close()

		if err != nil {
			t.Errorf("%s decode: %v", path, err)

			continue
		}

		if len(got) != 0 {
			t.Errorf("%s body: got %d entries, want 0", path, len(got))
		}
	}
}
