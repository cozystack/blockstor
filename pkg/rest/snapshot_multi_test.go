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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// TestSnapshotMultiReturns501 pins the multi-RD snapshot endpoint as
// explicitly out-of-scope. golinstor expects a deterministic
// non-success here; falling through to 404 (the previous behaviour)
// is harder to distinguish from an unknown-route bug.
func TestSnapshotMultiReturns501(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/actions/snapshot/multi",
		[]byte(`{"resource_definitions":["pvc-1"],"snapshot_name":"snap-1"}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("status: got %d, want 501", resp.StatusCode)
	}

	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json prefix", got)
	}
}
