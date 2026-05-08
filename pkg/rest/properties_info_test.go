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

// TestPropertiesInfoControllerReturns200: linstor CLI hits this in
// `controller list-properties --info`. Empty array is acceptable —
// it just shouldn't 404.
func TestPropertiesInfoControllerReturns200(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	for _, path := range propertiesInfoPaths() {
		resp := httpGet(t, base+path)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: status %d, want 200", path, resp.StatusCode)
		}
	}
}

// TestPropertiesInfoShape: each entry has at least name+namespace.
func TestPropertiesInfoShape(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/controller/properties/info")
	defer func() { _ = resp.Body.Close() }()

	var got []map[string]any

	err := json.NewDecoder(resp.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	// We don't ship a populated property catalogue yet — empty array is
	// the expected shape. Once the catalogue lands we'll assert per-entry
	// shape here.
	if got == nil {
		t.Errorf("expected JSON array, got nil")
	}
}
