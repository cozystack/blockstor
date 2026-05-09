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

// TestPhysicalStorageList: list endpoints answer 200 with []. The
// `linstor physical-storage list` CLI parses an empty array fine.
func TestPhysicalStorageList(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	for _, path := range []string{
		"/v1/physical-storage",
		"/v1/nodes/n1/physical-storage",
	} {
		resp := httpGet(t, base+path)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: got %d, want 200", path, resp.StatusCode)
		}
	}
}

// TestPhysicalStorageCreateNotImplemented: the device-pool create
// endpoint returns 501 with a LINSTOR-shaped ApiCallRc explaining
// the cozystack boundary.
func TestPhysicalStorageCreateNotImplemented(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/physical-storage/n1",
		[]byte(`{"provider_kind":"LVM_THIN","device_paths":["/dev/sdb"]}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("status: got %d, want 501", resp.StatusCode)
	}
}
