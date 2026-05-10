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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
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

// TestPhysicalStorageListWithDevices pins the Phase 10.7 wire
// shape: GET /v1/physical-storage groups Available devices by
// (size, rotational); GET /v1/nodes/{node}/physical-storage
// returns the per-node flat slice. Pinning the JSON shape here
// catches drift against piraeus-operator + golinstor's wire
// expectations without spinning up the full controller stack.
func TestPhysicalStorageListWithDevices(t *testing.T) {
	st := store.NewInMemory()

	rotational := false
	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		StableID:   "0xWWN-A",
		DevicePath: "/dev/disk/by-id/wwn-0xWWN-A",
		SizeBytes:  1000204886016,
		Model:      "Samsung SSD 980 PRO",
		Serial:     "S6BUNS0R123456",
		Rotational: &rotational,
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed device: %v", err)
	}

	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:       "n2-sda",
		NodeName:   "n2",
		StableID:   "0xWWN-B",
		DevicePath: "/dev/disk/by-id/wwn-0xWWN-B",
		SizeBytes:  1000204886016, // same size+rota as n1-sda → same bucket
		Model:      "Samsung SSD 980 PRO",
		Serial:     "S6BUNS0R654321",
		Rotational: &rotational,
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed device: %v", err)
	}

	// An Attaching device must NOT show up in the list.
	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:     "n1-sdb",
		NodeName: "n1",
		Phase:    "Attaching",
	}); err != nil {
		t.Fatalf("seed device: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/physical-storage")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var entries []physicalStorageEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("entries: got %d, want 1 (both same-bucket SSDs)", len(entries))
	}

	bucket := entries[0]
	if bucket.Size != 1000204886016 {
		t.Errorf("bucket size: got %d, want 1000204886016", bucket.Size)
	}

	if len(bucket.Nodes) != 2 {
		t.Errorf("bucket nodes: got %d, want 2 (n1 + n2)", len(bucket.Nodes))
	}

	if devs := bucket.Nodes["n1"]; len(devs) != 1 || devs[0].Model != "Samsung SSD 980 PRO" {
		t.Errorf("n1 devices: got %+v, want one Samsung", devs)
	}
}

// TestPhysicalStorageListForNodeFiltersAttachTo pins that a
// device with AttachTo set (operator picked it up for a pool) is
// excluded from the per-node available list — operators should
// see only truly free devices.
func TestPhysicalStorageListForNodeFiltersAttachTo(t *testing.T) {
	st := store.NewInMemory()

	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:     "n1-free",
		NodeName: "n1",
		Phase:    "Available",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:     "n1-attached",
		NodeName: "n1",
		Phase:    "Available",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "thin1",
			ProviderKind:    "LVM_THIN",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1/physical-storage")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var devs []physicalStorageDeviceWireRepetition
	if err := json.NewDecoder(resp.Body).Decode(&devs); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(devs) != 1 {
		t.Errorf("devices: got %d, want 1 (attached one filtered out)", len(devs))
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
