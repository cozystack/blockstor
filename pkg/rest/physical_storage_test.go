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

// TestPSListAggregatesAcrossNodes pins the Bug 51 wire pipe: the
// satellite-side discovery runnable publishes one PhysicalDevice
// CRD per free disk on its own node; `GET /v1/physical-storage`
// MUST aggregate the per-node CRDs into the cluster-wide bucketed
// envelope upstream LINSTOR's `linstor ps l` parses.
//
// Without this aggregation `linstor ps l` returns an empty list
// even when each satellite has correctly populated its local
// PhysicalDevice store — the exact failure mode bug 51 documents.
// Three nodes here cover the "more than one node per bucket"
// merge path (size-+-rotational equivalence) and the
// "single-node bucket" path simultaneously.
func TestPSListAggregatesAcrossNodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	rotational := false

	// Two SSDs on n1 + n2 land in the same (size, rotational)
	// bucket; n3's HDD goes to its own bucket.
	seeds := []*apiv1.PhysicalDevice{
		{
			Name:       "n1.wwn-a",
			NodeName:   "n1",
			StableID:   "wwn-a",
			DevicePath: "/dev/disk/by-id/wwn-a",
			SizeBytes:  1_000_000_000_000,
			Model:      "SSD-A",
			Serial:     "SN-A",
			Rotational: &rotational,
			Phase:      "Available",
		},
		{
			Name:       "n2.wwn-b",
			NodeName:   "n2",
			StableID:   "wwn-b",
			DevicePath: "/dev/disk/by-id/wwn-b",
			SizeBytes:  1_000_000_000_000,
			Model:      "SSD-B",
			Serial:     "SN-B",
			Rotational: &rotational,
			Phase:      "Available",
		},
		{
			Name:       "n3.wwn-c",
			NodeName:   "n3",
			StableID:   "wwn-c",
			DevicePath: "/dev/disk/by-id/wwn-c",
			SizeBytes:  2_000_000_000_000, // different size → different bucket
			Model:      "HDD-C",
			Serial:     "SN-C",
			// Rotational nil = bucket has rotational=nil
			Phase: "Available",
		},
	}

	for _, d := range seeds {
		if err := st.PhysicalDevices().Create(ctx, d); err != nil {
			t.Fatalf("seed %s: %v", d.Name, err)
		}
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

	// One bucket for the two SSDs (size+rotational match) + one
	// for the HDD → 2 buckets total.
	if len(entries) != 2 {
		t.Fatalf("buckets: got %d, want 2 (SSD-bucket merging n1+n2 plus HDD-bucket)", len(entries))
	}

	// Locate the SSD bucket and verify both nodes are present.
	var ssdBucket *physicalStorageEntry

	for i := range entries {
		if entries[i].Size == 1_000_000_000_000 {
			ssdBucket = &entries[i]

			break
		}
	}

	if ssdBucket == nil {
		t.Fatalf("SSD bucket missing from %+v", entries)
	}

	if len(ssdBucket.Nodes) != 2 {
		t.Errorf("SSD bucket nodes: got %d, want 2 (n1+n2)", len(ssdBucket.Nodes))
	}

	if devs := ssdBucket.Nodes["n1"]; len(devs) != 1 || devs[0].WWN != "wwn-a" {
		t.Errorf("n1 entry: got %+v, want one wwn-a", devs)
	}

	if devs := ssdBucket.Nodes["n2"]; len(devs) != 1 || devs[0].WWN != "wwn-b" {
		t.Errorf("n2 entry: got %+v, want one wwn-b", devs)
	}

	// HDD bucket must carry only n3.
	var hddBucket *physicalStorageEntry

	for i := range entries {
		if entries[i].Size == 2_000_000_000_000 {
			hddBucket = &entries[i]

			break
		}
	}

	if hddBucket == nil {
		t.Fatalf("HDD bucket missing from %+v", entries)
	}

	if len(hddBucket.Nodes) != 1 || len(hddBucket.Nodes["n3"]) != 1 {
		t.Errorf("HDD bucket: got %+v, want exactly n3->[wwn-c]", hddBucket.Nodes)
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

// TestPhysicalStorageCreateNoMatchingDevice: 404 when none of the
// requested device paths matches a free PhysicalDevice on the
// node — surfaces the failure rather than silently succeeding so
// piraeus-operator can retry after the discovery loop catches up.
func TestPhysicalStorageCreateNoMatchingDevice(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/physical-storage/n1",
		[]byte(`{"provider_kind":"LVM_THIN","device_paths":["/dev/sdb"]}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestPhysicalStorageCreateFlipsAttachTo: the happy path. POST
// with a matching DevicePath flips Spec.AttachTo on the chosen
// PhysicalDevice; the satellite reconciler later picks it up.
func TestPhysicalStorageCreateFlipsAttachTo(t *testing.T) {
	st := store.NewInMemory()

	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		DevicePath: "/dev/disk/by-id/wwn-0xWWN-A",
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/physical-storage/n1",
		[]byte(`{
			"provider_kind": "LVM_THIN",
			"device_paths": ["/dev/disk/by-id/wwn-0xWWN-A"],
			"with_storage_pool": {
				"name": "thin1",
				"props": {
					"StorDriver/LvmVg": "vg",
					"StorDriver/ThinPool": "tp"
				}
			}
		}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status: got %d, want 202 (upstream-shaped async response)", resp.StatusCode)
	}

	if loc := resp.Header.Get("Location"); loc != "/v1/nodes/n1/physical-storage" {
		t.Errorf("Location header: got %q, want /v1/nodes/n1/physical-storage", loc)
	}

	got, err := st.PhysicalDevices().Get(t.Context(), "n1-sda")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.AttachTo == nil {
		t.Fatalf("AttachTo: got nil, want set")
	}

	if got.AttachTo.StoragePoolName != "thin1" {
		t.Errorf("StoragePoolName: got %q, want thin1", got.AttachTo.StoragePoolName)
	}

	if got.AttachTo.ProviderKind != "LVM_THIN" {
		t.Errorf("ProviderKind: got %q, want LVM_THIN", got.AttachTo.ProviderKind)
	}

	if got.AttachTo.VGName != "vg" {
		t.Errorf("VGName: got %q, want vg", got.AttachTo.VGName)
	}

	if got.AttachTo.ThinPoolName != "tp" {
		t.Errorf("ThinPoolName: got %q, want tp", got.AttachTo.ThinPoolName)
	}
}

// TestPhysicalStorageCreateAutoCreatesStoragePool pins the
// Phase 10.7 step-2 contract: a CDP POST that names a
// StoragePool which doesn't exist yet must materialise the
// pool via the same request rather than leaving the satellite
// in PoolMissing until an operator applies the pool
// separately. The pool's Spec carries the provider-specific
// Props the satellite reads to instantiate the backend.
func TestPhysicalStorageCreateAutoCreatesStoragePool(t *testing.T) {
	st := store.NewInMemory()

	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		DevicePath: "/dev/disk/by-id/wwn-0xWWN-A",
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/physical-storage/n1",
		[]byte(`{
			"provider_kind": "LVM_THIN",
			"device_paths": ["/dev/disk/by-id/wwn-0xWWN-A"],
			"with_storage_pool": {
				"name": "thin1",
				"props": {
					"StorDriver/LvmVg": "vg",
					"StorDriver/ThinPool": "tp"
				}
			}
		}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", resp.StatusCode)
	}

	pool, err := st.StoragePools().Get(t.Context(), "n1", "thin1")
	if err != nil {
		t.Fatalf("StoragePool should exist after CDP: %v", err)
	}

	if pool.ProviderKind != "LVM_THIN" {
		t.Errorf("ProviderKind: got %q, want LVM_THIN", pool.ProviderKind)
	}

	if pool.Props["StorDriver/LvmVg"] != "vg" {
		t.Errorf("StorDriver/LvmVg: got %q, want vg", pool.Props["StorDriver/LvmVg"])
	}

	if pool.Props["StorDriver/ThinPool"] != "tp" {
		t.Errorf("StorDriver/ThinPool: got %q, want tp", pool.Props["StorDriver/ThinPool"])
	}
}

// TestPhysicalStorageCreatePreservesExistingStoragePool pins
// the operator-wins half of the contract: a pre-existing
// StoragePool CRD must not be overwritten by a CDP request,
// even when the request's `with_storage_pool.props` would have
// produced a different config. The GitOps source of truth wins.
func TestPhysicalStorageCreatePreservesExistingStoragePool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		DevicePath: "/dev/disk/by-id/wwn-0xWWN-A",
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed device: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "thin1",
		NodeName:        "n1",
		ProviderKind:    "LVM_THIN",
		Props: map[string]string{
			"StorDriver/LvmVg":    "operator-vg",
			"StorDriver/ThinPool": "operator-tp",
		},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/physical-storage/n1",
		[]byte(`{
			"provider_kind": "LVM_THIN",
			"device_paths": ["/dev/disk/by-id/wwn-0xWWN-A"],
			"with_storage_pool": {
				"name": "thin1",
				"props": {"StorDriver/LvmVg": "cdp-vg"}
			}
		}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", resp.StatusCode)
	}

	pool, err := st.StoragePools().Get(ctx, "n1", "thin1")
	if err != nil {
		t.Fatalf("StoragePool Get: %v", err)
	}

	if pool.Props["StorDriver/LvmVg"] != "operator-vg" {
		t.Errorf("pre-existing pool config overwritten: %+v", pool.Props)
	}
}

// TestPhysicalStorageCreateRejectsAttached: 404 when the matching
// device is already attached (Spec.AttachTo set). Operators must
// re-run discovery / wait for delete-as-completion before re-using
// the device — silently succeeding here would race the reconciler.
func TestPhysicalStorageCreateRejectsAttached(t *testing.T) {
	st := store.NewInMemory()

	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		DevicePath: "/dev/disk/by-id/wwn-0xWWN-A",
		Phase:      "Attaching",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "earlier-pool",
			ProviderKind:    "LVM_THIN",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/physical-storage/n1",
		[]byte(`{"provider_kind":"LVM_THIN","device_paths":["/dev/disk/by-id/wwn-0xWWN-A"]}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404 (device already attached)", resp.StatusCode)
	}
}

// TestPhysicalStorageCreateMatchesKernelNamePath pins Bug 68
// end-to-end: operators type `linstor ps cdp ... zfs <node> /dev/vda`
// expecting the kernel-name path to match the PhysicalDevice CRD's
// DevicePath. Bug 69 changes DevicePath to `/dev/vda`-shape (was
// `/dev/disk/by-id/by-path-vda`), but this test ALSO pins the
// matching contract from the REST handler's POV — a regression that
// reverted to by-id matching would re-break operator UX even with
// the discovery path fix in place.
//
// Scenario: PhysicalDevice CRD already has DevicePath=/dev/vda
// (post-Bug-69 shape). POST body says device_paths=["/dev/vda"].
// Handler MUST find the CRD and flip Spec.AttachTo. Without this
// test, a subtle handler-side rewrite to "always lookup by-id"
// would silently break `linstor ps cdp` for the canonical
// operator workflow.
func TestPhysicalStorageCreateMatchesKernelNamePath(t *testing.T) {
	st := store.NewInMemory()

	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:       "n1-vda",
		NodeName:   "n1",
		DevicePath: "/dev/vda",
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/physical-storage/n1",
		[]byte(`{
			"provider_kind": "ZFS",
			"device_paths": ["/dev/vda"],
			"with_storage_pool": {
				"name": "data",
				"props": {"StorDriver/ZPool": "data"}
			}
		}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("Bug 68: status: got %d, want 202 (kernel-name path must match the CRD's DevicePath)",
			resp.StatusCode)
	}

	got, err := st.PhysicalDevices().Get(t.Context(), "n1-vda")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.AttachTo == nil {
		t.Errorf("Bug 68: AttachTo: nil — handler didn't flip the CRD even though /dev/vda matched DevicePath")
	}

	// The auto-created StoragePool exists per the same request — Bug
	// 68's symptom on the live stand was "operator runs cdp, nothing
	// happens" because the device-paths matcher silently failed. Pin
	// the StoragePool side-effect so a future regression that only
	// flips AttachTo without provisioning the pool still fails the
	// test.
	pool, err := st.StoragePools().Get(t.Context(), "n1", "data")
	if err != nil {
		t.Errorf("Bug 68: StoragePool 'data' on n1 must be auto-created on cdp; got Get error %v", err)
	} else if pool.ProviderKind != "ZFS" {
		t.Errorf("Bug 68: StoragePool ProviderKind: got %q, want ZFS", pool.ProviderKind)
	}
}
