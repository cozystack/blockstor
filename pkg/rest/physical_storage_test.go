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
	"io"
	"net/http"
	"strings"
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

// TestPhysicalStorageCreateNormalizesProviderKind pins Bug 73.
// The python-linstor CLI sends lowercase provider names —
// `linstor ps cdp zfs ...` posts `"provider_kind":"zfs"`. The
// StoragePool CRD enum only allows the upstream-canonical
// uppercase forms (`LVM`, `LVM_THIN`, `ZFS`, `ZFS_THIN`, `FILE`,
// `FILE_THIN`, `DISKLESS`). Without normalisation the CDP POST
// reaches apiserver as `zfs`, gets rejected by the OpenAPI
// enum, and surfaces as a confusing "Unsupported value" error
// to the operator — same UX as upstream LINSTOR's bug fixed
// circa LBSAT-1041.
//
// Pin the lowercase + the LINSTOR-CLI compressed forms so the
// REST handler accepts every variant the CLI emits and stamps
// the canonical uppercase value on the auto-created pool.
func TestPhysicalStorageCreateNormalizesProviderKind(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"zfs", "ZFS"},
		{"ZFS", "ZFS"},
		{"zfsthin", "ZFS_THIN"},
		{"ZFS_THIN", "ZFS_THIN"},
		{"lvm", "LVM"},
		{"LVM", "LVM"},
		{"lvmthin", "LVM_THIN"},
		{"LVM_THIN", "LVM_THIN"},
		{"file", "FILE"},
		{"filethin", "FILE_THIN"},
		{"diskless", "DISKLESS"},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
				Name:       "n1-vda",
				NodeName:   "n1",
				DevicePath: "/dev/vda",
				Phase:      "Available",
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body := `{"provider_kind":"` + tc.input + `","device_paths":["/dev/vda"],` +
				`"with_storage_pool":{"name":"p1"}}`
			resp := httpPost(t, base+"/v1/physical-storage/n1", []byte(body))
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("status: got %d, want 202 for provider_kind=%q",
					resp.StatusCode, tc.input)
			}

			pool, err := st.StoragePools().Get(ctx, "n1", "p1")
			if err != nil {
				t.Fatalf("StoragePool Get: %v", err)
			}

			if pool.ProviderKind != tc.want {
				t.Errorf("Bug 73: provider_kind=%q should normalise to %q, got %q",
					tc.input, tc.want, pool.ProviderKind)
			}
		})
	}
}

// TestPhysicalStorageListWireShapeMatchesUpstreamLINSTOR is the
// wave2 6.W08 pin: GET /v1/physical-storage MUST emit the exact
// JSON shape upstream LINSTOR's `JsonGenTypes.PhysicalStorage` +
// `JsonGenTypes.PhysicalStorageDevice` serialise — same field
// names, same case, same nesting, same `omitempty`/NON_EMPTY
// semantics. Re-deriving the shape with a typed decoder would only
// pin the Go-side struct tags and would happily accept a future
// rename like `device_path` instead of `device`; upstream's CLI +
// piraeus-operator parse the raw map with hard-coded keys, so a
// drift here breaks both consumers silently.
//
// Cross-listed with wave1 6.6. Source of truth for the field set:
// linstor-server/controller/src/main/java/com/linbit/linstor/api/
// rest/v1/serializer/JsonGenTypes.java::PhysicalStorage +
// PhysicalStorageDevice. The bucket envelope is
// {size, rotational, nodes{<node>: [{device, model, serial, wwn}]}}
// — `size` and `rotational` are NON_EMPTY (omitted when zero / nil),
// each device entry's four fields are also NON_EMPTY.
//
// Filter chain pinned upstream (lsblk_test.go +
// physicaldevice_discovery_test.go): > 1 GiB, no FS, no DRBD
// signature (major 147), no zvol (major 230). This pin doesn't
// re-test the filter — it locks the wire shape only.
func TestPhysicalStorageListWireShapeMatchesUpstreamLINSTOR(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	rotational := false
	if err := st.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		StableID:   "0x5000c500a1b2c3d4",
		DevicePath: "/dev/disk/by-id/wwn-0x5000c500a1b2c3d4",
		SizeBytes:  1_000_204_886_016,
		Model:      "Samsung SSD 980 PRO",
		Serial:     "S6BUNS0R123456",
		Rotational: &rotational,
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/physical-storage")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	// Decode into a generic map so we pin the raw key names, not
	// the Go struct tags — a rename like `size_bytes` -> `size`
	// would slip past a typed decoder if the Go field tag also
	// changed.
	var raw []map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode raw: %v (body=%s)", err, body)
	}

	if len(raw) != 1 {
		t.Fatalf("buckets: got %d, want 1 (single seed device)", len(raw))
	}

	bucket := raw[0]

	// Upstream PhysicalStorage shape — three top-level keys.
	// Other keys MUST NOT appear (golinstor's strict-mode decoder
	// would reject them on older clients).
	for _, k := range []string{"size", "rotational", "nodes"} {
		if _, ok := bucket[k]; !ok {
			t.Errorf("bucket key %q missing from %v (upstream JsonGenTypes.PhysicalStorage shape)", k, bucket)
		}
	}

	for k := range bucket {
		switch k {
		case "size", "rotational", "nodes":
		default:
			t.Errorf("unexpected top-level key %q (drift from upstream PhysicalStorage shape: only size/rotational/nodes allowed)", k)
		}
	}

	// `size` is a JSON number (Long upstream). json.Unmarshal
	// stores it as float64 unless we used json.Number — verify
	// the value still round-trips.
	if size, ok := bucket["size"].(float64); !ok || int64(size) != 1_000_204_886_016 {
		t.Errorf("size: got %v (%T), want 1000204886016 (float64)", bucket["size"], bucket["size"])
	}

	if rot, ok := bucket["rotational"].(bool); !ok || rot {
		t.Errorf("rotational: got %v (%T), want false (bool)", bucket["rotational"], bucket["rotational"])
	}

	nodes, ok := bucket["nodes"].(map[string]any)
	if !ok {
		t.Fatalf("nodes: got %T, want map[string]any", bucket["nodes"])
	}

	devsRaw, ok := nodes["n1"].([]any)
	if !ok || len(devsRaw) != 1 {
		t.Fatalf("nodes.n1: got %v (%T), want one device entry", nodes["n1"], nodes["n1"])
	}

	dev, ok := devsRaw[0].(map[string]any)
	if !ok {
		t.Fatalf("nodes.n1[0]: got %T, want map[string]any", devsRaw[0])
	}

	// Upstream PhysicalStorageDevice shape — four NON_EMPTY keys.
	// `device` / `model` / `serial` / `wwn` are exact upstream
	// names; the python CLI's storpool_cmds.py parses them by
	// literal key lookup.
	wantDev := map[string]string{
		"device": "/dev/disk/by-id/wwn-0x5000c500a1b2c3d4",
		"model":  "Samsung SSD 980 PRO",
		"serial": "S6BUNS0R123456",
		"wwn":    "0x5000c500a1b2c3d4",
	}
	for k, want := range wantDev {
		got, ok := dev[k].(string)
		if !ok || got != want {
			t.Errorf("device.%s: got %q (%T), want %q (upstream PhysicalStorageDevice key)", k, dev[k], dev[k], want)
		}
	}

	for k := range dev {
		switch k {
		case "device", "model", "serial", "wwn":
		default:
			t.Errorf("unexpected device key %q (drift from upstream PhysicalStorageDevice shape: only device/model/serial/wwn allowed)", k)
		}
	}
}

// TestPhysicalStorageCreateAdvisesUnmanagedHostState is the wave2
// 6.W09 pin: a successful CDP one-shot MUST surface the operator-
// runbook advisory that the OS-level VG / thin LV (or zpool) are
// NOT managed by LINSTOR after the call — `linstor sp delete`
// clears only the controller record, never `vgremove` /
// `zpool destroy` / `wipefs`. Without this advisory, operators who
// teardown a pool and try to re-`linstor ps cdp` the same device
// hit a confusing "device busy / signature exists" error from
// pvcreate, because the previous VG header is still on disk.
//
// Cross-listed with Bug 68 (the underlying CDP end-to-end fix).
// Source of truth: tests/scenarios/wave2-06-storage-backends.md
// §6.W09 — "WARNING: OS-level VG / thin LV are NOT managed by
// LINSTOR after — `sp delete` does not clean them up."
//
// The wire shape mirrors upstream LINSTOR's `Flux<ApiCallRc>`
// (PhysicalStorage.java::createDevicePool concats SUCCESS + pool
// register replies); the python CLI walks the list and prints
// each entry's `message`. Surfacing the advisory as a maskWarn
// ApiCallRc lands it under the standard `WARNING:` log line the
// CLI emits for warn-band entries, so audit-log greppers catch
// it consistently with the rest of the blockstor warn-band
// (warnRscNotFound, warnSnapshotNotFound, etc.).
func TestPhysicalStorageCreateAdvisesUnmanagedHostState(t *testing.T) {
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
			"provider_kind": "LVM_THIN",
			"device_paths": ["/dev/vda"],
			"with_storage_pool": {
				"name": "thin1",
				"props": {
					"StorDriver/LvmVg": "vg",
					"StorDriver/ThinPool": "tp"
				}
			}
		}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", resp.StatusCode)
	}

	var entries []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(entries) < 2 {
		t.Fatalf("entries: got %d, want >= 2 (SUCCESS + WARNING runbook note)", len(entries))
	}

	// Pin the SUCCESS line first — matches upstream's two-step
	// (createDevicePool then createStorPool) Flux concat.
	if entries[0].RetCode == 0 {
		t.Errorf("entries[0] RetCode: got 0, want maskInfo bit set (SUCCESS line)")
	}

	if !strings.Contains(entries[0].Message, "physical-storage attach accepted") {
		t.Errorf("entries[0] Message: got %q, want SUCCESS line", entries[0].Message)
	}

	// Locate the WARNING entry — the order is fixed today
	// (SUCCESS at [0], WARNING at [1]), but future code may
	// interleave per-device records, so scan the slice.
	var warn *apiv1.APICallRc

	for i := range entries {
		if strings.HasPrefix(entries[i].Message, "WARNING:") {
			warn = &entries[i]

			break
		}
	}

	if warn == nil {
		t.Fatalf("WARNING entry missing from %+v — wave2 6.W09 requires the operator-runbook advisory in the response body", entries)
	}

	// The advisory text must call out the three concrete things
	// `linstor sp delete` will NOT do, so operators can grep the
	// audit log for the exact phrases their runbooks reference.
	// Drift on any of these strings is a wave2 6.W09 regression.
	for _, want := range []string{
		"OS-level VG / thin LV are NOT auto-managed",
		"linstor sp delete",
		"operator runbook",
	} {
		if !strings.Contains(warn.Message, want) {
			t.Errorf("WARNING message missing %q substring: got %q", want, warn.Message)
		}
	}

	// The advisory must ride the warn band, not the info band —
	// audit-log filters keyed on `(ret_code & maskWarn) != 0`
	// would miss an info-tagged advisory.
	const maskWarnBit = int64(0x0002_0000_0000)
	if warn.RetCode&maskWarnBit == 0 {
		t.Errorf("WARNING RetCode: got %#x, want maskWarn (0x%x) bit set so python-linstor prints it as WARNING", warn.RetCode, maskWarnBit)
	}
}

// TestPhysicalStorageCreateRejectsUnknownProviderKind pins the
// other half: a truly unrecognised provider must surface a 400
// rather than be passed through unchanged into the apiserver
// (where it would surface as the same opaque "Unsupported
// value" error Bug 73 fixes). Bug 73.
func TestPhysicalStorageCreateRejectsUnknownProviderKind(t *testing.T) {
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
		[]byte(`{"provider_kind":"banana","device_paths":["/dev/vda"]}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("unknown provider_kind must return 400, got %d", resp.StatusCode)
	}
}

// TestPhysicalStorageCreatePropagatesPoolNameToProps pins Bug 88:
// `linstor ps cdp --pool-name X --storage-pool Y <kind> <node> <dev>`
// passes `pool_name` at the top level without populating
// `with_storage_pool.props`. The REST handler MUST derive the
// kind-specific StorDriver/* key from `pool_name` and stamp it on
// the auto-created StoragePool CRD's Spec.Props so the satellite
// provider can instantiate the backend on its next reconcile —
// otherwise it loops on "<kind> attach requires <Key>" and
// Status.{FreeCapacity, TotalCapacity, SupportsSnapshots} stay zero.
//
// Per-kind dispatch mirrors upstream LINSTOR's
// `controller/.../api/rest/v1/PhysicalStorage.java::deviceProviderToStorPoolProperty`
// and the satellite's `pkg/satellite/factory.go::NewProviderFromKind`
// key set:
//   - ZFS              → StorDriver/ZPool       = pool_name
//   - ZFS_THIN         → StorDriver/ZPoolThin   = pool_name
//   - LVM              → StorDriver/LvmVg       = pool_name
//   - LVM_THIN (bare)  → StorDriver/LvmVg       = "linstor_"+pool_name,
//     StorDriver/ThinPool    = pool_name
//   - LVM_THIN (vg/lv) → StorDriver/LvmVg       = vg,
//     StorDriver/ThinPool    = lv
//   - FILE / FILE_THIN → StorDriver/FileDir     = pool_name
func TestPhysicalStorageCreatePropagatesPoolNameToProps(t *testing.T) {
	cases := []struct {
		name         string
		providerKind string
		poolName     string
		// For ZFS_THIN the satellite factory accepts either
		// `ZPool` or `ZPoolThin` (kind-specific key wins), but the
		// canonical key MUST be present so `linstor sp l`'s
		// PoolName column renders. Pin the kind-specific key only.
		wantProps map[string]string
	}{
		{
			name:         "zfs-thick",
			providerKind: "ZFS",
			poolName:     "data",
			wantProps: map[string]string{
				"StorDriver/ZPool": "data",
			},
		},
		{
			name:         "zfs-thin",
			providerKind: "ZFS_THIN",
			poolName:     "tank",
			wantProps: map[string]string{
				"StorDriver/ZPoolThin": "tank",
			},
		},
		{
			name:         "lvm-thick",
			providerKind: "LVM",
			poolName:     "vg1",
			wantProps: map[string]string{
				"StorDriver/LvmVg": "vg1",
			},
		},
		{
			// Upstream LvmThinDriverKind: bare pool_name → vg
			// becomes `linstor_<name>`, thin LV becomes
			// `<name>`. piraeus-operator's CDP request shape.
			name:         "lvm-thin-bare",
			providerKind: "LVM_THIN",
			poolName:     "thin1",
			wantProps: map[string]string{
				"StorDriver/LvmVg":    "linstor_thin1",
				"StorDriver/ThinPool": "thin1",
			},
		},
		{
			// vg/lv syntax: explicit split.
			name:         "lvm-thin-explicit",
			providerKind: "LVM_THIN",
			poolName:     "vg2/tp2",
			wantProps: map[string]string{
				"StorDriver/LvmVg":    "vg2",
				"StorDriver/ThinPool": "tp2",
			},
		},
		{
			name:         "file",
			providerKind: "FILE",
			poolName:     "/var/lib/blockstor/file",
			wantProps: map[string]string{
				"StorDriver/FileDir": "/var/lib/blockstor/file",
			},
		},
		{
			name:         "file-thin",
			providerKind: "FILE_THIN",
			poolName:     "/var/lib/blockstor/file-thin",
			wantProps: map[string]string{
				"StorDriver/FileDir": "/var/lib/blockstor/file-thin",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
				Name:       "n1-vda",
				NodeName:   "n1",
				DevicePath: "/dev/vda",
				Phase:      "Available",
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			// Bug 88 reproducer: the CLI sends `pool_name` at the
			// top level and `with_storage_pool.name` only — no
			// `with_storage_pool.props`. The handler MUST infer
			// the StorDriver/* key from `pool_name`.
			body := `{
				"provider_kind": "` + tc.providerKind + `",
				"pool_name": "` + tc.poolName + `",
				"device_paths": ["/dev/vda"],
				"with_storage_pool": {"name": "sp1"}
			}`

			resp := httpPost(t, base+"/v1/physical-storage/n1", []byte(body))
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("status: got %d, want 202", resp.StatusCode)
			}

			pool, err := st.StoragePools().Get(ctx, "n1", "sp1")
			if err != nil {
				t.Fatalf("StoragePool Get: %v", err)
			}

			if pool.ProviderKind != tc.providerKind {
				t.Errorf("ProviderKind: got %q, want %q",
					pool.ProviderKind, tc.providerKind)
			}

			for key, want := range tc.wantProps {
				got := pool.Props[key]
				if got != want {
					t.Errorf("Bug 88: Spec.Props[%q]: got %q, want %q (pool_name=%q kind=%q produced %+v) — satellite provider would reject with %q attach requires %s",
						key, got, want, tc.poolName, tc.providerKind, pool.Props, tc.providerKind, key)
				}
			}
		})
	}
}

// TestPhysicalStorageCreateExplicitPropsWinOverPoolName pins that
// when the operator DOES pass `with_storage_pool.props` (the
// upstream-shaped manifest path), the explicit values win over
// the Bug 88 pool_name-fallback inference. Keeps GitOps-supplied
// configs from being silently overwritten by the CLI shorthand.
func TestPhysicalStorageCreateExplicitPropsWinOverPoolName(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
		Name:       "n1-vda",
		NodeName:   "n1",
		DevicePath: "/dev/vda",
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Operator passes both: explicit StorDriver/ZPool=explicit
	// MUST win over pool_name=fallback (otherwise the GitOps
	// manifest's intent gets shadowed by the CLI shorthand).
	resp := httpPost(t, base+"/v1/physical-storage/n1",
		[]byte(`{
			"provider_kind": "ZFS",
			"pool_name": "fallback",
			"device_paths": ["/dev/vda"],
			"with_storage_pool": {
				"name": "sp1",
				"props": {"StorDriver/ZPool": "explicit"}
			}
		}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", resp.StatusCode)
	}

	pool, err := st.StoragePools().Get(ctx, "n1", "sp1")
	if err != nil {
		t.Fatalf("StoragePool Get: %v", err)
	}

	if pool.Props["StorDriver/ZPool"] != "explicit" {
		t.Errorf("explicit Props beat pool_name fallback: got %q, want %q",
			pool.Props["StorDriver/ZPool"], "explicit")
	}
}

// TestPhysicalStorageCreateAcceptsVdoEnable pins Bug 326: the LINSTOR
// CLI (`linstor ps cdp …`) serialises `vdo_enable` and the sibling
// VDO knobs (`vdo_logical_size_kib`, `vdo_slab_size_kib`) into the
// `PhysicalStorageCreate` envelope unconditionally — even when the
// operator passes none of the `--vdo-*` flags the Python CLI fills
// them with default-zero / default-false values. blockstor's strict
// JSON decoder (Bug 161 / Bug 197) used to reject the body with
// "unknown field 'vdo_enable'", blocking `linstor ps cdp` end-to-end
// on real stands.
//
// The fix is wire-compat accept-and-ignore: blockstor does not stack
// a VDO layer under storage pools, but it MUST accept the field so
// the CLI round-trips. Three sub-cases cover the regression surface:
//
//   - omitted (backward compat with existing CLIs / piraeus-operator)
//   - vdo_enable=false (the common path the CLI fills by default)
//   - vdo_enable=true (caller actually wants VDO — we still accept
//     but emit an extra WARNING ApiCallRc so the operator learns
//     the request landed without VDO setup)
//
// All sibling VDO / RAID / SED / *_create_arguments knobs are
// exercised alongside vdo_enable in the false-path case so the
// strict decoder accepts the full upstream `PhysicalStorageCreate`
// shape — drift on any of these wire keys would re-break the CLI.
func TestPhysicalStorageCreateAcceptsVdoEnable(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantVDOWarn bool
	}{
		{
			name: "omitted",
			body: `{
				"provider_kind": "ZFS",
				"device_paths": ["/dev/sda"],
				"with_storage_pool": {
					"name": "data",
					"props": {"StorDriver/ZPool": "data"}
				}
			}`,
			wantVDOWarn: false,
		},
		{
			name: "vdo_enable_false_with_siblings",
			body: `{
				"provider_kind": "ZFS",
				"device_paths": ["/dev/sda"],
				"vdo_enable": false,
				"vdo_logical_size_kib": 0,
				"vdo_slab_size_kib": 0,
				"sed": false,
				"raid_level": "",
				"lv_create_arguments": [],
				"pv_create_arguments": [],
				"vg_create_arguments": [],
				"zpool_create_arguments": [],
				"with_storage_pool": {
					"name": "data",
					"props": {"StorDriver/ZPool": "data"}
				}
			}`,
			wantVDOWarn: false,
		},
		{
			name: "vdo_enable_true_emits_warning",
			body: `{
				"provider_kind": "ZFS",
				"device_paths": ["/dev/sda"],
				"vdo_enable": true,
				"with_storage_pool": {
					"name": "data",
					"props": {"StorDriver/ZPool": "data"}
				}
			}`,
			wantVDOWarn: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewInMemory()

			if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
				Name:       "n1-sda",
				NodeName:   "n1",
				DevicePath: "/dev/sda",
				Phase:      "Available",
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPost(t, base+"/v1/physical-storage/n1", []byte(tc.body))
			defer func() { _ = resp.Body.Close() }()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("status: got %d, want 202; body=%s", resp.StatusCode, body)
			}

			if strings.Contains(string(body), "unknown field") {
				t.Errorf("response body still contains 'unknown field' — Bug 326 regression: %s", body)
			}

			var entries []apiv1.APICallRc
			if err := json.Unmarshal(body, &entries); err != nil {
				t.Fatalf("decode: %v", err)
			}

			var vdoWarn *apiv1.APICallRc

			for i := range entries {
				if strings.Contains(entries[i].Message, "vdo_enable=true") {
					vdoWarn = &entries[i]

					break
				}
			}

			if tc.wantVDOWarn && vdoWarn == nil {
				t.Errorf("vdo_enable=true must surface a VDO-not-implemented WARNING entry; got entries=%+v", entries)
			}

			if tc.wantVDOWarn && vdoWarn != nil {
				const maskWarnBit = int64(0x0002_0000_0000)
				if vdoWarn.RetCode&maskWarnBit == 0 {
					t.Errorf("VDO advisory RetCode: got %#x, want maskWarn bit set so python-linstor prints it as WARNING", vdoWarn.RetCode)
				}
			}

			if !tc.wantVDOWarn && vdoWarn != nil {
				t.Errorf("unexpected VDO-not-implemented WARNING when vdo_enable was %q: %+v", tc.name, vdoWarn)
			}

			// The attach must still flip AttachTo regardless of the
			// VDO knobs — the fields are accept-and-ignore.
			got, err := st.PhysicalDevices().Get(t.Context(), "n1-sda")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}

			if got.AttachTo == nil {
				t.Errorf("AttachTo: got nil, want set (VDO knobs must not block the attach)")
			}
		})
	}
}

// TestPhysicalStorageCreateAcceptsCLIWireShape pins the real wire
// shape the production LINSTOR CLI sends on `linstor ps cdp
// --pool-name data --storage-pool data zfs <node> /dev/sda` — the
// exact reproducer from the Bug 326 user report (stand dev-kvaps-1).
// The body intentionally carries every knob the upstream
// `PhysicalStorageCreate` envelope defines so the strict decoder
// covers the full union of fields the CLI may serialise; a future
// addition to the upstream envelope is the only way this test breaks.
func TestPhysicalStorageCreateAcceptsCLIWireShape(t *testing.T) {
	st := store.NewInMemory()

	if err := st.PhysicalDevices().Create(t.Context(), &apiv1.PhysicalDevice{
		Name:       "n1-sda",
		NodeName:   "n1",
		DevicePath: "/dev/sda",
		Phase:      "Available",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// This body mirrors the request golinstor 0.55.x emits for the
	// exact CLI invocation in the Bug 326 report. The decoder must
	// accept every key (none of them is `unknown field`).
	resp := httpPost(t, base+"/v1/physical-storage/n1",
		[]byte(`{
			"provider_kind": "ZFS",
			"pool_name": "data",
			"device_paths": ["/dev/sda"],
			"with_storage_pool": {"name": "data"},
			"vdo_enable": false,
			"vdo_logical_size_kib": 0,
			"vdo_slab_size_kib": 0,
			"sed": false
		}`))
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("Bug 326 reproducer body must return 202, got %d: %s", resp.StatusCode, body)
	}

	if strings.Contains(string(body), "unknown field") {
		t.Fatalf("Bug 326 regression: response still mentions 'unknown field': %s", body)
	}
}
