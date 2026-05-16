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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 135 — LVM-thin storage-pool create accepts garbage path with
// State=Ok. Per the v3 report, `linstor sp c lvmthin <node> <pool>
// /total/garbage/path` succeeds, the SP CRD persists with State=Ok
// in `sp l`, and the satellite later fails silently when the first
// volume hits it.
//
// Fix shape mirrors Bug 89 (busy-device refusal): the REST handler
// pre-flights the requested backing storage against the satellite's
// stamped knowledge — for storage pools that knowledge is the
// `Aux/DiscoveredVGs` / `Aux/DiscoveredZPools` props on the target
// Node CRD (Option B from the design memo). When the satellite has
// advertised a discovery list and the requested VG / zpool isn't
// in it, the create is refused with 4xx + a LINSTOR-shaped envelope
// rather than letting a garbage CRD land in the store.
//
// When the satellite has NOT advertised a discovery list yet
// (mid-bootstrap, older satellite that doesn't publish the props),
// the validator stays permissive — matches the Bug 89 nil-Free
// path so a freshly-bootstrapped cluster doesn't deadlock on the
// validator.

// TestBug135LVMThinRefusesNonexistentVolumeGroup pins the headline
// case from the v3 report: posting an LVM_THIN pool whose
// `StorDriver/LvmVg` isn't in the satellite's discovered-VGs set
// MUST return 4xx + envelope. The SP CRD MUST NOT land in the store
// — `sp l` must show nothing.
func TestBug135LVMThinRefusesNonexistentVolumeGroup(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Seed the satellite Node with an explicit discovery list that
	// does NOT contain the requested VG. Satellite-side discovery
	// stamps these props on every tick.
	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{
			"Aux/DiscoveredVGs": "myvg,othervg",
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "garbage-pool",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"StorDriver/LvmVg":    "/garbage/no-such-vg",
			"StorDriver/ThinPool": "thin",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Fatalf("status: got %d (success), want 4xx", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty; want at least one ApiCallRc entry")
	}

	if rcs[0].RetCode >= 0 {
		t.Errorf("ret_code: got %#x (success band), want error band",
			rcs[0].RetCode)
	}

	if !strings.Contains(strings.ToLower(rcs[0].Message), "vg") &&
		!strings.Contains(strings.ToLower(rcs[0].Message), "volume group") {
		t.Errorf("message: %q, want VG / volume-group mention",
			rcs[0].Message)
	}

	// No CRD must have landed.
	_, getErr := st.StoragePools().Get(ctx, "n1", "garbage-pool")
	if getErr == nil {
		t.Errorf("StoragePool CRD persisted after refused create")
	}
}

// TestBug135LVMRefusesNonexistentVG covers the LVM (non-thin) provider
// kind: same shape as LVM_THIN but with no ThinPool segment.
func TestBug135LVMRefusesNonexistentVG(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{
			"Aux/DiscoveredVGs": "myvg",
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "thick-pool",
		ProviderKind:    apiv1.StoragePoolKindLVM,
		Props: map[string]string{
			"StorDriver/LvmVg": "no-such-vg",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Fatalf("status: got %d (success), want 4xx", resp.StatusCode)
	}

	_, getErr := st.StoragePools().Get(ctx, "n1", "thick-pool")
	if getErr == nil {
		t.Errorf("StoragePool CRD persisted after refused create")
	}
}

// TestBug135ZFSRefusesNonexistentPool covers ZFS / ZFS_THIN: the
// satellite advertises discovered zpools in `Aux/DiscoveredZPools`.
// A pool whose `StorDriver/ZPool` isn't in that set is refused.
func TestBug135ZFSRefusesNonexistentPool(t *testing.T) {
	for _, kind := range []string{apiv1.StoragePoolKindZFS, apiv1.StoragePoolKindZFSThin} {
		t.Run(kind, func(t *testing.T) {
			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.Nodes().Create(ctx, &apiv1.Node{
				Name: "n1",
				Type: apiv1.NodeTypeSatellite,
				Props: map[string]string{
					"Aux/DiscoveredZPools": "tank,other",
				},
			}); err != nil {
				t.Fatalf("seed node: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			propKey := "StorDriver/ZPool"
			if kind == apiv1.StoragePoolKindZFSThin {
				propKey = "StorDriver/ZPoolThin"
			}

			body, err := json.Marshal(apiv1.StoragePool{
				StoragePoolName: "zfs-pool",
				ProviderKind:    kind,
				Props: map[string]string{
					propKey: "nonexistent",
				},
			})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				t.Fatalf("status: got %d (success), want 4xx", resp.StatusCode)
			}

			_, getErr := st.StoragePools().Get(ctx, "n1", "zfs-pool")
			if getErr == nil {
				t.Errorf("StoragePool CRD persisted after refused create")
			}
		})
	}
}

// TestBug135ValidLVMThinPathAccepted is the happy path: when the
// requested VG is in the satellite's `Aux/DiscoveredVGs` set, the
// create lands as 201 + CRD persisted. Pins the validator does NOT
// over-reject — a VG the satellite has advertised must remain
// usable end-to-end.
func TestBug135ValidLVMThinPathAccepted(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{
			"Aux/DiscoveredVGs": "myvg,othervg",
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "thinpool",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"StorDriver/LvmVg":    "myvg",
			"StorDriver/ThinPool": "thin",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, getErr := st.StoragePools().Get(ctx, "n1", "thinpool")
	if getErr != nil {
		t.Fatalf("StoragePool Get: %v", getErr)
	}

	if got.Props["StorDriver/LvmVg"] != "myvg" {
		t.Errorf("LvmVg round-trip: got %q, want %q",
			got.Props["StorDriver/LvmVg"], "myvg")
	}
}

// TestBug135PermissiveWhenSatelliteSilent pins the bootstrap-safety
// path: a satellite that hasn't yet published `Aux/DiscoveredVGs` /
// `Aux/DiscoveredZPools` (fresh start, older satellite build) MUST
// NOT cause every SP create to fail. Mirrors the Bug 89 nil-Free
// fall-through. Without this, a piraeus-operator manifest applied
// against a freshly-restarted satellite would deadlock on the
// validator until discovery catches up.
func TestBug135PermissiveWhenSatelliteSilent(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Node has no Aux/Discovered* props — satellite mid-bootstrap.
	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "thinpool",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"StorDriver/LvmVg":    "preboot-vg",
			"StorDriver/ThinPool": "thin",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (silent-satellite fall-through)",
			resp.StatusCode)
	}
}

// TestBug135RefusesAliasGarbagePath covers the v3 report's exact
// reproducer: `linstor sp c lvmthin <node> <pool> /total/garbage/path`.
// The Python CLI emits the path as `StorDriver/StorPoolName` (Bug 63
// alias); `expandStorPoolNameAlias` splits on the first '/' which
// puts VG="" + ThinPool="total/garbage/path". An empty VG when the
// satellite has advertised a discovery list MUST refuse the create
// — without this carve-out the v3 reproducer slips past pre-flight.
func TestBug135RefusesAliasGarbagePath(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{
			"Aux/DiscoveredVGs": "myvg,othervg",
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Reproduces the exact payload the python CLI sends for
	// `linstor sp c lvmthin n1 garbage-pool /total/garbage/path`.
	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "garbage-pool",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"StorDriver/StorPoolName": "/total/garbage/path",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Fatalf("status: got %d (success), want 4xx", resp.StatusCode)
	}

	_, getErr := st.StoragePools().Get(ctx, "n1", "garbage-pool")
	if getErr == nil {
		t.Errorf("StoragePool CRD persisted after refused create (v3 repro)")
	}
}

// TestBug135FileKindNotValidated pins that FILE / FILE_THIN /
// DISKLESS providers skip the discovered-VG/zpool check — they
// have no backing VG/zpool to validate against. Without this carve-
// out, a node with no FILE-specific advertisement would block every
// FILE_THIN pool the operator wants to apply.
func TestBug135FileKindNotValidated(t *testing.T) {
	for _, kind := range []string{
		apiv1.StoragePoolKindFile,
		apiv1.StoragePoolKindFileThin,
		apiv1.StoragePoolKindDiskless,
	} {
		t.Run(kind, func(t *testing.T) {
			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.Nodes().Create(ctx, &apiv1.Node{
				Name: "n1",
				Type: apiv1.NodeTypeSatellite,
				Props: map[string]string{
					"Aux/DiscoveredVGs":    "myvg",
					"Aux/DiscoveredZPools": "tank",
				},
			}); err != nil {
				t.Fatalf("seed node: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			poolProps := map[string]string{}
			switch kind {
			case apiv1.StoragePoolKindFile, apiv1.StoragePoolKindFileThin:
				poolProps["StorDriver/FileDir"] = "/var/lib/blockstor/x"
			}

			body, err := json.Marshal(apiv1.StoragePool{
				StoragePoolName: "p1",
				ProviderKind:    kind,
				Props:           poolProps,
			})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("status: got %d, want 201 (file/diskless skip validation)",
					resp.StatusCode)
			}
		})
	}
}
