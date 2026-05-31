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
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Finding 7 (P2): `PUT /v1/resource-definitions/{rd}/resources/{dst}/migrate-disk/{src}`
// (no `/{storagepool}` suffix) returned 404 — only the path-suffix
// variant was wired. python-linstor's common `linstor r migrate-disk
// <to_node> <rd> <from_node>` CLI form URL-encodes WITHOUT the pool
// segment and ships the pool in the JSON body. The test below pins
// the wire shape so the bodyless route stays wired.
func TestMigrateDiskBodylessVariant(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd1",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Source replica must be diskful (no DISKLESS flag) for the
	// migrate-disk validation to pass.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd1",
		NodeName: "src",
	}); err != nil {
		t.Fatalf("seed src replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"storage_pool":"zfs-thin"}`)

	resp := httpPut(t, base+"/v1/resource-definitions/rd1/resources/dst/migrate-disk/src", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 || rcs[0].RetCode != maskInfo {
		t.Errorf("envelope: got %+v, want one maskInfo entry", rcs)
	}

	// The destination replica must now exist with the requested pool
	// stamped and the BlockstorMigratingFrom prop pointing back at src.
	dstRes, err := st.Resources().Get(ctx, "rd1", "dst")
	if err != nil {
		t.Fatalf("read back dst replica: %v", err)
	}

	if dstRes.Props["StorPoolName"] != "zfs-thin" {
		t.Errorf("dst StorPoolName prop: got %q, want zfs-thin", dstRes.Props["StorPoolName"])
	}

	if dstRes.Props[MigratingFromProp] != "src" {
		t.Errorf("dst MigratingFrom prop: got %q, want src", dstRes.Props[MigratingFromProp])
	}
}

// TestMigrateDiskBodylessVariant_EmptyBodyTolerated pins the
// no-pool form: an entirely empty body is accepted, and the
// destination replica is created without a stamped pool — the
// controller's auto-diskful path picks one during the next
// reconcile. Matches upstream LINSTOR's behaviour for the bodyless
// CLI form `linstor r migrate-disk` with no `--storage-pool` flag.
func TestMigrateDiskBodylessVariant_EmptyBodyTolerated(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd1",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd1",
		NodeName: "src",
	}); err != nil {
		t.Fatalf("seed src replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Send a truly empty body — the handler must short-circuit the
	// EOF-rejection path that decodeJSON otherwise triggers.
	resp := httpPut(t, base+"/v1/resource-definitions/rd1/resources/dst/migrate-disk/src", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	dstRes, err := st.Resources().Get(ctx, "rd1", "dst")
	if err != nil {
		t.Fatalf("read back dst replica: %v", err)
	}

	// No pool stamped — the migrating-from prop is set so the migration
	// reconciler still picks the right src.
	if dstRes.Props[MigratingFromProp] != "src" {
		t.Errorf("dst MigratingFrom prop: got %q, want src", dstRes.Props[MigratingFromProp])
	}
}
