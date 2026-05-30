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

// Finding 6 (P2) tests: POST + PUT + DELETE on
// `/v1/storage-pool-definitions` were unwired and 405'd. The handlers
// below pin the wire shape so a future regression (e.g. someone
// reverts `registerStoragePoolDefinitions`) surfaces as a CI failure
// rather than a silent 405 to the python CLI.

// TestStoragePoolDefinitionCreate_OK pins the POST happy path: HTTP
// 201 with an APICallRc[] envelope and a maskInfo success ret_code,
// followed by a single GET-list entry carrying the seeded props.
func TestStoragePoolDefinitionCreate_OK(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"storage_pool_name":"zfs-thin","props":{"MaxOversubscriptionRatio":"1.5"}}`)
	resp := httpPost(t, base+"/v1/storage-pool-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope len: got %d, want 1", len(rcs))
	}

	if rcs[0].RetCode != maskInfo {
		t.Errorf("ret_code: got %d, want maskInfo (%d)", rcs[0].RetCode, maskInfo)
	}

	// The created definition must surface through the existing list
	// endpoint (merged with the per-pool synthesis path).
	listResp := httpGet(t, base+"/v1/storage-pool-definitions")
	defer func() { _ = listResp.Body.Close() }()

	var list []struct {
		StoragePoolName string            `json:"storage_pool_name"`
		Props           map[string]string `json:"props"`
	}

	if err := json.NewDecoder(listResp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}

	if len(list) != 1 {
		t.Fatalf("list len: got %d, want 1", len(list))
	}

	if list[0].StoragePoolName != "zfs-thin" {
		t.Errorf("list[0].storage_pool_name: got %q, want %q", list[0].StoragePoolName, "zfs-thin")
	}

	if list[0].Props["MaxOversubscriptionRatio"] != "1.5" {
		t.Errorf("list[0].props[MaxOversubscriptionRatio]: got %q, want 1.5",
			list[0].Props["MaxOversubscriptionRatio"])
	}
}

// TestStoragePoolDefinitionCreate_DuplicateRejected pins the
// CREATE-only contract: a second POST against the same name returns
// 409 with FAIL_EXISTS_STOR_POOL_DFN OR'd into the apiCallRcError
// mask. Mirrors Finding 1 — POST must not silently mutate.
func TestStoragePoolDefinitionCreate_DuplicateRejected(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"storage_pool_name":"zfs-thin"}`)

	first := httpPost(t, base+"/v1/storage-pool-definitions", body)
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first POST: got %d, want 201", first.StatusCode)
	}

	second := httpPost(t, base+"/v1/storage-pool-definitions", body)
	defer func() { _ = second.Body.Close() }()

	if second.StatusCode != http.StatusConflict {
		t.Fatalf("second POST status: got %d, want 409", second.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(second.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code: got %d, want apiCallRcError bit set", rcs[0].RetCode)
	}

	if rcs[0].RetCode&apiCallRcFailExistsStorPoolDfn == 0 {
		t.Errorf("ret_code: got %d, want FAIL_EXISTS_STOR_POOL_DFN (%d) OR'd in",
			rcs[0].RetCode, apiCallRcFailExistsStorPoolDfn)
	}
}

// TestStoragePoolDefinitionModify_OverridesAndDeletes pins the PUT
// happy path: override_props merges, delete_props strips a key,
// delete_namespaces clears every key under a prefix.
func TestStoragePoolDefinitionModify_OverridesAndDeletes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePoolDefinitions().Create(ctx, &store.StoragePoolDefinition{
		Name: "zfs-thin",
		Props: map[string]string{
			"MaxOversubscriptionRatio": "1.0",
			"Aux/team":                 "blue",
			"Aux/zone":                 "us-east-1a",
		},
	}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{
		"override_props": {"MaxOversubscriptionRatio":"1.5"},
		"delete_props":   ["Aux/team"],
		"delete_namespaces": ["Aux"]
	}`)

	resp := httpPut(t, base+"/v1/storage-pool-definitions/zfs-thin", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 || rcs[0].RetCode != maskInfo {
		t.Fatalf("envelope: got %+v, want one maskInfo entry", rcs)
	}

	got, err := st.StoragePoolDefinitions().Get(ctx, "zfs-thin")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if got.Props["MaxOversubscriptionRatio"] != "1.5" {
		t.Errorf("override missed: got %q, want 1.5", got.Props["MaxOversubscriptionRatio"])
	}

	// delete_namespaces "Aux" must have dropped Aux/team AND Aux/zone.
	if _, present := got.Props["Aux/team"]; present {
		t.Errorf("Aux/team still present after delete_namespaces")
	}

	if _, present := got.Props["Aux/zone"]; present {
		t.Errorf("Aux/zone still present after delete_namespaces")
	}
}

// TestStoragePoolDefinitionDelete_OK pins the DELETE happy path on a
// definition that has no per-node pool references.
func TestStoragePoolDefinitionDelete_OK(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePoolDefinitions().Create(ctx, &store.StoragePoolDefinition{
		Name: "zfs-thin",
	}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/storage-pool-definitions/zfs-thin")
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

	if _, getErr := st.StoragePoolDefinitions().Get(ctx, "zfs-thin"); getErr == nil {
		t.Errorf("definition still present after DELETE")
	}
}

// TestStoragePoolDefinitionDelete_RefusedWhenPoolReferences pins the
// 409 + FAIL_IN_USE refusal path: a definition whose name matches at
// least one per-node StoragePool cannot be deleted until the pools
// are dropped first. Mirrors upstream LINSTOR's
// `CtrlStorPoolDfnApiCallHandler` refusal.
func TestStoragePoolDefinitionDelete_RefusedWhenPoolReferences(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePoolDefinitions().Create(ctx, &store.StoragePoolDefinition{
		Name: "zfs-thin",
	}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "zfs-thin",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/storage-pool-definitions/zfs-thin")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Fatalf("ret_code: missing apiCallRcError bit: %d", rcs[0].RetCode)
	}

	if rcs[0].RetCode&apiCallRcFailInUse == 0 {
		t.Errorf("ret_code: got %d, want FAIL_IN_USE (%d) OR'd in",
			rcs[0].RetCode, apiCallRcFailInUse)
	}

	// Definition must still exist after the refusal.
	if _, err := st.StoragePoolDefinitions().Get(ctx, "zfs-thin"); err != nil {
		t.Errorf("definition was deleted despite refusal: %v", err)
	}
}

// TestStoragePoolDefinitionDelete_IdempotentWhenAbsent pins the
// already-absent path: a delete of a definition that doesn't exist
// folds into 200 + the warn-band envelope so audit-log greppers can
// still distinguish the no-op replay from a real delete. Matches the
// Bug 66 pattern for RDs / SPs / nodes.
func TestStoragePoolDefinitionDelete_IdempotentWhenAbsent(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/storage-pool-definitions/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (idempotent)", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope len: got %d, want 1", len(rcs))
	}

	if rcs[0].RetCode != warnStoragePoolDfnNotFound {
		t.Errorf("ret_code: got %d, want warnStoragePoolDfnNotFound (%d)",
			rcs[0].RetCode, warnStoragePoolDfnNotFound)
	}
}
