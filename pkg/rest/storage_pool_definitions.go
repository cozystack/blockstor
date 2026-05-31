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
	"maps"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Finding 6 (P2): POST/PUT/DELETE on `/v1/storage-pool-definitions`
// were unwired and returned 405 Method Not Allowed. Upstream OpenAPI
// (rest_v1_openapi.yaml lines 59-161) defines all three verbs:
//
//   - POST   /v1/storage-pool-definitions          — create a definition
//   - PUT    /v1/storage-pool-definitions/{name}   — modify props
//   - DELETE /v1/storage-pool-definitions/{name}   — drop the definition
//
// `linstor sp-d c` / `linstor sp-d set-property` / `linstor sp-d d`
// drive these endpoints; piraeus-operator's chart provisioning POSTs
// definitions up-front before any per-node StoragePool is created.
// Without these handlers an operator-driven `linstor sp-d c` failed
// the create call with a bare 405, the python CLI crashed on the
// non-JSON body, and the chart-provisioning loop stalled.
//
// The store-backed surface intentionally stays minimal: a definition
// is just a `{name, props}` pair. The wire shape mirrors upstream's
// JsonGenTypes.StoragePoolDefinition — the `storage_pool_name` field
// the python CLI keys on plus an open-ended props map.
//
// DELETE refuses with 409 + FAIL_IN_USE when any per-node StoragePool
// still references the definition by name. Mirrors upstream LINSTOR's
// CtrlStorPoolDfnApiCallHandler refusal — operators must drop the
// referencing pools first.

// registerStoragePoolDefinitions wires the POST / PUT / DELETE handlers
// for the controller-scope StoragePoolDefinition registry. The matching
// GET endpoints (list + single) are registered alongside the GET-only
// surface in resource_group_extras.go and upstream_parity_bug_225_229.go
// — those continue to flow through their existing handlers (which now
// merge the registry rows with the synthesized per-pool-name list).
func (s *Server) registerStoragePoolDefinitions(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/storage-pool-definitions",
		s.requireStore(s.handleStoragePoolDefinitionCreate))
	mux.HandleFunc("PUT /v1/storage-pool-definitions/{name}",
		s.requireStore(s.handleStoragePoolDefinitionModify))
	mux.HandleFunc("DELETE /v1/storage-pool-definitions/{name}",
		s.requireStore(s.handleStoragePoolDefinitionDelete))
}

// storagePoolDefinitionWire is the JSON wire shape both the existing
// GET handlers and the new POST/PUT/DELETE handlers emit. Mirrors
// upstream's `JsonGenTypes.StoragePoolDefinition` minus the upstream-
// only `uuid` field (blockstor's process-local registry has no
// persistent identity to surface).
type storagePoolDefinitionWire struct {
	StoragePoolName string            `json:"storage_pool_name"`
	Props           map[string]string `json:"props,omitempty"`
}

// handleStoragePoolDefinitionCreate serves POST /v1/storage-pool-definitions.
//
// Body shape: `{storage_pool_name, props}`. `storage_pool_name` is
// required; props is optional. Returns 201 + ApiCallRc envelope on
// success, 400 on missing/invalid name, 409 + FAIL_EXISTS_STOR_POOL_DFN
// when a definition with the same name already exists.
//
// CREATE-only: re-POSTing the same name is NOT an upsert (mirrors
// Finding 1's fix for the per-node StoragePool surface — POST must
// not silently mutate existing rows). Operators that want to mutate
// props use PUT.
func (s *Server) handleStoragePoolDefinitionCreate(w http.ResponseWriter, r *http.Request) {
	var body storagePoolDefinitionWire

	if !decodeJSON(w, r, &body) {
		return
	}

	if body.StoragePoolName == "" {
		writeError(w, http.StatusBadRequest, "storage_pool_name is required")

		return
	}

	// REST-boundary identifier validation runs before any store call
	// so a whitespace-only / RFC-1123-illegal name fails fast with a
	// LINSTOR envelope (rather than leaking the k8s "metadata.name is
	// invalid" error). Mirrors Bug 97 / handleNodeStoragePoolCreate.
	nameErr := validateLinstorName("storage pool definition", body.StoragePoolName)
	if nameErr != nil {
		writeError(w, http.StatusBadRequest, nameErr.Error())

		return
	}

	def := store.StoragePoolDefinition{
		Name:  body.StoragePoolName,
		Props: body.Props,
	}

	err := s.Store.StoragePoolDefinitions().Create(r.Context(), &def)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
				RetCode: apiCallRcError | apiCallRcFailExistsStorPoolDfn,
				Message: "storage pool definition already exists: " + body.StoragePoolName,
				ObjRefs: map[string]string{
					objRefStorPoolDfn: body.StoragePoolName,
				},
			}})

			return
		}

		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "storage pool definition created: " + body.StoragePoolName,
	}})
}

// handleStoragePoolDefinitionModify serves PUT /v1/storage-pool-definitions/{name}.
//
// Wire shape: `GenericPropsModify` (override_props, delete_props,
// delete_namespaces) — the same envelope every other LINSTOR `modify`
// endpoint accepts. Props are merged on top of the existing row;
// delete_props / delete_namespaces drop matching keys before the
// merge lands.
//
// Returns 200 + ApiCallRc envelope on success; 404 + writeStoreError
// when the named definition doesn't exist. The definition is auto-
// created on PUT when no row exists yet — operators that drive the
// surface with `linstor sp-d set-property new-def key val` get the
// definition stamped in one round-trip without a separate POST.
func (s *Server) handleStoragePoolDefinitionModify(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	var body apiv1.GenericPropsModify

	if !decodeJSON(w, r, &body) {
		return
	}

	def, err := s.Store.StoragePoolDefinitions().Get(r.Context(), name)
	switch {
	case errors.Is(err, store.ErrNotFound):
		def = store.StoragePoolDefinition{Name: name, Props: map[string]string{}}
	case err != nil:
		writeStoreError(w, err)

		return
	}

	if def.Props == nil {
		def.Props = map[string]string{}
	}

	maps.Copy(def.Props, body.OverrideProps)

	for _, k := range body.DeleteProps {
		delete(def.Props, k)
	}

	// delete_namespaces: drop every key under the given namespace
	// prefix. Mirrors upstream LINSTOR's `delete_namespaces` behaviour
	// — separator is '/', prefix matches literally.
	for _, ns := range body.DeleteNamespace {
		prefix := ns + "/"

		for k := range def.Props {
			if strings.HasPrefix(k, prefix) {
				delete(def.Props, k)
			}
		}
	}

	// Try Update first; on ErrNotFound (PUT-creates path) fall back to
	// Create so the wire surface is upsert-idempotent.
	err = s.Store.StoragePoolDefinitions().Update(r.Context(), &def)
	if errors.Is(err, store.ErrNotFound) {
		err = s.Store.StoragePoolDefinitions().Create(r.Context(), &def)
	}

	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "storage pool definition modified: " + name,
	}})
}

// handleStoragePoolDefinitionDelete serves DELETE /v1/storage-pool-definitions/{name}.
//
// Refuses with 409 + FAIL_IN_USE when any per-node StoragePool still
// references the named definition. Operators must drop the referencing
// pools first — blockstor never cascades a definition-delete into the
// per-node SP rows, matching upstream LINSTOR's
// `CtrlStorPoolDfnApiCallHandler` refusal pattern.
//
// Idempotent on NotFound (Bug 66 family): a delete of an already-
// absent definition folds into 200 + `warnStoragePoolDfnNotFound` so
// audit-log greppers can distinguish the no-op replay from a real
// delete, matching the pattern Bug 56 set for RDs.
func (s *Server) handleStoragePoolDefinitionDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	if refused := s.refuseSPDfnDeleteIfReferenced(w, r, name); refused {
		return
	}

	err := s.Store.StoragePoolDefinitions().Delete(r.Context(), name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: warnStoragePoolDfnNotFound,
				Message: "storage pool definition already absent: " + name,
			}})

			return
		}

		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "storage pool definition deleted: " + name,
	}})
}

// refuseSPDfnDeleteIfReferenced walks the per-node StoragePool table
// and refuses the definition delete (409 + FAIL_IN_USE) when at least
// one pool still carries the named definition. Returns true when the
// HTTP error has already been written (the caller must stop) and
// false when the delete may proceed.
//
// Match is case-sensitive, mirroring how the StoragePool CRD spec
// stores `storage_pool_name` verbatim. Per-node pool names that
// differ only in case from the definition name are NOT treated as
// references — upstream LINSTOR treats those as different objects too.
func (s *Server) refuseSPDfnDeleteIfReferenced(w http.ResponseWriter, r *http.Request, name string) bool {
	pools, err := s.Store.StoragePools().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return true
	}

	var refs []string

	for i := range pools {
		if pools[i].StoragePoolName != name {
			continue
		}

		refs = append(refs, pools[i].NodeName+"/"+pools[i].StoragePoolName)
	}

	if len(refs) == 0 {
		return false
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: "The specified storage pool definition '" + name +
			"' can not be deleted as per-node storage pools are still using it.",
		Details: "Per-node storage pools still bound to the definition: " +
			strings.Join(refs, ", "),
		Correc: "Delete the listed per-node storage pools first " +
			"(`linstor sp d <node> <pool>`), then re-issue the definition delete.",
		ObjRefs: map[string]string{
			objRefStorPoolDfn: name,
		},
	}})

	return true
}
