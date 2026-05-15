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
	"maps"
	"net/http"
	"sync"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerKeyValueStore wires `/v1/key-value-store` endpoints.
// linstor-csi uses one or two instances (`csi-backup-mapping`,
// per-PVC snapshot bookkeeping) to track CSI volume + snapshot
// metadata that doesn't have a first-class CRD. csi-sanity drives
// the full ListSnapshots / CreateSnapshot lifecycle through this
// surface, so a no-op stub fails 5+ snapshot tests.
//
// Phase 10.4 retired the KVEntry CRD because production blockstor
// no longer reads any per-PVC blob through it (cozystack relies on
// RD `Aux/csi-volume-annotations` instead), but the csi-sanity
// pinned linstor-csi version (v1.10.1) still writes here. A small
// in-memory bag is enough for contract testing — the data doesn't
// have to outlive the apiserver process.
func (s *Server) registerKeyValueStore(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/key-value-store", s.requireStore(handleKVList))
	mux.HandleFunc("GET /v1/key-value-store/{instance}", s.requireStore(handleKVGet))
	mux.HandleFunc("POST /v1/key-value-store/{instance}", s.requireStore(handleKVModify))
	mux.HandleFunc("PUT /v1/key-value-store/{instance}", s.requireStore(handleKVModify))
	mux.HandleFunc("DELETE /v1/key-value-store/{instance}", s.requireStore(handleKVDelete))
}

// kvBag is the in-memory KV store backing the /v1/key-value-store
// endpoints. Process-local so the data is per-pod, but persists
// across requests within a process — enough for csi-sanity's
// lifecycle tests. Keyed by instance name, value is the prop bag.
//
//nolint:gochecknoglobals // process-local store, mutex-guarded
var (
	kvBagMu sync.Mutex
	kvBag   = map[string]map[string]string{}
)

// handleKVList returns every instance as a `KV{Name, Props}` entry
// in a flat array — the wire shape golinstor's KeyValueStoreService
// decodes into `[]KV`.
func handleKVList(w http.ResponseWriter, _ *http.Request) {
	kvBagMu.Lock()
	defer kvBagMu.Unlock()

	out := make([]apiv1.KV, 0, len(kvBag))
	for name, props := range kvBag {
		entry := apiv1.KV{Name: name}
		if len(props) > 0 {
			entry.Props = map[string]string{}
			maps.Copy(entry.Props, props)
		}

		out = append(out, entry)
	}

	writeJSON(w, http.StatusOK, out)
}

// handleKVGet returns the named instance wrapped in a single-element
// array. golinstor's `KeyValueStoreService.Get` decodes into `[]KV`
// (see upstream's keyvaluestore.go) — a bare object response breaks
// linstor-csi's snapshot lookup with "cannot unmarshal object into
// Go value of type []client.KV".
func handleKVGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")
	entry := apiv1.KV{Name: name}

	kvBagMu.Lock()
	if props, ok := kvBag[name]; ok && len(props) > 0 {
		entry.Props = map[string]string{}
		maps.Copy(entry.Props, props)
	}
	kvBagMu.Unlock()

	writeJSON(w, http.StatusOK, []apiv1.KV{entry})
}

// handleKVModify accepts a `GenericPropsModify` body and applies
// the OverrideProps / DeleteProps merge onto the instance's bag.
// Auto-creates the instance on first write — matches upstream
// LINSTOR's "PUT /kv/foo" creates-or-modifies semantic.
//
// Returns 200 + the LINSTOR `[]APICallRc` envelope (Bug 121).
// python-linstor 1.27.1's KeyValueStore.modify codepath JSON-decodes
// the body unconditionally, so a 200 with an empty body trips
// `json.loads("")` and the operator sees
//
//	Error: Unable to parse REST json data: Expecting value: line 1 column 1 (char 0)
//
// even though the bag mutation was applied. The envelope's
// `ret_code` carries MASK_INFO so the CLI's `rc.is_success()`
// branch fires.
//
// Body validation (Bug 122): a request whose top-level keys are
// none of `override_props` / `delete_props` / `delete_namespaces`
// is rejected with 400 + envelope. Pre-fix, a raw JSON-document
// PUT like `{"X":"y2"}` was silently decoded into an empty
// GenericPropsModify struct (Go's `encoding/json` ignores unknown
// fields by default), the handler reported "success", and the
// mutation was dropped on the floor. We reject rather than treat
// it as whole-state replacement: the upstream LINSTOR
// `key-value-store` REST surface is patch-only, and a silent
// semantic switch between two clients hitting the same endpoint
// is the worse failure mode.
func handleKVModify(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	patch, ok := decodeKVModifyBody(w, r)
	if !ok {
		return
	}

	kvBagMu.Lock()

	bag, exists := kvBag[name]
	if !exists {
		bag = map[string]string{}
		kvBag[name] = bag
	}

	maps.Copy(bag, patch.OverrideProps)

	for _, key := range patch.DeleteProps {
		delete(bag, key)
	}

	kvBagMu.Unlock()

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "key-value-store modified",
		ObjRefs: map[string]string{objRefKeyValueStore: name},
	}})
}

// objRefKeyValueStore is the `ObjRefs` key the KV-store handlers
// stamp on success / failure envelopes. Matches upstream LINSTOR's
// audit-log classifier string for the KV subject — operator-side
// log greppers that already route on the upstream name pick the
// blockstor envelopes up without an extra rule.
const objRefKeyValueStore = "KeyValueStore"

// decodeKVModifyBody parses the PUT/POST body for `handleKVModify`.
// Bug 122: the upstream LINSTOR KV REST surface is patch-only, so a
// body whose top-level keys are anything other than
// `override_props` / `delete_props` / `delete_namespaces` is
// rejected with 400 + envelope. Returns the parsed patch and ok=true
// on success; on failure it writes the 400 envelope itself and
// returns ok=false so the caller just returns.
func decodeKVModifyBody(w http.ResponseWriter, r *http.Request) (apiv1.GenericPropsModify, bool) {
	var (
		patch apiv1.GenericPropsModify
		raw   map[string]json.RawMessage
	)

	err := json.NewDecoder(r.Body).Decode(&raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "decode body: "+err.Error())

		return patch, false
	}

	for key := range raw {
		switch key {
		case "override_props", "delete_props", "delete_namespaces":
			continue
		default:
			writeError(w, http.StatusBadRequest,
				"unexpected field "+key+" in PUT body; "+
					"use {\"override_props\":{...},\"delete_props\":[...],"+
					"\"delete_namespaces\":[...]}")

			return patch, false
		}
	}

	if rawOverride, ok := raw["override_props"]; ok {
		err = json.Unmarshal(rawOverride, &patch.OverrideProps)
		if err != nil {
			writeError(w, http.StatusBadRequest, "decode override_props: "+err.Error())

			return patch, false
		}
	}

	if rawDelete, ok := raw["delete_props"]; ok {
		err = json.Unmarshal(rawDelete, &patch.DeleteProps)
		if err != nil {
			writeError(w, http.StatusBadRequest, "decode delete_props: "+err.Error())

			return patch, false
		}
	}

	if rawNS, ok := raw["delete_namespaces"]; ok {
		err = json.Unmarshal(rawNS, &patch.DeleteNamespace)
		if err != nil {
			writeError(w, http.StatusBadRequest, "decode delete_namespaces: "+err.Error())

			return patch, false
		}
	}

	return patch, true
}

// handleKVDelete drops the instance. Missing → no-op so an
// operator-side teardown script can re-run safely.
//
// Returns 200 + the LINSTOR `[]APICallRc` envelope (Bug 121).
// Same crash mode as handleKVModify on python-linstor 1.27.1:
// the pre-fix empty body tripped `json.loads("")` in
// KeyValueStore.delete and the operator's `linstor
// key-value-store delete <name>` exited non-zero even when the
// instance was successfully removed.
func handleKVDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	kvBagMu.Lock()
	delete(kvBag, name)
	kvBagMu.Unlock()

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "key-value-store deleted",
		ObjRefs: map[string]string{"KeyValueStore": name},
	}})
}
