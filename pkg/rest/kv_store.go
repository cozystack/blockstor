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
func handleKVModify(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	var patch apiv1.GenericPropsModify

	err := json.NewDecoder(r.Body).Decode(&patch)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	kvBagMu.Lock()
	defer kvBagMu.Unlock()

	bag, ok := kvBag[name]
	if !ok {
		bag = map[string]string{}
		kvBag[name] = bag
	}

	maps.Copy(bag, patch.OverrideProps)

	for _, k := range patch.DeleteProps {
		delete(bag, k)
	}

	w.WriteHeader(http.StatusOK)
}

// handleKVDelete drops the instance. Missing → no-op so an
// operator-side teardown script can re-run safely.
func handleKVDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	kvBagMu.Lock()
	delete(kvBag, name)
	kvBagMu.Unlock()

	w.WriteHeader(http.StatusOK)
}
