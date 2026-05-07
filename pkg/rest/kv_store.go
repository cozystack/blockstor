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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerKeyValueStore wires /v1/key-value-store endpoints. linstor-csi
// uses these for its own per-volume bookkeeping.
func (s *Server) registerKeyValueStore(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/key-value-store", s.requireStore(s.handleKVList))
	mux.HandleFunc("GET /v1/key-value-store/{instance}", s.requireStore(s.handleKVGet))
	mux.HandleFunc("POST /v1/key-value-store/{instance}", s.requireStore(s.handleKVSet))
	mux.HandleFunc("PUT /v1/key-value-store/{instance}", s.requireStore(s.handleKVSet))
	mux.HandleFunc("DELETE /v1/key-value-store/{instance}", s.requireStore(s.handleKVDelete))
}

func (s *Server) handleKVList(w http.ResponseWriter, r *http.Request) {
	insts, err := s.Store.KeyValueStore().ListInstances(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	out := make([]apiv1.KV, 0, len(insts))

	for _, name := range insts {
		props, gErr := s.Store.KeyValueStore().GetInstance(r.Context(), name)
		if gErr != nil {
			continue
		}

		out = append(out, apiv1.KV{Name: name, Props: props})
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	props, err := s.Store.KeyValueStore().GetInstance(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, apiv1.KV{Name: name, Props: props})
}

func (s *Server) handleKVSet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	var modify apiv1.GenericPropsModify

	err := json.NewDecoder(r.Body).Decode(&modify)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	err = s.Store.KeyValueStore().SetKeys(r.Context(), name, modify)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("instance")

	err := s.Store.KeyValueStore().DeleteInstance(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
