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

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerNodes wires the /v1/nodes endpoints on mux. It is split out of
// Server.Start so each resource group lives in its own file.
func (s *Server) registerNodes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/nodes", s.requireStore(s.handleNodesList))
	mux.HandleFunc("GET /v1/nodes/{node}", s.requireStore(s.handleNodeGet))
	mux.HandleFunc("POST /v1/nodes", s.requireStore(s.handleNodeCreate))
	mux.HandleFunc("PUT /v1/nodes/{node}", s.requireStore(s.handleNodeUpdate))
	mux.HandleFunc("DELETE /v1/nodes/{node}", s.requireStore(s.handleNodeDelete))
}

// requireStore guards endpoints that need persistence; it returns 503 if the
// Store is nil. We can serve /v1/controller/version without a store, so this
// gate is per-handler rather than global.
func (s *Server) requireStore(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Store == nil {
			writeError(w, http.StatusServiceUnavailable, "store not configured")

			return
		}

		next(w, r)
	}
}

func (s *Server) handleNodesList(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.Store.Nodes().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, nodes)
}

func (s *Server) handleNodeGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	n, err := s.Store.Nodes().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, n)
}

func (s *Server) handleNodeCreate(w http.ResponseWriter, r *http.Request) {
	var n apiv1.Node

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(&n)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if n.Name == "" {
		writeError(w, http.StatusBadRequest, "node name is required")

		return
	}

	err = s.Store.Nodes().Create(r.Context(), &n)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, n)
}

func (s *Server) handleNodeUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	var n apiv1.Node

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(&n)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// The path name wins over any body name, so callers can omit it.
	n.Name = name

	err = s.Store.Nodes().Update(r.Context(), &n)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, n)
}

func (s *Server) handleNodeDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	err := s.Store.Nodes().Delete(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// writeStoreError maps store sentinel errors to HTTP statuses so handlers
// don't repeat the same switch.
func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// writeError sends the LINSTOR-shaped `[]ApiCallRc` error envelope.
// golinstor (and therefore linstor-csi) unmarshals failure responses
// into a slice — sending a `{"error": "..."}` object made every
// failed CSI call surface as `json: cannot unmarshal object into Go
// value of type client.ApiCallError` instead of the actual error
// message.
//
// retCode follows the upstream convention: high bit set means
// FATAL/error; we use 0xC000_0000 which is the masked-but-untyped
// "generic error" the controller uses when no specific code applies.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, []apiv1.APICallRc{{
		RetCode: apiCallRcError,
		Message: msg,
	}})
}

// apiCallRcError is upstream LINSTOR's ERROR mask (high bit set on a
// 64-bit mask). golinstor checks for this bit to decide pass/fail.
const apiCallRcError uint64 = 0xC000_0000_0000_0000
