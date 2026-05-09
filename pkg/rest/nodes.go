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

	// Per-interface CRUD: clusters with separate replication and
	// management networks need to add/remove NetInterfaces on a
	// running Node without a full PUT-of-the-whole-Node round-trip.
	// Maps onto Node.Spec.NetInterfaces[] inside the same Node CRD.
	mux.HandleFunc("POST /v1/nodes/{node}/net-interfaces",
		s.requireStore(s.handleNetInterfaceCreate))
	mux.HandleFunc("PUT /v1/nodes/{node}/net-interfaces/{name}",
		s.requireStore(s.handleNetInterfaceUpdate))
	mux.HandleFunc("DELETE /v1/nodes/{node}/net-interfaces/{name}",
		s.requireStore(s.handleNetInterfaceDelete))
}

// handleNetInterfaceCreate appends a NetInterface to the Node's spec.
// Idempotent: a second create with the same name updates in place.
func (s *Server) handleNetInterfaceCreate(w http.ResponseWriter, r *http.Request) {
	mutateNetInterface(w, r, s, func(n *apiv1.Node, iface apiv1.NetInterface) error {
		for i := range n.NetInterfaces {
			if n.NetInterfaces[i].Name == iface.Name {
				n.NetInterfaces[i] = iface

				return nil
			}
		}

		n.NetInterfaces = append(n.NetInterfaces, iface)

		return nil
	})
}

// handleNetInterfaceUpdate is the per-name replace. The path's
// {name} wins over any name in the body so callers can omit it.
func (s *Server) handleNetInterfaceUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	mutateNetInterface(w, r, s, func(n *apiv1.Node, iface apiv1.NetInterface) error {
		iface.Name = name

		for i := range n.NetInterfaces {
			if n.NetInterfaces[i].Name == name {
				n.NetInterfaces[i] = iface

				return nil
			}
		}

		// Update on a missing interface is also a create — matches
		// upstream LINSTOR's PUT-creates semantic for `linstor n
		// interface modify`.
		n.NetInterfaces = append(n.NetInterfaces, iface)

		return nil
	})
}

// handleNetInterfaceDelete drops the named NetInterface. Missing →
// no-op (idempotent).
func (s *Server) handleNetInterfaceDelete(w http.ResponseWriter, r *http.Request) {
	nodeName := r.PathValue("node")
	name := r.PathValue("name")

	node, err := s.Store.Nodes().Get(r.Context(), nodeName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	out := node.NetInterfaces[:0]

	for i := range node.NetInterfaces {
		if node.NetInterfaces[i].Name == name {
			continue
		}

		out = append(out, node.NetInterfaces[i])
	}

	node.NetInterfaces = out

	err = s.Store.Nodes().Update(r.Context(), &node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// mutateNetInterface decodes a NetInterface body, runs the supplied
// mutation against the node's interface list, and persists. Used by
// both create and update so the decoder + Get + Update plumbing stays
// in one place.
func mutateNetInterface(w http.ResponseWriter, r *http.Request, s *Server, mutate func(*apiv1.Node, apiv1.NetInterface) error) {
	nodeName := r.PathValue("node")

	var iface apiv1.NetInterface

	err := json.NewDecoder(r.Body).Decode(&iface)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if iface.Name == "" && r.PathValue("name") == "" {
		writeError(w, http.StatusBadRequest, "interface name is required")

		return
	}

	node, err := s.Store.Nodes().Get(r.Context(), nodeName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	err = mutate(&node, iface)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	err = s.Store.Nodes().Update(r.Context(), &node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, node)
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

	err := json.NewDecoder(r.Body).Decode(&n)
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

	err := json.NewDecoder(r.Body).Decode(&n)
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

// apiCallRcError carries upstream LINSTOR's MASK_ERROR + WARN bits.
// Upstream uses 0xC000_0000_0000_0000 as a `long` literal — that's
// a negative int64 once you set both top bits. We literal-cast to
// match the wire shape golinstor expects.
const apiCallRcError int64 = -0x4000_0000_0000_0000
