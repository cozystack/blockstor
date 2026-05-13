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
	"net/http"
	"slices"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerNodeLifecycle wires the eviction / restore / lost endpoints.
// They mark intent on the Node CRD; replica migration is the
// reconciler's job (Phase 6).
func (s *Server) registerNodeLifecycle(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/nodes/{node}/evacuate",
		s.requireStore(s.handleNodeEvacuate))
	mux.HandleFunc("POST /v1/nodes/{node}/restore",
		s.requireStore(s.handleNodeRestore))
	// Upstream LINSTOR uses DELETE here (golinstor's NodeService.Lost
	// does `doDELETE`); the legacy POST form is kept alongside for
	// shell scripts that hit it directly via curl without honouring
	// the OpenAPI spec.
	mux.HandleFunc("POST /v1/nodes/{node}/lost",
		s.requireStore(s.handleNodeLost))
	mux.HandleFunc("DELETE /v1/nodes/{node}/lost",
		s.requireStore(s.handleNodeLost))
	// Reconnect is golinstor's `NodeService.Reconnect` — PUT with
	// no body. It tells the controller to drop and re-establish the
	// satellite TCP. blockstor doesn't run a persistent satellite
	// TCP so this is a no-op that just acknowledges the request.
	mux.HandleFunc("PUT /v1/nodes/{node}/reconnect",
		s.requireStore(s.handleNodeReconnect))
}

// handleNodeReconnect acknowledges a satellite-reconnect request.
// blockstor's satellite-as-controller-runtime (Phase 10) uses k8s
// API watches, not TCP keepalives, so there's no socket to bounce —
// returning success matches the operator's mental model that the
// request was accepted.
func (s *Server) handleNodeReconnect(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	// Verify the node exists so a typo doesn't silently succeed.
	_, err := s.Store.Nodes().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node " + name + " reconnect requested",
	}})
}

// handleNodeEvacuate adds the EVICTED flag — a soft "drain me" hint
// the autoplacer respects (won't pick this node for new replicas) and
// the migration reconciler watches for.
func (s *Server) handleNodeEvacuate(w http.ResponseWriter, r *http.Request) {
	updateNodeFlags(w, r, s, addFlag("EVICTED"))
}

// handleNodeRestore removes EVICTED. Lost-and-found in production: a
// node we tried to drain came back online before the migration
// completed.
func (s *Server) handleNodeRestore(w http.ResponseWriter, r *http.Request) {
	updateNodeFlags(w, r, s, removeFlag("EVICTED"))
}

// handleNodeLost is the permanent action — upstream LINSTOR's
// `controller drop-node` removes the Node from the controller
// entirely; orphan Resources are re-placed on the next reconcile
// (Phase 6 work). blockstor mirrors that: delete the Node CRD, not
// just stamp flags. Missing-node is folded into success so a
// re-run of an operator teardown script doesn't fail on
// already-cleaned state.
func (s *Server) handleNodeLost(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	err := s.Store.Nodes().Delete(r.Context(), name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node lost: " + name,
	}})
}

// updateNodeFlags loads the Node, applies each mutator, and persists.
// Common shape across all three endpoints; lives here so the handler
// bodies stay one-line.
func updateNodeFlags(w http.ResponseWriter, r *http.Request, s *Server, mutators ...func([]string) []string) {
	name := r.PathValue("node")

	node, err := s.Store.Nodes().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	for _, m := range mutators {
		node.Flags = m(node.Flags)
	}

	err = s.Store.Nodes().Update(r.Context(), &node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node " + name + " flags updated",
	}})
}

// addFlag returns a mutator that adds flag if it's not already there.
// Idempotent so repeated POSTs don't grow the flag list.
func addFlag(flag string) func([]string) []string {
	return func(flags []string) []string {
		if slices.Contains(flags, flag) {
			return flags
		}

		return append(flags, flag)
	}
}

// removeFlag returns a mutator that drops every occurrence of flag.
func removeFlag(flag string) func([]string) []string {
	return func(flags []string) []string {
		out := flags[:0]

		for _, f := range flags {
			if f != flag {
				out = append(out, f)
			}
		}

		return out
	}
}
