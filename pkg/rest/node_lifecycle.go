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
)

// registerNodeLifecycle wires the eviction / restore / lost endpoints.
// They mark intent on the Node CRD; replica migration is the
// reconciler's job (Phase 6).
func (s *Server) registerNodeLifecycle(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/nodes/{node}/evacuate",
		s.requireStore(s.handleNodeEvacuate))
	mux.HandleFunc("POST /v1/nodes/{node}/restore",
		s.requireStore(s.handleNodeRestore))
	mux.HandleFunc("POST /v1/nodes/{node}/lost",
		s.requireStore(s.handleNodeLost))
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

// handleNodeLost is the permanent action — node is gone, replicas
// must be re-created elsewhere even without local cooperation.
func (s *Server) handleNodeLost(w http.ResponseWriter, r *http.Request) {
	updateNodeFlags(w, r, s, addFlag("LOST"), addFlag("EVICTED"))
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

	w.WriteHeader(http.StatusOK)
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
