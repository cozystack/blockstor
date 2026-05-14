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
	"context"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"

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
//
// Per UG9 §"Evacuating a node": refuse when any resource on the node
// has observed state.in_use=true (Primary, with a consumer mounting
// it). Stamping EVICTED silently would let the autoplacer/migrator
// strand an actively-mounted volume — a data-availability hazard the
// operator must consciously accept.
//
// `state.in_use == nil` is "satellite hasn't reported yet" and is
// NOT a refusal — the operator may legitimately evacuate a node
// before any satellite observation has landed.
//
// ?force=true bypasses the check, matching the precedent set by
// handleRGDelete (mirrors upstream LINSTOR's `--force`).
func (s *Server) handleNodeEvacuate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	if r.URL.Query().Get("force") != "true" {
		resources, err := s.Store.Resources().List(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())

			return
		}

		var inUse []string

		for i := range resources {
			res := &resources[i]
			if res.NodeName != name {
				continue
			}

			if res.State.InUse != nil && *res.State.InUse {
				inUse = append(inUse, res.Name)
			}
		}

		if len(inUse) > 0 {
			sort.Strings(inUse)
			writeError(w, http.StatusConflict, fmt.Sprintf(
				"cannot evacuate: %d resource(s) on node %s are in use; "+
					"demote or stop the consumer(s) first: %s",
				len(inUse), name, strings.Join(inUse, ", ")))

			return
		}
	}

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
//
// Scenario 4.W04 contract: the node is irrecoverable by definition
// (UG9 §"Auto-evict" warns "aggressive — never run on a recoverable
// node"). Resource CRDs hosted on this node MUST be cascade-deleted
// by the REST handler itself, NOT via the satellite finalizer — the
// satellite that owned `SatelliteResourceFinalizer` is gone with
// the node, so a plain DeletionTimestamp stamp would hang every
// orphan Resource forever and brick the next RD-create that
// recycles the name/port. StoragePool CRDs on the lost node are
// dropped the same way; they can never be probed again and leaving
// them pollutes `linstor sp l` and the autoplacer's free-space
// ranking. Surviving peer replicas are left alone so the
// TieBreaker reconciler can stamp.
func (s *Server) handleNodeLost(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("node")

	err := s.cascadeOrphansForLostNode(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	err = s.Store.Nodes().Delete(r.Context(), name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "node lost: " + name,
	}})
}

// cascadeOrphansForLostNode walks Resources + StoragePools and
// deletes every row whose NodeName matches the lost node. NotFound
// from a per-child Delete is swallowed (a child that already
// vanished — race with a parallel cascade or a previous partial
// teardown — must not fail the whole `node lost` call; the parent
// handler is itself idempotent for this exact reason). The first
// non-NotFound error short-circuits the cascade so the operator
// sees an actionable signal before the Node row vanishes.
func (s *Server) cascadeOrphansForLostNode(ctx context.Context, name string) error {
	resources, err := s.Store.Resources().List(ctx)
	if err != nil {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	for i := range resources {
		if resources[i].NodeName != name {
			continue
		}

		err = s.Store.Resources().Delete(ctx, resources[i].Name, name)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err //nolint:wrapcheck // surfaced via writeStoreError
		}
	}

	pools, err := s.Store.StoragePools().ListByNode(ctx, name)
	if err != nil {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	for i := range pools {
		err = s.Store.StoragePools().Delete(ctx, name, pools[i].StoragePoolName)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err //nolint:wrapcheck // surfaced via writeStoreError
		}
	}

	return nil
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
