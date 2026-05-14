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
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerNodeLifecycle wires the eviction / restore / lost endpoints.
// They mark intent on the Node CRD; replica migration is the
// reconciler's job (Phase 6).
func (s *Server) registerNodeLifecycle(mux *http.ServeMux) {
	// Multi-node evacuate (scenario 4.W06, cross-listed wave1 4.21).
	// Distinct path from the single-node variant so Go 1.22 ServeMux
	// routes by literal-vs-wildcard specificity without ambiguity.
	mux.HandleFunc("POST /v1/nodes/evacuate",
		s.requireStore(s.handleNodeEvacuateMulti))
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

	if !isForce(r) {
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

// evacuateMultiRequest is the wire shape of `POST /v1/nodes/evacuate`.
// Mirrors upstream LINSTOR's variadic `linstor node evacuate <n1>
// <n2> …` CLI form — the controller-side picks an ordering that
// doesn't lose redundancy at any point (UG9 §"Evacuating multiple
// nodes"). The REST surface's responsibility is the atomic intent
// stamp; replica migration is the reconciler's job (Phase 6).
type evacuateMultiRequest struct {
	Nodes []string `json:"nodes"`
}

// handleNodeEvacuateMulti is the variadic counterpart of
// handleNodeEvacuate. It stamps EVICTED on every node in the request
// body as an atomic unit: if ANY pre-check fails (unknown node,
// in-use resource, no surviving migration target), NO node receives
// the flag.
//
// "No candidate target" is a hard refusal because every remaining
// live node being inside the evacuating set (or already evicted)
// means the autoplacer has nowhere to land the displaced replicas —
// the operator would silently strand every replica that previously
// lived on the evacuating set. The reconciler treats EVICTED as
// "AutoplaceTarget=false", so evicted nodes don't count toward the
// candidate pool here either.
//
// ?force=true bypasses the in-use check (matching the single-node
// variant + handleRGDelete precedent). The no-candidate guard is
// NOT bypassed by force — there's no escape hatch for "migrate to
// nowhere".
func (s *Server) handleNodeEvacuateMulti(w http.ResponseWriter, r *http.Request) {
	var req evacuateMultiRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if len(req.Nodes) == 0 {
		writeError(w, http.StatusBadRequest,
			"nodes: at least one node name is required")

		return
	}

	ctx := r.Context()
	evacuating := evacuatingSet(req.Nodes)

	nodes, ok := s.loadEvacuateTargets(ctx, w, req.Nodes)
	if !ok {
		return
	}

	if !s.checkEvacuateCandidate(ctx, w, req.Nodes, evacuating) {
		return
	}

	if !isForce(r) && !s.checkEvacuateInUse(ctx, w, evacuating) {
		return
	}

	// All pre-checks passed — stamp EVICTED on every node.
	// Idempotent per addFlag's slices.Contains short-circuit.
	for i := range nodes {
		nodes[i].Flags = addFlag("EVICTED")(nodes[i].Flags)

		err = s.Store.Nodes().Update(ctx, &nodes[i])
		if err != nil {
			writeStoreError(w, err)

			return
		}
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "nodes evacuating: " + strings.Join(req.Nodes, ", "),
	}})
}

// isForce centralises the `?force=true` query-string check used by
// both the single-node and multi-node evacuate guards. Routed
// through strconv.ParseBool so the literal `"true"` lives in
// stdlib, not in every call site (goconst flags package-wide
// `"true"` repetition above its threshold).
func isForce(r *http.Request) bool {
	v, _ := strconv.ParseBool(r.URL.Query().Get("force"))

	return v
}

// evacuatingSet folds the variadic node list into a lookup map.
// Kept separate from the handler body so the pre-check helpers
// share the same shape without re-allocating per call.
func evacuatingSet(names []string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, name := range names {
		out[name] = true
	}

	return out
}

// loadEvacuateTargets fetches every named node up-front so an
// unknown name (operator typo) fails the whole call with 404
// before any flag is stamped. Returns ok=false on any error path
// (response already written).
func (s *Server) loadEvacuateTargets(ctx context.Context, w http.ResponseWriter, names []string) ([]apiv1.Node, bool) {
	nodes := make([]apiv1.Node, 0, len(names))

	for _, name := range names {
		node, err := s.Store.Nodes().Get(ctx, name)
		if err != nil {
			writeStoreError(w, err)

			return nil, false
		}

		nodes = append(nodes, node)
	}

	return nodes, true
}

// checkEvacuateCandidate verifies that at least one live (non-
// evacuating, non-EVICTED) node remains in the cluster. Without it
// the reconciler has nowhere to migrate displaced replicas, so the
// REST surface refuses with 409 before any flag stamps.
func (s *Server) checkEvacuateCandidate(ctx context.Context, w http.ResponseWriter, names []string, evacuating map[string]bool) bool {
	allNodes, err := s.Store.Nodes().List(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return false
	}

	for i := range allNodes {
		n := &allNodes[i]
		if evacuating[n.Name] {
			continue
		}

		if slices.Contains(n.Flags, "EVICTED") {
			continue
		}

		return true
	}

	writeError(w, http.StatusConflict, fmt.Sprintf(
		"cannot evacuate %s: no candidate target node remains "+
			"(every live node is in the evacuating set or already "+
			"EVICTED); bring up a fresh target node before draining",
		strings.Join(names, ", ")))

	return false
}

// checkEvacuateInUse fails the call with 409 if ANY resource on any
// of the evacuating nodes is observed Primary. Mirrors the single-
// node variant's UG9 §"Evacuating a node" guard, atomic across the
// whole set so a partial drain can't half-apply.
func (s *Server) checkEvacuateInUse(ctx context.Context, w http.ResponseWriter, evacuating map[string]bool) bool {
	resources, err := s.Store.Resources().List(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return false
	}

	var inUse []string

	for i := range resources {
		res := &resources[i]
		if !evacuating[res.NodeName] {
			continue
		}

		if res.State.InUse != nil && *res.State.InUse {
			inUse = append(inUse, res.Name+"@"+res.NodeName)
		}
	}

	if len(inUse) == 0 {
		return true
	}

	sort.Strings(inUse)
	writeError(w, http.StatusConflict, fmt.Sprintf(
		"cannot evacuate: %d resource(s) on requested nodes "+
			"are in use; demote or stop the consumer(s) first: %s",
		len(inUse), strings.Join(inUse, ", ")))

	return false
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
