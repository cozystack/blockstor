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
)

// Wire-shape keys used in JSON responses on the resource-connection
// endpoints. Extracted as constants because goconst flags every
// duplicate and because the upstream LINSTOR
// `ResourceConnection` schema is the authoritative source — keep
// the literals in one spot so a schema change is one edit.
const (
	keyResourceConnectionNodeA = "node_a"
	keyResourceConnectionNodeB = "node_b"
	keyResourceConnectionProps = "props"
)

// resourceConnectionRegistry holds per-(rd, nodeA, nodeB) DRBD tuning
// props in memory — scenario 5.W04 (`linstor resource-connection
// drbd-peer-options <rd> <a> <b> --max-buffers 8192`).
//
// Scope: distinct from node-connection (per-(a, b), every RD —
// scenario 5.W03), distinct from RD/resource scope (every connection
// of the RD — 5.W01). Keying on the (rd, sorted-pair) tuple keeps
// the operator-set tuning bound to exactly one connection block of
// one resource's mesh.
//
// Storage is process-local rather than CRD-backed because the
// dispatcher path that consumes these props for ResourceConnection
// scope is still being wired (the satellite renderer is the
// authoritative consumer); persisting in the apiserver process keeps
// the REST surface honest without implying CRD ownership we don't
// have yet. Restarting the apiserver loses the tuning — same
// behaviour as `linstorRemoteRegistry`, and `linstor
// resource-connection list` will surface an empty Props map until
// the operator re-PATCHes.
type resourceConnectionRegistry struct {
	mu sync.RWMutex
	// entries[rd][pairKey] = props map (DrbdOptions/PeerDevice/...,
	// DrbdOptions/Net/...). pairKey is the sorted (a, b) tuple
	// joined by NUL — so a PATCH on `n1 n2` and a PATCH on `n2 n1`
	// reach the same map.
	entries map[string]map[string]map[string]string
}

// resourceConnectionPairSeparator joins the sorted (nodeA, nodeB)
// names into a single map key. NUL is reserved in LINSTOR node names
// (they're RFC1123-ish), so we can't collide with an actual name.
const resourceConnectionPairSeparator = "\x00"

func newResourceConnectionRegistry() *resourceConnectionRegistry {
	return &resourceConnectionRegistry{
		entries: map[string]map[string]map[string]string{},
	}
}

// pairKey returns the canonical map key for the (a, b) pair. Sorted
// lexicographically so `n1 n2` and `n2 n1` are the same connection.
func pairKey(nodeA, nodeB string) string {
	if nodeA <= nodeB {
		return nodeA + resourceConnectionPairSeparator + nodeB
	}

	return nodeB + resourceConnectionPairSeparator + nodeA
}

// get returns a snapshot of the props map for (rd, a, b) — never the
// underlying map, so callers can't mutate registry state. Empty map
// when the pair has never been PATCHed.
func (r *resourceConnectionRegistry) get(rd, nodeA, nodeB string) map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	props, ok := r.entries[rd][pairKey(nodeA, nodeB)]
	if !ok {
		return map[string]string{}
	}

	out := make(map[string]string, len(props))
	maps.Copy(out, props)

	return out
}

// merge applies the override / delete delta to (rd, a, b). Matches
// upstream LINSTOR's `override_props` + `delete_props` envelope
// shape — a PATCH carrying both fields applies override first, then
// delete (consistent with the RD-scope handler).
func (r *resourceConnectionRegistry) merge(rd, nodeA, nodeB string, override map[string]string, del []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.entries[rd] == nil {
		r.entries[rd] = map[string]map[string]string{}
	}

	key := pairKey(nodeA, nodeB)
	if r.entries[rd][key] == nil {
		r.entries[rd][key] = map[string]string{}
	}

	maps.Copy(r.entries[rd][key], override)

	for _, k := range del {
		delete(r.entries[rd][key], k)
	}
}

// registerResourceConnections wires the LINSTOR `resource-connection`
// REST surface — scenario 5.W04. Distinct from the no-op
// `node-connection` family (scenario 5.W03 / pkg/rest/
// node_connections.go) because the scope is per-(rd, a, b), not
// per-(a, b): the same operator-driven tuning would either apply to
// every RD in the cluster (node-connection) or to one resource's
// connection block (resource-connection). Operators tune
// max-buffers / ping-timeout differently across RDs (a hot snapshot
// RD wants different replication tuning than a sleepy backup RD),
// so the resource-connection scope is the one that gets persisted
// here.
//
// We expose every verb so golinstor / `linstor resource-connection`
// don't 404 on a misguided call. Read and write paths are real —
// unlike node-connection, the value matters: the satellite renderer
// will consume these props to stamp `connection { net { ... } }`
// inside the matching `.res` block.
func (s *Server) registerResourceConnections(mux *http.ServeMux) {
	// Lazy-init mirrors `linstorRemotes` — buildMux runs once at
	// startup, so a pointer field added on the Server is safe to
	// populate here without a constructor change.
	if s.resourceConnections == nil {
		s.resourceConnections = newResourceConnectionRegistry()
	}

	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resource-connections",
		s.handleResourceConnectionsList)
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resource-connections/{a}/{b}",
		s.handleResourceConnectionGet)
	// `drbd-peer-options` PATCH is the canonical operator-driven
	// write path — `linstor resource-connection drbd-peer-options
	// <rd> <a> <b> --max-buffers 8192` lands here.
	mux.HandleFunc("PATCH /v1/resource-definitions/{rd}/resource-connections/{a}/{b}/drbd-peer-options",
		s.handleResourceConnectionDRBDPeerOptions)
	// The bare PATCH endpoint accepts the same envelope so callers
	// that pre-compute the upstream LINSTOR prop key on the client
	// side don't need a separate subcommand. Same merge semantics.
	mux.HandleFunc("PATCH /v1/resource-definitions/{rd}/resource-connections/{a}/{b}",
		s.handleResourceConnectionDRBDPeerOptions)
	// PUT is reserved on the upstream surface but unused by the CLI;
	// accept-and-merge keeps tooling happy without diverging the
	// write path.
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resource-connections/{a}/{b}",
		s.handleResourceConnectionDRBDPeerOptions)
}

// handleResourceConnectionsList returns every (a, b) pair we've
// recorded props for under the given RD. Empty array when no
// operator has PATCHed yet — matches golinstor's expected
// `[]ResourceConnection` shape on the list endpoint.
func (s *Server) handleResourceConnectionsList(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	s.resourceConnections.mu.RLock()
	defer s.resourceConnections.mu.RUnlock()

	out := make([]map[string]any, 0, len(s.resourceConnections.entries[rd]))

	for key, props := range s.resourceConnections.entries[rd] {
		nodeA, nodeB := splitPairKey(key)

		propsCopy := make(map[string]string, len(props))
		maps.Copy(propsCopy, props)

		out = append(out, map[string]any{
			keyResourceConnectionNodeA: nodeA,
			keyResourceConnectionNodeB: nodeB,
			keyResourceConnectionProps: propsCopy,
		})
	}

	writeJSON(w, http.StatusOK, out)
}

// splitPairKey is the inverse of pairKey — extracts (a, b) from the
// stored map key. Returned in sorted order; the caller doesn't see
// the original PATCH ordering since it was discarded at write time.
func splitPairKey(key string) (string, string) {
	for i := range len(key) {
		if key[i] == 0 { // NUL separator
			return key[:i], key[i+1:]
		}
	}

	return key, ""
}

// handleResourceConnectionGet returns the per-pair props bag. Empty
// `props: {}` when the pair has never been PATCHed — matches the
// upstream shape so golinstor decodes into a zero-prop struct
// without erroring.
func (s *Server) handleResourceConnectionGet(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	nodeA := r.PathValue("a")
	nodeB := r.PathValue("b")

	props := s.resourceConnections.get(rd, nodeA, nodeB)

	writeJSON(w, http.StatusOK, map[string]any{
		keyResourceConnectionNodeA: nodeA,
		keyResourceConnectionNodeB: nodeB,
		keyResourceConnectionProps: props,
	})
}

// resourceConnectionModifyBody mirrors upstream LINSTOR's
// `ResourceConnectionModify` shape — the same `override_props` +
// `delete_props` envelope every other modify endpoint uses (RD-scope
// `handleRDUpdate`, NodeConnection, …). Keeping the wire shape
// uniform lets golinstor reuse its serialiser.
type resourceConnectionModifyBody struct {
	OverrideProps    map[string]string `json:"override_props,omitempty"`
	DeleteProps      []string          `json:"delete_props,omitempty"`
	DeleteNamespaces []string          `json:"delete_namespaces,omitempty"`
}

// handleResourceConnectionDRBDPeerOptions PATCHes the (rd, a, b)
// props bag. Scenario 5.W04: `linstor resource-connection
// drbd-peer-options pvc-1 n1 n2 --max-buffers 8192` lands here with
// `{"override_props": {"DrbdOptions/PeerDevice/max-buffers": "8192"}}`
// and the registry persists the prop on the ResourceConnection.
//
// `--unset-max-buffers` (scenario 5.W02) lands here too via
// `delete_props: ["DrbdOptions/PeerDevice/max-buffers"]` — same
// merge semantics as `handleRDUpdate`.
func (s *Server) handleResourceConnectionDRBDPeerOptions(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	nodeA := r.PathValue("a")
	nodeB := r.PathValue("b")

	var patch resourceConnectionModifyBody

	err := json.NewDecoder(r.Body).Decode(&patch)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	s.resourceConnections.merge(rd, nodeA, nodeB, patch.OverrideProps, patch.DeleteProps)

	// 204 mirrors `handleNoContent` — the upstream LINSTOR contract
	// returns an APICallRc envelope on RD modify (we follow there) but
	// `resource-connection drbd-peer-options` lands as 204 in the
	// Java implementation, and golinstor accepts both shapes.
	w.WriteHeader(http.StatusNoContent)
}
