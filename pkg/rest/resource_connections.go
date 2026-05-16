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
	"net/http"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerResourceConnections wires the resource-connection / paths
// surface from scenario 3.7 (UG9 §"Creating multiple DRBD paths with
// LINSTOR"). Paths are stored as a JSON-encoded string under a
// dedicated RD prop — see resourceConnectionPathsPropKey — so the
// storage layer (kube/inmemory) doesn't need a new CRD just to hold
// what is effectively an N-slot list per peer pair.
//
// The endpoint is logically symmetric in (nodeA, nodeB); writes under
// one order are readable under the swapped order with A/B addresses
// flipped accordingly. Operators expect this because drbd-9 has no
// notion of a "primary" endpoint within a connection.
func (s *Server) registerResourceConnections(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resource-connections/{nodeA}/{nodeB}/paths",
		s.requireStore(s.handleResourceConnectionPathsList))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/resource-connections/{nodeA}/{nodeB}/paths",
		s.requireStore(s.handleResourceConnectionPathCreate))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}/resource-connections/{nodeA}/{nodeB}/paths/{name}",
		s.requireStore(s.handleResourceConnectionPathDelete))
	// Bug 99: `linstor resource-connection list <rd>` calls
	// `GET /v1/resource-definitions/{rd}/resource-connections` and
	// expects a JSON array of `{node_a, node_b, props, flags, port}`
	// objects. blockstor does not yet model per-(rd, a, b) connection
	// state outside the multi-path subkey, so the list is always
	// empty for now — the important contract is the JSON envelope
	// (top-level `[]`) so the python CLI doesn't crash with a
	// ParseError. The path is also surfaced at the legacy upstream
	// `/v1/resource-connections/{rd}` shape some older clients still
	// hit.
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resource-connections",
		s.requireStore(s.handleResourceConnectionList))
	mux.HandleFunc("GET /v1/resource-connections/{rd}",
		s.requireStore(s.handleResourceConnectionList))
}

// resourceConnectionListEntry is the wire shape python-linstor's
// ResourceConnection class consumes (see linstor/responses.py:1841).
// We keep the type local because the storage layer does not yet
// produce these — once it does, this stub grows a real builder.
type resourceConnectionListEntry struct {
	NodeA string            `json:"node_a"`
	NodeB string            `json:"node_b"`
	Flags []string          `json:"flags,omitempty"`
	Props map[string]string `json:"props,omitempty"`
	Port  *int32            `json:"port,omitempty"`
}

// handleResourceConnectionList returns the per-pair connection list
// for the named RD. We respond 200 with `[]` whenever the RD exists
// but no per-pair state has been stamped; the store error envelope
// (typically 404) when the RD itself is unknown. Bug 99: previously
// this path 404'd at the router and the python CLI couldn't parse
// the body.
func (s *Server) handleResourceConnectionList(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	// Existence probe — returns the canonical not-found envelope on
	// typos so the operator sees the actual problem.
	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// blockstor stores per-pair state only under the multi-path
	// subkey today; there are no top-level resource-connection
	// objects to enumerate. Returning an empty array is the same
	// shape upstream LINSTOR returns for a fresh RD before any
	// `resource-connection set-property` calls land.
	writeJSON(w, http.StatusOK, []resourceConnectionListEntry{})
}

// resourceConnectionPathsPropKey is the RD-prop namespace under which
// the JSON-encoded path list lives. The (a, b) tuple is normalised
// (lexicographically sorted) so a store lookup under either order
// finds the same entry — see canonicalConnectionKey.
const resourceConnectionPathsPropKey = "Cozystack/ResourceConnectionPaths/"

// resourceConnectionPath is the wire shape POST/GET use. node_*_address
// fields match the snake_case golinstor uses on the upstream
// `/v1/resource-connections` surface.
type resourceConnectionPath struct {
	Name         string `json:"name"`
	NodeAAddress string `json:"node_a_address"`
	NodeBAddress string `json:"node_b_address"`
}

// canonicalConnectionKey returns (sortedA, sortedB, swap) where
// sortedA <= sortedB lexicographically. swap is true when the caller
// passed the pair in reverse order; the handler uses that to flip
// A/B addresses on the way in/out so the stored entry always lives
// under the canonical order.
func canonicalConnectionKey(a, b string) (string, string, bool) {
	if a <= b {
		return a, b, false
	}

	return b, a, true
}

// propKey is the full RD-prop key for a (nodeA, nodeB) pair. The pair
// is canonicalised before joining so the key is identical whether
// the operator posted under (a, b) or (b, a).
func propKey(nodeA, nodeB string) string {
	sortedA, sortedB, _ := canonicalConnectionKey(nodeA, nodeB)

	return resourceConnectionPathsPropKey + sortedA + "/" + sortedB
}

// loadPaths reads + decodes the stored path list for the (nodeA,
// nodeB) pair on rd. Returns (paths, swap, error); `swap` mirrors
// canonicalConnectionKey's third return so callers know whether to
// flip A/B before encoding the response.
func loadPaths(rd *apiv1.ResourceDefinition, nodeA, nodeB string) ([]resourceConnectionPath, bool, error) {
	_, _, swap := canonicalConnectionKey(nodeA, nodeB)

	raw, ok := rd.Props[propKey(nodeA, nodeB)]
	if !ok || raw == "" {
		return nil, swap, nil
	}

	var paths []resourceConnectionPath

	err := json.Unmarshal([]byte(raw), &paths)
	if err != nil {
		return nil, swap, errors.Wrap(err, "decode resource-connection paths")
	}

	return paths, swap, nil
}

// storePaths encodes + persists the path list onto rd. Pass an empty
// slice to delete the prop key entirely (so an emptied connection
// doesn't leak as a zombie `[]` value in storage).
func storePaths(rd *apiv1.ResourceDefinition, nodeA, nodeB string, paths []resourceConnectionPath) error {
	key := propKey(nodeA, nodeB)

	if len(paths) == 0 {
		delete(rd.Props, key)

		return nil
	}

	if rd.Props == nil {
		rd.Props = map[string]string{}
	}

	encoded, err := json.Marshal(paths)
	if err != nil {
		return errors.Wrap(err, "encode resource-connection paths")
	}

	rd.Props[key] = string(encoded)

	return nil
}

// swapPathAB returns p with A/B fields flipped. Used by the request
// edge to translate operator-facing (nodeA, nodeB) order into the
// canonical storage order, and again on the way out.
func swapPathAB(p resourceConnectionPath) resourceConnectionPath {
	return resourceConnectionPath{
		Name:         p.Name,
		NodeAAddress: p.NodeBAddress,
		NodeBAddress: p.NodeAAddress,
	}
}

// swapAll flips A/B on every entry. Cheap (per-path tuple).
func swapAll(paths []resourceConnectionPath) []resourceConnectionPath {
	out := make([]resourceConnectionPath, len(paths))
	for i := range paths {
		out[i] = swapPathAB(paths[i])
	}

	return out
}

// handleResourceConnectionPathsList answers GET. Always returns a
// JSON array (possibly empty) — golinstor decodes a `null` body as
// a malformed response, so the empty-but-present invariant matters.
func (s *Server) handleResourceConnectionPathsList(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	nodeA := r.PathValue("nodeA")
	nodeB := r.PathValue("nodeB")

	rd, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	paths, swap, err := loadPaths(&rd, nodeA, nodeB)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if swap {
		paths = swapAll(paths)
	}

	if paths == nil {
		paths = []resourceConnectionPath{}
	}

	writeJSON(w, http.StatusOK, paths)
}

// handleResourceConnectionPathCreate answers POST. Idempotent on
// path.Name — re-posting an existing name replaces its addresses
// rather than appending a duplicate.
func (s *Server) handleResourceConnectionPathCreate(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	nodeA := r.PathValue("nodeA")
	nodeB := r.PathValue("nodeB")

	var body resourceConnectionPath

	if !decodeJSON(w, r, &body) {
		return
	}

	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "path name is required")

		return
	}

	err := s.upsertResourceConnectionPath(r.Context(), rdName, nodeA, nodeB, body)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusCreated)
}

// upsertResourceConnectionPath does the RD read-modify-write so the
// HTTP handler stays under funlen. Stores in canonical order so a
// later GET under either (a, b) or (b, a) reads the same blob and
// only needs to flip A/B at the wire edge.
func (s *Server) upsertResourceConnectionPath(ctx context.Context, rdName, nodeA, nodeB string, incoming resourceConnectionPath) error {
	rd, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	paths, swap, err := loadPaths(&rd, nodeA, nodeB)
	if err != nil {
		return err
	}

	// Translate the request's A/B into canonical-order A/B before
	// merging.
	canonical := incoming
	if swap {
		canonical = swapPathAB(incoming)
	}

	paths = upsertByName(paths, canonical)

	err = storePaths(&rd, nodeA, nodeB, paths)
	if err != nil {
		return err
	}

	err = s.Store.ResourceDefinitions().Update(ctx, &rd)
	if err != nil {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	return nil
}

// upsertByName replaces an existing path with the same Name (in place,
// preserving order) or appends when no match exists.
func upsertByName(paths []resourceConnectionPath, incoming resourceConnectionPath) []resourceConnectionPath {
	for i := range paths {
		if paths[i].Name == incoming.Name {
			paths[i] = incoming

			return paths
		}
	}

	return append(paths, incoming)
}

// handleResourceConnectionPathDelete answers DELETE on
// /paths/{name}. Removing the last path also drops the storage key so
// GET reverts to the canonical empty list.
func (s *Server) handleResourceConnectionPathDelete(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	nodeA := r.PathValue("nodeA")
	nodeB := r.PathValue("nodeB")
	name := r.PathValue("name")

	rd, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	paths, _, err := loadPaths(&rd, nodeA, nodeB)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	paths = deleteByName(paths, name)

	err = storePaths(&rd, nodeA, nodeB, paths)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	err = s.Store.ResourceDefinitions().Update(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// deleteByName drops the entry with matching Name. Order-preserving.
// Returns the input untouched when no match exists — DELETE on a
// non-existent path is a no-op on success (mirrors upstream LINSTOR's
// behaviour on `resource-connection path delete`).
func deleteByName(paths []resourceConnectionPath, name string) []resourceConnectionPath {
	out := paths[:0]

	for i := range paths {
		if paths[i].Name == name {
			continue
		}

		out = append(out, paths[i])
	}

	return out
}

// The dispatcher reads the same `Cozystack/ResourceConnectionPaths/`
// prop namespace directly off RD.Spec.Props — see
// pkg/dispatcher/connections.go::connectionsFromRD. We deliberately
// don't export a "give me the decoded connections" helper here
// because pkg/rest would form an import cycle with pkg/dispatcher.
// The prop key + JSON shape are the contract between the two
// packages; both sides have a comment pointing at the other.
