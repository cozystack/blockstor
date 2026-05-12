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
)

// registerNodeConnections wires the upstream LINSTOR `/v1/node-
// connections{,/{src}/{dst}}` surface. Java LINSTOR uses these to
// stamp per-peer DRBD options on the inter-satellite connection
// matrix; cozystack runs flat L2 + sets DRBD options at RD/RG
// scope, so the matrix is always empty in practice.
//
// We expose every verb so golinstor / `linstor node-connection`
// don't 404 on a misguided call: GET returns the empty list/object,
// PUT/POST accept (no-op), DELETE accepts (no-op). Returning an
// error here would break operators experimenting with the command
// even when they don't actually need the feature.
func (s *Server) registerNodeConnections(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/node-connections", handleEmptyArray)
	mux.HandleFunc("GET /v1/node-connections/{src}/{dst}", handleNodeConnectionGet)
	mux.HandleFunc("PUT /v1/node-connections/{src}/{dst}", handleNoContent)
	mux.HandleFunc("POST /v1/node-connections/{src}/{dst}", handleNoContent)
	mux.HandleFunc("PATCH /v1/node-connections/{src}/{dst}", handleNoContent)
	mux.HandleFunc("DELETE /v1/node-connections/{src}/{dst}", handleNoContent)
}

// handleEmptyArray writes a JSON `[]` body — the canonical empty-list
// answer that golinstor decodes into a zero-element slice.
func handleEmptyArray(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []struct{}{})
}

// handleNodeConnectionGet returns an empty NodeConnection object
// (no props) for a specific pair. golinstor's
// `client.NodeConnections.Get(nodeA, nodeB)` decodes this into a
// zero-value struct, which downstream callers treat as "no
// per-peer options set" — equivalent to never having called PUT.
func handleNodeConnectionGet(w http.ResponseWriter, r *http.Request) {
	src := r.PathValue("src")
	dst := r.PathValue("dst")

	writeJSON(w, http.StatusOK, map[string]any{
		"node_a":     src,
		"node_b":     dst,
		"properties": map[string]string{},
	})
}

// handleNoContent accepts the request body without parsing it and
// returns 204 — the canonical "request accepted, nothing to
// report" answer for non-persistent writes.
func handleNoContent(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
