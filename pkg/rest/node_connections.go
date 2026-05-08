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

// registerNodeConnections wires `GET /v1/node-connections`. Java LINSTOR
// returns the inter-satellite connection matrix here; cozystack runs flat
// L2, so we report none and let golinstor's reconciler treat that as the
// healthy default. Without this stub the call 404s and linstor-csi's
// status polls log an error every cycle.
func (s *Server) registerNodeConnections(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/node-connections", handleEmptyArray)
	mux.HandleFunc("GET /v1/node-connections/{src}/{dst}", handleEmptyArray)
}

// handleEmptyArray writes a JSON `[]` body — the canonical empty-list
// answer that golinstor decodes into a zero-element slice.
func handleEmptyArray(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []struct{}{})
}
