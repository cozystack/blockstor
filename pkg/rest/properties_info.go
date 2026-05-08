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

// registerPropertiesInfo wires the per-object property catalogue
// endpoints. linstor CLI hits these in `<obj> list-properties --info`
// to render the autocomplete catalogue. We currently return an empty
// JSON array everywhere — populating the catalogue from upstream's
// property metadata is Phase 6 work — but the endpoints have to exist
// or the CLI breaks.
func (s *Server) registerPropertiesInfo(mux *http.ServeMux) {
	for _, path := range propertiesInfoPaths() {
		mux.HandleFunc("GET "+path, handlePropertiesInfo)
	}
}

// propertiesInfoPaths returns the full set of `properties/info` URLs
// that linstor CLI looks up at startup. Listed once so the goconst
// linter and the test stay in sync from a single source. Returned as
// a function (not a global) per repo style.
func propertiesInfoPaths() []string {
	return []string{
		"/v1/controller/properties/info",
		"/v1/nodes/properties/info",
		"/v1/storage-pool-definitions/properties/info",
		"/v1/resource-definitions/properties/info",
		"/v1/resource-groups/properties/info",
		"/v1/volume-definitions/properties/info",
		"/v1/resources/properties/info",
		"/v1/volumes/properties/info",
	}
}

// handlePropertiesInfo returns an empty array. We initialise the slice
// (never `nil`) so JSON encoders and clients see `[]` instead of
// `null`, which a couple of CLI versions choke on.
func handlePropertiesInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []map[string]string{})
}
