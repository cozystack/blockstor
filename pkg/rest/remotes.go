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

// registerRemotes wires the LINSTOR `remotes` endpoints. Cozystack
// doesn't use cross-cluster snapshot shipping, so the typed-by-kind
// stubs return empty arrays — golinstor's snapshot list path calls
// `/v1/remotes/s3` before enumerating snapshots, and a 404 there
// surfaces as `failed to list available s3 remotes` from linstor-csi.
//
// Empty `[]` keeps the wire format LINSTOR-shaped without exposing
// any remote-shipping surface area we don't implement.
func (s *Server) registerRemotes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/remotes", handleEmptyArray)
	mux.HandleFunc("GET /v1/remotes/s3", handleEmptyArray)
	mux.HandleFunc("GET /v1/remotes/linstor", handleEmptyArray)
	mux.HandleFunc("GET /v1/remotes/ebs", handleEmptyArray)
}

func handleEmptyArray(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []map[string]string{})
}
