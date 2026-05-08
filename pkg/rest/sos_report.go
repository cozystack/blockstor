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

// registerSOSReport wires the `linstor sos-report download` endpoint.
// Bundling controller logs + every satellite's local state into a
// tarball is operationally useful but non-trivial — we 501 today so
// the CLI gets a clear error rather than 404'ing.
func (s *Server) registerSOSReport(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/sos-report", handleSOSReport)
}

func handleSOSReport(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "sos-report bundling not yet implemented")
}
