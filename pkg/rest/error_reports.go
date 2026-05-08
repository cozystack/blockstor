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

// registerErrorReports wires the error-reports endpoints. linstor CLI's
// `error-reports list` and `error-reports show` hit them.
//
// We don't yet capture controller-side errors as durable reports — the
// LIST endpoint returns an empty array, GET returns 404. When the
// controller starts buffering reports (Phase 6), this is the surface.
func (s *Server) registerErrorReports(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/error-reports", handleErrorReportsList)
	mux.HandleFunc("GET /v1/error-reports/{id}", handleErrorReportGet)
}

// handleErrorReportsList returns an empty array. We don't 503 the way
// requireStore would — the endpoint stays available even if the store
// is offline so client tooling doesn't choke.
func handleErrorReportsList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []any{})
}

// handleErrorReportGet always 404s for now; reports aren't persisted.
func handleErrorReportGet(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, "error report not found")
}
