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

// registerSOSReport wires the `linstor sos-report` endpoints.
// Bundling controller logs + every satellite's local state into a
// tarball is operationally useful but non-trivial — we 501 today so
// the CLI gets a clear error rather than 404'ing.
//
// Bug 127: golinstor calls `GET /v1/sos-report` for `sos-report
// create` and `GET /v1/sos-report/download` for `sos-report download`.
// Pre-fix we only wired the create side; download fell through to the
// Bug 103 catch-all and returned the generic "endpoint not registered"
// envelope, so operators saw two different ERROR lines for the same
// half-implemented feature. Both verbs now route to canned 501
// envelopes that explicitly name the action ("sos-report bundling not
// yet implemented" / "sos-report download not yet implemented") so the
// CLI surfaces a single, consistent not-implemented story.
func (s *Server) registerSOSReport(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/sos-report", handleSOSReport)
	mux.HandleFunc("GET /v1/sos-report/download", handleSOSReportDownload)
}

func handleSOSReport(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "sos-report bundling not yet implemented")
}

// handleSOSReportDownload mirrors handleSOSReport's canned envelope so
// the `download` and `create` sides surface the same typed ERROR line
// in `linstor`. See Bug 127 notes on registerSOSReport.
func handleSOSReportDownload(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "sos-report download not yet implemented")
}
