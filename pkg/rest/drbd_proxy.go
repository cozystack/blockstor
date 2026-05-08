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

// registerDRBDProxy wires the DRBD-proxy compatibility endpoints. We
// don't actually run drbd-proxy in cozystack-style clusters — the
// network is flat L2 and DRBD-9's native protocol handles everything.
// But the linstor CLI calls these (in `drbd-proxy enable` etc.) and we
// want a deterministic 501 instead of a confusing 404.
func (s *Server) registerDRBDProxy(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/drbd-proxy/enable/{a}/{b}",
		handleNotImplemented)
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/drbd-proxy/disable/{a}/{b}",
		handleNotImplemented)
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/drbd-proxy",
		handleNotImplemented)
}

// handleNotImplemented is the shared 501 responder. The body matches
// the LINSTOR ApiCallRc shape so the CLI renders a sensible error.
func handleNotImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "drbd-proxy is not supported in this build")
}
