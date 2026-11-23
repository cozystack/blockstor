// SPDX-License-Identifier: Apache-2.0

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

// registerREST128Unsupported answers the REST 1.28 endpoints blockstor does
// not serve with an explicit 501, so that advertising 1.28 and later, which
// opens python-linstor's client-side gates for them, ends in a reason
// rather than in a 404 that reads as a broken server.
func (s *Server) registerREST128Unsupported(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/controller/database/export", handleDatabaseExportUnsupported)

	for _, route := range []string{
		"POST /v1/controller/auth/initialize-token-auth",
		"GET /v1/controller/auth/token",
		"POST /v1/controller/auth/token",
		"PUT /v1/controller/auth/token/{id}",
		"DELETE /v1/controller/auth/token/{id}",
	} {
		mux.HandleFunc(route, handleTokenAuthUnsupported)
	}

	mux.HandleFunc("POST /v1/nodes/exec/drbd-reactorctl/{command}", handleReactorctlUnsupported)
}

// handleDatabaseExportUnsupported refuses `linstor controller export-db`:
// blockstor keeps its state in Kubernetes objects, not in a database of its
// own, so there is nothing LINSTOR's export would write.
func handleDatabaseExportUnsupported(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "database export is not supported: blockstor keeps its state "+
		"in Kubernetes custom resources, back those up with the cluster's own tooling")
}

// handleTokenAuthUnsupported refuses LINSTOR's API token management:
// blockstor authenticates REST clients with TLS client certificates.
func handleTokenAuthUnsupported(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "API token authentication is not supported: blockstor "+
		"authenticates REST clients with TLS client certificates")
}

// handleReactorctlUnsupported refuses the drbd-reactorctl passthrough:
// blockstor's controller runs in Kubernetes and needs no drbd-reactor.
func handleReactorctlUnsupported(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented, "drbd-reactorctl is not supported: blockstor does not run "+
		"drbd-reactor, its controller is made highly available by Kubernetes")
}
