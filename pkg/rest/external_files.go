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

// registerExternalFiles wires linstor's external-file endpoints. We
// don't yet ship arbitrary files to satellites — cozystack manages
// host config via Talos extensions / kubelet — but the `external-file
// list` CLI hits these on startup.
//
// LIST → []. GET on a single path → 404. Wider implementation lands
// when a real per-cluster need shows up.
func (s *Server) registerExternalFiles(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/files", handleExternalFilesList)
	mux.HandleFunc("GET /v1/files/{path...}", handleExternalFileGet)
}

func handleExternalFilesList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []map[string]string{})
}

func handleExternalFileGet(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, "external file not found")
}
