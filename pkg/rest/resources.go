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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerResources wires the /v1/view/resources aggregate. linstor-csi
// relies on this in its volume reconciliation loop.
func (s *Server) registerResources(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/resources", s.requireStore(s.handleResourcesView))
}

func (s *Server) handleResourcesView(w http.ResponseWriter, r *http.Request) {
	resList, err := s.Store.Resources().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Wrap each Resource in ResourceWithVolumes so the wire shape matches
	// upstream golinstor's expectation. Volumes will be populated once the
	// satellite reports them in Phase 3.
	out := make([]apiv1.ResourceWithVolumes, 0, len(resList))
	for i := range resList {
		out = append(out, apiv1.ResourceWithVolumes{Resource: resList[i]})
	}

	writeJSON(w, http.StatusOK, out)
}
