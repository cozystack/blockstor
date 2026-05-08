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

// registerAdjust wires the adjust nudges. Both endpoints are
// fire-and-forget: they verify the target exists and return 200; the
// per-replica work (`drbdadm adjust`) happens out-of-band via the
// satellite reconcile loop, which already runs adjust on every Apply.
//
// Upstream LINSTOR runs `drbdadm adjust` synchronously here. We
// intentionally don't — synchronous adjust blocks the REST handler on
// kernel I/O and surfaces flaky timeouts. The reconciler's next pass
// picks up the change idempotently.
func (s *Server) registerAdjust(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/adjust",
		s.requireStore(s.handleAdjustAll))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/resources/{node}/adjust",
		s.requireStore(s.handleAdjustOne))
}

// handleAdjustAll verifies the RD exists, then returns 200. A real
// implementation would bump a generation token the satellite watches,
// but until WatchResources lands the reconciler polls.
func (s *Server) handleAdjustAll(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleAdjustOne is the per-replica counterpart.
func (s *Server) handleAdjustOne(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	_, err := s.Store.Resources().Get(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}
