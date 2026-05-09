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

// registerResourceLifecycle wires activate/deactivate. Upstream LINSTOR
// uses these for piraeus-operator's node-maintenance workflow:
// deactivate brings the kernel resource down without deleting the
// .res file or storage; activate brings it back up. We implement both
// as a flag toggle on Resource.Spec.Flags; the satellite reconciler
// reads INACTIVE and switches drbdadm up↔down accordingly.
func (s *Server) registerResourceLifecycle(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/resources/{node}/activate",
		s.requireStore(s.handleResourceActivate))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/resources/{node}/deactivate",
		s.requireStore(s.handleResourceDeactivate))
}

// handleResourceActivate clears the INACTIVE flag on the named replica.
// Idempotent: removing an already-absent flag is a no-op. The satellite
// brings the kernel resource back up on its next reconcile.
func (s *Server) handleResourceActivate(w http.ResponseWriter, r *http.Request) {
	mutateResourceFlag(w, r, s, "INACTIVE", false)
}

// handleResourceDeactivate sets the INACTIVE flag. Storage stays;
// drbdadm down runs on the satellite. The Resource CRD is intact so
// activate flips it back without losing port/node-id allocations.
func (s *Server) handleResourceDeactivate(w http.ResponseWriter, r *http.Request) {
	mutateResourceFlag(w, r, s, "INACTIVE", true)
}

// mutateResourceFlag is the shared add/remove path for activate +
// deactivate. set=true adds the flag; set=false removes every
// occurrence. Idempotent.
func mutateResourceFlag(w http.ResponseWriter, r *http.Request, s *Server, flag string, set bool) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	res, err := s.Store.Resources().Get(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	res.Flags = applyFlagMutation(res.Flags, flag, set)

	err = s.Store.Resources().Update(r.Context(), &res)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// applyFlagMutation adds or removes flag depending on set. Used by the
// activate/deactivate handlers; kept distinct from the Node-level
// addFlag/removeFlag helpers because those are consumed via mutator
// closures in node_lifecycle.go.
func applyFlagMutation(flags []string, flag string, set bool) []string {
	out := flags[:0]
	seen := false

	for _, existing := range flags {
		if existing == flag {
			seen = true

			if !set {
				continue
			}
		}

		out = append(out, existing)
	}

	if set && !seen {
		out = append(out, flag)
	}

	return out
}
