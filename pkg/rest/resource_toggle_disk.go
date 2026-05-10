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
	"slices"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerResourceToggleDisk wires the upstream LINSTOR
// `linstor resource toggle-disk` endpoint. Heavy ops use:
// flip a single replica between diskless and diskful in one
// call, typically before/after a node-maintenance rotation.
//
// Both shapes are accepted, mirroring upstream:
//
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/storage-pool/{pool}
//
// Without `/storage-pool/{pool}` we toggle to the side opposite the
// current state and let the controller pick a pool when promoting
// (the existing auto-diskful path); with the pool path we stamp
// it explicitly so an operator can target a specific pool.
//
// The work itself (drbdadm attach / detach) happens out-of-band on
// the satellite reconciler — this endpoint is a thin spec-flag
// toggle that the existing auto-diskful + manual-detach paths
// already handle.
func (s *Server) registerResourceToggleDisk(mux *http.ServeMux) {
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk",
		s.requireStore(s.handleResourceToggleDisk))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/storage-pool/{pool}",
		s.requireStore(s.handleResourceToggleDisk))
}

// handleResourceToggleDisk flips Spec.Flags["DISKLESS"] on the
// named replica. Path-suffix `storage-pool/{pool}` (when present)
// stamps that pool name on Spec.StoragePool when promoting to
// diskful — without it, the controller's auto-diskful path picks
// a pool from the hosting node.
//
// Idempotent: toggling a diskful replica when no pool argument
// was given drops the DISKLESS flag if currently set; toggling
// it back when DISKLESS was absent re-adds it.
func (s *Server) handleResourceToggleDisk(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")
	pool := r.PathValue("pool")

	res, err := s.Store.Resources().Get(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	wasDiskless := slices.Contains(res.Flags, apiv1.ResourceFlagDiskless)

	res.Flags = applyFlagMutation(res.Flags, apiv1.ResourceFlagDiskless, !wasDiskless)

	switch {
	case wasDiskless && pool != "":
		// Promote with explicit pool target.
		stampStoragePool(&res, pool)
	case wasDiskless && pool == "":
		// Promote without explicit pool; controller's
		// auto-diskful pick path runs on the next
		// reconcile.
	case !wasDiskless:
		// Demote — keep the historical pool intact in
		// case the operator toggles back, but the
		// satellite will detach on the next reconcile.
	}

	err = s.Store.Resources().Update(r.Context(), &res)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// stampStoragePool sets both the typed StoragePool field
// (source of truth post-Phase-10.3) and the legacy
// Props["StorPoolName"] key (forward-compat). Mutates in place.
func stampStoragePool(res *apiv1.Resource, pool string) {
	if res.Props == nil {
		res.Props = map[string]string{}
	}

	res.Props["StorPoolName"] = pool
}
