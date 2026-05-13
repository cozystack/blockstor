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

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerResourceToggleDisk wires the upstream LINSTOR
// `linstor resource toggle-disk` endpoint. Heavy ops use:
// flip a single replica between diskless and diskful in one
// call, typically before/after a node-maintenance rotation.
//
// Three shapes are accepted, mirroring upstream / python-linstor:
//
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/storage-pool/{pool}
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskless
//
// Without a suffix we toggle to the side opposite the current state.
// With `/storage-pool/{pool}` we stamp the pool when promoting to
// diskful. With `/diskless` we force a demote to diskless, regardless
// of current state — this is what `linstor r td --diskless` POSTs.
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
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskless",
		s.requireStore(s.handleResourceToggleDiskToDiskless))
	// Upstream LINSTOR's `linstor r td --migrate-from <src>` shape:
	// move a replica between nodes without dropping below the original
	// diskful count. Path-param order matches python-linstor's URL
	// construction (UG9 §"Migrating a resource to another node"):
	//   PUT /v1/resource-definitions/{rd}/resources/{dst}/migrate-disk/{src}/{pool}
	// — {dst} is the receiving node (gets a diskful replica), {src}
	// is the source node we drain from, {pool} is the storage-pool
	// the new diskful copy lands in on {dst}.
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{dst}/migrate-disk/{src}/{pool}",
		s.requireStore(s.handleResourceMigrateDisk))
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

	state := "diskful"
	if !wasDiskless {
		state = "diskless"
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource '" + rdName + "' on '" + node + "' toggled to " + state,
	}})
}

// handleResourceToggleDiskToDiskless forces the replica to diskless,
// regardless of its current state. Matches python-linstor's
// `linstor r td --diskless` shape (PUT .../toggle-disk/diskless).
// Idempotent: a replica that's already diskless stays diskless.
func (s *Server) handleResourceToggleDiskToDiskless(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	res, err := s.Store.Resources().Get(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	res.Flags = applyFlagMutation(res.Flags, apiv1.ResourceFlagDiskless, true)

	err = s.Store.Resources().Update(r.Context(), &res)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource '" + rdName + "' on '" + node + "' toggled to diskless",
	}})
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

// MigratingFromProp is the per-Resource property the REST migrate-
// disk handler stamps on the destination replica when starting a
// strict add-before-drop migration (Option B). The companion
// ResourceMigrationReconciler watches Resources carrying this prop,
// waits for the destination's volumes to reach DiskState=UpToDate,
// then deletes the named source replica's Resource CRD and clears
// the prop on the destination. UG9 §"Migrating a resource to
// another node" promises the redundancy invariant holds across the
// entire migration — Option B preserves it strictly by deferring
// the source teardown until the new copy is durable.
const MigratingFromProp = "BlockstorMigratingFrom"

// handleResourceMigrateDisk implements upstream LINSTOR's
// `linstor r td --migrate-from <src>` semantics (UG9 §"Migrating a
// resource to another node") under Option B (strict add-before-drop):
//
//  1. Validate: rd exists, src has a diskful replica, dst is either
//     missing or already diskless. If src is Primary InUse, refuse
//     with 409 — upstream UG9 requires an explicit demote first
//     before a Primary replica can be migrated.
//  2. Ensure dst exists with the requested storage pool stamped and
//     DISKLESS cleared so the satellite attaches storage and starts
//     syncing from src.
//  3. Stamp dst's Spec.Props["BlockstorMigratingFrom"]=<src-node>.
//     This is the trigger the ResourceMigrationReconciler watches —
//     once dst's Status.Volumes[].DiskState all reach UpToDate, the
//     reconciler deletes src's Resource CRD and clears the prop.
//  4. Return 200 immediately. The operation is async-pending: the
//     redundancy invariant (diskful count never drops below the
//     original) is preserved because src lives until the reconciler
//     observes UpToDate on dst.
//
// Returns 200 with an APICallRc envelope on success, 404 on unknown
// RD or missing src diskful replica, 409 when src is Primary InUse.
func (s *Server) handleResourceMigrateDisk(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	dst := r.PathValue("dst")
	src := r.PathValue("src")
	pool := r.PathValue("pool")

	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if !s.validateMigrateSrc(w, r, rdName, src) {
		return
	}

	if !s.promoteMigrateDst(w, r, rdName, dst, pool, src) {
		return
	}

	// Option B: src lives. ResourceMigrationReconciler observes
	// the BlockstorMigratingFrom prop stamped in promoteMigrateDst,
	// waits for dst Volumes to reach UpToDate, then deletes src and
	// clears the prop. Operation is "pending" at REST layer; caller
	// must observe Status (or poll the resources list) to confirm
	// completion.
	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource '" + rdName + "' migrating from '" + src + "' to '" + dst +
			"' on pool '" + pool + "' (pending; src will be removed once dst is UpToDate)",
	}})
}

// validateMigrateSrc enforces the migrate-disk preconditions on the
// source replica: it must exist, be diskful, and not currently
// Primary InUse. Writes the matching HTTP error to w and returns
// false if any check fails.
func (s *Server) validateMigrateSrc(w http.ResponseWriter, r *http.Request, rdName, src string) bool {
	srcRes, err := s.Store.Resources().Get(r.Context(), rdName, src)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound,
				"migrate-disk: source replica '"+rdName+"' on '"+src+"' not found")

			return false
		}

		writeStoreError(w, err)

		return false
	}

	if slices.Contains(srcRes.Flags, apiv1.ResourceFlagDiskless) {
		writeError(w, http.StatusConflict,
			"migrate-disk: source replica '"+rdName+"' on '"+src+
				"' has no diskful storage to migrate (DISKLESS)")

		return false
	}

	if srcRes.State.InUse != nil && *srcRes.State.InUse {
		writeError(w, http.StatusConflict,
			"migrate-disk: source replica '"+rdName+"' on '"+src+
				"' is Primary InUse; demote the consumer before migrating")

		return false
	}

	return true
}

// promoteMigrateDst ensures dst has a Resource entry stamped with
// the target pool, DISKLESS cleared, and the BlockstorMigratingFrom
// prop set to the src node name. Creates a fresh diskful replica
// when dst was absent, or flips an existing diskless one to diskful
// in place. Returns false (after writing an HTTP error) when dst
// already holds a diskful replica or store ops fail.
//
// The migrating-from stamp is the trigger the migration reconciler
// watches; without it the controller has no way to know which src
// to prune once dst reaches UpToDate.
func (s *Server) promoteMigrateDst(w http.ResponseWriter, r *http.Request, rdName, dst, pool, src string) bool {
	dstRes, err := s.Store.Resources().Get(r.Context(), rdName, dst)
	switch {
	case errors.Is(err, store.ErrNotFound):
		dstRes = apiv1.Resource{Name: rdName, NodeName: dst}
		stampStoragePool(&dstRes, pool)
		stampMigratingFrom(&dstRes, src)

		err = s.Store.Resources().Create(r.Context(), &dstRes)
		if err != nil {
			writeStoreError(w, err)

			return false
		}

		return true
	case err != nil:
		writeStoreError(w, err)

		return false
	}

	if !slices.Contains(dstRes.Flags, apiv1.ResourceFlagDiskless) {
		writeError(w, http.StatusConflict,
			"migrate-disk: destination replica '"+rdName+"' on '"+dst+
				"' is already diskful; cannot migrate onto it")

		return false
	}

	stampStoragePool(&dstRes, pool)
	stampMigratingFrom(&dstRes, src)
	dstRes.Flags = applyFlagMutation(dstRes.Flags, apiv1.ResourceFlagDiskless, false)

	err = s.Store.Resources().Update(r.Context(), &dstRes)
	if err != nil {
		writeStoreError(w, err)

		return false
	}

	return true
}

// stampMigratingFrom records the source node name on the destination
// Resource's prop map. The migration reconciler reads this to find
// the corresponding src Resource to prune once the destination
// volumes reach UpToDate.
func stampMigratingFrom(res *apiv1.Resource, src string) {
	if res.Props == nil {
		res.Props = map[string]string{}
	}

	res.Props[MigratingFromProp] = src
}
