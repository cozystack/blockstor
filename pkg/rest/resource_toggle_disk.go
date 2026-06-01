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
// Several shapes are accepted, mirroring upstream / python-linstor
// 1.27.1 (which builds `/toggle-disk/{diskless,diskful}[/{pool}]`):
//
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/storage-pool/{pool}
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskless
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskless/{pool}
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskful
//	PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskful/{pool}
//
// Without a suffix we toggle to the side opposite the current state.
// With `/storage-pool/{pool}` we stamp the pool when promoting to
// diskful (legacy shape kept for tests / older clients). With
// `/diskless[/{pool}]` we force a demote to diskless — what
// `linstor r td --diskless` POSTs; the optional {pool} is the
// diskless pool name and currently ignored (we don't model per-
// replica diskless-pool placement, only DISKLESS flag flips). With
// `/diskful[/{pool}]` we force a promote to diskful — Bug 93:
// `linstor r td <node> <rd> --storage-pool <pool>` POSTs the
// `/diskful/{pool}` variant; previously this hit a bare 404 page
// which the python CLI couldn't parse.
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
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskless/{pool}",
		s.requireStore(s.handleResourceToggleDiskToDiskless))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskful",
		s.requireStore(s.handleResourceToggleDiskToDiskful))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{node}/toggle-disk/diskful/{pool}",
		s.requireStore(s.handleResourceToggleDiskToDiskful))
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
	// Finding 7 (P2): bodyless variant of the same endpoint. Upstream
	// OpenAPI (rest_v1_openapi.yaml lines 2611-2646) defines BOTH the
	// path-suffix and the bodyless forms. python-linstor's common
	// `linstor r migrate-disk <to_node> <rd> <from_node>` CLI form
	// (no `--storage-pool` flag) URL-encodes WITHOUT the trailing
	// `/{pool}` segment and ships the pool name in the JSON body
	// instead. Without this route the CLI hit a bare 404 envelope
	// and migration via the common no-pool form was unusable.
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{dst}/migrate-disk/{src}",
		s.requireStore(s.handleResourceMigrateDiskBodyless))
}

// migrateDiskBody is the JSON payload accepted by the bodyless
// migrate-disk variant. Mirrors upstream's `JsonGenTypes.
// ResourceMigrateDisk` — the `storage_pool` field is the pool the
// new diskful copy lands in on the destination node, and the
// optional `layer_list` is forwarded through Spec.LayerStack so the
// satellite assembles the same stack as the source replica. Both
// fields are optional: an empty `storage_pool` falls through to the
// controller's auto-diskful pool picker on the destination node,
// matching upstream LINSTOR's behaviour when no pool is supplied.
type migrateDiskBody struct {
	StoragePool string   `json:"storage_pool,omitempty"`
	LayerList   []string `json:"layer_list,omitempty"`
}

// handleResourceMigrateDiskBodyless serves the bodyless migrate-disk
// variant. Decodes the pool from the JSON body, then delegates to the
// shared `migrateDisk` helper so the validation + 2-phase add-before-
// drop semantics stay identical to the path-suffix variant.
//
// An absent body is tolerated — the empty pool is forwarded to the
// migrate helper, which leaves Spec.StoragePool unset on the
// destination and lets the controller's auto-diskful path pick a
// pool from the hosting node during the next reconcile.
func (s *Server) handleResourceMigrateDiskBodyless(w http.ResponseWriter, r *http.Request) {
	var body migrateDiskBody

	// The bodyless variant tolerates a truly empty body (no JSON at
	// all). decodeJSON rejects EOF with a typed envelope; route around
	// that for this endpoint only by short-circuiting when
	// Content-Length is zero.
	if r.ContentLength != 0 {
		if !decodeJSON(w, r, &body) {
			return
		}
	}

	s.migrateDiskCommon(w, r, body.StoragePool)
}

// migrateDiskCommon factors out the shared body of the two
// migrate-disk handlers (path-suffix + bodyless). Lifts the pool from
// either source, then re-uses the original handler's lookup +
// validation + promotion sequence.
func (s *Server) migrateDiskCommon(w http.ResponseWriter, r *http.Request, pool string) {
	rdName := r.PathValue("rd")
	dst := r.PathValue("dst")
	src := r.PathValue("src")

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

	poolMsg := "(no pool specified; controller picks one)"
	if pool != "" {
		poolMsg = "on pool '" + pool + "'"
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource '" + rdName + "' migrating from '" + src + "' to '" + dst +
			"' " + poolMsg + " (pending; src will be removed once dst is UpToDate)",
	}})
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
//
// Bug 281 (P2): the toggle path used to be a Get → mutate → Update
// triplet that hit HTTP 409 under rapid diskful↔diskless churn
// (recovery-bitmap-drop e2e). Routed through PatchResourceSpec so
// the closure re-runs against the fresh resourceVersion on
// optimistic-lock conflicts.
func (s *Server) handleResourceToggleDisk(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")
	pool := r.PathValue("pool")

	// Bug 40: `linstor r td --cancel` aborts an in-flight conversion.
	// Upstream LINSTOR uses an explicit cancel verb; we accept the
	// same intent on the existing toggle-disk endpoint via a
	// `?cancel=true` query param. The satellite reconciler reads
	// Spec.ToggleDiskCancel and unwinds the partial state. The Flags
	// path below is skipped: cancel must NOT also flip DISKLESS —
	// the reconciler does that itself as the last step of the
	// rollback so an external observer sees DISKLESS reappear only
	// AFTER drbdadm down + storage Delete succeed.
	if queryFlag(r, "cancel") {
		err := s.Store.Resources().PatchResourceSpec(r.Context(), rdName, node,
			func(res *apiv1.Resource) error {
				res.ToggleDiskCancel = true

				return nil
			})
		if err != nil {
			writeStoreError(w, err)

			return
		}

		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "resource '" + rdName + "' on '" + node + "' toggle-disk cancel requested",
		}})

		return
	}

	var wasDiskless bool

	err := s.Store.Resources().PatchResourceSpec(r.Context(), rdName, node,
		func(res *apiv1.Resource) error {
			wasDiskless = slices.Contains(res.Flags, apiv1.ResourceFlagDiskless)
			res.Flags = applyFlagMutation(res.Flags, apiv1.ResourceFlagDiskless, !wasDiskless)

			if wasDiskless && pool != "" {
				stampStoragePool(res, pool)
			}

			return nil
		})
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

	// Bug 281: same Get → Update race as handleResourceToggleDisk.
	err := s.Store.Resources().PatchResourceSpec(r.Context(), rdName, node,
		func(res *apiv1.Resource) error {
			res.Flags = applyFlagMutation(res.Flags, apiv1.ResourceFlagDiskless, true)

			return nil
		})
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource '" + rdName + "' on '" + node + "' toggled to diskless",
	}})
}

// handleResourceToggleDiskToDiskful forces the replica to diskful,
// regardless of its current state. Matches python-linstor's
// `linstor r td <node> <rd> [--storage-pool <pool>]` shape
// (PUT .../toggle-disk/diskful[/{pool}]). Bug 93: the back-to-diskful
// path was previously unreachable, so an operator who demoted a
// replica with `r td --diskless` had no way to re-promote it via REST.
//
// When {pool} is present we stamp it on Spec so the satellite picks
// the requested pool when reattaching storage. When absent the
// controller's auto-diskful path picks a pool on the hosting node
// during the next reconcile. Idempotent: a replica that's already
// diskful stays diskful (and gets its pool re-stamped if one was
// supplied).
func (s *Server) handleResourceToggleDiskToDiskful(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")
	pool := r.PathValue("pool")

	// Bug 281: same Get → Update race as handleResourceToggleDisk.
	err := s.Store.Resources().PatchResourceSpec(r.Context(), rdName, node,
		func(res *apiv1.Resource) error {
			// Both DISKLESS and TIE_BREAKER must be stripped: an
			// auto-managed witness Resource is always created with
			// `{DISKLESS, TIE_BREAKER}` flags, so promoting it via
			// `r td --storage-pool <pool>` must clear BOTH or the
			// CRD ends up `flags=[TIE_BREAKER]` on a diskful peer.
			// shouldTieBreakerExist / shouldKeepExistingWitness
			// then double-count the slot (filterTieBreaker
			// matches on the TIE_BREAKER flag, not on the disk
			// state) and the diskful peer is treated as a witness
			// in the next ensureTiebreaker pass — silent
			// invariant violation. Strip both regardless of which
			// the caller intended; promoting a non-witness
			// diskless is the same operation and dropping a flag
			// that isn't there is a no-op.
			res.Flags = applyFlagMutation(res.Flags, apiv1.ResourceFlagDiskless, false)
			res.Flags = applyFlagMutation(res.Flags, apiv1.ResourceFlagTieBreaker, false)

			if pool != "" {
				stampStoragePool(res, pool)
			}

			return nil
		})
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource '" + rdName + "' on '" + node + "' toggled to diskful",
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
	// The path-suffix variant carries the pool name in the URL.
	// The bodyless variant decodes it from the JSON body. Both
	// converge on `migrateDiskCommon` which runs the shared lookup
	// + validation + promotion sequence and writes the final
	// envelope.
	s.migrateDiskCommon(w, r, r.PathValue("pool"))
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

	// Bug 298: route through PatchResourceSpec so a concurrent reconciler
	// write (allocator stamping DRBDPort/NodeID, observer touching Status)
	// can't 409 the migrate flip silently — without this the test's
	// 300s `wait for UpToDate` ticks down because the DISKLESS flag was
	// only cleared on the loser's wire-snapshot, never persisted.
	// Idempotent re-validation of the DISKLESS precondition inside the
	// retry closure: if the closure re-runs against a fresh snapshot
	// where some other actor cleared DISKLESS between the outer Get and
	// the inner mutate, we still want the migrate semantics to apply
	// (stamp pool + migrating-from prop). Refuse only on a stale-state
	// race where the resource has been hard-promoted to diskful + the
	// migrating-from prop is absent.
	err = s.Store.Resources().PatchResourceSpec(r.Context(), rdName, dst,
		func(res *apiv1.Resource) error {
			if !slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) &&
				(res.Props == nil || res.Props[MigratingFromProp] == "") {
				return errors.New("destination became diskful mid-flight")
			}

			stampStoragePool(res, pool)
			stampMigratingFrom(res, src)
			res.Flags = applyFlagMutation(res.Flags, apiv1.ResourceFlagDiskless, false)

			return nil
		})
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
