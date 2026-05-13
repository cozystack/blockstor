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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// AutoTiebreakerSuppressedUntilAnnotation is stamped on an RD when an
// operator (or an internal cleanup path) deletes a TIE_BREAKER replica.
// While the annotation timestamp is in the future, the RD-level
// reconciler skips its auto-witness branch. Without the suppression
// window, `linstor r d <tiebreaker-node> <rd>` returns success and
// then the reconciler re-stamps a fresh witness within milliseconds,
// silently undoing operator intent.
//
// 5 minutes covers a normal operator follow-up (e.g. scale to 3
// diskful before quorum changes) and naturally expires for the
// steady-state auto-quorum path. The window is intentionally short
// so a forgotten suppression doesn't permanently disable the
// auto-witness invariant.
const (
	AutoTiebreakerSuppressedUntilAnnotation = "blockstor.io/auto-tiebreaker-suppressed-until"
	autoTiebreakerSuppressionWindow         = 5 * time.Minute
)

// registerAutoplace wires `POST /v1/resource-definitions/{rd}/autoplace` and
// the per-resource list/POST/DELETE used by linstor-csi for explicit placement.
//
// The `make-available` route mirrors upstream LINSTOR's
// `POST /v1/resource-definitions/{rd}/resources/{node}/make-available`
// — linstor-csi v0.21+ calls it from `Attach` (the
// ControllerPublishVolume implementation) to promote a TIE_BREAKER
// witness into a real DISKLESS replica, or create one on demand.
// Without it the call hits 404, csi falls back to a manual diskless
// `POST .../resources` create, which collides with the existing
// witness and the replica never reaches a usable state.
func (s *Server) registerAutoplace(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/autoplace",
		s.requireStore(s.handleAutoplace))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resources",
		s.requireStore(s.handleResourceList))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resources/{node}",
		s.requireStore(s.handleResourceGet))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/resources",
		s.requireStore(s.handleResourceCreate))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/resources/{node}/make-available",
		s.requireStore(s.handleResourceMakeAvailable))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}/resources/{node}",
		s.requireStore(s.handleResourceDelete))
}

// handleResourceList answers `GET /v1/resource-definitions/{rd}/resources`,
// the per-RD aggregate linstor-csi polls during ControllerPublishVolume to
// answer "is the resource on this node?". Wraps each Resource in
// ResourceWithVolumes so the wire shape matches /v1/view/resources.
func (s *Server) handleResourceList(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	resList, err := s.Store.Resources().ListByDefinition(r.Context(), rdName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	out := make([]apiv1.ResourceWithVolumes, 0, len(resList))
	for i := range resList {
		out = append(out, apiv1.ResourceWithVolumes{Resource: resList[i]})
	}

	writeJSON(w, http.StatusOK, out)
}

// handleResourceGet answers `GET /v1/resource-definitions/{rd}/resources/{node}`,
// returning the single Resource on that node or 404.
func (s *Server) handleResourceGet(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	res, err := s.Store.Resources().Get(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, apiv1.ResourceWithVolumes{Resource: res})
}

// handleAutoplace selects up to `place_count` nodes that have a storage
// pool of the requested kind/name and creates Resource objects on them.
//
// Phase 2.5 keeps the placement logic deliberately simple — we trust the
// CRD store as state and never reach out to a satellite. Phase 3's
// autoplacer will weigh free capacity, traits, anti-affinity, etc.
func (s *Server) handleAutoplace(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	var req apiv1.AutoPlaceRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	rd, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// linstor-csi (and piraeus-operator's
	// LinstorSatelliteConfiguration.spec.storageClasses[*].layerList) sets
	// `layer_list` on the autoplace call rather than on RD create. Persist
	// it onto rd.LayerStack here so the dispatcher → satellite chain sees
	// the right composition. RD-level LayerStack wins if already set
	// (operator-supplied via REST POST or CRD create).
	if len(req.LayerList) > 0 && len(rd.LayerStack) == 0 {
		rd.LayerStack = append([]string(nil), req.LayerList...)

		err = s.Store.ResourceDefinitions().Update(r.Context(), &rd)
		if err != nil {
			writeStoreError(w, err)

			return
		}
	}

	filter := mergeAutoplaceFilter(r.Context(), s.Store, &rd, &req.SelectFilter)

	// snapshot-restore-resource stamps BlockstorRestoreFromSnapshot
	// on the new RD. Without satellite-to-satellite zfs/thin send-recv
	// (upstream's cross-node clone path), a replica landed on a node
	// that doesn't have the snapshot locally would have to fall back
	// to a blank CreateVolume + DRBD initial-sync — and the metadata-
	// from-clone peer interacts badly with the fresh-create peer,
	// yielding incorrect data. Until send-recv lands, default the
	// candidate node list to the snapshot's nodes when the caller
	// didn't pin one explicitly.
	constrainAutoplaceToSnapshotNodes(r.Context(), s.Store, &rd, &filter)

	// Clones from snapshots must land on pools whose ProviderKind
	// matches the source's. zfs send/recv and dd/lvm payloads are
	// not interchangeable; a ZFS_THIN→LVM_THIN clone fails opaquely
	// at satellite SendSnapshot/RecvSnapshot time. Pin the
	// provider-kind filter to the source's so the placer drops
	// mismatched candidates and 409s fail-fast with an operator-
	// actionable error instead.
	srcKind := resolveCloneSourceProviderKind(r.Context(), s.Store, &rd)
	if srcKind != "" {
		filter.ProviderList = []string{srcKind}
	}

	if !s.runPlaceAndReport(w, r, rdName, &filter, srcKind) {
		return
	}

	// Java LINSTOR replies with a `[]ApiCallRc` envelope on success.
	// golinstor's RD.Autoplace ignores an empty body, but tools that
	// surface API messages (e.g. the linstor CLI) want a real result
	// to log. Return MASK_INFO + RC_PLACEMENT_DONE-style entry so the
	// shape matches the oracle's.
	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: apiCallRcInfo | apiCallRcRDAutoplaceDone,
		Message: "Resource definition '" + rdName + "' auto-placed",
	}})
}

// runPlaceAndReport drives the placer and writes the appropriate
// HTTP error on shortfall. Returns true on success (caller writes
// the success body), false on any error path (caller returns).
// Pulled out of handleAutoplace to keep that function under the
// cyclomatic / funlen budget once the snapshot-clone provider-kind
// constraint was added.
func (s *Server) runPlaceAndReport(w http.ResponseWriter, r *http.Request, rdName string, filter *apiv1.AutoSelectFilter, srcKind string) bool {
	placed, want, err := placer.New(s.Store).Place(r.Context(), rdName, filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return false
	}

	if placed < want {
		if srcKind != "" {
			writeError(w, http.StatusConflict,
				"cannot place: snapshot is on "+srcKind+
					" but no "+srcKind+" pool found on any candidate node")

			return false
		}

		writeError(w, http.StatusConflict,
			"not enough candidate storage pools for the requested placement")

		return false
	}

	return true
}

// resolveCloneSourceProviderKind returns the ProviderKind of the
// pool backing the source RD when `rd` was born from a snapshot
// (BlockstorRestoreFromSnapshot prop). Returns "" when the RD is
// not a clone, when the prop is malformed, or when the source has
// no diskful replica we can read a StorPoolName off of.
//
// Used by handleAutoplace to constrain candidate pools to a
// matching ProviderKind — zfs send and dd/lvm payloads are not
// interchangeable, so a cross-provider clone would fail opaquely
// at satellite SendSnapshot/RecvSnapshot time.
//
// Lookup path: BlockstorRestoreFromSnapshot → source RD name →
// first non-Diskless Resource on source RD → its StorPoolName +
// NodeName → StoragePool.ProviderKind. We walk Resources rather
// than trusting a hypothetical Snapshot.ProviderKind field because
// the snapshot CRD doesn't stamp it today (potential future
// optimisation — see the report).
func resolveCloneSourceProviderKind(ctx context.Context, st store.Store, rd *apiv1.ResourceDefinition) string {
	const restoreFromKey = "BlockstorRestoreFromSnapshot"

	stamp := rd.Props[restoreFromKey]
	if stamp == "" {
		return ""
	}

	srcRD, _, ok := strings.Cut(stamp, ":")
	if !ok || srcRD == "" {
		return ""
	}

	resList, err := st.Resources().ListByDefinition(ctx, srcRD)
	if err != nil {
		return ""
	}

	for i := range resList {
		res := &resList[i]
		if slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		stor := res.Props["StorPoolName"]
		if stor == "" {
			continue
		}

		pool, err := st.StoragePools().Get(ctx, res.NodeName, stor)
		if err != nil {
			continue
		}

		if pool.ProviderKind == "" || pool.ProviderKind == apiv1.StoragePoolKindDiskless {
			continue
		}

		return pool.ProviderKind
	}

	return ""
}

// constrainAutoplaceToSnapshotNodes restricts the filter's
// NodeNameList to the snapshot's nodes when the RD was created via
// snapshot-restore-resource and the caller didn't pin nodes
// explicitly. See the call site for the why — without local
// satellite-to-satellite send-recv, a clone on a node without the
// snapshot can't converge to correct data.
//
// No-ops when:
//   - the RD lacks the BlockstorRestoreFromSnapshot prop
//   - the prop is malformed (missing colon)
//   - the caller already supplied a NodeNameList (respect explicit intent)
//   - the snapshot lookup fails (let placer fall back to all nodes)
func constrainAutoplaceToSnapshotNodes(ctx context.Context, st store.Store, rd *apiv1.ResourceDefinition, filter *apiv1.AutoSelectFilter) {
	if len(filter.NodeNameList) > 0 {
		return
	}

	const restoreFromKey = "BlockstorRestoreFromSnapshot"

	stamp := rd.Props[restoreFromKey]
	if stamp == "" {
		return
	}

	srcRD, snapName, ok := strings.Cut(stamp, ":")
	if !ok || srcRD == "" || snapName == "" {
		return
	}

	snap, err := st.Snapshots().Get(ctx, srcRD, snapName)
	if err != nil || len(snap.Nodes) == 0 {
		return
	}

	filter.NodeNameList = append([]string(nil), snap.Nodes...)
}

// apiCallRcInfo is upstream LINSTOR's MASK_INFO bit (0x0040_…).
// Combined with a per-action code it lets clients distinguish
// success-with-info from a fatal error.
const (
	apiCallRcInfo            int64 = 0x0040_0000_0000_0000
	apiCallRcRDAutoplaceDone int64 = 0x4231 // ApiConsts.RC_RSC_DFN_PLACED
	apiCallRcRscDeleted      int64 = 0x4200 // ApiConsts.RC_RSC_DELETED
)

// mergeAutoplaceFilter merges the request's filter on top of the parent
// ResourceGroup's stored select filter. Request fields win.
func mergeAutoplaceFilter(ctx context.Context, st store.Store, rd *apiv1.ResourceDefinition, req *apiv1.AutoSelectFilter) apiv1.AutoSelectFilter {
	out := apiv1.AutoSelectFilter{}

	if rd.ResourceGroupName != "" {
		rg, err := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)
		if err == nil {
			out = rg.SelectFilter
		}
	}

	if req.PlaceCount > 0 {
		out.PlaceCount = req.PlaceCount
	}

	if req.StoragePool != "" {
		out.StoragePool = req.StoragePool
	}

	if len(req.StoragePoolList) > 0 {
		out.StoragePoolList = req.StoragePoolList
	}

	if len(req.StoragePoolDisklessList) > 0 {
		out.StoragePoolDisklessList = req.StoragePoolDisklessList
	}

	if len(req.NodeNameList) > 0 {
		out.NodeNameList = req.NodeNameList
	}

	if len(req.ReplicasOnSame) > 0 {
		out.ReplicasOnSame = req.ReplicasOnSame
	}

	if len(req.ReplicasOnDifferent) > 0 {
		out.ReplicasOnDifferent = req.ReplicasOnDifferent
	}

	if req.DisklessOnRemaining {
		out.DisklessOnRemaining = true
	}

	if out.PlaceCount == 0 {
		out.PlaceCount = 1
	}

	return out
}

// handleResourceCreate creates one or more Resources from the upstream
// `[]ResourceCreate` envelope. The upstream OpenAPI shape is an array
// (the CLI's `linstor resource create n1 n2 n3 rd` posts one item per
// node); we also accept a bare object for backwards-compat with
// pre-existing blockstor callers.
func (s *Server) handleResourceCreate(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	envelopes, err := decodeResourceCreateBody(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if len(envelopes) == 0 {
		writeError(w, http.StatusBadRequest, "empty resource create body")

		return
	}

	_, ok := s.createResources(w, r, rdName, envelopes)
	if !ok {
		return
	}

	// Python CLI demands an ApiCallRc list envelope; upstream's
	// `linstor r c` walks it on every reply.
	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource(s) created on resource-definition: " + rdName,
	}})
}

// createResources walks the envelopes from a POST to
// /v1/resource-definitions/{rd}/resources and either creates each
// Resource fresh or promotes an existing diskless/tiebreaker replica
// to diskful (upstream LINSTOR semantics). Returns (created, true) on
// success; writes the HTTP error and returns (nil, false) on the
// first failure.
func (s *Server) createResources(w http.ResponseWriter, r *http.Request, rdName string, envelopes []apiv1.ResourceCreate) ([]apiv1.Resource, bool) {
	created := make([]apiv1.Resource, 0, len(envelopes))

	for i := range envelopes {
		env := &envelopes[i]
		res := env.Resource
		res.Name = rdName

		if res.NodeName == "" {
			writeError(w, http.StatusBadRequest, "node_name is required on every resource create entry")

			return nil, false
		}

		// Same CSI pass-through as handleAutoplace: linstor-csi may set
		// layer_list on the explicit-placement call rather than on RD create.
		// Persist onto rd.LayerStack if not already set so the satellite
		// reconciler sees the right composition.
		if len(env.LayerList) > 0 {
			rd, getErr := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
			if getErr == nil && len(rd.LayerStack) == 0 {
				rd.LayerStack = append([]string(nil), env.LayerList...)
				_ = s.Store.ResourceDefinitions().Update(r.Context(), &rd)
			}
		}

		out, ok := s.createOrPromoteResource(w, r, &res)
		if !ok {
			return nil, false
		}

		created = append(created, *out)
	}

	return created, true
}

// createOrPromoteResource creates res or promotes an existing
// diskless replica in place. Writes the HTTP error and returns
// (nil, false) on failure.
func (s *Server) createOrPromoteResource(w http.ResponseWriter, r *http.Request, res *apiv1.Resource) (*apiv1.Resource, bool) {
	err := s.Store.Resources().Create(r.Context(), res)
	if err == nil {
		return res, true
	}

	// Upstream LINSTOR semantics: `resource create <node> <rd>
	// --storage-pool <pool>` on top of an existing DISKLESS or
	// TIE_BREAKER replica converts it to diskful (effectively an
	// implicit toggle-disk-to-diskful). Mirror that here when
	// the only thing in the way is the diskless/tiebreaker flag.
	//
	// The same promote-instead-of-error path covers the linstor-csi
	// fallback after a make-available 404: csi posts a bare
	// `Flags: [DISKLESS]` create against a node that may already
	// carry a TIE_BREAKER witness, and the witness must be stripped
	// to its plain-DISKLESS form so the reconciler exposes a
	// usable DRBD device.
	wantsPromote := res.Props["StorPoolName"] != "" ||
		containsResourceFlag(res.Flags, apiv1.ResourceFlagDiskless)
	if errors.Is(err, store.ErrAlreadyExists) && wantsPromote {
		promoted, promErr := s.promoteDisklessReplica(r.Context(), res)
		if promErr != nil {
			writeStoreError(w, promErr)

			return nil, false
		}

		return promoted, true
	}

	writeStoreError(w, err)

	return nil, false
}

// promoteDisklessReplica takes a Resource the caller just tried to
// create, looks up the existing one on the same (node, RD), and if
// it's a DISKLESS / TIE_BREAKER replica converts it to match the
// requested shape:
//
//   - target carries a StorPoolName → promote to diskful: drop both
//     DISKLESS and TIE_BREAKER and stamp the new StorPoolName onto
//     Spec.Props (the upstream `linstor resource create --storage-pool`
//     toggle-disk semantics).
//   - target carries `Flags:[DISKLESS]` without a StorPoolName (the
//     linstor-csi fallback after make-available 404) → drop only
//     TIE_BREAKER and leave DISKLESS in place.
//
// The satellite reconciler picks the Resource change up via its
// watch and runs the normal storage-attach chain. Returns the updated
// Resource on success, or wraps ErrAlreadyExists when the existing
// replica is NOT a diskless witness (i.e. a real conflict the caller
// should surface as 409).
func (s *Server) promoteDisklessReplica(ctx context.Context, target *apiv1.Resource) (*apiv1.Resource, error) {
	existing, err := s.Store.Resources().Get(ctx, target.Name, target.NodeName)
	if err != nil {
		return nil, errors.Wrapf(err, "lookup existing replica %s.%s", target.Name, target.NodeName)
	}

	wantDiskful := target.Props["StorPoolName"] != "" &&
		!containsResourceFlag(target.Flags, apiv1.ResourceFlagDiskless)

	wasDiskless := false

	keep := existing.Flags[:0]

	for _, flag := range existing.Flags {
		switch flag {
		case apiv1.ResourceFlagTieBreaker:
			// Always strip the witness marker on any
			// caller-driven promote — the replica is now
			// owned by an operator/CSI request, not the
			// auto-placer.
			wasDiskless = true
		case apiv1.ResourceFlagDiskless:
			wasDiskless = true

			if wantDiskful {
				continue
			}

			keep = append(keep, flag)
		default:
			keep = append(keep, flag)
		}
	}

	if !wasDiskless {
		// Existing replica is a real diskful one — true conflict.
		return nil, errors.Wrapf(store.ErrAlreadyExists,
			"resource %q on node %q already diskful", target.Name, target.NodeName)
	}

	existing.Flags = keep

	if wantDiskful {
		if existing.Props == nil {
			existing.Props = map[string]string{}
		}

		existing.Props["StorPoolName"] = target.Props["StorPoolName"]
	}

	err = s.Store.Resources().Update(ctx, &existing)
	if err != nil {
		return nil, errors.Wrapf(err, "promote diskless %s.%s", target.Name, target.NodeName)
	}

	return &existing, nil
}

// containsResourceFlag is a small helper so the create/promote
// branching reads at the call site without an inline loop.
func containsResourceFlag(flags []string, want string) bool {
	return slices.Contains(flags, want)
}

// handleResourceMakeAvailable answers
// `POST /v1/resource-definitions/{rd}/resources/{node}/make-available`,
// the route linstor-csi v0.21+ uses from its `Attach`
// (ControllerPublishVolume) path. The upstream LINSTOR semantics:
//
//   - If no replica exists on the node: create one. Body's
//     `diskful=false` (the typical CSI case) means a DISKLESS
//     replica; `diskful=true` means a regular diskful one and
//     the body MAY include a `layer_list` carried over from the
//     request shape, which we persist onto the RD just like the
//     other create paths.
//   - If a DISKLESS / TIE_BREAKER witness already lives on the
//     node: promote it. For the CSI case (diskful=false) that means
//     strip TIE_BREAKER but keep DISKLESS so the satellite reconciler
//     brings up a real diskless DRBD device. For diskful=true the
//     existing promoteDisklessReplica path drops both flags and
//     stamps the new StorPoolName.
//   - If a diskful replica already lives on the node: no-op
//     (already available).
//
// Always responds with the upstream `[]ApiCallRc` envelope — golinstor
// discards the body but the Python CLI and `linstor` operator UI
// surface the message.
func (s *Server) handleResourceMakeAvailable(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	req, ok := decodeMakeAvailableBody(w, r)
	if !ok {
		return
	}

	// The RD MUST exist — matches upstream behaviour and lets
	// linstor-csi distinguish "no such volume" (404 → fail Attach)
	// from "make-available not wired" (which would also be 404 here
	// but is now impossible since the route is registered).
	rd, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Pass-through for CSI-supplied layer_list, identical to the
	// autoplace / explicit-create flows. RD-level LayerStack wins.
	if len(req.LayerList) > 0 && len(rd.LayerStack) == 0 {
		rd.LayerStack = append([]string(nil), req.LayerList...)

		err = s.Store.ResourceDefinitions().Update(r.Context(), &rd)
		if err != nil {
			writeStoreError(w, err)

			return
		}
	}

	ok = s.applyMakeAvailable(w, r, rdName, node, &req)
	if !ok {
		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "Resource '" + rdName + "' on node '" + node + "' made available",
	}})
}

// decodeMakeAvailableBody parses the optional JSON body. Upstream
// LINSTOR accepts an empty body as `{diskful:false}` — golinstor's
// MakeAvailable always posts the struct, but the python CLI / curl
// callers may omit it entirely.
func decodeMakeAvailableBody(w http.ResponseWriter, r *http.Request) (apiv1.ResourceMakeAvailable, bool) {
	var req apiv1.ResourceMakeAvailable

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return req, false
	}

	if len(bytes.TrimSpace(body)) == 0 {
		return req, true
	}

	err = json.Unmarshal(body, &req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return req, false
	}

	return req, true
}

// applyMakeAvailable dispatches to update-or-create depending on
// whether a Resource already lives on the target node. Writes the
// HTTP error response on failure and returns false.
func (s *Server) applyMakeAvailable(w http.ResponseWriter, r *http.Request, rdName, node string, req *apiv1.ResourceMakeAvailable) bool {
	existing, err := s.Store.Resources().Get(r.Context(), rdName, node)

	switch {
	case err == nil:
		err = s.makeAvailableUpdate(r.Context(), &existing, req)
	case errors.Is(err, store.ErrNotFound):
		err = s.makeAvailableCreate(r.Context(), rdName, node, req)
	}

	if err != nil {
		writeStoreError(w, err)

		return false
	}

	return true
}

// makeAvailableUpdate mutates an existing replica to match the
// make-available intent. TIE_BREAKER is always stripped (the witness
// is being "consumed" by an attach); DISKLESS is stripped only when
// the caller asked for diskful. Diskful replicas with no flag changes
// are a no-op (already available).
func (s *Server) makeAvailableUpdate(ctx context.Context, existing *apiv1.Resource, req *apiv1.ResourceMakeAvailable) error {
	changed := false

	keep := existing.Flags[:0]

	for _, flag := range existing.Flags {
		switch flag {
		case apiv1.ResourceFlagTieBreaker:
			// Tiebreaker witnesses always shed the marker on
			// make-available — the controller's tiebreaker
			// cleanup hands ownership to the consumer.
			changed = true
		case apiv1.ResourceFlagDiskless:
			if req.Diskful {
				// Promoting to diskful: drop DISKLESS too.
				changed = true

				continue
			}

			keep = append(keep, flag)
		default:
			keep = append(keep, flag)
		}
	}

	if !changed {
		return nil
	}

	existing.Flags = keep

	err := s.Store.Resources().Update(ctx, existing)
	if err != nil {
		return errors.Wrapf(err, "make-available update %s.%s", existing.Name, existing.NodeName)
	}

	return nil
}

// makeAvailableCreate creates a fresh replica when no existing one
// lives on the target node. Defaults to DISKLESS unless the caller
// asked for diskful (in which case the placer's regular path would
// be more appropriate, but we honour the explicit request).
func (s *Server) makeAvailableCreate(ctx context.Context, rdName, node string, req *apiv1.ResourceMakeAvailable) error {
	res := apiv1.Resource{
		Name:     rdName,
		NodeName: node,
	}

	if !req.Diskful {
		res.Flags = []string{apiv1.ResourceFlagDiskless}
	}

	err := s.Store.Resources().Create(ctx, &res)
	if err != nil {
		return errors.Wrapf(err, "make-available create %s.%s", rdName, node)
	}

	return nil
}

// decodeResourceCreateBody accepts either the upstream-LINSTOR
// `[]ResourceCreate` envelope (the shape the CLI posts) or a bare
// `ResourceCreate` object (legacy blockstor callers). Returns a
// normalised slice the handler iterates over.
func decodeResourceCreateBody(body []byte) ([]apiv1.ResourceCreate, error) {
	trimmed := bytes.TrimLeft(body, " \t\r\n")

	if len(trimmed) > 0 && trimmed[0] == '[' {
		var envelopes []apiv1.ResourceCreate

		err := json.Unmarshal(body, &envelopes)
		if err != nil {
			return nil, errors.Wrap(err, "decode resource create array")
		}

		return envelopes, nil
	}

	var single apiv1.ResourceCreate

	err := json.Unmarshal(body, &single)
	if err != nil {
		return nil, errors.Wrap(err, "decode resource create object")
	}

	return []apiv1.ResourceCreate{single}, nil
}

// handleResourceDelete drops a single Resource (replica) on a node.
// Upstream LINSTOR replies with an `[]ApiCallRc` JSON envelope; the
// `linstor` CLI insists on parsing one even when the HTTP status is
// 200/204. Returning a bare `204 No Content` makes the CLI emit
// "Unable to parse REST json data: Expecting value", so we mirror
// the upstream shape: HTTP 200 + `MASK_INFO | RC_RSC_DELETED` entry.
//
// Special case: when the replica being deleted is a TIE_BREAKER, the
// parent RD gets an auto-tiebreaker-suppression annotation so the
// RD-level reconciler doesn't immediately re-stamp a fresh witness
// the next time `ensureTiebreaker` fires. Looked up BEFORE the
// Delete so a concurrent reconcile observes "Resource gone +
// annotation present" rather than "Resource gone + no annotation"
// (which would race the witness back in).
func (s *Server) handleResourceDelete(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	// Look up flags before delete so we know whether to stamp the
	// suppression annotation. A NotFound at this stage is fine —
	// the Delete call below will surface the right 404.
	existing, getErr := s.Store.Resources().Get(r.Context(), rdName, node)
	if getErr == nil && slices.Contains(existing.Flags, apiv1.ResourceFlagTieBreaker) {
		// Best-effort. Failure to stamp must not block the
		// operator-requested delete; the worst case without
		// the annotation is "auto-witness comes back in 5
		// seconds" — annoying, but not data-loss.
		_ = s.stampTiebreakerSuppression(r.Context(), rdName)
	}

	err := s.Store.Resources().Delete(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: apiCallRcInfo | apiCallRcRscDeleted,
		Message: "Resource '" + rdName + "' on node '" + node + "' deleted",
	}})
}

// stampTiebreakerSuppression writes the
// AutoTiebreakerSuppressedUntilAnnotation onto the parent RD with a
// `now + autoTiebreakerSuppressionWindow` deadline. The RD-side
// reconciler reads the annotation in `isTiebreakerSuppressed` and
// skips its auto-witness branch while the window is active.
//
// Idempotent: a fresh stamp always wins (later operator intent
// overrides earlier). NotFound on the parent RD is swallowed — a
// concurrent RD-delete cascade is the most common reason and the
// caller doesn't care.
func (s *Server) stampTiebreakerSuppression(ctx context.Context, rdName string) error {
	rd, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}

		return err //nolint:wrapcheck // best-effort, caller swallows
	}

	if rd.Annotations == nil {
		rd.Annotations = map[string]string{}
	}

	deadline := time.Now().Add(autoTiebreakerSuppressionWindow).UTC().Format(time.RFC3339)
	rd.Annotations[AutoTiebreakerSuppressedUntilAnnotation] = deadline

	return s.Store.ResourceDefinitions().Update(ctx, &rd) //nolint:wrapcheck // best-effort
}
