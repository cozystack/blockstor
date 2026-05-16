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
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// AutoTiebreakerSuppressedUntilAnnotation is re-exported from
// pkg/api/v1 so existing rest-package call sites (and tests that
// reference `rest.AutoTiebreakerSuppressedUntilAnnotation`) keep
// compiling. The canonical definition lives in pkg/api/v1 so the
// REST writer and the internal/controller reader share one constant
// without either package importing the other.
//
// autoTiebreakerSuppressionWindow: 5 minutes covers a normal operator
// follow-up (e.g. scale to 3 diskful before quorum changes) and
// naturally expires for the steady-state auto-quorum path. The window
// is intentionally short so a forgotten suppression doesn't
// permanently disable the auto-witness invariant.
const (
	AutoTiebreakerSuppressedUntilAnnotation = apiv1.AutoTiebreakerSuppressedUntilAnnotation
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

	// CreateVolume hot path — linstor-csi polls this endpoint during
	// ControllerPublishVolume right after the spawn; a sibling
	// apiserver replica's cache may still trail the spawn write.
	// See pkg/rest/cache_retry.go.
	_, err := getRDWithCacheRetry(r.Context(), s.Store, rdName)
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

	// CreateVolume hot path — RD may have been written via a sibling
	// apiserver replica seconds ago; cache trail surfaces as 404.
	// See pkg/rest/cache_retry.go.
	rd, err := getRDWithCacheRetry(r.Context(), s.Store, rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if !s.persistAutoplaceLayerList(w, r, &rd, req.LayerList) {
		return
	}

	filter := mergeAutoplaceFilter(r.Context(), s.Store, &rd, &req.SelectFilter)

	// Bug 94: when the caller pinned the placement to a specific node
	// via `linstor r c --auto-place 1 --node <name> <rd>` (which the
	// CLI lowers onto `select_filter.node_name_list`), refuse the
	// request if any name in the list doesn't resolve to a Node CRD.
	// Without this gate the placer's downstream "no candidate pools"
	// shortfall fired with a generic 409 — operators couldn't tell
	// "pool full" from "you typo'd the node name".
	if !s.refuseAutoplaceOnUnknownNodes(w, r, filter.NodeNameList) {
		return
	}

	// Scenario 4.W17 (`r c --auto-place +1 <rd>`): see
	// resolveAdditionalPlaceCount doc — the Python CLI's `+1` shorthand
	// posts `additional_place_count`, and we fold it into the effective
	// PlaceCount so the placer's target-driven loop adds exactly N
	// replicas on top of the current diskful count.
	err = s.resolveAdditionalPlaceCount(r.Context(), rdName, &filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

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

// refuseAutoplaceOnUnknownNodes is Bug 94's autoplace-side guard. The
// CLI's `linstor r c --auto-place 1 --node <name> <rd>` lands as
// `select_filter.node_name_list`; without this check, an unknown node
// name made the placer fall through to its generic "no candidate
// pools" shortfall message and the operator never learned that the
// real cause was a typo. We resolve every name through the Node store
// first and 404 with a LINSTOR envelope listing the missing names.
//
// Returns true when the caller may proceed (empty list or all names
// resolve), false when the HTTP error has already been written.
func (s *Server) refuseAutoplaceOnUnknownNodes(w http.ResponseWriter, r *http.Request, names []string) bool {
	for _, name := range names {
		_, err := s.Store.Nodes().Get(r.Context(), name)
		if err == nil {
			continue
		}

		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound,
				"node '"+name+"' not found: create the node first with "+
					"`linstor n c <name>` or pass a valid existing node name")

			return false
		}

		writeStoreError(w, err)

		return false
	}

	return true
}

// refuseResourceCreateOnUnknownPool is Bug 118's gate. When the
// caller pinned a storage pool by name (`linstor r c <node> <rd>
// --storage-pool <pool>` lands as
// `body.resource.props["StorPoolName"]`), the pool must already
// exist on the target node. Without this check the Resource CRD
// persisted with a dangling pool reference: the satellite
// reconciler would forever wait for a pool that never
// materializes, and the operator's only feedback was "SUCCESS" on
// the create. Mirrors Bug 94's gate shape — 404 + LINSTOR envelope
// naming the offending (pool, node) pair. Skipped when no pool is
// named (diskless replicas, RD-prop inheritance, autoplace-style
// filter-driven selection). Returns true when the caller may
// proceed, false when the HTTP error has already been written.
func (s *Server) refuseResourceCreateOnUnknownPool(w http.ResponseWriter, r *http.Request, res *apiv1.Resource) bool {
	pool := res.Props["StorPoolName"]
	if pool == "" {
		return true
	}

	_, err := s.Store.StoragePools().Get(r.Context(), res.NodeName, pool)
	if err == nil {
		return true
	}

	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound,
			"storage pool '"+pool+"' not found on node '"+res.NodeName+
				"': create the pool first with `linstor sp c <node> <pool> ...` "+
				"or pass a valid existing pool name")

		return false
	}

	writeStoreError(w, err)

	return false
}

// persistAutoplaceLayerList writes a CSI-supplied layer_list onto the
// RD's LayerStack so the dispatcher → satellite chain sees the right
// composition. Pulled out of handleAutoplace to keep that function under
// the funlen budget once W17's additional_place_count branch was added.
//
// linstor-csi (and piraeus-operator's
// LinstorSatelliteConfiguration.spec.storageClasses[*].layerList) sets
// layer_list on the autoplace call rather than on RD create.
// RD-level LayerStack wins if already set (operator-supplied via REST
// POST or CRD create) — we never overwrite an existing stack. Returns
// false on write error (HTTP response already emitted), true on success
// or no-op.
func (s *Server) persistAutoplaceLayerList(w http.ResponseWriter, r *http.Request, rd *apiv1.ResourceDefinition, layerList []string) bool {
	if len(layerList) == 0 || len(rd.LayerStack) > 0 {
		return true
	}

	rd.LayerStack = append([]string(nil), layerList...)

	err := s.Store.ResourceDefinitions().Update(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return false
	}

	return true
}

// runPlaceAndReport drives the placer and writes the appropriate
// HTTP error on shortfall. Returns true on success (caller writes
// the success body), false on any error path (caller returns).
// Pulled out of handleAutoplace to keep that function under the
// cyclomatic / funlen budget once the snapshot-clone provider-kind
// constraint was added.
//
// Shortfall envelopes (F13 / CLI parity): the Python linstor CLI
// renders `cause` / `correction` / `details` as labelled blocks in
// `linstor r c --auto-place`. A bare `message`-only error renders as
// a terse one-liner that hides every actionable criterion. Both the
// CapacityShortfallError path and the generic "no candidate pools"
// path now emit the full envelope shape so the CLI surfaces the
// same diagnostic upstream Java LINSTOR does (criteria bullet list +
// "Auto-place configuration details" block).
func (s *Server) runPlaceAndReport(w http.ResponseWriter, r *http.Request, rdName string, filter *apiv1.AutoSelectFilter, srcKind string) bool {
	placed, want, err := placer.New(s.Store).Place(r.Context(), rdName, filter)
	if err != nil {
		// Capacity-shortfall (Bug 35) is operator-actionable, not a
		// 500. Surface as a structured 409 envelope so the Python
		// CLI prints the numeric capacity gap alongside the criteria
		// bullet list.
		var capErr *placer.CapacityShortfallError
		if errors.As(err, &capErr) {
			writeAutoplaceShortfall(w, filter, srcKind, capErr)

			return false
		}

		writeError(w, http.StatusInternalServerError, err.Error())

		return false
	}

	if placed < want {
		writeAutoplaceShortfall(w, filter, srcKind, nil)

		return false
	}

	return true
}

// resolveAdditionalPlaceCount implements the W17 `--auto-place +N`
// semantic: when the caller set `additional_place_count`, the effective
// PlaceCount becomes `count(existing diskful, non-evicted) + additional`
// and the regular placer loop drives the rest. When additional is unset
// or zero, this is a no-op and the placer behaves as a pure target.
//
// Counting matches the placer's own `countDiskfulReplicas` (diskful only,
// non-disabled nodes only) so a tiebreaker witness or an evicted-node
// replica doesn't suppress the increment. The increment is computed
// after every other filter merge so an operator who supplies BOTH a
// `place_count: 5` AND `additional_place_count: 1` ends up with
// `existing + 1` (upstream semantic — additional overrides target).
func (s *Server) resolveAdditionalPlaceCount(ctx context.Context, rdName string, filter *apiv1.AutoSelectFilter) error {
	if filter.AdditionalPlaceCount <= 0 {
		return nil
	}

	existing, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return errors.Wrap(err, "list resources for additional_place_count")
	}

	disabled, err := s.disabledNodes(ctx)
	if err != nil {
		return err
	}

	diskful := 0

	for i := range existing {
		if _, off := disabled[existing[i].NodeName]; off {
			continue
		}

		if slices.Contains(existing[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		diskful++
	}

	filter.PlaceCount = apiv1.LaxInt32(diskful) + filter.AdditionalPlaceCount
	// Once consumed, drop the delta so it doesn't leak into the
	// shortfall envelope's "Additional replica count: N" line — the
	// effective PlaceCount is what callers reason about post-merge.
	filter.AdditionalPlaceCount = 0

	return nil
}

// writeAutoplaceShortfall renders the upstream-shaped ApiCallRc
// envelope for a failed autoplace call: structured cause + details +
// correction (F13). When capErr is non-nil the Details block also
// carries the numeric "required N KiB, max free M KiB" line so the
// operator can size-down or grow a pool without re-running the call
// to find the gap.
//
// The criteria list mirrors upstream
// `CtrlRscAutoPlaceApiCallHandler.failNotEnoughCandidates`: storage-
// pool name (when constrained), free-space minimum (when the placer
// has a required size), access-context, and online-ness. The
// configuration block mirrors `AutoSelectFilterApi.asHelpString`:
// only the fields actually set on the filter render, so a bare
// `place_count=99` call doesn't drown the operator in empty rows.
func writeAutoplaceShortfall(w http.ResponseWriter, filter *apiv1.AutoSelectFilter, srcKind string, capErr *placer.CapacityShortfallError) {
	var details strings.Builder

	details.WriteString("Not enough nodes fulfilling the following auto-place criteria:\n")

	poolNames := autoplaceFilterPoolNames(filter)
	if len(poolNames) > 0 {
		fmt.Fprintf(&details, " * has a deployed storage pool named %v\n", poolNames)
	}

	if capErr != nil && capErr.RequiredKib > 0 {
		fmt.Fprintf(&details,
			" * the storage pools have to have at least '%d' free space\n",
			capErr.RequiredKib,
		)
	}

	details.WriteString(" * the current access context has enough privileges to use the node and the storage pool\n")
	details.WriteString(" * the node is online\n")
	details.WriteString("\n")
	details.WriteString("Auto-place configuration details:\n")
	writeAutoplaceConfig(&details, filter)

	if capErr != nil {
		fmt.Fprintf(&details,
			"\nCapacity shortfall: required %d KiB, max free %d KiB\n",
			capErr.RequiredKib, capErr.MaxFreeKib,
		)
	}

	cause := "Not enough nodes fulfilling the auto-place criteria for the requested placement"

	correction := "Add more nodes or storage pools, or relax the placement constraints " +
		"(reduce place_count, drop node/storage-pool/provider filters, " +
		"or free capacity on existing pools)."

	if srcKind != "" {
		cause = "snapshot is on " + srcKind +
			" but no " + srcKind + " pool found on any candidate node"
		correction = "Add a " + srcKind +
			" storage pool on a candidate node, or restore the snapshot to a node that already has one."
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailNotEnoughNodes,
		Message: "Not enough available nodes",
		Cause:   cause,
		Details: details.String(),
		Correc:  correction,
	}})
}

// autoplaceFilterPoolNames returns the union of single-pool +
// pool-list filter fields. Used by the shortfall envelope so the
// criteria bullet renders the operator's effective pool constraint
// rather than an empty `[]`.
func autoplaceFilterPoolNames(filter *apiv1.AutoSelectFilter) []string {
	if filter == nil {
		return nil
	}

	if len(filter.StoragePoolList) > 0 {
		return filter.StoragePoolList
	}

	if filter.StoragePool != "" {
		return []string{filter.StoragePool}
	}

	return nil
}

// writeAutoplaceConfig renders the subset of the AutoSelectFilter
// that mirrors upstream LINSTOR's
// `AutoSelectFilterApi.asHelpString("   ")` — only fields the caller
// actually set get a line, so a bare call doesn't drown the operator
// in empty rows. Split into writeAutoplacePools / writeAutoplaceTopology
// to stay under the gocyclo budget; each helper covers a logically
// related slice of the filter.
func writeAutoplaceConfig(buf *strings.Builder, filter *apiv1.AutoSelectFilter) {
	const indent = "   "

	if filter == nil {
		return
	}

	if filter.PlaceCount > 0 {
		fmt.Fprintf(buf, "%sReplica count: %d\n", indent, filter.PlaceCount)
	}

	if filter.AdditionalPlaceCount > 0 {
		fmt.Fprintf(buf, "%sAdditional replica count: %d\n", indent, filter.AdditionalPlaceCount)
	}

	writeAutoplacePools(buf, filter, indent)
	writeAutoplaceTopology(buf, filter, indent)

	if filter.DisklessOnRemaining {
		fmt.Fprintf(buf, "%sDiskless on remaining: true\n", indent)
	}
}

// writeAutoplacePools renders the pool / node-list slice of the
// filter (StoragePool, NodeNameList, NotPlaceWith, LayerStack,
// ProviderList). Split off from writeAutoplaceConfig for gocyclo.
func writeAutoplacePools(buf *strings.Builder, filter *apiv1.AutoSelectFilter, indent string) {
	if len(filter.NodeNameList) > 0 {
		fmt.Fprintf(buf, "%sNode name: %v\n", indent, filter.NodeNameList)
	}

	if filter.StoragePool != "" {
		fmt.Fprintf(buf, "%sStorage pool name: %s\n", indent, filter.StoragePool)
	}

	if len(filter.StoragePoolList) > 0 {
		fmt.Fprintf(buf, "%sStorage pool names: %v\n", indent, filter.StoragePoolList)
	}

	if len(filter.StoragePoolDisklessList) > 0 {
		fmt.Fprintf(buf, "%sStorage pool diskless name: %v\n", indent, filter.StoragePoolDisklessList)
	}

	if len(filter.NotPlaceWithRsc) > 0 {
		fmt.Fprintf(buf, "%sDo not place with resource: %v\n", indent, filter.NotPlaceWithRsc)
	}

	if filter.NotPlaceWithRscRegex != "" {
		fmt.Fprintf(buf, "%sDo not place with resource (regex): %s\n", indent, filter.NotPlaceWithRscRegex)
	}

	if len(filter.LayerStack) > 0 {
		fmt.Fprintf(buf, "%sLayer stack: %v\n", indent, filter.LayerStack)
	}

	if len(filter.ProviderList) > 0 {
		fmt.Fprintf(buf, "%sAllowed Provider: %v\n", indent, filter.ProviderList)
	}
}

// writeAutoplaceTopology renders the topology slice of the filter
// (replicas_on_same / _on_different / x_replicas_on_different_map).
func writeAutoplaceTopology(buf *strings.Builder, filter *apiv1.AutoSelectFilter, indent string) {
	if len(filter.ReplicasOnSame) > 0 {
		fmt.Fprintf(buf, "%sReplicas on nodes with same properties: %v\n", indent, filter.ReplicasOnSame)
	}

	if len(filter.ReplicasOnDifferent) > 0 {
		fmt.Fprintf(buf, "%sReplicas on nodes with different properties: %v\n", indent, filter.ReplicasOnDifferent)
	}

	if len(filter.XReplicasOnDifferentMap) > 0 {
		fmt.Fprintf(buf, "%sX-replicas on different properties (per-key cap): %v\n", indent, filter.XReplicasOnDifferentMap)
	}
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

// apiCallRcFailNotEnoughNodes mirrors upstream
// `ApiConsts.FAIL_NOT_ENOUGH_NODES` (= 996). The shortfall envelope
// in writeAutoplaceShortfall ORs this with MASK_ERROR
// (apiCallRcError) so the wire shape matches
// `CtrlRscAutoPlaceApiCallHandler.failNotEnoughCandidates`. Tools
// that classify replies by `ret_code` (e.g. cli-parity contract
// tests) need the same sub-code, not a generic
// "high-bit set" error.
const apiCallRcFailNotEnoughNodes int64 = 996

// mergeAutoplaceFilter merges the request's filter on top of the parent
// ResourceGroup's stored select filter. Request fields win.
//
// Scenario 4.W15 — StoragePool resolution chain (high → low priority):
//
//	request.SelectFilter.StoragePool  (operator typed `--storage-pool`)
//	rd.Props["StorPoolName"]          (RD-level sticky default, this tier)
//	rg.SelectFilter.StoragePool       (RG-level default)
//	none → placer picks any matching pool
//
// The RD-prop tier sits between the request and the RG so an operator
// who did `linstor rd set-property <rd> StorPoolName pool` can pin
// future autoplace / spawn replicas to that pool without rewriting the
// shared RG, while still being overridable by an explicit
// `r c --auto-place --storage-pool other` invocation.
func mergeAutoplaceFilter(ctx context.Context, st store.Store, rd *apiv1.ResourceDefinition, req *apiv1.AutoSelectFilter) apiv1.AutoSelectFilter {
	out := apiv1.AutoSelectFilter{}

	if rd.ResourceGroupName != "" {
		// CreateVolume hot path — RG may have been created on a sibling
		// apiserver replica milliseconds ago. Retry on NotFound to
		// absorb cache lag rather than silently falling back to the
		// empty SelectFilter (which would mis-place replicas).
		// See pkg/rest/cache_retry.go.
		rg, err := getRGWithCacheRetry(ctx, st, rd.ResourceGroupName)
		if err == nil {
			out = rg.SelectFilter
		}
	}

	// Scenario 4.W15 RD-prop tier: an RD-level Props["StorPoolName"]
	// overrides whatever the RG defaulted to. The request's own
	// StoragePool below still wins, so the operator's per-call
	// `--storage-pool` flag stays authoritative.
	if rdPool := rd.Props["StorPoolName"]; rdPool != "" {
		out.StoragePool = rdPool
		// Drop any RG-inherited StoragePoolList — the explicit RD
		// prop is a single-pool pin, so a list-form RG default would
		// re-widen the candidate set and contradict operator intent.
		out.StoragePoolList = nil
	}

	if req.PlaceCount > 0 {
		out.PlaceCount = req.PlaceCount
	}

	// Scenario 4.W17: `--auto-place +1` posts AdditionalPlaceCount
	// instead of PlaceCount; carry the request-side value forward so
	// resolveAdditionalPlaceCount can fold it into the effective
	// place_count. Unlike PlaceCount the RG never stores an
	// "additional" knob (it's a per-call delta intent, not a target),
	// so we only ever take the request's value here.
	if req.AdditionalPlaceCount > 0 {
		out.AdditionalPlaceCount = req.AdditionalPlaceCount
	}

	mergeAutoplaceFilterFromRequest(&out, req)

	if out.PlaceCount == 0 {
		out.PlaceCount = 1
	}

	return out
}

// mergeAutoplaceFilterFromRequest applies the per-field "request wins"
// overrides onto the RG-default-seeded out. Pulled out of
// mergeAutoplaceFilter so the latter stays under the gocyclo budget
// once the Bug 131 ProviderList copy was added.
//
// Every field on AutoSelectFilter that the wire shape exposes (per
// upstream LINSTOR / pkg/api/v1.AutoSelectFilter) is propagated here —
// the only fields the caller never sees are OverrideVlmID
// (per-spawn-call, not a select filter knob) and LayerStack (which
// also lacks a request-side merge today: a separate issue, but not
// Bug 131's scope).
func mergeAutoplaceFilterFromRequest(out, req *apiv1.AutoSelectFilter) {
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

	if len(req.XReplicasOnDifferentMap) > 0 {
		// Scenario 9.W08: bucket-cap form of replicas_on_different.
		// Copy so a later mutation on the request body can't reach
		// into the merged filter (the RG-level map is reference-
		// shared with the parent ResourceGroup).
		out.XReplicasOnDifferentMap = maps.Clone(req.XReplicasOnDifferentMap)
	}

	// Bug 131: copy the request's provider_list onto the merged
	// filter so the placer's matchesPoolFilter actually enforces it.
	// Pre-fix this field was silently dropped — autoplace returned
	// 200 even when no candidate pool's ProviderKind matched, and
	// replicas landed on the wrong tier. Request wins over RG-default
	// (mirrors every other slice field on this struct); a copy is
	// taken so a later mutation on the request body can't reach into
	// the merged filter.
	if len(req.ProviderList) > 0 {
		out.ProviderList = append([]string(nil), req.ProviderList...)
	}

	if req.DisklessOnRemaining {
		out.DisklessOnRemaining = true
	}
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

		// Enforce the cluster-wide naming convention up front: the CRD
		// metadata.name will be `<rd>.<node>`, so an embedded '.' in
		// either side would shift the boundary and either collide with
		// another (rd, node) pair or stage a CRD the CEL rule on the
		// type would later reject with a 422. Catch it here with a
		// friendly 400.
		if strings.Contains(res.NodeName, ".") {
			writeError(w, http.StatusBadRequest,
				"node_name must not contain '.': metadata.name must equal <rd>.<node>")

			return nil, false
		}

		if strings.Contains(rdName, ".") {
			writeError(w, http.StatusBadRequest,
				"resource_definition name must not contain '.': metadata.name must equal <rd>.<node>")

			return nil, false
		}

		// Bug 94: refuse to stage a Resource CRD pointing at a node
		// the controller never registered. Without this gate
		// `linstor r c <bogus-node> <rd>` happily wrote
		// `<rd>.<bogus-node>` into the store and the satellite
		// reconciler then had no way to reach the named node — the
		// phantom CRD survived forever as orphaned state. We do the
		// existence check here (not in the per-replica store create)
		// so the operator sees a 404 + LINSTOR envelope with the
		// exact unresolved name + an actionable correction hint.
		_, nodeErr := s.Store.Nodes().Get(r.Context(), res.NodeName)
		if errors.Is(nodeErr, store.ErrNotFound) {
			writeError(w, http.StatusNotFound,
				"node '"+res.NodeName+"' not found: create the node first with "+
					"`linstor n c <name>` or pass a valid existing node name")

			return nil, false
		}

		if nodeErr != nil {
			writeStoreError(w, nodeErr)

			return nil, false
		}

		if !s.refuseResourceCreateOnUnknownPool(w, r, &res) {
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
	//
	// CreateVolume hot path: retry on NotFound to absorb sibling
	// apiserver replica cache lag — see pkg/rest/cache_retry.go.
	rd, err := getRDWithCacheRetry(r.Context(), s.Store, rdName)
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
// Idempotent on NotFound (Bug 56): CSI spec § DeleteVolume mandates
// idempotence — the driver retries until it sees success, so a 404
// on either an unknown {rd} or an unknown {node} segment breaks the
// second-delete-after-success retry path. Both branches fold to a
// 200 + ApiCallRc envelope carrying the warn-mask `warnRscNotFound`
// ret_code and an "already absent" message, distinct from the
// MASK_INFO-only "deleted" reply so operators reading API logs can
// tell a real drop from a no-op replay. Mirrors upstream LINSTOR's
// behaviour (`linstor r d` on a missing pair returns
// `WARNING: … not found.` exit 0).
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
	// the Delete call below folds the missing replica into the
	// idempotent 200 + warn envelope.
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
		if errors.Is(err, store.ErrNotFound) {
			// Bug 56 idempotent envelope. Bug 67: a no-op DELETE
			// must NOT bump sibling annotations — only a REAL drop
			// changes the peer set the survivors render into .res.
			// Spurious bumps on the CSI retry path would churn the
			// satellite reconciler loop (every replay = one more
			// drbdadm adjust on every survivor) and confuse audit
			// tooling that watches `blockstor.io/peer-changed`.
			//
			// Bug 124: still drain the local cache on the no-op
			// branch — a retry-after-success-on-sibling-replica
			// can land here while this replica's informer cache
			// still has the row.
			s.waitForResourceDeletionVisible(r.Context(), rdName, node)

			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: warnRscNotFound,
				Message: "resource already absent: " + rdName + " on " + node,
			}})

			return
		}

		writeStoreError(w, err)

		return
	}

	// Bug 124: wait for the local informer cache to observe the
	// per-replica drop so the very next `r l` / `view/resources` on
	// this apiserver replica reflects it. See
	// pkg/rest/cache_invalidation.go.
	s.waitForResourceDeletionVisible(r.Context(), rdName, node)

	// Bug 67: notify surviving sibling Resources of the peer change so
	// the satellite reconcilers re-derive their peer set without the
	// dropped replica. The satellite's controller-runtime watch is
	// scoped by `nodeNamePredicate` to its OWN Resource CRDs, so a
	// peer-Resource Delete on another node never wakes the local
	// reconciler — bumping an annotation on each survivor is the
	// minimal-cost event the local watch DOES see. Best-effort: a
	// failure here does NOT roll the delete back (the row is already
	// gone) — the next user-initiated event or full reconcile sync
	// will eventually converge. Order of operations is deliberately
	// "delete first, bump second" so a survivor that reconciles on
	// the annotation event observes the post-delete Resource list,
	// not the racing pre-delete state.
	s.bumpPeerChangedOnSiblings(r.Context(), rdName, node)

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: apiCallRcInfo | apiCallRcRscDeleted,
		Message: "resource deleted: " + rdName + " on " + node,
	}})
}

// bumpPeerChangedOnSiblings stamps an RFC3339Nano timestamp on every
// surviving Resource of `rdName`, excluding `removedNode` (which is
// already gone by the time we get here — Get would return NotFound).
// The annotation value advances on every call, so repeated peer-drops
// produce strictly monotonic timestamps; the satellite's watch sees
// each as a fresh Update event regardless of clock resolution.
//
// RFC3339Nano is used (not RFC3339) so two bumps within the same
// second still produce distinct values. The satellite doesn't parse
// the value — only "did the annotation change since I last reconciled"
// matters — but distinct strings keep the controller-runtime
// resourceVersion gates from short-circuiting consecutive Updates as
// "no semantic change".
//
// Best-effort throughout: List or Update failures are swallowed
// because the operator-requested Delete has already succeeded; a
// stale-peer-set survivor will catch up on the next event (RD spec
// change, satellite heartbeat, full informer resync). Logging the
// failures here would be useful but the REST package has no logger in
// scope on this path — the satellite-side teardown already logs when
// it eventually picks the change up.
func (s *Server) bumpPeerChangedOnSiblings(ctx context.Context, rdName, removedNode string) {
	siblings, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return
	}

	stamp := time.Now().UTC().Format(time.RFC3339Nano)

	for i := range siblings {
		sib := &siblings[i]
		if sib.NodeName == removedNode {
			// Defensive: the deleted Resource should already be
			// absent from the list, but a racing Create could
			// surface a fresh replica on the same node — never
			// re-bump the row we just removed.
			continue
		}

		if sib.Annotations == nil {
			sib.Annotations = map[string]string{}
		}

		sib.Annotations[apiv1.PeerChangedAnnotation] = stamp

		// Update is best-effort. A concurrent satellite SetState
		// using SSA on Status doesn't race this Spec/metadata path
		// (different field owners + different subresources), so the
		// typical failure mode here is a conflict from another REST
		// writer (rare) or NotFound from a same-instant peer Delete
		// (already-fine outcome).
		_ = s.Store.Resources().Update(ctx, sib)
	}
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
