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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
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

	// KeepTiebreakerUntilAnnotation: re-export the canonical constant
	// so the REST package can stamp it without importing internals.
	//
	// keepTiebreakerOverrideWindow: 5 minutes is long enough to cover
	// a typical follow-up sequence (e.g. delete the second diskful
	// after explicitly opting in to keep the witness, then create a
	// fresh diskful elsewhere) without permanently masking the
	// auto-witness invariant if the operator walks away. Matches the
	// suppression window for consistency.
	KeepTiebreakerUntilAnnotation = apiv1.KeepTiebreakerUntilAnnotation
	keepTiebreakerOverrideWindow  = 5 * time.Minute
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
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/resources/{node}",
		s.requireStore(s.handleResourceCreateOnNode))
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
		out = append(out, wrapResourceWithVolumes(&resList[i]))
	}

	// Bug 188 (P0): scrub deny-listed sensitive keys (passphrase,
	// password, shared-secret, ...) from every Resource's Props map
	// before the JSON encode. Sibling `/v1/view/resources` already
	// redacts via Bug 115 / buildResourceView, but the RD-scoped read
	// path bypassed that pipeline — `linstor r lp <rd>` rendered
	// DrbdOptions/EncryptPassphrase verbatim. Resources().List returns
	// a value copy, so the in-place mutation is local to this response.
	for i := range out {
		redactSensitiveProps(out[i].Props)
	}

	// Bug-hunt v0.1.3 Finding 2: strip controller-internal annotations
	// before the JSON encode. The reconcilers stamp `blockstor.io/*`
	// and `bug<N>.blockstor.cozystack.io/*` keys for in-process
	// bookkeeping; the wire shape must not carry them. See
	// pkg/rest/internal_annotations.go.
	stripInternalAnnotationsFromResourcesWithVolumes(out)

	writeJSON(w, http.StatusOK, out)
}

// wrapResourceWithVolumes wraps a Resource into ResourceWithVolumes so
// the `volumes` JSON key is always present (never `null`). Bug-hunt
// v0.1.3 Finding 3: handleResourceList / handleResourceGet wrapped via
// `ResourceWithVolumes{Resource: res}` leaving the outer Volumes nil,
// which `encoding/json` serialises as `"volumes":null`. Upstream
// LINSTOR's `Resource` schema has no `volumes` key at all, but the
// Python CLI's `responses.py` derefs `rsc._rest_data['volumes']`
// unconditionally and crashes on `None.iter`. Mirrors the
// ensureVolumesForView contract used by /v1/view/resources: a non-nil
// (possibly empty) slice survives the JSON encode as `[]`.
func wrapResourceWithVolumes(res *apiv1.Resource) apiv1.ResourceWithVolumes {
	out := apiv1.ResourceWithVolumes{Resource: *res, Volumes: res.Volumes}
	if out.Volumes == nil {
		out.Volumes = []apiv1.Volume{}
	}

	return out
}

// handleResourceGet answers `GET /v1/resource-definitions/{rd}/resources/{node}`,
// returning the single Resource on that node or 404.
//
// Bug 143: `linstor r lp <bogus-rd> <bogus-node>` reads its property
// map from this endpoint's response. Pre-fix we went straight to
// Resources().Get, which only knows about the (rd, node) tuple and
// returns the same generic "resource X on node Y not found" envelope
// whether the RD is missing, the node is missing, or just the replica
// is. Python-linstor's CLI then rendered the empty response with the
// noisy "No property map found" message, hiding the precondition
// failure. The fix mirrors Bug 94 (unknown node on r c) / Bug 118
// (unknown pool on r c) / Bug 133 / Bug 134: probe the RD store first
// so the 404 envelope explicitly labels the missing object as a
// "resource definition" when that's what the operator typo'd, before
// falling through to the per-replica Get whose ErrNotFound names the
// (rd, node) pair.
func (s *Server) handleResourceGet(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound,
				"resource definition '"+rdName+"' not found: check the "+
					"name with `linstor rd l` or create it first")

			return
		}

		writeStoreError(w, err)

		return
	}

	res, err := s.Store.Resources().Get(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Bug 188 (P0): scrub deny-listed sensitive keys before emit.
	// Resources().Get returns a value copy, so the in-place mutation
	// stays local to this response. Mirrors the list-side scrub above.
	redactSensitiveProps(res.Props)

	// Bug-hunt v0.1.3 Finding 2: same controller-internal-annotation
	// strip applied on the cluster-wide view and per-RD list paths.
	res.Annotations = stripInternalAnnotations(res.Annotations)

	// Bug-hunt v0.1.3 Finding 3: wrap via wrapResourceWithVolumes so the
	// outer `volumes` JSON key serialises as `[]` instead of `null` when
	// the Resource carries no Volumes; protects python-linstor's
	// `rsc._rest_data['volumes']` dereference.
	writeJSON(w, http.StatusOK, wrapResourceWithVolumes(&res))
}

// handleAutoplace selects up to `place_count` nodes that have a storage
// pool of the requested kind/name and creates Resource objects on them.
//
// Phase 2.5 keeps the placement logic deliberately simple — we trust the
// CRD store as state and never reach out to a satellite. Phase 3's
// autoplacer will weigh free capacity, traits, anti-affinity, etc.
func (s *Server) handleAutoplace(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	rawBody, req, ok := decodeAutoplaceBody(w, r)
	if !ok {
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

	// Bug 156: when the operator explicitly passes
	// `--diskless-on-remaining false`, also suppress the
	// auto-tiebreaker reconciler's TIE_BREAKER witness.
	if !s.applyBug156AutoTiebreakerOptOut(r.Context(), w, rawBody, rdName, &rd) {
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

	// Bug 335: reject auto-place=N with N>1 on a non-replicated
	// LayerStack. Without DRBD (the only block-replication layer
	// blockstor currently ships) `place_count=2` would silently
	// allocate two independent local volumes on two nodes — the data
	// diverges on the first write and the operator only finds out
	// much later.
	//
	// Three operator-actionable ways forward are listed in the error
	// envelope: add DRBD to the layer list, drop the place_count to
	// 1, or (TODO) wait for shared-LUN support.
	//
	// Gate runs after resolveAdditionalPlaceCount so the effective
	// PlaceCount reflects `--auto-place +1` too: 1+1=2 on a STORAGE-
	// only RD is the same data-divergence footgun as a literal
	// `--auto-place 2`.
	if !s.refuseAutoplaceMultiPlaceWithoutReplication(r.Context(), w, &rd, &filter) {
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

	// Issue #45: REST-level capacity gate. Mirrors the parallel
	// `rejectIfExceedsOversubGate` on the spawn path so linstor-csi's
	// autoplace call fails-fast with a structured 409 BEFORE the
	// placer is invoked. Without this gate, a CreateVolume against a
	// StorageClass pinned to a now-full pool (`FreeCapacity=0` /
	// `MaxVlmSizeInKib=0`) still placed the replica — the PVC
	// reached Bound immediately and only failed later when the
	// satellite tried to allocate the backing LV. The gate consults
	// `computeSizeInfo.MaxVlmSizeInKib` (the same value `linstor rg
	// query-size-info` surfaces) so over-subscription ratios and the
	// shared-LUN dedup are honoured uniformly across spawn /
	// autoplace.
	if !s.rejectAutoplaceIfExceedsCapacityGate(r.Context(), w, rdName, &filter, srcKind) {
		return
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

// refuseAutoplaceMultiPlaceWithoutReplication is Bug 335's
// data-divergence guard. Refuses an autoplace request whose effective
// PlaceCount > 1 lands on a RD whose effective LayerStack carries no
// replication layer (today: DRBD).
//
// Stand reproduction (pre-fix):
//
//	$ linstor r c test3 --auto-place=2 -l STORAGE -s stand
//	→ 200 OK, 2 independent local volumes on 2 nodes
//	→ first write to either replica diverges silently
//
// Effective layer stack is RD → RG → default (mirrors the read-side
// resolver used by the dispatcher / controller). When everything is
// empty we fall through to `["DRBD","STORAGE"]` so legacy RDs (and
// every test that omits LayerStack) continue to pass — they DO have
// replication by default and the gate must not fire on them.
//
// TODO(shared-lun): when shared-LUN active-active support lands
// (likely thin LVM with lvmlockd, cooperative deactivate-others +
// activate-on-one semantics for the rest), extend this gate to
// permit multi-place with an explicit `--shared-lun` flag. Until
// then, shared-LUN multi-place is unsupported and the safe default
// is to reject the request loudly.
//
// Returns true when the caller may proceed (PlaceCount<=1 OR the
// stack contains a replication layer), false when the HTTP error has
// already been written.
func (s *Server) refuseAutoplaceMultiPlaceWithoutReplication(ctx context.Context, w http.ResponseWriter, rd *apiv1.ResourceDefinition, filter *apiv1.AutoSelectFilter) bool {
	if filter.PlaceCount <= 1 {
		return true
	}

	stack := s.resolveAutoplaceLayerStack(ctx, rd)
	if apiv1.ContainsReplicationLayer(stack) {
		return true
	}

	writeError(w, http.StatusBadRequest,
		"placeCount="+strconv.FormatInt(int64(filter.PlaceCount), 10)+
			" with no replication layer (DRBD) would create independent local "+
			"volumes whose data diverges on the first write. Either add DRBD to "+
			"the layer list, drop the place_count to 1 (single local volume), or "+
			"wait for shared-LUN multi-host support (currently unsupported).")

	return false
}

// resolveAutoplaceLayerStack returns the effective LayerStack for the
// autoplace handler's data-divergence gate. RD → RG → default mirrors
// the same precedence the dispatcher and the controller-side
// resolveRDLayerStack use, so the REST gate and the witness gate
// (Bug 334) agree on what "no replication layer" means.
func (s *Server) resolveAutoplaceLayerStack(ctx context.Context, rd *apiv1.ResourceDefinition) []string {
	if rd != nil && len(rd.LayerStack) > 0 {
		return rd.LayerStack
	}

	if rd != nil && rd.ResourceGroupName != "" {
		rg, err := getRGWithCacheRetry(ctx, s.Store, rd.ResourceGroupName)
		if err == nil && len(rg.SelectFilter.LayerStack) > 0 {
			return rg.SelectFilter.LayerStack
		}
	}

	return apiv1.DefaultLayerStack()
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

// refuseResourceCreateOnUnknownRD is Bug 144's gate. Without it
// `POST /v1/resource-definitions/<bogus-rd>/resources` happily
// allocated a DRBD minor/port and persisted a `<bogus>.<node>`
// Resource CRD that the satellite reconciler could never reconcile
// (no parent RD to read VolumeDefinitions from). Mirror the Bug
// 94/118/134 envelope shape and refuse with 404 + LINSTOR envelope
// naming the missing RD.
func (s *Server) refuseResourceCreateOnUnknownRD(w http.ResponseWriter, r *http.Request, rdName string) bool {
	_, err := getRDWithCacheRetry(r.Context(), s.Store, rdName)
	if err == nil {
		return true
	}

	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound,
			"resource definition '"+rdName+"' not found: check the "+
				"name with `linstor rd l` or create it first")

		return false
	}

	writeStoreError(w, err)

	return false
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

	// Bug 205: typed-Patch via PatchResourceDefinitionSpec — the
	// closure mutates a freshly-fetched live RD on every retry, so
	// concurrent disjoint edits (RG-supplied props, racing r-conn
	// add, autoplace LayerStack stamp) all converge instead of
	// being lost by a stale-wire-snapshot replay.
	err := s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(r.Context(), rd.Name, func(live *apiv1.ResourceDefinition) error {
		if len(live.LayerStack) > 0 {
			// A racing writer already set LayerStack — leave it
			// alone (RD-level wins over autoplace-supplied list,
			// matching the original guard at the top of this fn).
			return nil
		}

		live.LayerStack = append([]string(nil), layerList...)

		return nil
	})
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

// rejectAutoplaceIfExceedsCapacityGate is the REST-level capacity
// guard for Issue #45. Mirrors `rejectIfExceedsOversubGate` in
// spawn.go: consults `computeSizeInfo.MaxVlmSizeInKib` (the same
// value `linstor rg query-size-info` surfaces) and fails-fast with
// a structured 409 BEFORE the placer is invoked when any of the
// RD's VolumeDefinitions exceeds the effective cluster cap for the
// merged filter.
//
// Returns true when the caller may proceed (no VDs, no eligible
// pools at this filter, or every VD fits the cap), false when the
// HTTP error has already been written.
//
// Precedence vs. the placer's per-pool capacity gate (Bug 35):
//   - placer gate compares `pool.FreeCapacity < requiredKib` after
//     pool selection and only when there are VDs on the RD.
//   - this REST gate compares `vd.SizeKib > MaxVlmSizeInKib`, where
//     `MaxVlmSizeInKib` already accounts for over-subscription
//     ratios and shared-LUN dedup across the cluster.
//
// The two gates are complementary: the REST gate refuses fast when
// no candidate pool can possibly host the volume; the placer gate
// catches per-pool shortfalls under topology constraints (e.g.
// anti-affinity).
//
// Empty / zero VolumeDefinitions (definitions-only spawn before VDs
// are written) skip the check — nothing to gate.
//
// MaxVlmSizeInKib==0 means there are no eligible pools at the
// requested placement (e.g. unknown storage_pool name, or every
// candidate already filtered out by the RG/SP filter). The placer's
// own "not enough candidate storage pools" envelope has richer
// per-pool context for that case, so the gate stays silent and lets
// runPlaceAndReport surface the failure.
func (s *Server) rejectAutoplaceIfExceedsCapacityGate(ctx context.Context, w http.ResponseWriter, rdName string, filter *apiv1.AutoSelectFilter, srcKind string) bool {
	vds, err := s.Store.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		writeStoreError(w, err)

		return false
	}

	if len(vds) == 0 {
		return true
	}

	var requiredKib int64
	for i := range vds {
		if vds[i].SizeKib > requiredKib {
			requiredKib = vds[i].SizeKib
		}
	}

	if requiredKib <= 0 {
		return true
	}

	info, err := s.computeSizeInfo(ctx, filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return false
	}

	capKib := info.SpaceInfo.MaxVlmSizeInKib
	if capKib <= 0 {
		// No candidate pools at the requested placement — defer to
		// the placer's "not enough candidate storage pools" path so
		// the 409 envelope carries the per-filter exclusion reasons
		// (writeAutoplaceShortfall has richer context than this
		// gate).
		return true
	}

	if requiredKib <= capKib {
		return true
	}

	// Reuse writeAutoplaceShortfall so the 409 wire shape matches
	// the placer's CapacityShortfallError path: same ApiCallRc code
	// (apiCallRcFailNotEnoughNodes), same criteria bullet list, same
	// Auto-place configuration block. Operators (and tests) can
	// classify both REST-gate and placer-gate shortfalls with one
	// rule.
	writeAutoplaceShortfall(w, filter, srcKind, &placer.CapacityShortfallError{
		RequiredKib: requiredKib,
		MaxFreeKib:  capKib,
	})

	return false
}

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

// disklessOnRemainingExplicitlyFalse decodes the raw autoplace body
// into a key-presence map and reports whether the caller passed
// `diskless_on_remaining: false` explicitly. A bare `false` and a
// missing field both decode to the same Go zero value, so this
// helper inspects the wire shape directly — mirrors the
// `rgSelectFilterKeys` pattern in pkg/rest/resource_groups.go.
//
// Returns false on malformed JSON or missing key; callers must
// validate the body separately before relying on this answer.
func disklessOnRemainingExplicitlyFalse(raw []byte) bool {
	if len(bytes.TrimSpace(raw)) == 0 {
		return false
	}

	var envelope map[string]json.RawMessage

	err := json.Unmarshal(raw, &envelope)
	if err != nil {
		return false
	}

	raw, ok := envelope["diskless_on_remaining"]
	if !ok {
		return false
	}

	// python-linstor 1.27.1 wire shape: when the operator does NOT
	// pass --diskless-on-remaining, the CLI serialises the field as
	// JSON `null` (not omitted). json.Unmarshal(null, &bool) sets
	// the bool to zero (false) without error, which would flip the
	// default-case autoplace into "no auto-witness" and strip the
	// TIE_BREAKER from every default RD. Treat null as "absent".
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}

	var explicit bool

	err = json.Unmarshal(raw, &explicit)
	if err != nil {
		return false
	}

	return !explicit
}

// decodeAutoplaceBody reads the request body once and returns both
// the raw bytes (for the Bug 156 zero-value-aware probe) and the
// typed `AutoPlaceRequest`. Returns ok=false after writing the
// envelope when the body is unreadable or malformed; callers must
// abort the handler.
//
// Hoisted out of handleAutoplace to keep its funlen under budget
// once the Bug 156 wire-shape probe landed.
func decodeAutoplaceBody(w http.ResponseWriter, r *http.Request) ([]byte, apiv1.AutoPlaceRequest, bool) {
	var req apiv1.AutoPlaceRequest

	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		writeDecodeError(w, err)

		return nil, req, false
	}

	// Bug 158/161: typed-envelope decode + DisallowUnknownFields so a
	// stray top-level field (or an empty / malformed body) surfaces as
	// a stable 400 + LINSTOR envelope with no Go-side type leak. The
	// raw bytes are still needed by the Bug 156 zero-value-aware probe
	// downstream — we just discipline the decode path here.
	dec := json.NewDecoder(bytes.NewReader(rawBody))
	dec.DisallowUnknownFields()

	err = dec.Decode(&req)
	if err != nil {
		writeDecodeError(w, err)

		return nil, req, false
	}

	return rawBody, req, true
}

// applyBug156AutoTiebreakerOptOut is the wire-level hook for Bug
// 156: when the autoplace body carries an explicit
// `diskless_on_remaining: false`, stamp
// `DrbdOptions/AutoAddQuorumTiebreaker=false` on the RD so the
// controller's auto-witness reconciler stays out of the way. The
// flag's name leads operators to expect "no diskless residual,
// including the witness"; pre-fix the witness was always stamped.
//
// We probe the raw body for the literal key (zero-value bool can't
// be distinguished from "field absent" through the typed decode).
// Returns false on error after writing the envelope (caller must
// abort the handler); returns true on success or no-op.
//
// On success the in-memory `rd` copy is refreshed so the downstream
// merge sees the stamped prop.
func (s *Server) applyBug156AutoTiebreakerOptOut(
	ctx context.Context, w http.ResponseWriter, rawBody []byte, rdName string,
	rd *apiv1.ResourceDefinition,
) bool {
	if !disklessOnRemainingExplicitlyFalse(rawBody) {
		return true
	}

	err := s.stampAutoTiebreakerOptOut(ctx, rdName)
	if err != nil {
		writeStoreError(w, err)

		return false
	}

	refreshed, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		writeStoreError(w, err)

		return false
	}

	*rd = refreshed

	return true
}

// stampAutoTiebreakerOptOut writes `DrbdOptions/AutoAddQuorumTiebreaker=false`
// onto the named RD so the internal/controller reconciler's
// `isAutoTieBreakerEnabled` reads false and skips witness creation.
// Idempotent: re-stamping the same value is a no-op.
//
// Bug 156: the operator's explicit `--diskless-on-remaining false`
// intent is recorded as a per-RD prop because the auto-witness
// decision happens out-of-band in the controller — the REST handler
// can't suppress it inline.
func (s *Server) stampAutoTiebreakerOptOut(ctx context.Context, rdName string) error {
	const (
		propKey      = "DrbdOptions/AutoAddQuorumTiebreaker"
		propValueOff = "false"
	)

	// Bug 205: typed-Patch via PatchResourceDefinitionSpec — the
	// closure re-runs on every conflict against the live RD, so
	// disjoint concurrent edits (RG-supplied props, racing r-conn
	// add) converge with the tiebreaker opt-out instead of being
	// lost by a stale-wire-snapshot replay.
	err := s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName, func(live *apiv1.ResourceDefinition) error {
		if live.Props != nil && live.Props[propKey] == propValueOff {
			return nil
		}

		if live.Props == nil {
			live.Props = map[string]string{}
		}

		live.Props[propKey] = propValueOff

		return nil
	})
	if err != nil {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	return nil
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
		writeDecodeError(w, err)

		return
	}

	envelopes, err := decodeResourceCreateBody(body)
	if err != nil {
		// Bug 158/161: route every decode failure through the
		// typed-envelope helper so an empty body, malformed JSON,
		// wrong shape, OR an unknown top-level field surfaces as a
		// stable LINSTOR `[]ApiCallRc` reply with no Go-side type
		// leak. decodeResourceCreateBody returns the underlying
		// decoder error wrapped via cockroachdb/errors.Wrap, so the
		// helper's errors.As checks unwrap it correctly.
		writeDecodeError(w, err)

		return
	}

	if len(envelopes) == 0 {
		writeError(w, http.StatusBadRequest, "empty resource create body")

		return
	}

	created, ok := s.createResources(w, r, rdName, envelopes)
	if !ok {
		return
	}

	// Bug 263 (P1, stand-caught): notify surviving sibling Resources
	// of the new peer so each satellite reconciles and re-renders its
	// .res to add the freshly joined replica's `on <node>` block /
	// `drbdadm adjust`. The satellite-side c-r sibling watch DOES
	// fire on the Spec create, but typically lands BEFORE the
	// controller-side allocator has stamped Status.DRBDNodeID on the
	// new replica — `waitForControllerAllocation` then requeues at
	// 2s and the follow-up Status patch event can be filtered or
	// coalesced by controller-runtime under load. Stamping the
	// annotation HERE (after every per-envelope create has committed)
	// guarantees a deterministic wake-up on each survivor; the
	// annotation value is monotonic with Bug 67's delete-path stamp
	// so the satellite watch can't short-circuit on resourceVersion.
	//
	// Bumps are emitted per created replica (excluding the just-
	// created node itself — its own reconciler picks it up via the
	// `For` watch with the node-name predicate). Best-effort: a
	// failure here MUST NOT roll the create back (the row is already
	// gone); the satellite's own watch + the 2s requeue still
	// converges, just less deterministically.
	for i := range created {
		s.bumpPeerChangedOnSiblings(r.Context(), rdName, created[i].NodeName)
	}

	// Python CLI demands an ApiCallRc list envelope; upstream's
	// `linstor r c` walks it on every reply.
	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource(s) created on resource-definition: " + rdName,
	}})
}

// handleResourceCreateOnNode answers `POST /v1/resource-definitions/
// {rd}/resources/{node}` — the single-node create variant linstor-csi
// v1.10.1 uses when a PVC pins a fixed nodeList (the demo "local"
// SC's `placementCount=1` + `nodeList=<worker>` shape). Upstream
// LINSTOR's OpenAPI registers BOTH the bulk
// `/resources` array-shaped POST AND this single-node alias; CSI
// clients pick whichever matches the placement scope. Before this
// route blockstor returned HTTP 405 (method not allowed) and CSI's
// CreateVolume looped forever ("the path POST … is registered, but
// not for this verb").
//
// Wire shape: accepts EITHER a single `ResourceCreate` object OR a
// 1-element array (same `decodeResourceCreateBody` tolerance as the
// bulk POST). The {node} URL segment is the load-bearing source of
// truth — when the body's NodeName is empty we fill it from the
// path; when both are set we refuse the conflict rather than
// silently honour one over the other.
func (s *Server) handleResourceCreateOnNode(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeDecodeError(w, err)

		return
	}

	// Tolerate empty body BEFORE decoding — the {node} path segment
	// carries the complete create intent in that case (linstor-csi
	// v1.10.1 posts no body for the simple per-node Resource.Create
	// shape). decodeResourceCreateBody errs out on empty input, but
	// for the single-node alias that's a legitimate request — the
	// CSI wire shape just uses the path to convey the intent.
	var envelopes []apiv1.ResourceCreate

	if len(bytes.TrimSpace(body)) == 0 {
		envelopes = []apiv1.ResourceCreate{{Resource: apiv1.Resource{}}}
	} else {
		envelopes, err = decodeResourceCreateBody(body)
		if err != nil {
			writeDecodeError(w, err)

			return
		}
	}

	if len(envelopes) > 1 {
		writeError(w, http.StatusBadRequest,
			"single-node resource create accepts at most one envelope, got "+
				strconv.Itoa(len(envelopes)))

		return
	}

	env := &envelopes[0]

	switch {
	case env.Resource.NodeName == "":
		env.Resource.NodeName = node
	case env.Resource.NodeName != node:
		writeError(w, http.StatusBadRequest,
			"single-node resource create: body node '"+env.Resource.NodeName+
				"' does not match URL path node '"+node+"'")

		return
	}

	created, ok := s.createResources(w, r, rdName, envelopes)
	if !ok {
		return
	}

	// Bug 263 (P1): bump peer-changed annotations on siblings so each
	// satellite reconciles its .res with the new replica. Same pattern
	// as handleResourceCreate above.
	for i := range created {
		s.bumpPeerChangedOnSiblings(r.Context(), rdName, created[i].NodeName)
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource created on resource-definition: " + rdName +
			" / node: " + node,
	}})
}

// createResources walks the envelopes from a POST to
// /v1/resource-definitions/{rd}/resources and either creates each
// Resource fresh or promotes an existing diskless/tiebreaker replica
// to diskful (upstream LINSTOR semantics). Returns (created, true) on
// success; writes the HTTP error and returns (nil, false) on the
// first failure.
func (s *Server) createResources(w http.ResponseWriter, r *http.Request, rdName string, envelopes []apiv1.ResourceCreate) ([]apiv1.Resource, bool) {
	// Bug 144: refuse r c when the parent RD doesn't exist. Without
	// this gate the per-envelope createOneResource happily wrote
	// `<bogus-rd>.<node>` into the store with allocated minor+port,
	// leaving an orphan Resource the satellite reconciler could never
	// reconcile (no RD to read VolumeDefinitions from).
	if !s.refuseResourceCreateOnUnknownRD(w, r, rdName) {
		return nil, false
	}

	created := make([]apiv1.Resource, 0, len(envelopes))

	for i := range envelopes {
		out, ok := s.createOneResource(w, r, rdName, &envelopes[i])
		if !ok {
			return nil, false
		}

		created = append(created, *out)
	}

	return created, true
}

// createOneResource runs the full per-envelope wire-boundary
// pipeline for a single Resource create entry: shape validation,
// Bug 94 / Bug 118 gates, layer-list pass-through, store
// persistence, and the Bug 145 post-write SP-deletion race
// check. Writes the HTTP error on the first failure and returns
// (nil, false); returns (&out, true) on success.
//
// Pulled out of createResources to keep that loop under
// funlen's 60-line limit — the envelope-walk is now a thin
// wrapper, each per-envelope concern lives here.
func (s *Server) createOneResource(w http.ResponseWriter, r *http.Request, rdName string, env *apiv1.ResourceCreate) (*apiv1.Resource, bool) {
	res := env.Resource
	res.Name = rdName

	if !validateResourceCreateShape(w, rdName, &res) {
		return nil, false
	}

	// H3 (corner-case): the modern `linstor r c <node> <rd>
	// --drbd-diskless` (and its `--nvme-initiator` sibling) post the
	// wire flag DRBD_DISKLESS, whereas the deprecated `--diskless`
	// alias still posts the canonical DISKLESS. Every diskless-detection
	// site in blockstor (the placer's splitByDiskless, the satellite's
	// applyStorageIfDiskful, the quorum/tiebreaker math, …) keys on the
	// canonical apiv1.ResourceFlagDiskless == "DISKLESS" only. Without
	// folding DRBD_DISKLESS into DISKLESS at the wire boundary, a replica
	// requested with the RECOMMENDED `--drbd-diskless` flag would be
	// classified DISKFUL — the satellite would carve backing storage for
	// a replica the operator explicitly asked to be diskless, and the
	// quorum/tiebreaker arithmetic would miscount it. Canonicalise here,
	// once, so the rest of the pipeline sees a single spelling.
	normalizeDisklessFlag(&res)

	if !s.checkResourceCreateNodeAndPool(w, r, &res) {
		return nil, false
	}

	// Same CSI pass-through as handleAutoplace: linstor-csi may set
	// layer_list on the explicit-placement call rather than on RD create.
	// Persist onto rd.LayerStack if not already set so the satellite
	// reconciler sees the right composition.
	if len(env.LayerList) > 0 {
		// Bug 204b shape: typed-Patch with retry-on-conflict; the
		// "only if unset" guard re-runs against the live RD on every
		// retry so a concurrent reconciler write can't surface a 409
		// (and a concurrent LayerStack set isn't clobbered).
		_ = s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(r.Context(), rdName,
			func(rd *apiv1.ResourceDefinition) error {
				if len(rd.LayerStack) == 0 {
					rd.LayerStack = append([]string(nil), env.LayerList...)
				}

				return nil
			})
	}

	// Bug 327 (P1, recurring — reported 5×): a bare `linstor r c <node>
	// <rd>` (no `--diskless`, no `--storage-pool`) MUST produce a
	// DISKFUL replica even when a TIE_BREAKER peer already lives on
	// another node. The LINSTOR Python CLI posts a ResourceCreate body
	// with only `resource.node_name` populated — no flags, no
	// StorPoolName — relying on upstream CtrlRscCrtApiHelper to
	// resolve the pool from the parent RG's `SelectFilter.StoragePool`
	// (or a sibling diskful replica) before staging.
	//
	// The pre-fix handler persisted the wire body verbatim. The
	// satellite then saw a Resource with no DISKLESS flag AND no
	// StorPoolName → dispatcher fell through to `rd.Spec.Props["
	// StorPoolName"]` (typically empty on RG-spawned RDs) → empty pool
	// → satellite emitted `disk none;` and the DRBD slot came up
	// Diskless even though the operator never asked for it.
	//
	// Fix: mirror upstream. When the request is a fresh create (Bug 260's
	// witness-takeover path is the existing-Resource case, handled later
	// by `createOrPromoteResource`), the new Resource is not explicitly
	// DISKLESS, and no StorPoolName was pinned, resolve one before the
	// store Create so the persisted CRD already carries a real pool.
	// A TIE_BREAKER peer on another node MUST NOT leak its DISKLESS +
	// TIEBREAKER flags into this spawn — we only stamp StorPoolName,
	// never copy flags from peers.
	s.resolveStorPoolForFreshCreate(r.Context(), rdName, &res)

	// Issue #45: per-pool capacity gate on the real linstor-csi
	// CreateVolume path. linstor-csi's manual scheduler (StorageClass
	// with `nodeList` + `placementCount=1`) fires
	// `POST /v1/resource-definitions/{rd}/resources/{node}` via
	// golinstor's `Resources.Create`, which routes here — NOT through
	// `/autoplace`. The PR #47 capacity gate on `/autoplace` therefore
	// never sees this request, and pre-fix a CreateVolume against a
	// now-full pool placed the replica anyway: the PVC reached Bound
	// immediately and only failed later when the satellite tried to
	// allocate the backing LV.
	//
	// The gate consults the resolved StoragePool's FreeCapacity on the
	// target node directly — not `computeSizeInfo` — because this code
	// path knows the EXACT (node, pool) target, so the autoplace gate's
	// cluster-wide MaxVlmSizeInKib aggregation would mask a full pool
	// behind sibling pools on other nodes. A 13 GiB lvm-thin on
	// worker-1 at 100% used while worker-2's lvm-thin is empty MUST
	// refuse `r c worker-1 <rd>` even though the cluster-wide cap
	// remains 13 GiB.
	if !s.rejectResourceCreateIfPoolFull(w, r, rdName, &res) {
		return nil, false
	}

	out, ok := s.createOrPromoteResource(w, r, &res)
	if !ok {
		return nil, false
	}

	// Bug 145 post-write re-check. The pre-write Bug 118 gate
	// (refuseResourceCreateOnUnknownPool) and the Resource
	// store Create are not atomic: a concurrent
	// `linstor sp d <pool>` can interleave between the two
	// and slip past Bug 152's still-referenced refusal —
	// the refusal walks the Resource list BEFORE this
	// goroutine has persisted the new Resource, sees an
	// empty reference set, and proceeds to drop the SP.
	// Without this re-check the Resource CRD then lands
	// with a dangling `Props["StorPoolName"]` and the
	// satellite reconciler waits forever for a pool that
	// never returns.
	//
	// The fix closes the race in one direction: after the
	// Resource has been persisted, look up the SP one more
	// time. If it's gone, undo the create and surface the
	// same 404 envelope the pre-write gate would have
	// emitted. In the opposite ordering (sp-d's walk runs
	// after our Resource Create) Bug 152's refusal already
	// fires on the SP-delete side. Either ordering yields
	// a clean error envelope and zero orphan Resources.
	if !s.refuseResourceCreateOnSPDeletedRace(w, r, out) {
		return nil, false
	}

	// Bug 174 post-write re-check on the Node side. Mirrors the
	// Bug 145 SP-delete close above: the pre-write Bug 94 unknown-
	// node gate and the Resource store Create are not atomic, so a
	// concurrent `linstor n d <node>` can slip between the two and
	// drop the Node out from under the just-persisted Resource. The
	// `n d` handler's own post-Delete re-walk closes the other half
	// of the race; this re-check closes the half where the Resource
	// Create lands AFTER the `n d` handler's post-walk has already
	// committed the delete.
	if !s.refuseResourceCreateOnNodeDeletedRace(w, r, out) {
		return nil, false
	}

	return out, true
}

// checkResourceCreateNodeAndPool runs the Bug 94 (unknown node)
// and Bug 118 (unknown pool) gates back-to-back. Returns true
// when the caller may proceed; false when the HTTP error has
// already been written.
func (s *Server) checkResourceCreateNodeAndPool(w http.ResponseWriter, r *http.Request, res *apiv1.Resource) bool {
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

		return false
	}

	if nodeErr != nil {
		writeStoreError(w, nodeErr)

		return false
	}

	return s.refuseResourceCreateOnUnknownPool(w, r, res)
}

// refuseResourceCreateOnSPDeletedRace closes the Bug 145 TOCTOU
// window. Called immediately after the Resource has been
// persisted, it re-verifies that the pinned StorPool still
// exists on the target node. On a miss, it rolls back the
// just-persisted Resource (so no orphan CRD survives) and
// writes the same 404 + envelope shape the pre-write Bug 118
// gate uses.
//
// Returns true when the caller may continue; false when the
// HTTP error has already been written and the Resource has
// been rolled back.
func (s *Server) refuseResourceCreateOnSPDeletedRace(w http.ResponseWriter, r *http.Request, res *apiv1.Resource) bool {
	pool := res.Props["StorPoolName"]
	if pool == "" {
		return true
	}

	_, err := s.Store.StoragePools().Get(r.Context(), res.NodeName, pool)
	if err == nil {
		return true
	}

	if errors.Is(err, store.ErrNotFound) {
		// Roll back the just-persisted Resource so the operator
		// doesn't get a phantom CRD on a failed call. Best-effort:
		// if the rollback itself errors (store gone, context
		// cancelled), the operator still gets the 404 envelope
		// and the satellite-side reconciler will surface the
		// dangling-pool error on its next tick.
		_ = s.Store.Resources().Delete(r.Context(), res.Name, res.NodeName)

		writeError(w, http.StatusNotFound,
			"storage pool '"+pool+"' was deleted on node '"+res.NodeName+
				"' concurrently with the resource create (Bug 145): "+
				"retry after re-creating the pool, or pick a different pool")

		return false
	}

	writeStoreError(w, err)

	return false
}

// refuseResourceCreateOnNodeDeletedRace closes the Bug 174 TOCTOU
// window on the Resource-create side. Called immediately after the
// Resource has been persisted, it re-verifies that the pinned Node
// still exists. On a miss, it rolls back the just-persisted
// Resource (so no orphan CRD survives) and writes the same 404 +
// envelope shape the pre-write Bug 94 gate uses.
//
// Returns true when the caller may continue; false when the HTTP
// error has already been written and the Resource has been rolled
// back. Mirrors refuseResourceCreateOnSPDeletedRace exactly.
func (s *Server) refuseResourceCreateOnNodeDeletedRace(w http.ResponseWriter, r *http.Request, res *apiv1.Resource) bool {
	_, err := s.Store.Nodes().Get(r.Context(), res.NodeName)
	if err == nil {
		return true
	}

	if errors.Is(err, store.ErrNotFound) {
		// Roll back the just-persisted Resource so the operator
		// doesn't get a phantom CRD on a failed call. Best-effort:
		// if the rollback itself errors (store gone, context
		// cancelled), the operator still gets the 404 envelope
		// and the satellite-side reconciler will surface the
		// dangling-node error on its next tick.
		_ = s.Store.Resources().Delete(r.Context(), res.Name, res.NodeName)

		writeError(w, http.StatusNotFound,
			"node '"+res.NodeName+"' was deleted concurrently with "+
				"the resource create (Bug 174): retry after re-creating "+
				"the node, or pick a different node")

		return false
	}

	writeStoreError(w, err)

	return false
}

// createOrPromoteResource creates res or promotes an existing
// diskless replica in place. Writes the HTTP error and returns
// (nil, false) on failure.
// tieBreakerCollapseRetryAttempts and tieBreakerCollapseRetryDelay
// bound the Bug 359 race window between (a) the RD reconciler's
// `removeWitnesses` Delete that fires when `r d <last-diskful-peer>`
// drops the diskful count to one (the Bug-338 carve-out collapses
// the orphan TIE_BREAKER) and (b) an operator `r c <ex-witness-node>
// <rd>` relocate that lands inside the same reconcile tick. The
// kubectl Delete on the witness CRD finishes synchronously from the
// reconciler's POV, but the k8s apiserver still serves the CRD as
// "exists, DeletionTimestamp set, finalizer pending" for ~tens of ms
// until the satellite strips its finalizer. During that window REST
// `Resources().Create(...)` hits AlreadyExists, `Resources().Get(...)`
// may return NotFound (if the finalizer-strip races just ahead of
// the Get), and `promoteDisklessReplica` then surfaces NotFound from
// its internal PatchResourceSpec. Pre-Bug-359 we surfaced that as a
// 404 "not found" envelope to the operator — they never asked for a
// promote, the witness collapse was an internal carve-out.
//
// The fix retries the whole create-or-promote sequence for ~1s with
// a 200ms cadence; the AlreadyExists window closes the moment GC
// finishes, after which `Resources().Create` succeeds as a fresh
// replica and the relocate converges. Two retries is the worst case
// observed on the dev stand; five gives 3x headroom for CI noise.
const (
	tieBreakerCollapseRetryAttempts = 5
	tieBreakerCollapseRetryDelay    = 200 * time.Millisecond
)

// errWitnessMidDelete marks the third Bug-359 interleaving: the
// witness row still exists but its deletionTimestamp is set (DELETE
// flag on the wire object) — the apiserver would ACCEPT a spec patch
// against it and the pending finalizer-strip would then swallow the
// promote wholesale. Wraps store.ErrNotFound so the existing
// createOrPromoteResourceAttempt retry plumbing treats a dying row
// exactly like an already-gone one.
var errWitnessMidDelete = errors.Wrap(store.ErrNotFound,
	"witness mid-delete (DELETE flag set, finalizer strip pending)")

func (s *Server) createOrPromoteResource(w http.ResponseWriter, r *http.Request, res *apiv1.Resource) (*apiv1.Resource, bool) {
	// Bug 359: a single attempt races the RD reconciler's
	// `removeWitnesses` Delete when the same `r d` that triggered
	// the TIE_BREAKER collapse is immediately followed by a
	// relocate `r c <ex-witness-node>`. See the constant block above
	// for the timing analysis. The retry envelope only fires on the
	// witness-collapse path (AlreadyExists → Get returns NotFound,
	// or promote returns NotFound); a real conflict on a non-witness
	// replica surfaces as 409 on the first attempt as before.
	for attempt := range tieBreakerCollapseRetryAttempts {
		out, ok, retry := s.createOrPromoteResourceAttempt(w, r, res)
		if !retry {
			return out, ok
		}

		if attempt == tieBreakerCollapseRetryAttempts-1 {
			break
		}

		select {
		case <-r.Context().Done():
			writeError(w, http.StatusGatewayTimeout,
				"resource create: context cancelled during witness-collapse retry")

			return nil, false
		case <-time.After(tieBreakerCollapseRetryDelay):
		}
	}

	// All retries exhausted — surface a 503 envelope so CSI /
	// operator tooling can distinguish "transient race, retry me"
	// from a true 404. Pre-Bug-359 we surfaced this race as a bare
	// "not found" 404 which conflated the witness-collapse window
	// with a real missing-RD or missing-pool error.
	writeError(w, http.StatusServiceUnavailable,
		"resource create racing tiebreaker collapse on "+res.NodeName+
			", retry the create after the witness CRD finalizer strip completes")

	return nil, false
}

// createOrPromoteResourceAttempt runs one pass of the Bug-260
// create-or-promote pipeline. Returns (result, ok, retry):
//   - ok==true, retry==false: created or promoted; caller proceeds.
//   - ok==false, retry==false: terminal error already written to w.
//   - ok==false, retry==true: Bug-359 race detected (witness CRD
//     mid-delete); caller should sleep and re-attempt. NOTHING has
//     been written to w in this case.
func (s *Server) createOrPromoteResourceAttempt(w http.ResponseWriter, r *http.Request, res *apiv1.Resource) (*apiv1.Resource, bool, bool) {
	err := s.Store.Resources().Create(r.Context(), res)
	if err == nil {
		return res, true, false
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
	//
	// Bug 260 (P1): the pre-fix gate was gated on `StorPoolName!=""`
	// OR `Flags:[DISKLESS]`. A bare `linstor r c <node> <rd>` (no
	// `--storage-pool`, no flags) on top of an existing TIE_BREAKER
	// hit the gate and surfaced 409 — but upstream
	// CtrlRscCrtApiHelper.resourceToggleDisk treats this shape as
	// "promote the witness". Detect the witness by probing the
	// existing replica's flags; if it carries TIE_BREAKER, allow
	// the promote path and resolve the storage pool from sibling
	// diskful replicas (or the parent RG default) inside
	// promoteDisklessReplica.
	wantsPromote := res.Props["StorPoolName"] != "" ||
		containsResourceFlag(res.Flags, apiv1.ResourceFlagDiskless)
	if errors.Is(err, store.ErrAlreadyExists) && !wantsPromote {
		existing, getErr := s.Store.Resources().Get(r.Context(), res.Name, res.NodeName)
		if getErr == nil && containsResourceFlag(existing.Flags, apiv1.ResourceFlagTieBreaker) {
			wantsPromote = true
		} else if errors.Is(getErr, store.ErrNotFound) {
			// Bug 359 race: Create saw AlreadyExists but Get saw the
			// CRD already gone — the Bug-338 witness collapse finished
			// its Delete + finalizer strip between our Create and Get.
			// Ask the caller to retry; the next Create should succeed
			// as a fresh replica.
			return nil, false, true
		}
	}

	if errors.Is(err, store.ErrAlreadyExists) && wantsPromote {
		promoted, promErr := s.promoteDisklessReplica(r.Context(), res)
		if promErr != nil {
			if errors.Is(promErr, store.ErrNotFound) {
				// Bug 359 race: PromoteDisklessReplica's
				// PatchResourceSpec saw the witness vanish under it
				// between our flags probe and the patch closure —
				// same collapse window described in
				// createOrPromoteResource. Retry the whole sequence;
				// the witness is fully gone now.
				return nil, false, true
			}

			writeStoreError(w, promErr)

			return nil, false, false
		}

		return promoted, true, false
	}

	writeStoreError(w, err)

	return nil, false, false
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
	// Bug 260 (P1): `linstor r c <node> <rd>` (no `--storage-pool`,
	// no flags) on top of an existing TIE_BREAKER lands here with
	// `StorPoolName=""` AND no DISKLESS flag on the target. Upstream
	// CtrlRscCrtApiHelper.resourceToggleDisk treats this shape as
	// "promote the witness to diskful" and resolves the pool from
	// sibling diskful replicas (first match) or the parent RG's
	// `SelectFilter.StoragePool` default. Mirror that here: resolve
	// once, BEFORE the patch closure, so the closure stamps a
	// consistent StorPoolName even across retries.
	wantsToggle := !containsResourceFlag(target.Flags, apiv1.ResourceFlagDiskless)
	resolvedPool := target.Props["StorPoolName"]

	if wantsToggle && resolvedPool == "" {
		resolved, resolveErr := s.resolveTakeoverStorPool(ctx, target.Name, target.NodeName)
		if resolveErr != nil {
			return nil, resolveErr
		}

		resolvedPool = resolved
	}

	wantDiskful := resolvedPool != "" && wantsToggle

	var (
		promoted   apiv1.Resource
		notDiskful bool
	)

	// Bug 205: typed-Patch via PatchResourceSpec — the closure
	// re-runs on every conflict against the live replica, so a
	// racing satellite SetState (Status subresource) or operator-
	// driven flag-toggle on the same Resource converges instead of
	// being silently dropped by the wholesale `Update`. The
	// witness-vs-real-conflict check is re-evaluated each retry
	// against fresh state.
	err := s.Store.Resources().PatchResourceSpec(ctx, target.Name, target.NodeName, func(live *apiv1.Resource) error {
		// Bug 359 third interleaving: the witness row is MID-DELETE
		// (the Bug-338 collapse set its deletionTimestamp; the wire
		// object surfaces that as the DELETE flag) but the finalizer
		// has not been stripped yet, so the apiserver still ACCEPTS
		// spec patches against it. Promoting such a row "succeeds"
		// and is then swallowed wholesale when the deletion finishes
		// — the operator's `r d <peer>` + `r c <ex-witness-node>`
		// relocate silently lost its create (caught live: the r-full
		// Phase-3 r c returned SUCCESS yet the RD ended with a single
		// replica and no witness). Refuse to patch a dying row and
		// route to the same retry the NotFound interleavings use; the
		// next attempt's Create lands fresh once the finalizer strip
		// completes.
		if containsResourceFlag(live.Flags, apiv1.ResourceFlagDelete) {
			return errWitnessMidDelete
		}

		keep, wasDiskless := stripDisklessAndWitnessFlags(live.Flags, wantDiskful)

		if !wasDiskless {
			notDiskful = true

			return nil
		}

		live.Flags = keep

		if wantDiskful {
			if live.Props == nil {
				live.Props = map[string]string{}
			}

			live.Props["StorPoolName"] = resolvedPool
		}

		promoted = *live

		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, errors.Wrapf(err, "lookup existing replica %s.%s", target.Name, target.NodeName)
		}

		return nil, errors.Wrapf(err, "promote diskless %s.%s", target.Name, target.NodeName)
	}

	if notDiskful {
		return nil, errors.Wrapf(store.ErrAlreadyExists,
			"resource %q on node %q already diskful", target.Name, target.NodeName)
	}

	return &promoted, nil
}

// resolveTakeoverStorPool implements the Bug 260 fallback chain for
// `linstor r c <node> <rd>` against an existing TIE_BREAKER witness
// when the wire request omits `--storage-pool`. Mirrors upstream
// CtrlRscCrtApiHelper.resourceToggleDisk's "use the sibling's pool,
// else the RG default" rule.
//
// Returns ("", nil) when nothing matches — promoteDisklessReplica
// then leaves DISKLESS in place (TIE_BREAKER still stripped) so the
// next operator `r c --storage-pool <p>` (or the satellite-driven
// make-available chain) can finish the conversion. Errors are
// wrapped so the caller's `writeStoreError` surfaces an
// operator-actionable envelope.
func (s *Server) resolveTakeoverStorPool(ctx context.Context, rdName, takeoverNode string) (string, error) {
	siblings, listErr := s.Store.Resources().ListByDefinition(ctx, rdName)
	if listErr == nil {
		for i := range siblings {
			if siblings[i].NodeName == takeoverNode {
				continue
			}

			if containsResourceFlag(siblings[i].Flags, apiv1.ResourceFlagDiskless) {
				continue
			}

			if pool := siblings[i].Props["StorPoolName"]; pool != "" {
				return pool, nil
			}
		}
	}

	// Fall through to the parent RG's SelectFilter.StoragePool.
	rd, rdErr := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if rdErr != nil {
		if errors.Is(rdErr, store.ErrNotFound) {
			return "", nil
		}

		return "", errors.Wrapf(rdErr, "lookup RD %q for takeover pool resolution", rdName)
	}

	if rd.ResourceGroupName == "" {
		return "", nil
	}

	rg, rgErr := s.Store.ResourceGroups().Get(ctx, rd.ResourceGroupName)
	if rgErr != nil {
		if errors.Is(rgErr, store.ErrNotFound) {
			return "", nil
		}

		return "", errors.Wrapf(rgErr, "lookup RG %q for takeover pool resolution", rd.ResourceGroupName)
	}

	if pool := rg.SelectFilter.StoragePool; pool != "" {
		return pool, nil
	}

	// Bug 364 (P1): also walk the RG's StoragePoolList default. The
	// linstor-csi driver posts no body to
	// `POST /v1/resource-definitions/{rd}/resources/{node}` and
	// relies on RG-side propagation for the pool name; when the
	// SC sets `linstor.csi.linbit.com/storagePool: <p>`,
	// linstor-csi's RGCreate path lands the value under
	// SelectFilter.StoragePoolList[0] (not .StoragePool). Pre-fix the
	// fresh-create resolution only checked SelectFilter.StoragePool,
	// so an `r c <node> <rd>` against such an RG persisted a Resource
	// with empty Props["StorPoolName"] — the satellite reconciler
	// then had no pool to bind to and the replica wedged at
	// `Provisioning` forever. Matches the existing
	// `resolveGatePoolName` walk shape (the per-pool capacity gate
	// already tolerates StoragePoolList tier-4 for the same reason).
	if len(rg.SelectFilter.StoragePoolList) > 0 {
		return rg.SelectFilter.StoragePoolList[0], nil
	}

	return "", nil
}

// resolveStorPoolForFreshCreate implements the Bug 327 fix for the
// fresh-create code path (no existing replica on the target node):
// when the wire body carries no DISKLESS flag and no StorPoolName,
// stamp `Spec.Props["StorPoolName"]` from the same fallback chain
// upstream LINSTOR's CtrlRscCrtApiHelper uses — sibling diskful
// replica first, parent RG `SelectFilter.StoragePool` second.
//
// Bug 327: a TIE_BREAKER peer on another node MUST NOT leak its
// DISKLESS + TIE_BREAKER flags into a new diskful spawn elsewhere.
// `r c <node>` with no `--diskless` flag creates a diskful replica
// regardless of what the existing peers look like — we only mine
// peers for their StorPoolName, never copy flags.
//
// Best-effort throughout. Any lookup failure is swallowed so the
// downstream `createOrPromoteResource` still runs — its
// `refuseResourceCreateOnUnknownPool` gate already returns a clean
// 404 envelope when no pool can be reached at all, and the worst
// outcome of swallowing is "Resource lands with empty pool" which
// is the pre-fix status quo, not a regression.
func (s *Server) resolveStorPoolForFreshCreate(ctx context.Context, rdName string, res *apiv1.Resource) {
	// Explicit `--diskless` from the operator: honour intent and
	// never stamp a pool. The dispatcher's diskless branch keeps
	// `Spec.Props["StorPoolName"]` empty intentionally.
	if containsResourceFlag(res.Flags, apiv1.ResourceFlagDiskless) {
		return
	}

	// Operator pinned a pool with `--storage-pool` — pre-existing
	// behaviour wins, no resolution needed.
	if res.Props["StorPoolName"] != "" {
		return
	}

	pool, err := s.resolveTakeoverStorPool(ctx, rdName, res.NodeName)
	if err != nil || pool == "" {
		return
	}

	if res.Props == nil {
		res.Props = map[string]string{}
	}

	res.Props["StorPoolName"] = pool
}

// validateResourceCreateShape runs the wire-shape gates that don't
// touch the store: NodeName presence, the `<rd>.<node>` naming
// boundary (CRD metadata.name CEL rule), and the upstream LINSTOR
// flag enum (Bug 167). Hoisted out of `createOneResource` so that
// function stays under the funlen budget after the Issue #45
// per-pool capacity gate landed.
//
// Returns true when the caller may proceed; false when the HTTP
// error has already been written.
func validateResourceCreateShape(w http.ResponseWriter, rdName string, res *apiv1.Resource) bool {
	if res.NodeName == "" {
		writeError(w, http.StatusBadRequest, "node_name is required on every resource create entry")

		return false
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

		return false
	}

	if strings.Contains(rdName, ".") {
		writeError(w, http.StatusBadRequest,
			"resource_definition name must not contain '.': metadata.name must equal <rd>.<node>")

		return false
	}

	// Bug 167: refuse Resource-create entries that carry a flag string
	// outside the documented upstream LINSTOR enum. Pre-fix the phantom
	// flag persisted onto the CRD; the satellite reconciler then had to
	// guess whether the typo was a no-op or a misspelled `DISKLESS`.
	flagErr := validateResourceFlags(res.Flags)
	if flagErr != nil {
		writeError(w, http.StatusBadRequest, flagErr.Error())

		return false
	}

	return true
}

// rejectResourceCreateIfPoolFull is the Issue #45 capacity gate on
// the real linstor-csi CreateVolume path. Mirrors the parallel
// `rejectAutoplaceIfExceedsCapacityGate` on /autoplace and
// `rejectIfExceedsOversubGate` on /spawn so a CreateVolume against
// a now-full pool fails-fast with a structured 409 envelope BEFORE
// the Resource is persisted (no orphan CRD on the reject path).
//
// Why this gate is per-pool (not computeSizeInfo-shaped):
//   - `/autoplace` and `/spawn` are placer-driven: they accept a
//     filter and the placer picks pools. `computeSizeInfo` returns
//     the cluster-wide worst-case-of-top-N cap so any pool the placer
//     could pick is covered.
//   - This handler routes through `POST /v1/resource-definitions/
//     {rd}/resources/{node}` (the golinstor `Resources.Create`
//     endpoint linstor-csi's `manual` scheduler uses when the SC sets
//     `nodeList`). The caller has already named the EXACT (node,
//     pool) target, so the cluster-wide cap would mask a full target
//     pool behind sibling pools on other nodes. A 13 GiB lvm-thin on
//     worker-1 at 100% used while worker-2's lvm-thin is empty MUST
//     refuse `r c worker-1 <rd>` even though the cluster-wide cap
//     remains 13 GiB.
//
// Skipped when:
//   - Resource is DISKLESS / TIE_BREAKER (no backing storage needed).
//   - No pool name could be resolved (consistent with
//     `refuseResourceCreateOnUnknownPool` — diskless fallback path).
//   - RD has no VolumeDefinitions yet (definitions-only create; the
//     subsequent VD POST will get the gate next time around).
//
// Returns true when the caller may proceed; false when the HTTP
// error has already been written.
func (s *Server) rejectResourceCreateIfPoolFull(w http.ResponseWriter, r *http.Request, rdName string, res *apiv1.Resource) bool {
	// Diskless / tiebreaker witnesses don't allocate backing storage.
	if containsResourceFlag(res.Flags, apiv1.ResourceFlagDiskless) ||
		containsResourceFlag(res.Flags, apiv1.ResourceFlagTieBreaker) {
		return true
	}

	poolName := s.resolveGatePoolName(r.Context(), rdName, res)
	if poolName == "" {
		// Pool unresolved — `refuseResourceCreateOnUnknownPool` already
		// returns true in this case (diskless fallback), and the
		// satellite reconciler's `disk none;` path takes over. The
		// capacity gate is specifically about a NAMED pool that's
		// full; "no pool at all" is a different code path.
		return true
	}

	pool, err := s.Store.StoragePools().Get(r.Context(), res.NodeName, poolName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// `refuseResourceCreateOnUnknownPool` ran first and would
			// have returned the 404 already. Defensive: if we got here
			// the SP must exist on the target node, so this branch is
			// a TOCTOU residual — let the post-write Bug 145 re-check
			// handle it.
			return true
		}

		writeStoreError(w, err)

		return false
	}

	requiredKib, err := s.sumRDVolumeDefinitionsKib(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return false
	}

	if requiredKib <= 0 {
		// No VDs yet (e.g. a definitions-only create racing ahead of
		// the VD POST). Skip the gate — there's nothing to size
		// against. Mirrors `rejectAutoplaceIfExceedsCapacityGate` and
		// `rejectIfExceedsOversubGate` semantics.
		return true
	}

	if pool.FreeCapacity >= requiredKib {
		return true
	}

	// Reuse the same RetCode + envelope shape as the /autoplace gate
	// (PR #47) so operators and tools that classify replies by the
	// `ret_code` + sub-code pair handle both gates uniformly.
	filter := &apiv1.AutoSelectFilter{
		StoragePool:  poolName,
		NodeNameList: []string{res.NodeName},
		PlaceCount:   1,
	}

	writeAutoplaceShortfall(w, filter, "", &placer.CapacityShortfallError{
		RequiredKib: requiredKib,
		MaxFreeKib:  pool.FreeCapacity,
	})

	return false
}

// resolveGatePoolName returns the StoragePool name the Issue #45
// capacity gate should look up FreeCapacity against. Mirrors the
// upstream LINSTOR fallback chain
// (`CtrlRscCrtApiHelper.resolveStorPoolName`) but tolerates a wider
// set of input shapes since linstor-csi's `CreateVolume` posts an
// EMPTY body to `POST /v1/resource-definitions/{rd}/resources/{node}`
// and relies on RG-level propagation for the pool name. The four
// tiers, in priority order:
//
//  1. `res.Props["StorPoolName"]` — explicit `--storage-pool` from
//     the operator or the CSI client.
//  2. `rd.Props["StorPoolName"]` — RD-level sticky default (Scenario
//     4.W15).
//  3. `rg.SelectFilter.StoragePool` — RG single-pool default
//     (`linstor rg c --storage-pool <p>`).
//  4. `rg.SelectFilter.StoragePoolList[0]` — RG list-pool default,
//     which is the shape linstor-csi posts when the SC sets
//     `linstor.csi.linbit.com/storagePool: <p>` (the per-pool
//     fallback `resolveStorPoolForFreshCreate` already covers tiers
//     1-3 by stamping res.Props, but it does NOT cover tier 4 — by
//     design, since the placer was the one to pick a specific pool
//     out of the list, and the per-resource Props gets stamped at
//     placer time. The capacity gate has to repeat tier 4 here so a
//     CSI request with no body and only an RG StoragePoolList is
//     still gated.
//
// Returns "" when no tier matches — the gate then skips with a
// no-op (consistent with `refuseResourceCreateOnUnknownPool`'s
// "no pool, no gate" semantic on the diskless fallback path). Best-
// effort throughout: any store lookup error is swallowed so a
// transient k8s read doesn't escalate a CreateVolume into a 500.
func (s *Server) resolveGatePoolName(ctx context.Context, rdName string, res *apiv1.Resource) string {
	if pool := res.Props["StorPoolName"]; pool != "" {
		return pool
	}

	rd, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return ""
	}

	if pool := rd.Props["StorPoolName"]; pool != "" {
		return pool
	}

	if rd.ResourceGroupName == "" {
		return ""
	}

	rg, err := s.Store.ResourceGroups().Get(ctx, rd.ResourceGroupName)
	if err != nil {
		return ""
	}

	if pool := rg.SelectFilter.StoragePool; pool != "" {
		return pool
	}

	if len(rg.SelectFilter.StoragePoolList) > 0 {
		return rg.SelectFilter.StoragePoolList[0]
	}

	return ""
}

// sumRDVolumeDefinitionsKib returns the largest VolumeDefinition's
// SizeKib on the named RD. Every volume of an RD provisions against
// the same pool (upstream LINSTOR contract), so the per-pool
// capacity gate must clear the biggest of them. Returns 0 — no
// filter — when the RD has no VDs yet. Mirrors
// `Placer.requiredKib` exactly so the gate semantics agree with the
// placer's own per-pool check on the autoplace path.
func (s *Server) sumRDVolumeDefinitionsKib(ctx context.Context, rdName string) (int64, error) {
	vds, err := s.Store.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		return 0, err //nolint:wrapcheck // handler wraps via writeStoreError
	}

	var maxKib int64

	for i := range vds {
		if vds[i].SizeKib > maxKib {
			maxKib = vds[i].SizeKib
		}
	}

	return maxKib, nil
}

// stripDisklessAndWitnessFlags walks `flags` and produces the post-promote
// flag list: TIE_BREAKER is always stripped; DISKLESS is stripped only
// when the caller asked for diskful. Returns the kept slice and whether
// the original flag set contained a witness (DISKLESS or TIE_BREAKER) —
// promoteDisklessReplica needs the latter to surface ErrAlreadyExists on
// real diskful replicas. Pulled out of promoteDisklessReplica to keep
// that function under the funlen budget after the Bug 205 Patch refactor.
func stripDisklessAndWitnessFlags(flags []string, wantDiskful bool) ([]string, bool) {
	// Delegate to the canonical witness→diskful flag transition shared
	// with the autoplace path (corner-D2b). Keeping one implementation
	// guarantees `r c <node> --storage-pool` (this path) and
	// `r c --auto-place +1` (the placer's upgrade-over-witness path)
	// strip exactly the same flags.
	return apiv1.PromoteWitnessFlags(flags, wantDiskful)
}

// containsResourceFlag is a small helper so the create/promote
// branching reads at the call site without an inline loop.
func containsResourceFlag(flags []string, want string) bool {
	return slices.Contains(flags, want)
}

// normalizeDisklessFlag folds the wire flag DRBD_DISKLESS into the
// canonical DISKLESS on a freshly-decoded resource-create body. The
// modern `linstor r c --drbd-diskless` / `--nvme-initiator` CLI flags
// emit DRBD_DISKLESS; the deprecated `--diskless` alias emits the
// canonical DISKLESS (verified via `linstor --curl` against the
// upstream oracle, client 1.27.1). blockstor's diskless-detection
// surface keys exclusively on apiv1.ResourceFlagDiskless == "DISKLESS",
// so a body carrying only DRBD_DISKLESS would otherwise be treated as
// a diskful create. Replace every DRBD_DISKLESS occurrence with
// DISKLESS in place, de-duplicating if both spellings are present so a
// later exact-match flag walk doesn't see a phantom duplicate. No-op
// when the flag is absent (the common diskful / explicit-DISKLESS
// case), so the hot path pays only a single slice scan.
func normalizeDisklessFlag(res *apiv1.Resource) {
	if !slices.Contains(res.Flags, rscFlagDrbdDiskless) {
		return
	}

	out := res.Flags[:0]
	seenDiskless := false

	for _, flag := range res.Flags {
		isDiskless := flag == rscFlagDrbdDiskless || flag == apiv1.ResourceFlagDiskless
		if !isDiskless {
			out = append(out, flag)

			continue
		}

		if seenDiskless {
			continue
		}

		seenDiskless = true

		out = append(out, apiv1.ResourceFlagDiskless)
	}

	res.Flags = out
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
	//
	// Bug 204b shape: typed-Patch with retry-on-conflict; the "only
	// if unset" guard re-runs against the live RD on every retry so
	// a concurrent reconciler write can't surface a 409 to the
	// make-available caller.
	if len(req.LayerList) > 0 && len(rd.LayerStack) == 0 {
		err = s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(r.Context(), rdName,
			func(live *apiv1.ResourceDefinition) error {
				if len(live.LayerStack) == 0 {
					live.LayerStack = append([]string(nil), req.LayerList...)
				}

				return nil
			})
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
		writeDecodeError(w, err)

		return req, false
	}

	// Empty body is the documented opt-in default (diskful:false) —
	// don't tip it into the typed-decode path, just return the zero req.
	if len(bytes.TrimSpace(body)) == 0 {
		return req, true
	}

	// Bug 158/161: typed-envelope decode + DisallowUnknownFields.
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()

	err = dec.Decode(&req)
	if err != nil {
		writeDecodeError(w, err)

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
	// Bug 205: typed-Patch via PatchResourceSpec — the closure
	// re-runs on every conflict against the live replica, so a
	// racing satellite SetState (Status subresource) or operator-
	// driven flag-toggle converges with the make-available promote
	// instead of being silently dropped by the wholesale `Update`.
	err := s.Store.Resources().PatchResourceSpec(ctx, existing.Name, existing.NodeName, func(live *apiv1.Resource) error {
		changed := false
		keep := live.Flags[:0]

		for _, flag := range live.Flags {
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
			// No flag change — Spec stays identical to the live
			// value and PatchResourceSpec ends up as a no-op patch.
			return nil
		}

		live.Flags = keep

		return nil
	})
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
//
// Bug 161: uses `json.NewDecoder` + `DisallowUnknownFields()` (not the
// permissive `json.Unmarshal`) so a stray top-level `props` (instead
// of `resource.props`) — the v5-report repro — is refused at the wire
// boundary rather than slipping past the Bug 117/118 SP-existence
// gate. The decoder's unknown-field error propagates verbatim so the
// caller's `writeDecodeError` (Bug 158) maps it to a 400 + LINSTOR
// envelope naming the offending key.
func decodeResourceCreateBody(body []byte) ([]apiv1.ResourceCreate, error) {
	trimmed := bytes.TrimLeft(body, " \t\r\n")

	if len(trimmed) > 0 && trimmed[0] == '[' {
		var envelopes []apiv1.ResourceCreate

		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()

		err := dec.Decode(&envelopes)
		if err != nil {
			//nolint:wrapcheck // raw decoder error preserved so writeDecodeError can route by type
			return nil, err
		}

		return envelopes, nil
	}

	var single apiv1.ResourceCreate

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()

	err := dec.Decode(&single)
	if err != nil {
		//nolint:wrapcheck // raw decoder error preserved so writeDecodeError can route by type
		return nil, err
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
//
// Bug B.1 (hunt-v3): `linstor r d --keep-tiebreaker <diskful> <rd>`
// must NOT reap the auto-managed TIE_BREAKER witness in the same
// reconcile that observes diskful drop. Upstream LINSTOR's CLI help
// states "Keeps the tiebreaker instead of accidentally deleting it";
// without the override the RD reconciler hits the Bug-338 carve-out
// (diskful=1 + witness=1, no non-witness diskless → collapse) and
// removes the witness right after the `r d` completes. We stamp a
// short-lived `KeepTiebreakerUntilAnnotation` so the reconciler's
// `shouldKeepExistingWitness` reads it and short-circuits to true
// while the deadline is in the future. The stamp lands BEFORE the
// Delete commits, mirroring the suppression annotation's
// already-proven order: a Reconcile woken by the Delete event will
// observe the annotation by the time it lists Resources.
func (s *Server) handleResourceDelete(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")
	keepTiebreaker := queryFlag(r, "keep_tiebreaker")

	if keepTiebreaker {
		// Best-effort. Failure to stamp doesn't block the
		// operator-requested delete; the worst case without the
		// annotation is "the witness is collapsed within ~5s of the
		// r d" — annoying, but recoverable by a follow-up `r c` to
		// re-create the witness manually. NotFound on the parent RD
		// is folded into nil inside stampKeepTiebreaker (concurrent
		// rd-delete cascade) so the same call path is safe here.
		_ = s.stampKeepTiebreaker(r.Context(), rdName)
	}

	// Look up the Resource before any destructive action. The flag
	// inspection drives the legacy tiebreaker-suppression stamp on
	// the witness path.
	existing, getErr := s.Store.Resources().Get(r.Context(), rdName, node)
	if getErr != nil {
		if errors.Is(getErr, store.ErrNotFound) {
			// Bug 56 idempotent envelope, Bug 67 no-bump on no-op,
			// Bug 124 informer-cache drain — unchanged semantics
			// from the pre-Bug-342 single-Delete-call path.
			s.waitForResourceDeletionVisible(r.Context(), rdName, node)

			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: warnRscNotFound,
				Message: "resource already absent: " + rdName + " on " + node,
			}})

			return
		}

		writeStoreError(w, getErr)

		return
	}

	// U130 guard: refuse removing the LAST UpToDate diskful replica
	// while a peer is still mid-sync (SyncTarget/Inconsistent). Dropping
	// the only source strands the syncing peer and leaves the resource
	// with no UpToDate backing storage. Runs BEFORE any destructive
	// action; the legacy tiebreaker-suppression stamp below only fires
	// on the witness path, which the guard skips (diskful-only). An
	// explicit `?force=true` overrides. See
	// resource_delete_last_uptodate_u130.go.
	if !s.refuseLastUpToDateDiskfulMidSyncDelete(r.Context(), w, rdName, &existing, queryFlag(r, "force")) {
		return
	}

	// `r d` physically removes the replica for ALL replica kinds —
	// diskful, diskless, and tiebreaker — matching upstream LINSTOR
	// `linstor resource delete`. The satellite resource controller's
	// finalizer-driven handleDelete tears DRBD down on the node
	// (drbdadm down), surviving siblings run del-peer + forget-peer
	// to reclaim the bitmap slot (tearDownRemovedPeers, woken by
	// bumpPeerChangedOnSiblings below), DeleteVolume frees the
	// backing storage (ZVOL/LV/file — diskful has real backing,
	// diskless/tiebreaker resolve to an empty pool and skip it), then
	// the satellite strips its finalizer and the CRD is finalised.
	//
	// History (Bug 342): a previous revision rerouted a diskful `r d`
	// to a toggle-disk-to-diskless (CRD survived with a DISKLESS
	// flag, never deleted) to mask a relocate wedge — a fresh node-id
	// handshaking against peers that had already forgotten the slot
	// landed in Connecting/StandAlone. That wedge is now closed by
	// the node-id-occupied invariant (the departed replica's id stays
	// occupied via the survivors' observed PeerDRBDNodeID until the
	// kernel confirms forget, so a subsequent relocate gets a fresh
	// id), so the toggle mask is obsolete and physical delete is safe
	// again. The `r td` / toggle-disk command path
	// (handleResourceToggleDiskToDiskless) is unaffected — it still
	// toggles; only `r d` deletes.
	//
	// TIE_BREAKER still gets the auto-tiebreaker-suppression stamp so
	// the RD-level reconciler doesn't immediately re-stamp a fresh
	// witness the next time `ensureTiebreaker` fires.
	s.maybeStampTiebreakerSuppressionOnDelete(r.Context(), rdName, &existing)

	err := s.Store.Resources().Delete(r.Context(), rdName, node)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Race between Get above and Delete here (concurrent
			// finalizer-strip or cascade); fold to the same
			// idempotent envelope as the initial NotFound branch.
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

	// Bug 67: notify surviving sibling Resources of the peer change
	// so satellite reconcilers re-derive their peer set without the
	// dropped replica. Order ("delete first, bump second") ensures a
	// reconciler woken by the annotation observes post-delete state.
	s.bumpPeerChangedOnSiblings(r.Context(), rdName, node)

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: apiCallRcInfo | apiCallRcRscDeleted,
		Message: "resource deleted: " + rdName + " on " + node,
	}})
}

// maybeStampTiebreakerSuppressionOnDelete stamps the
// auto-tiebreaker-suppression annotation on the parent RD when the
// replica being deleted is a TIE_BREAKER, so the RD-level reconciler
// doesn't immediately re-stamp a fresh witness the next time
// `ensureTiebreaker` fires.
//
// Best-effort: a stamp failure must not block the operator-requested
// delete; the worst case without the annotation is "auto-witness comes
// back in ~5s" — annoying, not data-loss. Extracted from
// handleResourceDelete to keep that handler under the funlen budget once
// the U130 mid-sync guard was added.
func (s *Server) maybeStampTiebreakerSuppressionOnDelete(ctx context.Context, rdName string, existing *apiv1.Resource) {
	if !slices.Contains(existing.Flags, apiv1.ResourceFlagTieBreaker) {
		return
	}

	_ = s.stampTiebreakerSuppression(ctx, rdName)
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

		// Bug 205: typed-Patch via PatchResourceSpec — re-fetches the
		// live sibling on every conflict so a racing peer-Delete or
		// peer-modify on the same row converges with the
		// PeerChanged stamp instead of being silently dropped by the
		// wholesale `Update` snapshot.
		//
		// Patch is still best-effort. A concurrent satellite SetState
		// using SSA on Status doesn't race this Spec/metadata path
		// (different field owners + different subresources), so the
		// typical failure mode here is a NotFound from a same-instant
		// peer Delete (already-fine outcome) — swallowed identical to
		// the wholesale `Update` it replaces.
		_ = s.Store.Resources().PatchResourceSpec(ctx, sib.Name, sib.NodeName, func(live *apiv1.Resource) error {
			if live.Annotations == nil {
				live.Annotations = map[string]string{}
			}

			live.Annotations[apiv1.PeerChangedAnnotation] = stamp

			return nil
		})
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
	deadline := time.Now().Add(autoTiebreakerSuppressionWindow).UTC().Format(time.RFC3339)

	// Bug 205: typed-Patch via PatchResourceDefinitionSpec — re-fetches
	// the live RD on every conflict so a racing RD-modify or r-conn
	// upsert on the same RD converges with the tiebreaker-suppression
	// annotation instead of being lost by a stale-wire-snapshot replay.
	// NotFound is still treated as "fine, RD already gone" — the
	// caller (best-effort cascade) doesn't care.
	err := s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName, func(rd *apiv1.ResourceDefinition) error {
		if rd.Annotations == nil {
			rd.Annotations = map[string]string{}
		}

		rd.Annotations[AutoTiebreakerSuppressedUntilAnnotation] = deadline

		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}

	return err //nolint:wrapcheck // best-effort, caller swallows
}

// stampKeepTiebreaker writes the KeepTiebreakerUntilAnnotation onto
// the parent RD with a `now + keepTiebreakerOverrideWindow` deadline.
// The RD-side reconciler reads the annotation in its keep-branch and
// preserves an existing TIE_BREAKER witness across shapes the Bug-338
// carve-out would otherwise collapse (e.g. 1 diskful + 1 witness + 0
// non-witness diskless).
//
// Idempotent: a fresh stamp always wins (later operator intent
// overrides earlier). NotFound on the parent RD is swallowed — a
// concurrent RD-delete cascade is the most common reason and the
// caller doesn't care.
func (s *Server) stampKeepTiebreaker(ctx context.Context, rdName string) error {
	deadline := time.Now().Add(keepTiebreakerOverrideWindow).UTC().Format(time.RFC3339)

	err := s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName, func(rd *apiv1.ResourceDefinition) error {
		if rd.Annotations == nil {
			rd.Annotations = map[string]string{}
		}

		rd.Annotations[KeepTiebreakerUntilAnnotation] = deadline

		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}

	return err //nolint:wrapcheck // best-effort, caller swallows
}
