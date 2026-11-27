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
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/propwhitelist"
)

// registerVolumesPerResource wires the per-Resource Volume routes
// upstream LINBIT linstor-server defines under
// `@Path("v1/resource-definitions/{rscName}/resources/{nodeName}/volumes")`
// (controller's Volumes.java). Bug 220 + Bug 221: these were entirely
// missing from blockstor's REST surface, so python-linstor's
// `linstor volume set-property` (PUT) and golinstor's
// `ResourceService.GetVolumes / GetVolume / ModifyVolume` (GET list /
// GET single / PUT) all 404'd.
//
// Wire shapes mirror upstream Volumes.java:
//
//	GET    .../volumes              → `[]apiv1.Volume`           (list)
//	GET    .../volumes/{vlmNr}      → `apiv1.Volume`             (single)
//	PUT    .../volumes/{vlmNr}      → []apiv1.APICallRc envelope (modify-props)
func (s *Server) registerVolumesPerResource(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resources/{node}/volumes",
		s.requireStore(s.handleVolumesPerResourceList))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resources/{node}/volumes/{vlmNr}",
		s.requireStore(s.handleVolumesPerResourceGet))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{node}/volumes/{vlmNr}",
		s.requireStore(s.handleVolumesPerResourceModify))
}

// handleVolumesPerResourceList answers
// `GET /v1/resource-definitions/{rd}/resources/{node}/volumes` — the
// per-Resource Volume list golinstor's `ResourceService.GetVolumes(rd,
// node)` polls. Returns a bare JSON array of `apiv1.Volume`, NOT a
// wrapping envelope (golinstor decodes a slice directly).
//
// Defensive non-nil slice at the wire edge so a Resource with no
// satellite-reported Volumes still surfaces as `[]` rather than
// `null` — the python CLI's `responses.py` walks the slice
// unconditionally and crashes on a nil.
func (s *Server) handleVolumesPerResourceList(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	// Replica-create hot path: `linstor v l` / golinstor volume reads
	// land right after autoplace / make-available wrote the replica,
	// while the local informer cache may still trail the write — see
	// pkg/rest/cache_retry.go.
	res, err := getResourceWithCacheRetry(r.Context(), s.Store, rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	s.forgetStaleReplica(r.Context(), &res)

	// Every volume definition is listed, built or not, as LINSTOR does
	// (see ensureVolumesForView). REST 1.28 added the `props` filter
	// here, on the volumes' own props.
	propFilter := propsQuery(r)
	listed := ensureVolumesForView(res.Volumes, s.placeholderPool(r.Context(), &res), s.definitionVolumeSizes(r.Context(), rdName))
	vols := make([]apiv1.Volume, 0, len(listed))

	for i := range listed {
		if matchProps(propFilter, listed[i].Props) {
			vols = append(vols, listed[i])
		}
	}

	writeJSON(w, http.StatusOK, vols)
}

// handleVolumesPerResourceGet answers
// `GET /v1/resource-definitions/{rd}/resources/{node}/volumes/{vlmNr}`
// — the per-Volume single GET golinstor's `ResourceService.GetVolume`
// calls. Returns a bare `apiv1.Volume` on hit, the typed 404 envelope
// on miss (either the (rd, node) Resource is missing or the vlmNr has
// no row).
func (s *Server) handleVolumesPerResourceGet(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	vlmNr, err := parseVolNum(r.PathValue("vlmNr"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// Same replica-create hot path as the list sibling above.
	res, err := getResourceWithCacheRetry(r.Context(), s.Store, rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	s.forgetStaleReplica(r.Context(), &res)

	if vol, ok := s.awaitReportedVolume(r.Context(), &res, vlmNr); ok {
		writeJSON(w, http.StatusOK, vol)

		return
	}

	// A volume the satellite has still not built answers without a
	// device path, as LINSTOR's does, rather than 404: linstor-csi's
	// attach then reports "could not determine device path" and
	// retries, instead of failing on a volume that does exist.
	if vol, ok := placeholderVolume(&res, vlmNr, s.placeholderPool(r.Context(), &res), s.definitionVolumeSizes(r.Context(), rdName)); ok {
		writeJSON(w, http.StatusOK, vol)

		return
	}

	writeError(w, http.StatusNotFound, fmt.Sprintf(
		"volume %d of resource %q on node %q not found", vlmNr, rdName, node))
}

// handleVolumesPerResourceModify applies a GenericPropsModify
// (override_props / delete_props / delete_namespaces) onto the
// matching `Resource.Spec.Volumes[i].Props` for (rd, node, vlmNr).
// Mirrors upstream's `CtrlVlmModifyApiCallHandler` — the per-Volume
// rung of the property hierarchy lives on the Resource CRD inline.
//
// Bug 204b shape: routes through `PatchResourceSpec` so the override
// / delete delta is re-applied to the freshly-fetched Resource on
// every conflict retry; a racing autoplace / toggle-disk / r-modify
// won't silently clobber the per-Volume prop merge.
//
// Returns 404 (typed envelope) when the Resource exists but no
// Volume row carries the requested VolumeNumber — silently no-op'ing
// would surface as a successful set-property whose value never lands,
// which is the kind of bug operators spend hours chasing.
func (s *Server) handleVolumesPerResourceModify(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	vlmNr, err := parseVolNum(r.PathValue("vlmNr"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// Decode the full Resource-modify shape so DisallowUnknownFields
	// tolerates the read-side wire shape some legacy callers
	// round-trip (Bug 163 pattern). Only the embedded
	// GenericPropsModify drives the merge; everything else is
	// informational. Upstream's `VolumeModify` JSON shape is the
	// bare GenericPropsModify; ResourceModify's embedded copy is
	// wire-compatible (same `override_props` / `delete_props` /
	// `delete_namespaces` JSON keys).
	var patch apiv1.GenericPropsModify

	if !decodeJSON(w, r, &patch) {
		return
	}

	if refuseUnwhitelistedProps(w, propwhitelist.Volume, patch.OverrideProps) {
		return
	}

	// Sentinel set inside the mutate closure so the post-Patch
	// branch knows whether the targeted VolumeNumber actually exists
	// on the live Resource. PatchResourceSpec re-runs the closure on
	// every conflict; the last successful run wins.
	var volumeMissing bool

	patchErr := s.Store.Resources().PatchResourceSpec(r.Context(), rdName, node, func(res *apiv1.Resource) error {
		idx := findVolumeIndexByNumber(res.Volumes, vlmNr)
		if idx < 0 {
			volumeMissing = true

			return nil
		}

		volumeMissing = false

		mergeVolumePropsPatch(&res.Volumes[idx], &patch)

		return nil
	})
	if patchErr != nil {
		writeStoreError(w, patchErr)

		return
	}

	if volumeMissing {
		writeError(w, http.StatusNotFound, fmt.Sprintf(
			"volume %d of resource %q on node %q not found", vlmNr, rdName, node))

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: fmt.Sprintf("volume modified: %s vlm=%d on node %s", rdName, vlmNr, node),
	}})
}

// findVolumeIndexByNumber returns the index of the Volume with the
// matching VolumeNumber inside the slice, or -1 when no row matches.
// Pulled out so handleVolumesPerResourceModify stays under gocognit.
func findVolumeIndexByNumber(vols []apiv1.Volume, vlmNr int32) int {
	for i := range vols {
		if vols[i].VolumeNumber == vlmNr {
			return i
		}
	}

	return -1
}

// mergeVolumePropsPatch overlays a GenericPropsModify delta onto the
// Volume's Props bag in place. Matches the per-Resource and per-VD
// modify handlers' merge ordering: override → delete keys → delete
// namespaces. Allocates a Props map only when the patch actually has
// something to add or remove.
func mergeVolumePropsPatch(vol *apiv1.Volume, patch *apiv1.GenericPropsModify) {
	if vol.Props == nil && (len(patch.OverrideProps) > 0 || len(patch.DeleteProps) > 0 || len(patch.DeleteNamespace) > 0) {
		vol.Props = map[string]string{}
	}

	// I1: empty override value deletes the key (set-property KEY "").
	applyPropsModify(vol.Props, patch.OverrideProps, patch.DeleteProps)

	deletePropNamespaces(vol.Props, patch.DeleteNamespace)
}

// placeholderVolume is volume vlmNr of the replica as the volume list
// answers it (ensureVolumesForView), and whether that list has it.
func placeholderVolume(res *apiv1.Resource, vlmNr int32, poolFor func(int32) string, vdSizes map[int32]int64) (apiv1.Volume, bool) {
	vols := ensureVolumesForView(res.Volumes, poolFor, vdSizes)

	idx := findVolumeIndexByNumber(vols, vlmNr)
	if idx < 0 {
		return apiv1.Volume{}, false
	}

	return vols[idx], true
}

// volumeReportBudget bounds how long a volume read waits for the satellite
// to report a volume its definition has. It stays well below the
// external-attacher's timeout.
const volumeReportBudget = 10 * time.Second

// awaitReportedVolume answers volume vlmNr of the replica, waiting for its
// satellite to report it with a device path when the replica is new:
// linstor-csi reads the volume's device path right after make-available
// and fails the attach on a missing volume or an empty path, and LINSTOR
// knows both from the moment the replica exists. The observer reports the
// disk state before the apply pass stamps the path, so the volume alone is
// not enough. Once the budget runs out the volume is answered as it
// stands. A volume number the definition does not have, and a replica
// being deleted, are answered at once.
func (s *Server) awaitReportedVolume(ctx context.Context, res *apiv1.Resource, vlmNr int32) (apiv1.Volume, bool) {
	deadline := time.Now().Add(volumeReportBudget)

	for {
		idx := findVolumeIndexByNumber(res.Volumes, vlmNr)
		if idx >= 0 && res.Volumes[idx].DevicePath != "" {
			return res.Volumes[idx], true
		}

		if !s.awaitVolumeReport(ctx, res, vlmNr, deadline) {
			if idx >= 0 {
				return res.Volumes[idx], true
			}

			return apiv1.Volume{}, false
		}
	}
}

// awaitVolumeReport waits one poll interval and re-reads the replica into
// res. It reports false, leaving res as it was, when there is nothing left
// to wait for: the budget ran out, the replica is being deleted, or its
// definition has no such volume.
func (s *Server) awaitVolumeReport(ctx context.Context, res *apiv1.Resource, vlmNr int32, deadline time.Time) bool {
	if slices.Contains(res.Flags, apiv1.ResourceFlagDelete) || time.Now().After(deadline) {
		return false
	}

	_, err := s.Store.VolumeDefinitions().Get(ctx, res.Name, vlmNr)
	if err != nil {
		return false
	}

	select {
	case <-ctx.Done():
		return false
	case <-time.After(cacheConvergePollInterval):
	}

	next, err := s.Store.Resources().Get(ctx, res.Name, res.NodeName)
	if err != nil {
		return false
	}

	s.forgetStaleReplica(ctx, &next)
	*res = next

	return true
}
