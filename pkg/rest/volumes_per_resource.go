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
	"fmt"
	"maps"
	"net/http"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
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

	res, err := s.Store.Resources().Get(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	vols := res.Volumes
	if vols == nil {
		vols = []apiv1.Volume{}
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

	res, err := s.Store.Resources().Get(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	for i := range res.Volumes {
		if res.Volumes[i].VolumeNumber == vlmNr {
			writeJSON(w, http.StatusOK, res.Volumes[i])

			return
		}
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

	maps.Copy(vol.Props, patch.OverrideProps)

	for _, k := range patch.DeleteProps {
		delete(vol.Props, k)
	}

	for _, ns := range patch.DeleteNamespace {
		for k := range vol.Props {
			if k == ns || strings.HasPrefix(k, ns+"/") {
				delete(vol.Props, k)
			}
		}
	}
}
