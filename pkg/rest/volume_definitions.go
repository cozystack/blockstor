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
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strconv"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// volumeDefinitionModifyBody is the shape upstream golinstor sends on
// `PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}` — driven
// by `linstor vd set-size`, `linstor vd set-property`, and the CSI
// ControllerExpandVolume path. Top-level fields are a modify delta,
// not the full VD spec.
//
// SizeKib is a pointer so we can distinguish "client omitted size_kib"
// (preserve existing) from "client sent size_kib=0" (explicit zero).
// Wholesale Decode(&VolumeDefinition) would conflate the two and the
// satellite reconciler's `vol.GetSizeKib() > status.UsableKib` grow
// branch would never fire after a no-op props-only modify because
// SizeKib was silently zeroed. See Bug 36 (4.6 audit).
type volumeDefinitionModifyBody struct {
	OverrideProps    map[string]string `json:"override_props,omitempty"`
	DeleteProps      []string          `json:"delete_props,omitempty"`
	DeleteNamespaces []string          `json:"delete_namespaces,omitempty"`
	SizeKib          *int64            `json:"size_kib,omitempty"`
	Flags            []string          `json:"flags,omitempty"`

	// Props mirrors the legacy callers that PUT the full VolumeDefinition
	// shape (matches the read-side wire field). Treated as an override
	// overlay on the existing Props map — equivalent to OverrideProps.
	Props map[string]string `json:"props,omitempty"`
}

// registerVolumeDefinitions wires
// /v1/resource-definitions/{rd}/volume-definitions[/{vn}] CRUD.
func (s *Server) registerVolumeDefinitions(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/volume-definitions",
		s.requireStore(s.handleVDView))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/volume-definitions",
		s.requireStore(s.handleVDList))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/volume-definitions",
		s.requireStore(s.handleVDCreate))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/volume-definitions/{vn}",
		s.requireStore(s.handleVDGet))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}",
		s.requireStore(s.handleVDUpdate))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}/volume-definitions/{vn}",
		s.requireStore(s.handleVDDelete))
}

// handleVDView is the cluster-wide aggregate for
// `linstor vd l` / golinstor's VolumeDefinitions.GetAll(). Returns
// upstream LINSTOR's shape: an array of ResourceDefinitionWithVolumeDefinition
// (each RD wrapping its inline volume_definitions array). The Python
// linstor CLI iterates `lstmsg.resource_definitions` → for each rd:
// `rsc_dfn.volume_definitions` — a flat per-VD entry would render
// the table empty because the attribute path doesn't match.
//
// Empty-VD RDs are dropped from the response so the CLI's
// per-row groupby doesn't show RDs without any defined volumes.
func (s *Server) handleVDView(w http.ResponseWriter, r *http.Request) {
	rds, err := s.Store.ResourceDefinitions().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	type rdWithVDs struct {
		Name              string                   `json:"name"`
		ExternalName      string                   `json:"external_name,omitempty"`
		ResourceGroupName string                   `json:"resource_group_name,omitempty"`
		Flags             []string                 `json:"flags,omitempty"`
		Props             map[string]string        `json:"props,omitempty"`
		VolumeDefinitions []apiv1.VolumeDefinition `json:"volume_definitions"`
	}

	out := make([]rdWithVDs, 0, len(rds))

	for i := range rds {
		vds, listErr := s.Store.VolumeDefinitions().List(r.Context(), rds[i].Name)
		if listErr != nil {
			writeError(w, http.StatusInternalServerError, listErr.Error())

			return
		}

		if len(vds) == 0 {
			continue
		}

		out = append(out, rdWithVDs{
			Name:              rds[i].Name,
			ExternalName:      rds[i].ExternalName,
			ResourceGroupName: rds[i].ResourceGroupName,
			Flags:             rds[i].Flags,
			Props:             rds[i].Props,
			VolumeDefinitions: vds,
		})
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleVDList(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	// Verify the parent RD exists so a missing RD is 404, not 200 with [].
	// k8s store does this internally; in-memory does not, so we do it here.
	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	vds, err := s.Store.VolumeDefinitions().List(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Defensive non-nil: linstor-csi's VD-list decoder treats a `null`
	// body as malformed. Both store backends `make()` their result,
	// but pin the invariant at the wire edge.
	if vds == nil {
		vds = []apiv1.VolumeDefinition{}
	}

	writeJSON(w, http.StatusOK, vds)
}

func (s *Server) handleVDGet(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	vd, err := s.Store.VolumeDefinitions().Get(r.Context(), rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, vd)
}

// handleVDCreate accepts either the upstream `VolumeDefinitionCreate`
// envelope (`{"volume_definition": {...}}`) or a bare VolumeDefinition body —
// both shapes appear in the wild.
func (s *Server) handleVDCreate(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	var envelope apiv1.VolumeDefinitionCreate

	dec := json.NewDecoder(r.Body)

	err := dec.Decode(&envelope)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	vd := envelope.VolumeDefinition

	err = s.Store.VolumeDefinitions().Create(r.Context(), rd, &vd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Matches upstream LINSTOR: POST /v1/resource-definitions/<n>/
	// volume-definitions returns 200 OK (not 201 Created). Java
	// LINSTOR is consistent about this — only top-level entity
	// creates return 201, child-volume creates stay 200 because
	// the parent already exists.
	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume definition created",
	}})
}

// handleVDUpdate applies a modify delta to an existing VolumeDefinition.
// PUT semantics for upstream LINSTOR's `vd set-size` / `vd set-property`
// are MERGE, not REPLACE — golinstor sends only the fields that changed
// (size_kib alone for CSI grow, override_props/delete_props alone for
// property modifies) and expects the rest of the VD spec to be
// preserved. A naive Decode(&fullVD) + Update silently zeroes SizeKib
// whenever the body omits it (see audit-4.6 finding). Fetch + merge.
func (s *Server) handleVDUpdate(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	var patch volumeDefinitionModifyBody

	err = json.NewDecoder(r.Body).Decode(&patch)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	existing, err := s.Store.VolumeDefinitions().Get(r.Context(), rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Capture the pre-merge size so we can detect an explicit shrink
	// after the patch has been applied. Done before mergeVolumeDefinitionPatch
	// so we compare against the stored spec, not the in-place mutated one.
	previousSizeKib := existing.SizeKib

	mergeVolumeDefinitionPatch(&existing, &patch)

	// Path-derived VolumeNumber wins — never trust the body's vol_num.
	existing.VolumeNumber = vn

	err = s.Store.VolumeDefinitions().Update(r.Context(), rd, &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	envelope := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume definition modified",
	}}

	// Shrink advisory (docs/known-issues.md #38). Upstream LINSTOR rejects
	// shrink-after-deploy at the API; blockstor accepts it (the satellite reconciler's
	// grow branch is `vol.GetSizeKib() > status.UsableKib`, so a shrink
	// in the spec never reaches `lvextend`/`zfs set volsize` and never
	// truncates the backing device). Still, the operator should see a
	// warning entry so the data-loss risk surfaces in the audit log —
	// shrinking the spec without also shrinking the FS will eventually
	// corrupt data if the FS later writes past the new size.
	//
	// Emit the warning as a SECOND envelope entry (success first, then
	// advisory) — matches upstream's order in ApiCallRcImpl where the
	// "operation succeeded" entry leads and per-resource warnings tail.
	if patch.SizeKib != nil && *patch.SizeKib < previousSizeKib {
		envelope = append(envelope, apiv1.APICallRc{
			RetCode: warnVlmDfnResizeShrink,
			Message: fmt.Sprintf(
				"shrinking volume %d from %d KiB to %d KiB (DATA LOSS RISK — caller intent assumed)",
				vn, previousSizeKib, *patch.SizeKib,
			),
			ObjRefs: map[string]string{
				"RscDfn": rd,
				"VlmNr":  strconv.FormatInt(int64(vn), 10),
			},
		})
	}

	writeJSON(w, http.StatusOK, envelope)
}

// mergeVolumeDefinitionPatch overlays the modify delta onto an existing
// VolumeDefinition in place. Split out of handleVDUpdate to keep the
// HTTP handler under the gocyclo budget; the merge rules are unit-
// tested through the handler.
func mergeVolumeDefinitionPatch(existing *apiv1.VolumeDefinition, patch *volumeDefinitionModifyBody) {
	if patch.SizeKib != nil {
		existing.SizeKib = *patch.SizeKib
	}

	// Props: overlay override_props (and the legacy `props` field —
	// some callers PUT the full VD shape) on top of existing, then
	// drop anything in delete_props. delete_namespaces is the upstream
	// "delete every key under prefix" knob.
	if len(patch.OverrideProps) > 0 || len(patch.Props) > 0 {
		if existing.Props == nil {
			existing.Props = map[string]string{}
		}

		maps.Copy(existing.Props, patch.OverrideProps)
		maps.Copy(existing.Props, patch.Props)
	}

	for _, k := range patch.DeleteProps {
		delete(existing.Props, k)
	}

	for _, ns := range patch.DeleteNamespaces {
		for k := range existing.Props {
			if k == ns || (len(k) > len(ns) && k[:len(ns)] == ns && k[len(ns)] == '/') {
				delete(existing.Props, k)
			}
		}
	}
}

// handleVDDelete drops a VolumeDefinition under an RD.
//
// Idempotent on NotFound (Bug 66): both NotFound shapes — the parent
// RD missing AND the (rd, vn) pair missing inside an extant RD — fold
// into a 200 + warn-mask envelope. linstor-csi's ControllerExpand /
// shrink paths re-issue `vd d` on retry; the bare 404 used to crash
// the Python CLI on its XML decoder fallback (see Bug 56 commentary).
func (s *Server) handleVDDelete(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	err = s.Store.VolumeDefinitions().Delete(r.Context(), rd, vn)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	if err != nil {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnVDNotFound,
			Message: fmt.Sprintf("volume definition already absent: %s/%d", rd, vn),
		}})

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: fmt.Sprintf("volume definition deleted: %s/%d", rd, vn),
	}})
}

func parseVolNum(raw string) (int32, error) {
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err //nolint:wrapcheck // returned to handler that wraps it
	}

	return int32(v), nil
}
