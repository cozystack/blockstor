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

	// Force is the wave2 4.W13 escape hatch for a spec-shrink: when the
	// operator has already shrunk the backing filesystem out-of-band
	// (`resize2fs -s <new-size>`, etc.) they need a way to bring the
	// LINSTOR spec back into sync with the now-smaller FS. Upstream
	// LINSTOR rejects all shrinks unconditionally; blockstor matches
	// that default but accepts force=true as an opt-in. Also honoured
	// via the `?force=true` query parameter so ad-hoc `curl` scripts
	// don't have to re-shape a golinstor payload.
	Force bool `json:"force,omitempty"`
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
		// Bug 140: duplicate-VD conflict gets a typed envelope with
		// the upstream FAIL_EXISTS_VLM_DFN sub-code plus actionable
		// cause/correction so scripts and audit-log greppers route
		// the same way they do for upstream's `linstor vd c` reply.
		// The bare writeStoreError fallback emitted apiCallRcError
		// alone — high-bit error, no sub-code, no cause/correction
		// — which the Python CLI rendered as a generic "object
		// already exists" line that didn't tell the operator which
		// VlmNr to twist.
		if errors.Is(err, store.ErrAlreadyExists) {
			writeVDExistsConflict(w, rd, vd.VolumeNumber)

			return
		}

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

// writeVDExistsConflict emits the Bug 140 typed conflict envelope on
// a duplicate `POST /v1/resource-definitions/{rd}/volume-definitions`.
// Wire shape matches upstream LINSTOR's `linstor vd c` reply on the
// same input: 409 Conflict + ApiCallRc with apiCallRcError |
// FAIL_EXISTS_VLM_DFN sub-code, an operator-actionable message
// naming the parent RD and the duplicate VlmNr, and a non-empty
// cause/correction so the Python CLI surfaces the refusal as an
// ERROR line (not a generic "object already exists").
//
// Per cli-parity-audit alignment, the correction names the two
// remedial commands: PUT to modify the existing VD (`vd m`) or
// POST with an explicit, free VolumeNumber (`vd c --vlmnr`).
func writeVDExistsConflict(w http.ResponseWriter, rd string, vn int32) {
	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailExistsVlmDfn,
		Message: fmt.Sprintf(
			"volume definition %d already exists on resource definition %q",
			vn, rd),
		Cause: fmt.Sprintf(
			"a volume definition with VlmNr=%d is already registered under %q; "+
				"`linstor vd c` without --vlmnr defaults to 0 and the second invocation "+
				"collides with the first",
			vn, rd),
		Correc: fmt.Sprintf(
			"to modify the existing volume use `linstor vd m %s %d <new-size>`; "+
				"to add a second volume pick a free VlmNr explicitly "+
				"(`linstor vd c --vlmnr=<N> %s <size>`)",
			rd, vn, rd),
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
		},
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
	// BEFORE the patch is applied. Done before mergeVolumeDefinitionPatch
	// so we compare against the stored spec, not the in-place mutated one.
	previousSizeKib := existing.SizeKib

	// Scenario 4.W13: reject any shrink (`new < previous`) unless the
	// operator opted in via `force=true`. Runs BEFORE the merge + store
	// write so a rejected shrink leaves the stored spec untouched — a
	// partial update would desync the controller spec from the
	// satellite reality.
	if rejectShrinkWithoutForce(w, r, &patch, rd, vn, previousSizeKib) {
		return
	}

	mergeVolumeDefinitionPatch(&existing, &patch)

	// Path-derived VolumeNumber wins — never trust the body's vol_num.
	existing.VolumeNumber = vn

	err = s.Store.VolumeDefinitions().Update(r.Context(), rd, &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Bug 136: on a grow, stamp the per-resource resize-pending
	// annotation. See stampResizePendingOnResources for rationale.
	if patch.SizeKib != nil && *patch.SizeKib > previousSizeKib {
		s.stampResizePendingOnResources(r.Context(), rd, vn, *patch.SizeKib)
	}

	envelope := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume definition modified",
	}}

	envelope = appendForceShrinkAdvisory(envelope, &patch, rd, vn, previousSizeKib)

	writeJSON(w, http.StatusOK, envelope)
}

// appendForceShrinkAdvisory appends the force-shrink warning entry
// to the success envelope when the patch reduced SizeKib. Only
// reachable when force=true (the strict-reject branch in
// rejectShrinkWithoutForce otherwise short-circuits with 400).
// Matches upstream's ApiCallRcImpl order where the "operation
// succeeded" entry leads and per-resource warnings tail. Bug 38 /
// scenario 4.W13.
func appendForceShrinkAdvisory(envelope []apiv1.APICallRc, patch *volumeDefinitionModifyBody, rd string, vn int32, previousSizeKib int64) []apiv1.APICallRc {
	if patch.SizeKib == nil || *patch.SizeKib >= previousSizeKib {
		return envelope
	}

	return append(envelope, apiv1.APICallRc{
		RetCode: warnVlmDfnResizeShrink,
		Message: fmt.Sprintf(
			"shrinking volume %d from %d KiB to %d KiB (force=true; DATA LOSS RISK — caller intent assumed)",
			vn, previousSizeKib, *patch.SizeKib,
		),
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			objRefVlmNr:  strconv.FormatInt(int64(vn), 10),
		},
	})
}

// rejectShrinkWithoutForce writes a 400 + FAIL_INVLD_VLM_SIZE
// envelope when the patch reduces SizeKib without `force=true` and
// returns true to signal the caller to short-circuit. The error path
// is split out of handleVDUpdate to keep the HTTP handler under the
// funlen budget; the formatted message stays inline here so a single
// grep against the binary finds the operator-actionable text.
//
// LINSTOR does NOT auto-shrink the backing FS — `lvreduce` after a
// spec-shrink without an in-FS `resize2fs -s` first would truncate
// live data. Upstream LINSTOR's
// CtrlVlmDfnModifyApiCallHandler.ensureShrinkingIsSupported raises
// FAIL_INVLD_VLM_SIZE (206 | MASK_ERROR) on the same input; mirror
// the wire code and 400 Bad Request HTTP status so golinstor's
// `client.ApiCallError` surfaces the message in `linstor`'s exit-1
// path.
func rejectShrinkWithoutForce(
	w http.ResponseWriter, r *http.Request, patch *volumeDefinitionModifyBody,
	rd string, vn int32, previousSizeKib int64,
) bool {
	if patch.SizeKib == nil || *patch.SizeKib >= previousSizeKib {
		return false
	}

	if shrinkForceRequested(r, patch) {
		return false
	}

	writeJSON(w, http.StatusBadRequest, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInvldVlmSize,
		Message: fmt.Sprintf(
			"cannot shrink volume %d from %d KiB to %d KiB: "+
				"filesystem shrink-then-resize required; LINSTOR does NOT auto-shrink. "+
				"Operator action: (1) `resize2fs -s <new>` or `xfs` dump+restore on the consumer, "+
				"(2) unmount or detach the volume, "+
				"(3) re-issue this PUT with `force=true` (body field) or `?force=true` (query).",
			vn, previousSizeKib, *patch.SizeKib,
		),
		ObjRefs: map[string]string{
			objRefRscDfn: rd,
			"VlmNr":      strconv.FormatInt(int64(vn), 10),
		},
	}})

	return true
}

// shrinkForceRequested returns true when the caller opted into the
// shrink escape hatch via either the JSON body's `force` field or the
// `?force=true` query parameter. The query parameter exists so ad-hoc
// `curl -X PUT … ?force=true` scripts work without re-shaping the
// JSON body around a golinstor-shaped payload. Both knobs must accept
// the literal string "true" (case-insensitive) — Go's
// `strconv.ParseBool` covers "1"/"t"/"true"/"True"/"TRUE" which is a
// strict superset of the documented form.
func shrinkForceRequested(r *http.Request, patch *volumeDefinitionModifyBody) bool {
	if patch.Force {
		return true
	}

	raw := r.URL.Query().Get("force")
	if raw == "" {
		return false
	}

	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}

	return v
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
		// Bug 139: even on the idempotent no-op branch, drain the
		// local cache so a re-issued DELETE during a real delete-in-
		// flight is still read-your-writes on the follow-up view.
		s.waitForVDDeletionVisible(r.Context(), rd, vn)

		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnVDNotFound,
			Message: fmt.Sprintf("volume definition already absent: %s/%d", rd, vn),
		}})

		return
	}

	// Bug 139: prune the deleted VolumeNumber off each child
	// Resource's Status.Volumes, then wait for the VD delete to be
	// observable on the local store. The satellite reconciler
	// eventually re-stamps Status.Volumes when it re-applies after
	// the RD spec change, but the gap surfaces the dropped volume
	// on `view/resources` for tens of seconds. Pre-stamping the
	// Status.Volumes update here closes the gap synchronously.
	s.pruneVolumesFromResources(r.Context(), rd, vn)
	s.waitForVDDeletionVisible(r.Context(), rd, vn)

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: fmt.Sprintf("volume definition deleted: %s/%d", rd, vn),
	}})
}

// pruneVolumesFromResources walks every Resource of the named RD
// and drops the deleted VolumeNumber from its Volumes slice. Bug
// 139: the satellite eventually re-stamps Status.Volumes after the
// RD-watch fires, but `view/resources` reads in the gap surface
// the phantom volume — pre-stamp here so the gap is zero.
//
// Best-effort: a single Resource failing to re-Update doesn't roll
// back the others nor the VD delete itself.
func (s *Server) pruneVolumesFromResources(ctx context.Context, rd string, vn int32) {
	if s == nil || s.Store == nil {
		return
	}

	resources, err := s.Store.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		return
	}

	for i := range resources {
		rsc := resources[i]
		if len(rsc.Volumes) == 0 {
			continue
		}

		out := make([]apiv1.Volume, 0, len(rsc.Volumes))

		dropped := false

		for j := range rsc.Volumes {
			if rsc.Volumes[j].VolumeNumber == vn {
				dropped = true

				continue
			}

			out = append(out, rsc.Volumes[j])
		}

		if !dropped {
			continue
		}

		rsc.Volumes = out

		_ = s.Store.Resources().Update(ctx, &rsc)
	}
}

func parseVolNum(raw string) (int32, error) {
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err //nolint:wrapcheck // returned to handler that wraps it
	}

	return int32(v), nil
}

// resizePendingAnnotationPrefix is the per-volume annotation key
// prefix the REST VD-grow handler stamps on each affected Resource
// (Bug 136). The full key is
// `<prefix><VolumeNumber>` and the value is the new SizeKib (decimal
// string, KiB). Per-volume so multi-volume RDs (rare today but on
// the roadmap) keep the grow signal distinguishable when several
// volumes resize at once.
//
// Operators read this via `kubectl get resource -o yaml`; the
// satellite reconciler doesn't strictly require it (the RD-watch
// in `enqueueResourcesForRD` already re-applies on any RD-spec
// change), but it gives a steady-state breadcrumb that explains
// why the satellite re-rendered and what the target size is.
const resizePendingAnnotationPrefix = "bug136.blockstor.cozystack.io/resize-pending-size-kib-vol-"

// stampResizePendingOnResources walks every Resource of the named
// RD and stamps the per-volume "resize pending" annotation with the
// new size. Best-effort by design: a single Resource failing to
// re-Update doesn't roll back the others nor the VD spec change
// itself. Bug 136.
func (s *Server) stampResizePendingOnResources(ctx context.Context, rd string, vn int32, sizeKib int64) {
	if s == nil || s.Store == nil {
		return
	}

	resources, err := s.Store.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		return
	}

	key := resizePendingAnnotationPrefix + strconv.FormatInt(int64(vn), 10)
	value := strconv.FormatInt(sizeKib, 10)

	for i := range resources {
		rsc := resources[i]
		if rsc.Annotations == nil {
			rsc.Annotations = map[string]string{}
		}

		rsc.Annotations[key] = value

		_ = s.Store.Resources().Update(ctx, &rsc)
	}
}
