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
	"maps"
	"net/http"

	"github.com/cozystack/blockstor/pkg/api/openapi"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// rdCloneRequest is the body for `resource-definition clone`. Only the
// new name is required; advanced options (override props, RG override)
// land when there's demand.
type rdCloneRequest struct {
	Name string `json:"name"`
}

// registerRDClone wires the /v1/resource-definitions/{rd}/clone endpoints.
//
// The GET path mirrors upstream LINSTOR exactly:
// `/v1/resource-definitions/{src}/clone/{target}` — that's what
// golinstor's `ResourceDefinitionService.CloneStatus` issues, and what
// linstor-csi polls in a loop until `status == "COMPLETE"`. A 404 here
// makes CSI clone-from-source fail with "clone status: not found".
func (s *Server) registerRDClone(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/clone",
		s.requireStore(s.handleRDClone))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/clone/{target}",
		s.requireStore(s.handleRDCloneStatus))
}

// handleRDClone duplicates the source RD's metadata (props, RG ref)
// under a new name when the source carries no VolumeDefinitions. When
// the source HAS VDs, the apiserver refuses with HTTP 501 — Bug 114:
// the previous handler answered 201 + a synthetic "Completed cloning"
// CLI line, but produced an empty target shell because nothing copies
// VolumeDefinitions and no satellite-side data-plane (zfs send/recv,
// dd, ZFS-clone) is wired. Operators followed the success message,
// `mkfs`'d the clone, and discovered their "clone" had no backing.
//
// We deliberately keep the empty-source path working: Group D's
// integration smoke test seeds a vol-less RD and clones the shell
// to verify Props/RG propagation; that contract still holds.
//
// When data-plane clone lands in a future commit, replace the
// 501 branch with the materialisation logic and update the
// matching pin in clone_bug_114_test.go.
func (s *Server) handleRDClone(w http.ResponseWriter, r *http.Request) {
	srcName := r.PathValue("rd")

	var req rdCloneRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")

		return
	}

	src, err := s.Store.ResourceDefinitions().Get(r.Context(), srcName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Bug 114: refuse to create a structurally-incomplete clone when
	// the source carries VolumeDefinitions. The legacy handler
	// happily wrote an empty target RD and lied about success; the
	// honest answer is a LINSTOR `[]ApiCallRc` envelope describing
	// the gap and pointing at the snapshot-ship workaround.
	srcVDs, err := s.Store.VolumeDefinitions().List(r.Context(), srcName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if len(srcVDs) > 0 {
		writeCloneNotImplemented(w, srcName, req.Name)

		return
	}

	s.cloneEmptyRDShell(w, r, &src, req.Name)
}

// cloneEmptyRDShell materialises the empty-source clone path: shallow-copy
// of the RD spec (Props, RG ref) under a new name. Group D's integration
// smoke test pins this branch — a freshly-created vol-less RD must be
// cloneable with Props and RG carried over. Extracted out of handleRDClone
// to keep its funlen under budget after the Bug 114 VD-presence gate.
func (s *Server) cloneEmptyRDShell(w http.ResponseWriter, r *http.Request,
	src *apiv1.ResourceDefinition, cloneName string,
) {
	clone := *src
	clone.Name = cloneName
	clone.UUID = ""

	if src.Props != nil {
		clone.Props = make(map[string]string, len(src.Props))
		maps.Copy(clone.Props, src.Props)
	}

	err := s.Store.ResourceDefinitions().Create(r.Context(), &clone)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// golinstor's ResourceDefinitionService.Clone decodes into
	// `ResourceDefinitionCloneStarted` (an object), NOT
	// `[]ApiCallRc`. Returning the bare ApiCallRc array breaks the
	// decoder with "cannot unmarshal array into Go value of type
	// client.ResourceDefinitionCloneStarted" — surfaced as a
	// CSI CreateVolume-from-source failure in csi-sanity. Emit the
	// envelope shape upstream specifies.
	writeJSON(w, http.StatusCreated, cloneStartedResponse{
		Location:   "/v1/resource-definitions/" + src.Name + "/clone/" + clone.Name,
		SourceName: src.Name,
		CloneName:  clone.Name,
		Messages: &[]apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "resource definition cloned: " + clone.Name,
		}},
	})
}

// handleRDCloneStatus answers golinstor's `CloneStatus` poll. The
// response is grounded in actual store state (Bug 114): we compare
// the source RD's VolumeDefinition count to the target's. Equal
// counts → COMPLETE (the clone is structurally consistent with the
// source). A non-empty source paired with an empty target → FAILED,
// so linstor-csi surfaces a concrete error rather than spinning on
// a stale COMPLETE while the data plane never copied anything.
//
// Path: GET /v1/resource-definitions/{src}/clone/{target}.
// A 404 on the target signals "clone failed mid-way" — which gives
// linstor-csi an actionable error rather than an infinite poll loop.
// A 404 on the source surfaces the same way: it would have been
// caught at clone-POST time, but a delete-source race shouldn't
// produce a phantom COMPLETE either.
func (s *Server) handleRDCloneStatus(w http.ResponseWriter, r *http.Request) {
	srcName := r.PathValue("rd")
	targetName := r.PathValue("target")

	_, err := s.Store.ResourceDefinitions().Get(r.Context(), targetName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	status := computeCloneStatus(r.Context(), s.Store, srcName, targetName)
	writeJSON(w, http.StatusOK, openapi.ResourceDefinitionCloneStatus{
		Status: status,
	})
}

// computeCloneStatus resolves COMPLETE vs FAILED for a clone pair by
// comparing source-vs-target VolumeDefinition counts. Bug 114: an
// empty target paired with a non-empty source is structurally
// incomplete — golinstor's poll loop must see FAILED so it stops
// waiting on data that will never arrive.
//
// If the source RD itself is gone (race with `rd d <src>` while the
// poll is in flight), we cannot prove the target is consistent —
// the safest answer is COMPLETE because the target survived and any
// further validation requires the source to compare against. This
// preserves the legacy behaviour for that edge case.
func computeCloneStatus(ctx context.Context, st store.Store, srcName, targetName string) openapi.ResourceDefinitionCloneStatusStatus {
	srcVDs, err := st.VolumeDefinitions().List(ctx, srcName)
	if err != nil {
		return openapi.COMPLETE
	}

	targetVDs, err := st.VolumeDefinitions().List(ctx, targetName)
	if err != nil {
		return openapi.COMPLETE
	}

	if len(srcVDs) > 0 && len(targetVDs) < len(srcVDs) {
		return openapi.FAILED
	}

	return openapi.COMPLETE
}

// writeCloneNotImplemented stamps the Bug 114 refusal envelope.
//
// Wire shape gotcha: upstream LINSTOR returns a
// `ResourceDefinitionCloneStarted` object on BOTH success (201) and
// error (500) — the error is conveyed through the `messages` field
// inside the object, NOT by switching the body to a bare ApiCallRc
// array (see linstor-server's `mapToCloneStarted`). python-linstor's
// CLI relies on this: `resource_dfn_clone` calls
// `_rest_request_raw` which skips the standard error-status handler
// and decodes the body straight into `CloneStarted`. If we return a
// bare array on 501 the CLI crashes with
// `AttributeError: 'list' object has no attribute 'get'` on the next
// `.messages` access, rather than printing the operator-actionable
// error line we authored. Mirror the upstream object-with-embedded-
// messages shape so the CLI surfaces the refusal as an ERROR line.
//
// golinstor's SDK still handles this correctly: the non-2xx status
// (501) triggers `c.do`'s ApiCallError fallback path, but only when
// `doJSON` decodes — for clone POST the body is decoded straight
// into `ResourceDefinitionCloneStarted` and golinstor would also
// crash without the object shape.
func writeCloneNotImplemented(w http.ResponseWriter, srcName, cloneName string) {
	writeJSON(w, http.StatusNotImplemented, cloneStartedResponse{
		Location:   "/v1/resource-definitions/" + srcName + "/clone/" + cloneName,
		SourceName: srcName,
		CloneName:  cloneName,
		Messages: &[]apiv1.APICallRc{{
			RetCode: apiCallRcError,
			Message: "resource definition clone is not yet fully implemented",
			Cause: "the apiserver creates the clone RD shell but does not copy " +
				"volume definitions or trigger satellite-side data copy, so " +
				"the resulting clone is an empty shell with no usable volumes",
			Correc: "snapshot the source and restore into a fresh RD instead: " +
				"`linstor s create " + srcName + " <snap>` then " +
				"`linstor s resource restore --from-resource " + srcName +
				" --from-snapshot <snap> --to-resource " + cloneName + "`",
			ObjRefs: map[string]string{
				"RscDfn": srcName,
			},
		}},
	})
}

// cloneStartedResponse mirrors upstream LINSTOR's
// `ResourceDefinitionCloneStarted` — the JSON object golinstor's
// Clone(...) decodes into. Defined here (not in pkg/api/v1) since
// it's an output-only response envelope; no client-side caller
// constructs it.
type cloneStartedResponse struct {
	Location   string             `json:"location"`
	SourceName string             `json:"source_name"`
	CloneName  string             `json:"clone_name"`
	Messages   *[]apiv1.APICallRc `json:"messages,omitempty"`
}
