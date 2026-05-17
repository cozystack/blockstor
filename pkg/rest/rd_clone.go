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
// new name is required.
//
// Bug 232: after Bug 222 bumped the wire-advertised rest_api_version
// from 1.23.0 to 1.27.0, python-linstor's `_require_version()` gates
// open up `override_props` / `delete_props` (gated on 1.26.0) and
// the `src_snap_name` snapshot-based clone path. Pre-fix the
// DisallowUnknownFields decoder rejects them as 400 + "unknown field"
// and the CLI crashes on the malformed envelope.
//
//   - `override_props` (map[string]string): properties to overwrite
//     on the cloned RD's prop set. Wired through:
//     handleRDClone applies these on top of the source Props after
//     the shallow-copy, so the operator's `--override-prop K=V`
//     lands on the cloned RD.
//   - `delete_props` ([]string): property keys to drop from the
//     cloned RD's prop set. Wired through alongside override_props.
//   - `src_snap_name` (string): name of the source snapshot the
//     clone should materialise from (vs. live-resource clone). Bug
//     239: a non-empty value MUST surface an explicit HTTP 501 +
//     CloneStarted-envelope refusal rather than silently dropping
//     to the live-RD shell-copy path. Pre-Bug-239 the field was
//     accepted-and-no-op (Bug 232), which gave operators a fresh
//     empty shell with no error — the "snap" intent vanished.
//     Until the snapshot-based clone data plane lands (Phase 12)
//     the operator should fall back to the snapshot-then-restore
//     workflow the writeCloneNotImplemented envelope hints at.
//
// TODO(bug-239-followup / Phase 12): when the snapshot-based clone
// data plane lands, replace the 501 branch with a snapshot-restore-
// equivalent path instead of the live-RD shell copy.
type rdCloneRequest struct {
	Name          string            `json:"name"`
	OverrideProps map[string]string `json:"override_props,omitempty"`
	DeleteProps   []string          `json:"delete_props,omitempty"`
	SrcSnapName   string            `json:"src_snap_name,omitempty"`
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

	// Bug 239: snapshot-based clone is not implemented yet. The Bug 232
	// decoder accepts `src_snap_name` so the CLI stops crashing on the
	// wire-shape mismatch, but silently dropping it gave operators a
	// fresh empty shell that lied about the snapshot. Surface an
	// explicit 501 + CloneStarted envelope so the operator sees the gap
	// (and the matching snapshot-then-restore workaround) before the
	// snapshot-clone data plane lands in Phase 12.
	if req.SrcSnapName != "" {
		writeSnapshotCloneNotImplemented(w, srcName, req.Name, req.SrcSnapName)

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

	s.cloneEmptyRDShell(w, r, &src, &req)
}

// cloneEmptyRDShell materialises the empty-source clone path: shallow-copy
// of the RD spec (Props, RG ref) under a new name. Group D's integration
// smoke test pins this branch — a freshly-created vol-less RD must be
// cloneable with Props and RG carried over. Extracted out of handleRDClone
// to keep its funlen under budget after the Bug 114 VD-presence gate.
//
// Bug 232: the `req.OverrideProps` map is applied on top of the
// source's Props after the shallow-copy, and `req.DeleteProps` keys
// are dropped before the Create lands. python-linstor 1.27.0 sends
// these via `linstor resource-definition clone --override-prop K=V`
// / `--delete-prop K`; wiring them through here keeps the operator's
// intent honoured for the empty-VD path. `req.SrcSnapName` is
// accepted-and-no-op (see rdCloneRequest docstring) — the snapshot-
// based clone data plane lands separately.
func (s *Server) cloneEmptyRDShell(w http.ResponseWriter, r *http.Request,
	src *apiv1.ResourceDefinition, req *rdCloneRequest,
) {
	clone := *src
	clone.Name = req.Name
	clone.UUID = ""

	if src.Props != nil || len(req.OverrideProps) > 0 {
		clone.Props = make(map[string]string, len(src.Props)+len(req.OverrideProps))
		maps.Copy(clone.Props, src.Props)
	}

	maps.Copy(clone.Props, req.OverrideProps)

	for _, k := range req.DeleteProps {
		delete(clone.Props, k)
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

// writeSnapshotCloneNotImplemented stamps the Bug 239 refusal envelope
// for the `src_snap_name`-bearing clone path. Same wire shape as
// writeCloneNotImplemented (CloneStarted-object on 501 so python-
// linstor's `resource_dfn_clone` can decode it without crashing), but
// the messages are scoped to the snapshot-clone gap rather than the
// VD-copy gap. The operator gets a concrete fallback that uses the
// `s create` + `s resource restore` workflow which IS wired today.
//
// Bug 232 used to accept `src_snap_name` and silently drop it,
// producing a fresh empty shell on the live-RD path with the wrong
// data shape — Bug 239 trades the silent-success for an explicit
// 501 so the operator either learns the gap immediately or scripts
// the snapshot+restore fallback.
func writeSnapshotCloneNotImplemented(w http.ResponseWriter, srcName, cloneName, srcSnapName string) {
	writeJSON(w, http.StatusNotImplemented, cloneStartedResponse{
		Location:   "/v1/resource-definitions/" + srcName + "/clone/" + cloneName,
		SourceName: srcName,
		CloneName:  cloneName,
		Messages: &[]apiv1.APICallRc{{
			RetCode: apiCallRcError,
			Message: "snapshot-based clone not implemented in this release (pending Phase 12)",
			Cause: "the apiserver accepts `src_snap_name` on the wire for python-linstor 1.27.0 " +
				"compatibility (Bug 232 + 237) but the satellite-side snapshot-clone data plane is " +
				"not yet wired; silently falling back to a live-RD shell copy would discard the " +
				"snapshot intent and produce a clone with the wrong contents",
			Correc: "use the snapshot-then-restore workflow which IS wired today: " +
				"`linstor s create " + srcName + " " + srcSnapName + "` (if the snapshot " +
				"doesn't already exist) then " +
				"`linstor s resource restore --from-resource " + srcName +
				" --from-snapshot " + srcSnapName + " --to-resource " + cloneName + "`",
			ObjRefs: map[string]string{
				objRefRscDfn: srcName,
				"SnapName":   srcSnapName,
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
