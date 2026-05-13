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
	"maps"
	"net/http"

	"github.com/cozystack/blockstor/pkg/api/openapi"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
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
// under a new name. Volume cloning is the satellite's job once the new
// RD enters the reconcile pass.
func (s *Server) handleRDClone(w http.ResponseWriter, r *http.Request) {
	srcName := r.PathValue("rd")

	var req rdCloneRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

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

	// Shallow-copy mutable fields. We deliberately don't copy the
	// generated UUID — Create assigns a fresh one.
	clone := src
	clone.Name = req.Name
	clone.UUID = ""

	if src.Props != nil {
		clone.Props = make(map[string]string, len(src.Props))
		maps.Copy(clone.Props, src.Props)
	}

	err = s.Store.ResourceDefinitions().Create(r.Context(), &clone)
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
		Location:   "/v1/resource-definitions/" + srcName + "/clone/" + clone.Name,
		SourceName: srcName,
		CloneName:  clone.Name,
		Messages: &[]apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "resource definition cloned: " + clone.Name,
		}},
	})
}

// handleRDCloneStatus answers golinstor's `CloneStatus` poll. Today
// blockstor's clone is synchronous w.r.t. RD creation — by the time
// POST .../clone returns the new RD is durably persisted — so we
// always answer COMPLETE if the target RD exists. If a future
// implementation makes cloning truly async (e.g. waits for the
// satellite to finish a zfs-send/recv), this handler is the
// place to surface CLONING/FAILED.
//
// Path: GET /v1/resource-definitions/{src}/clone/{target}.
// The {src} path segment is required by the upstream contract but
// unused here: we only need to confirm the target RD survived the
// POST. A 404 on the target signals "clone failed mid-way" — which
// gives linstor-csi an actionable error rather than an infinite
// poll loop.
func (s *Server) handleRDCloneStatus(w http.ResponseWriter, r *http.Request) {
	targetName := r.PathValue("target")

	_, err := s.Store.ResourceDefinitions().Get(r.Context(), targetName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, openapi.ResourceDefinitionCloneStatus{
		Status: openapi.COMPLETE,
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
