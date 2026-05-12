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
	"maps"
	"net/http"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerResourceDefinitions wires /v1/resource-definitions CRUD. Spawn,
// Clone, snapshot-restore, and per-volume endpoints land in later slices.
func (s *Server) registerResourceDefinitions(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/resource-definitions", s.requireStore(s.handleRDList))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}", s.requireStore(s.handleRDGet))
	mux.HandleFunc("POST /v1/resource-definitions", s.requireStore(s.handleRDCreate))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}", s.requireStore(s.handleRDUpdate))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}", s.requireStore(s.handleRDDelete))
}

func (s *Server) handleRDList(w http.ResponseWriter, r *http.Request) {
	rds, err := s.Store.ResourceDefinitions().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, rds)
}

func (s *Server) handleRDGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rd")

	rd, err := s.Store.ResourceDefinitions().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, rd)
}

func (s *Server) handleRDCreate(w http.ResponseWriter, r *http.Request) {
	var body apiv1.ResourceDefinitionCreate

	dec := json.NewDecoder(r.Body)
	// upstream LINSTOR has tolerated extra fields here historically; mirror
	// that to keep golinstor (and any home-grown clients) happy.
	err := dec.Decode(&body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	rd := body.ResourceDefinition
	if body.ExternalName != "" && rd.ExternalName == "" {
		rd.ExternalName = body.ExternalName
	}

	if rd.Name == "" {
		writeError(w, http.StatusBadRequest, "resource definition name is required")

		return
	}

	// Upstream LINSTOR parity: every RD belongs to an RG. The
	// well-known DfltRscGrp serves as the catch-all for clients that
	// don't specify one (linstor-csi, the legacy CSI shipper, manual
	// `linstor rd create` without `--resource-group`, etc). Without
	// this default some CLI subcommands fail open lookups and operator
	// workflows that walk `rd → rg → spawn args` break silently.
	err = s.ensureDefaultRGAssignment(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	err = s.Store.ResourceDefinitions().Create(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource definition created: " + rd.Name,
	}})
}

// DefaultResourceGroupName is the well-known RG every RD falls into
// when the caller didn't pin one. Matches upstream LINSTOR's
// `DfltRscGrp` literal so golinstor / linstor-csi callers that walk
// rd → rg discovery see the expected name.
const DefaultResourceGroupName = "DfltRscGrp"

// ensureDefaultRGAssignment sets rd.ResourceGroupName to the default
// when the caller didn't supply one, and lazily creates the
// well-known RG on first use. An explicit caller-supplied RG is left
// alone (existence is the caller's concern — matches upstream's
// "RD-create doesn't validate RG existence at the wire layer").
// Idempotent across concurrent RD-create races: ErrAlreadyExists from
// the RG-create path is swallowed.
func (s *Server) ensureDefaultRGAssignment(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	if rd.ResourceGroupName != "" {
		return nil
	}

	rd.ResourceGroupName = DefaultResourceGroupName

	_, err := s.Store.ResourceGroups().Get(ctx, DefaultResourceGroupName)
	if err == nil {
		return nil
	}

	if !errors.Is(err, store.ErrNotFound) {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	defaultRG := apiv1.ResourceGroup{
		Name:        DefaultResourceGroupName,
		Description: "Default LINSTOR resource group — autoplace catch-all for RDs without an explicit RG.",
	}

	err = s.Store.ResourceGroups().Create(ctx, &defaultRG)
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	return nil
}

// resourceDefinitionModifyBody is the shape upstream golinstor sends
// on `PUT /v1/resource-definitions/{rd}` — driven by `linstor rd
// set-property`, `linstor rd modify --resource-group`, and similar
// CLI subcommands. Top-level fields are the modify delta, not the
// full RD spec; the bare RD wire shape doesn't carry these
// modify-only keys.
type resourceDefinitionModifyBody struct {
	OverrideProps    map[string]string `json:"override_props,omitempty"`
	DeleteProps      []string          `json:"delete_props,omitempty"`
	DeleteNamespaces []string          `json:"delete_namespaces,omitempty"`
	DrbdPeerSlots    int32             `json:"drbd_peer_slots,omitempty"`
	DrbdPort         int32             `json:"drbd_port,omitempty"`
	// resource_group: upstream linstor CLI's `rd modify --resource-group`
	// (matches golinstor `ResourceDefinitionCreate.ResourceGroup`).
	ResourceGroup string `json:"resource_group,omitempty"`
	// resource_group_name: legacy callers that PUT the full RD shape
	// instead of the modify envelope (matches the read-side
	// `ResourceDefinition` wire field). Accept both — first non-empty wins.
	ResourceGroupName string `json:"resource_group_name,omitempty"`
}

func (s *Server) handleRDUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rd")

	var patch resourceDefinitionModifyBody

	err := json.NewDecoder(r.Body).Decode(&patch)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// PUT semantics for the upstream linstor CLI's `rd set-property`
	// are MERGE, not REPLACE — golinstor sends only the override_props
	// / delete_props delta and expects the rest of the RD spec to be
	// preserved. A naïve Decode(&fullRD) + Update wipes the whole
	// spec (VolumeDefinitions vanish, the resource reconciler can't
	// spawn replicas, the cluster stalls). Fetch + merge instead.
	existing, err := s.Store.ResourceDefinitions().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if existing.Props == nil && len(patch.OverrideProps) > 0 {
		existing.Props = map[string]string{}
	}

	maps.Copy(existing.Props, patch.OverrideProps)

	for _, k := range patch.DeleteProps {
		delete(existing.Props, k)
	}

	rgChange := patch.ResourceGroup
	if rgChange == "" {
		rgChange = patch.ResourceGroupName
	}

	if rgChange != "" {
		existing.ResourceGroupName = rgChange
	}

	err = s.Store.ResourceDefinitions().Update(r.Context(), &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource definition modified: " + existing.Name,
	}})
}

func (s *Server) handleRDDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rd")

	err := s.Store.ResourceDefinitions().Delete(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource definition deleted: " + name,
	}})
}
