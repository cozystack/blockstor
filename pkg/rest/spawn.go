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
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerSpawn wires /v1/resource-groups/{rg}/spawn — the call linstor-csi
// makes on every CreateVolume.
func (s *Server) registerSpawn(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-groups/{rg}/spawn", s.requireStore(s.handleSpawn))
}

// handleSpawn creates a ResourceDefinition + VolumeDefinitions from a
// ResourceGroup template. Replica placement (creating Resource objects on
// satellites) is a separate Phase 3 concern owned by the autoplacer; this
// handler only materialises the definition side, which is what linstor-csi
// expects from the Spawn call before it issues separate placement
// instructions.
func (s *Server) handleSpawn(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	var req apiv1.ResourceGroupSpawn

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	rg, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if req.ResourceDefinitionName == "" {
		writeError(w, http.StatusBadRequest, "resource_definition_name is required (auto-naming Phase 6)")

		return
	}

	rd := buildSpawnedRD(&req, rgName, &rg)

	err = s.Store.ResourceDefinitions().Create(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	err = s.spawnVolumeDefinitions(r.Context(), rd.Name, &rg, req.VolumeSizes)
	if err != nil {
		rollbackSpawn(r.Context(), s.Store, rd.Name)
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Upstream LINSTOR's `linstor rg spawn` autoplaces according to
	// the RG's SelectFilter — that's the operator-visible contract:
	// spawn = create RD + VDs + place replicas. Without this step,
	// `r l` would be empty after spawn until the operator runs a
	// separate `r c --auto-place=N`. The CSI flow follows the same
	// contract (linstor-csi calls /spawn, then expects to find
	// resources in /v1/view/resources on the next reconcile).
	placeErr := spawnAutoplace(r.Context(), s, rd.Name, &rg, &req)
	if placeErr != nil {
		log.FromContext(r.Context()).WithName("rest").
			Error(placeErr, "spawn autoplace", "rd", rd.Name)

		writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "resource definition spawned, autoplace deferred: " + rd.Name + ": " + placeErr.Error(),
		}})

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource definition spawned: " + rd.Name,
	}})
}

// spawnAutoplace runs the same placer/select-filter merge that
// /v1/resource-definitions/{rd}/autoplace does, but inline as part
// of the spawn call. Mirrors upstream LINSTOR's "spawn = create +
// place" semantic. PartialPlaceCount=0 (no RG constraint) skips
// placement entirely so plain-CRD spawns without a SelectFilter
// stay definition-only.
func spawnAutoplace(ctx context.Context, s *Server, rdName string, rg *apiv1.ResourceGroup, req *apiv1.ResourceGroupSpawn) error {
	rd, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return errors.Wrap(err, "get spawned RD")
	}

	// Merge spawn-time PartialFilter overrides on top of the RG's
	// SelectFilter — same precedence as upstream's spawn.
	filter := mergeAutoplaceFilter(ctx, s.Store, &rd, &req.SelectFilter)

	// Default to RG's PlaceCount when caller didn't override.
	if filter.PlaceCount == 0 {
		filter.PlaceCount = rg.SelectFilter.PlaceCount
	}

	if filter.PlaceCount == 0 {
		return nil
	}

	placed, want, err := placer.New(s.Store).Place(ctx, rdName, &filter)
	if err != nil {
		return errors.Wrap(err, "autoplace")
	}

	if placed < want {
		return errors.Errorf("not enough candidate storage pools: placed %d of %d", placed, want)
	}

	return nil
}

func buildSpawnedRD(req *apiv1.ResourceGroupSpawn, rgName string, rg *apiv1.ResourceGroup) apiv1.ResourceDefinition {
	rd := apiv1.ResourceDefinition{
		Name:              req.ResourceDefinitionName,
		ExternalName:      req.ResourceDefinitionExternal,
		ResourceGroupName: rgName,
	}

	if len(rg.Props) > 0 {
		rd.Props = make(map[string]string, len(rg.Props))
		maps.Copy(rd.Props, rg.Props)
	}

	return rd
}

const bytesPerKib = 1024

// spawnVolumeDefinitions creates one VD per requested size on the named RD.
// Volume numbers follow the slice index, matching upstream LINSTOR.
func (s *Server) spawnVolumeDefinitions(ctx context.Context, rdName string, rg *apiv1.ResourceGroup, sizes []int64) error {
	for i, sizeBytes := range sizes {
		volNum := int32(i)

		vd := apiv1.VolumeDefinition{
			VolumeNumber: volNum,
			SizeKib:      sizeBytes / bytesPerKib,
			Props:        copyVolumeGroupProps(rg.VolumeGroups, volNum),
		}

		err := s.Store.VolumeDefinitions().Create(ctx, rdName, &vd)
		if err != nil {
			return err //nolint:wrapcheck // wrapped by caller
		}
	}

	return nil
}

// copyVolumeGroupProps picks the props for the matching volume number out
// of the RG template, returning nil when there is no template entry.
func copyVolumeGroupProps(vgs []apiv1.VolumeGroup, volNumber int32) map[string]string {
	for i := range vgs {
		if vgs[i].VolumeNumber != volNumber {
			continue
		}

		if len(vgs[i].Props) == 0 {
			return nil
		}

		out := make(map[string]string, len(vgs[i].Props))
		maps.Copy(out, vgs[i].Props)

		return out
	}

	return nil
}

// rollbackSpawn best-effort cleans up a half-spawned RD. Errors are
// swallowed because we are already on an error path; the controller's
// reconciler will sweep stale RDs on next pass.
func rollbackSpawn(ctx context.Context, st store.Store, rdName string) {
	deleteCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	err := st.ResourceDefinitions().Delete(deleteCtx, rdName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		log.FromContext(ctx).WithName("rest").
			Error(err, "rollback spawn: delete RD", "rd", rdName)
	}
}
