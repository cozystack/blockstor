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
	"maps"
	"net/http"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerResourceModify wires `PUT /v1/resource-definitions/{rd}/resources/{node}`
// — the per-Resource property-modify route python-linstor's
// `linstor r set-property <node> <rd> <key> <val>` (a.k.a.
// golinstor's `ResourceService.Modify`) calls. Without this route the
// CLI hits 404 and falls back to a python traceback; tests that drive
// Resource-scope DrbdOptions/* via the CLI have to work around the gap
// with an envtest client (see Group F `TestGroupFRSetPropertyDrbdNet`).
//
// Wire shape mirrors the RD / Node modify endpoints: a
// `GenericPropsModify` envelope (override_props + delete_props) that is
// MERGED — not REPLACED — onto the Resource CRD's `Spec.Props`. Other
// Resource fields (NodeName, Flags, layer data) are preserved verbatim
// so a property update can't accidentally clobber the placement.
func (s *Server) registerResourceModify(mux *http.ServeMux) {
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/resources/{node}",
		s.requireStore(s.handleResourceModify))
}

// handleResourceModify applies a GenericPropsModify (override_props /
// delete_props) onto Resource.Spec.Props for the (rd, node) replica.
// The dispatcher's effective-props chain (Controller→RG→RD→Resource)
// picks the merged map up on its next reconcile; the per-Resource rung
// is the highest-precedence scope, so a value written here wins over
// inherited keys.
//
// Idempotent on no-op input (empty override + empty delete) — the
// update still goes through so the CRD's resourceVersion bumps and any
// satellite watcher observes a fresh event. Mirrors the Node modify
// handler's behaviour.
func (s *Server) handleResourceModify(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	// Bug 163 (P0): use the modify-shaped envelope rather than the
	// bare GenericPropsModify so DisallowUnknownFields tolerates the
	// full Resource read-side shape on `curl GET | jq | curl PUT`.
	// Only the embedded override_props / delete_props /
	// delete_namespaces drive the merge.
	var patch apiv1.ResourceModify

	if !decodeJSON(w, r, &patch) {
		return
	}

	// Bug 204b: route through PatchResourceSpec so the override /
	// delete delta is re-applied to the freshly-fetched Resource on
	// every retry. The previous `Get → mutate → Update` path used
	// the wholesale, un-retried Resource Update, so concurrent
	// toggle-disk / r-activate / autoplace writers could silently
	// clobber the prop mutation.
	err := s.Store.Resources().PatchResourceSpec(r.Context(), rdName, node, func(res *apiv1.Resource) error {
		if res.Props == nil && (len(patch.OverrideProps) > 0 || len(patch.DeleteProps) > 0) {
			res.Props = map[string]string{}
		}

		// Override before delete — matches upstream LINSTOR
		// CtrlRscModifyApiCallHandler ordering and the sibling
		// handleControllerPropsModify / handleNodeUpdate behaviour.
		maps.Copy(res.Props, patch.OverrideProps)

		for _, k := range patch.DeleteProps {
			delete(res.Props, k)
		}

		return nil
	})
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource modified: " + rdName + " on node " + node,
	}})
}
