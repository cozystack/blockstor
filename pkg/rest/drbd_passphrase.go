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
	"net/http"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// drbdPassphraseRequest carries the per-RD DRBD shared secret.
type drbdPassphraseRequest struct {
	Passphrase string `json:"passphrase"`
}

// registerDRBDPassphrase wires the per-RD shared-secret endpoint. The
// secret is stored as a property on the ResourceDefinition so it
// flows through the same drbd_options channel ApplyResources already
// serialises into the satellite-side .res file.
func (s *Server) registerDRBDPassphrase(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/encryption-passphrase",
		s.requireStore(s.handleDRBDPassphraseSet))
}

// handleDRBDPassphraseSet writes the secret onto the RD's props under
// the upstream-compatible `DrbdOptions/Net/shared-secret` key.
func (s *Server) handleDRBDPassphraseSet(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	var req drbdPassphraseRequest

	if !decodeJSON(w, r, &req) {
		return
	}

	if req.Passphrase == "" {
		writeError(w, http.StatusBadRequest, "passphrase is required")

		return
	}

	// Bug 204b shape: typed-Patch with retry-on-conflict so a
	// reconciler write on the RD between the read and the write
	// re-applies the secret onto fresh state instead of surfacing
	// a 409 to the operator.
	err := s.Store.ResourceDefinitions().PatchResourceDefinitionSpec(r.Context(), rdName,
		func(rd *apiv1.ResourceDefinition) error {
			if rd.Props == nil {
				rd.Props = map[string]string{}
			}

			rd.Props[drbdSharedSecretKey] = req.Passphrase

			return nil
		})
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Bug 374: emit `[]ApiCallRc` envelope on 200 instead of a bare
	// WriteHeader. python-linstor 1.27.1 (Bug 129) and golinstor
	// (Bug 45) unconditionally json-decode every non-204 2xx response
	// and crash with "Expecting value: line 1 column 1 (char 0)" on
	// an empty body. Sibling props-write paths
	// (handleControllerPropsModify, mutateResourceFlag) all emit a
	// MASK_INFO envelope so the CLI surfaces a clean success line.
	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "DRBD shared secret set on resource definition: " + rdName,
		ObjRefs: map[string]string{
			objRefRscDfn: rdName,
		},
	}})
}

// drbdSharedSecretKey is the upstream LINSTOR property name we mirror
// so existing tooling and golinstor clients can read it back without
// extra translation.
//
//nolint:gosec // this is a property-name constant, not the secret value itself
const drbdSharedSecretKey = "DrbdOptions/Net/shared-secret"
