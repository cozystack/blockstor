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

	rd, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if rd.Props == nil {
		rd.Props = map[string]string{}
	}

	rd.Props[drbdSharedSecretKey] = req.Passphrase

	err = s.Store.ResourceDefinitions().Update(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// drbdSharedSecretKey is the upstream LINSTOR property name we mirror
// so existing tooling and golinstor clients can read it back without
// extra translation.
//
//nolint:gosec // this is a property-name constant, not the secret value itself
const drbdSharedSecretKey = "DrbdOptions/Net/shared-secret"
