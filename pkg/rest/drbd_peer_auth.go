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

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/store"
)

// peerAuthProps returns the peer authentication props carries.
func peerAuthProps(props map[string]string) map[string]string {
	out := map[string]string{}

	for _, key := range []string{drbd.SharedSecretProp, drbd.CramHMACAlgProp} {
		if value, ok := props[key]; ok {
			out[key] = value
		}
	}

	return out
}

// prepareDRBDLayer resolves, for a definition a create door is about to
// write, the peer slots (store.ResolvePeerSlots) and the peer
// authentication (store.SeedPeerAuthentication) of its DRBD layer, and
// answers the request when either fails. A drbd_secret the .res cannot
// carry is the caller's mistake, the rest is ours. Returns false once it
// has written the response.
func (s *Server) prepareDRBDLayer(
	w http.ResponseWriter, r *http.Request, rd *apiv1.ResourceDefinition, peerSlots int32, secret string,
) bool {
	err := store.ResolvePeerSlots(r.Context(), s.Store, rd, peerSlots)
	if err != nil {
		writeStoreError(w, err)

		return false
	}

	err = store.SeedPeerAuthentication(r.Context(), s.Store, rd, secret)
	if err == nil {
		return true
	}

	if errors.Is(err, drbd.ErrUnrepresentableValue) {
		writeError(w, http.StatusBadRequest, "drbd_secret: "+err.Error())

		return false
	}

	writeStoreError(w, err)

	return false
}
