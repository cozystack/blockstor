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
	"strconv"
)

// handleVDPassphraseRotate serves `linstor vd modify-passphrase` (PUT
// .../volume-definitions/{vlmNr}/encryption-passphrase) and refuses it.
// blockstor has no per-volume LUKS keys: every LUKS volume is formatted
// and opened with the cluster passphrase in the Secret. The door used to
// store the value in the volume definition's properties, in plaintext on
// the CR, where nothing read it. A missing definition or volume still
// answers 404 first.
func (s *Server) handleVDPassphraseRotate(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	vlmNrRaw := r.PathValue("vlmNr")

	vlmNr, err := strconv.ParseInt(vlmNrRaw, 10, 32)
	if err != nil {
		writeError(w, http.StatusBadRequest,
			"invalid vlmNr path segment: "+vlmNrRaw+" is not an integer")

		return
	}

	_, err = s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	_, err = s.Store.VolumeDefinitions().Get(r.Context(), rdName, int32(vlmNr))
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeError(w, http.StatusBadRequest,
		"per-volume LUKS passphrases are not supported: every LUKS volume uses the cluster passphrase, "+
			"kept in a Secret by `linstor encryption create-passphrase`")
}
