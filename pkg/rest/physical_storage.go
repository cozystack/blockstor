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

import "net/http"

// registerPhysicalStorage wires the `linstor physical-storage` and
// `linstor physical-storage create-device-pool` endpoints.
//
// Cozystack provisions storage pools out-of-band through Talos
// extensions / kubelet, so the device-pool create path is explicitly
// out of scope: returning 501 keeps `linstor` CLI from claiming
// success and fixes a piraeus-operator code path that would
// otherwise loop on a 404. The list endpoint surfaces an empty bag
// (we don't enumerate raw devices) so the CLI's
// `linstor physical-storage list` doesn't error.
func (s *Server) registerPhysicalStorage(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/physical-storage", handleEmptyPhysicalStorage)
	mux.HandleFunc("GET /v1/nodes/{node}/physical-storage",
		handleEmptyPhysicalStorage)
	mux.HandleFunc("POST /v1/physical-storage/{node}",
		handlePhysicalStorageCreateNotImplemented)
}

// handleEmptyPhysicalStorage returns the empty-pool envelope golinstor
// expects. The shape is `[]` for the cluster-wide list and a JSON
// object with a `nodes: {}` entry for the per-node variant; both
// decode cleanly when empty.
func handleEmptyPhysicalStorage(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []struct{}{})
}

// handlePhysicalStorageCreateNotImplemented surfaces 501 with a
// LINSTOR-shaped ApiCallRc body explaining the boundary. piraeus-
// operator's `LinstorSatelliteConfiguration.spec.storagePools` would
// otherwise retry the call indefinitely.
func handlePhysicalStorageCreateNotImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented,
		"physical-storage create is out of scope for blockstor; "+
			"provision storage pools via Talos extensions / static node config")
}
