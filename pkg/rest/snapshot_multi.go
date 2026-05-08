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

// registerSnapshotMulti wires `POST /v1/actions/snapshot/multi`. golinstor
// uses this to take an atomic group snapshot across several
// ResourceDefinitions in a single call. Cozystack does not run multi-RD
// applications that require a single consistency point on the LINSTOR
// side (each PVC is its own RD with its own snapshot lifecycle), so this
// is intentionally out-of-scope. Returning 501 keeps the contract clean
// and lets clients fall back to per-RD snapshots.
func (s *Server) registerSnapshotMulti(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/actions/snapshot/multi", handleSnapshotMultiNotImplemented)
}

func handleSnapshotMultiNotImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotImplemented,
		"multi-resource snapshot is not implemented; take per-RD snapshots instead")
}
