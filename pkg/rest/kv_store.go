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

// registerKeyValueStore wires `/v1/key-value-store` endpoints. The
// LINSTOR REST contract exposes a generic KV bag for components
// that need persistent state without their own table — in
// practice only `linstor-csi` uses one instance
// (`csi-backup-mapping`) and only when L2L backup remotes are
// configured. Production clusters that don't ship snapshots
// between LINSTOR sites never write here.
//
// blockstor exposes the endpoints as a stub: GETs return empty,
// writes are accepted as no-ops so golinstor-based tooling that
// probes the contract doesn't trip on 404/405. If a real KV
// consumer ever shows up the implementation can grow back via a
// CRD or ConfigMap fallback; today it's not worth the operational
// surface.
func (s *Server) registerKeyValueStore(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/key-value-store", s.requireStore(handleKVList))
	mux.HandleFunc("GET /v1/key-value-store/{instance}", s.requireStore(handleKVGet))
	mux.HandleFunc("POST /v1/key-value-store/{instance}", s.requireStore(handleKVNoOp))
	mux.HandleFunc("PUT /v1/key-value-store/{instance}", s.requireStore(handleKVNoOp))
	mux.HandleFunc("DELETE /v1/key-value-store/{instance}", s.requireStore(handleKVNoOp))
}

// handleKVList returns an empty list — matches the on-cluster
// reality of a LINSTOR install without active KV consumers.
func handleKVList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []apiv1.KV{})
}

// handleKVGet returns the named instance as an empty KV envelope.
// Callers that probe a specific instance see "exists but empty"
// rather than 404 — same shape `linstor c kv show <name>` would
// render against a fresh upstream LINSTOR.
func handleKVGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, apiv1.KV{Name: r.PathValue("instance")})
}

// handleKVNoOp accepts the write but does not persist it.
// Returning 200 (not 501) keeps `linstor c kv set` happy for the
// rare interactive operator probe.
func handleKVNoOp(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
