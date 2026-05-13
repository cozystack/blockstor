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

// registerRemotes wires the LINSTOR `remotes` endpoints. Cozystack
// doesn't use cross-cluster snapshot shipping, so we return an empty
// `RemoteList` envelope — golinstor's `client.RemoteList` is an
// object with typed-array fields, NOT a bare array. Returning a raw
// `[]` surfaces as `cannot unmarshal array into Go value of type
// client.RemoteList` on every snapshot-list call.
func (s *Server) registerRemotes(mux *http.ServeMux) {
	// `/v1/remotes` (no type suffix) returns the typed envelope —
	// golinstor's `RemoteService.GetAll()` decodes into RemoteList.
	mux.HandleFunc("GET /v1/remotes", handleRemoteEnvelope)
	// `/v1/remotes/{type}` returns a flat array of that type —
	// golinstor's GetAllLinstor / GetAllS3 / GetAllEbs decode into
	// `[]LinstorRemote` / `[]S3Remote` / `[]EbsRemote`. Returning
	// the envelope here breaks the decoder ("cannot unmarshal
	// object into Go slice").
	mux.HandleFunc("GET /v1/remotes/s3", handleEmptyRemoteArray)
	mux.HandleFunc("GET /v1/remotes/linstor", handleEmptyRemoteArray)
	mux.HandleFunc("GET /v1/remotes/ebs", handleEmptyRemoteArray)
}

// emptyRemoteList is upstream LINSTOR's `RemoteList` zero-value: an
// object with three named arrays, all empty. golinstor decodes the
// response body into this shape unconditionally.
type emptyRemoteList struct {
	S3Remotes      []map[string]string `json:"s3_remotes"`
	LinstorRemotes []map[string]string `json:"linstor_remotes"`
	EbsRemotes     []map[string]string `json:"ebs_remotes"`
}

func handleRemoteEnvelope(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, emptyRemoteList{
		S3Remotes:      []map[string]string{},
		LinstorRemotes: []map[string]string{},
		EbsRemotes:     []map[string]string{},
	})
}

// handleEmptyRemoteArray returns a bare `[]` for the type-specific
// endpoints. Cozystack doesn't use cross-cluster shipping so there
// are never any remotes configured.
func handleEmptyRemoteArray(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []map[string]string{})
}
