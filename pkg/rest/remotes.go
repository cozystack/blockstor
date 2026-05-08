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
	mux.HandleFunc("GET /v1/remotes", handleEmptyRemotes)
	mux.HandleFunc("GET /v1/remotes/s3", handleEmptyRemotes)
	mux.HandleFunc("GET /v1/remotes/linstor", handleEmptyRemotes)
	mux.HandleFunc("GET /v1/remotes/ebs", handleEmptyRemotes)
}

// emptyRemoteList is upstream LINSTOR's `RemoteList` zero-value: an
// object with three named arrays, all empty. golinstor decodes the
// response body into this shape unconditionally.
type emptyRemoteList struct {
	S3Remotes      []map[string]string `json:"s3_remotes"`
	LinstorRemotes []map[string]string `json:"linstor_remotes"`
	EbsRemotes     []map[string]string `json:"ebs_remotes"`
}

func handleEmptyRemotes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, emptyRemoteList{
		S3Remotes:      []map[string]string{},
		LinstorRemotes: []map[string]string{},
		EbsRemotes:     []map[string]string{},
	})
}
