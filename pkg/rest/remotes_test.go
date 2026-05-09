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
	"encoding/json"
	"net/http"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// TestRemotesEnvelopeShape pins the empty-RemoteList response: the
// body must be a JSON object with three named empty arrays
// (s3_remotes, linstor_remotes, ebs_remotes), NOT a bare `[]`.
//
// Why this matters: golinstor's client.RemoteList is decoded
// unconditionally on every snapshot-list call. A bare-array response
// errors with "cannot unmarshal array into Go value of type
// client.RemoteList" — a regression that would break every
// linstor-csi DeleteSnapshot flow because the CSI driver surfaces
// the upstream snapshot-list error verbatim.
func TestRemotesEnvelopeShape(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// All four registered paths must surface the same envelope.
	for _, path := range []string{
		"/v1/remotes",
		"/v1/remotes/s3",
		"/v1/remotes/linstor",
		"/v1/remotes/ebs",
	} {
		t.Run(path, func(t *testing.T) {
			resp := httpGet(t, base+path)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status: got %d, want 200", resp.StatusCode)
			}

			// Decode into golinstor's expected shape, not into
			// `[]any` — that's the actual contract.
			var got emptyRemoteList
			if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
				t.Fatalf("decode: %v (response is bare array, not RemoteList object)", err)
			}

			if got.S3Remotes == nil {
				t.Errorf("s3_remotes: nil, want []")
			}

			if got.LinstorRemotes == nil {
				t.Errorf("linstor_remotes: nil, want []")
			}

			if got.EbsRemotes == nil {
				t.Errorf("ebs_remotes: nil, want []")
			}
		})
	}
}
