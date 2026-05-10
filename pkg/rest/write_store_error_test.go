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
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// TestWriteStoreErrorMapsSentinels pins the sentinel→HTTP-status
// mapping every handler relies on. Three branches:
//
//	store.ErrNotFound       → 404
//	store.ErrAlreadyExists  → 409
//	any other error         → 500
//
// linstor-csi and golinstor classify retries by HTTP status code
// (5xx retryable, 4xx fatal). A regression that flipped any branch
// would silently change retry behaviour cluster-wide — ErrNotFound
// surfacing as 500 would loop csi forever on a missing resource;
// ErrAlreadyExists as 500 would loop on a name-collision retry.
func TestWriteStoreErrorMapsSentinels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want int
	}{
		{"NotFound", store.ErrNotFound, 404},
		{"AlreadyExists", store.ErrAlreadyExists, 409},
		{"opaque", errOpaqueStoreFailure, 500},
	}

	for _, c := range cases {
		rr := httptest.NewRecorder()
		writeStoreError(rr, c.err)

		if got := rr.Code; got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

var errOpaqueStoreFailure = errors.New("apiserver: timeout")
