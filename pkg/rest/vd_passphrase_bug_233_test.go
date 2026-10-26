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
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// `linstor vd modify-passphrase` (PUT .../volume-definitions/{vlmNr}/
// encryption-passphrase) is refused. blockstor has no per-volume LUKS
// keys: every LUKS volume uses the cluster passphrase in the Secret. The
// door used to store the value in the volume definition's properties, in
// plaintext on the CR, where nothing read it.

// seedVDForBug233 seeds an RD + VD pair so the PUT under test has a
// real target to rotate. Centralised so the test cases don't drift on
// the seed-shape invariants.
func seedVDForBug233(t *testing.T, st store.Store) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd-luks",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "rd-luks", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}
}

// Every body shape is refused with a message that points at the cluster
// passphrase, and nothing lands in the volume definition's properties.
func TestBug233VDPassphraseIsRefused(t *testing.T) {
	for name, raw := range map[string][]byte{
		"wrapped":     mustMarshalBug233(map[string]string{"new_passphrase": "rotated-secret"}),
		"alias":       mustMarshalBug233(map[string]string{"passphrase": "rotated-secret"}),
		"bare string": mustMarshalBug233("rotated-bare-secret"),
		"empty":       mustMarshalBug233(map[string]string{"new_passphrase": ""}),
		"not json":    []byte("not json"),
	} {
		t.Run(name, func(t *testing.T) {
			st := store.NewInMemory()
			seedVDForBug233(t, st)

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPut(t,
				base+"/v1/resource-definitions/rd-luks/volume-definitions/0/encryption-passphrase", raw)
			defer func() { _ = resp.Body.Close() }()

			got, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("PUT VD passphrase: got %d, want 400 (body=%q)", resp.StatusCode, got)
			}

			var rcs []apiv1.APICallRc
			if err := json.Unmarshal(got, &rcs); err != nil || len(rcs) == 0 ||
				!strings.Contains(rcs[0].Message, "encryption create-passphrase") {
				t.Errorf("envelope %q, want one pointing at encryption create-passphrase", got)
			}

			vd, err := st.VolumeDefinitions().Get(t.Context(), "rd-luks", 0)
			if err != nil {
				t.Fatalf("get VD: %v", err)
			}

			for key := range vd.Props {
				if strings.Contains(strings.ToLower(key), "passphrase") {
					t.Errorf("the VD carries %s after a refused PUT", key)
				}
			}
		})
	}
}

// TestBug233VDPassphraseUnknownRD: a missing parent RD MUST 404 (not
// 405). Mirrors every other per-RD route's typed-not-found surface.
func TestBug233VDPassphraseUnknownRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "rotated-secret"})

	resp := httpPut(t,
		base+"/v1/resource-definitions/ghost/volume-definitions/0/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("PUT unknown RD: got %d, want 404", resp.StatusCode)
	}
}

// mustMarshalBug233 is a tiny test helper that json.Marshals or
// fails the test. Keeps the cases table above concise.
func mustMarshalBug233(v any) []byte {
	out, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return out
}
