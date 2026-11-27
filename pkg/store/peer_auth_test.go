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

package store_test

import (
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/store"
)

// A definition created with nothing set gets a secret of its own and the
// sha1 digest, as every definition LINSTOR creates does.
func TestSeedPeerAuthenticationStampsSecretAndDigest(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	first := &apiv1.ResourceDefinition{Name: "pvc-a"}
	second := &apiv1.ResourceDefinition{Name: "pvc-b"}

	for _, rd := range []*apiv1.ResourceDefinition{first, second} {
		if err := store.SeedPeerAuthentication(t.Context(), st, rd, ""); err != nil {
			t.Fatalf("SeedPeerAuthentication(%s): %v", rd.Name, err)
		}

		if len(rd.Props[drbd.SharedSecretProp]) != 20 {
			t.Errorf("%s: secret of length %d, want a generated 20", rd.Name, len(rd.Props[drbd.SharedSecretProp]))
		}

		if rd.Props[drbd.CramHMACAlgProp] != drbd.DefaultCramHMACAlg {
			t.Errorf("%s: digest %q, want %q", rd.Name, rd.Props[drbd.CramHMACAlgProp], drbd.DefaultCramHMACAlg)
		}
	}

	if first.Props[drbd.SharedSecretProp] == second.Props[drbd.SharedSecretProp] {
		t.Errorf("two definitions share one secret")
	}
}

// The request's drbd_secret is the definition's secret, as LINSTOR takes
// it, and it replaces one the props were copied with.
func TestSeedPeerAuthenticationTakesTheRequestedSecret(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	rd := &apiv1.ResourceDefinition{Name: "pvc-a", Props: map[string]string{"drbdoptions/net/shared-secret": "copied"}}

	if err := store.SeedPeerAuthentication(t.Context(), st, rd, "fixture-secret"); err != nil {
		t.Fatalf("SeedPeerAuthentication: %v", err)
	}

	if len(rd.Props) != 2 || rd.Props[drbd.SharedSecretProp] != "fixture-secret" {
		t.Errorf("got %v, want the requested secret alone", rd.Props)
	}
}

// A secret the .res could not carry is refused before anything is written.
func TestSeedPeerAuthenticationRefusesAnUnrepresentableSecret(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	rd := &apiv1.ResourceDefinition{Name: "pvc-a"}

	err := store.SeedPeerAuthentication(t.Context(), st, rd, "two\nlines")
	if !errors.Is(err, drbd.ErrUnrepresentableValue) {
		t.Errorf("got %v, want ErrUnrepresentableValue", err)
	}
}

// What an operator set is kept, on the definition or anywhere it
// inherits from; only the missing half is stamped.
func TestSeedPeerAuthenticationKeepsWhatAnOperatorSet(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		rdProps, rgProps, ctrlProps map[string]string
		wantSecret, wantDigest      string
	}{
		"definition secret": {
			rdProps:    map[string]string{drbd.SharedSecretProp: "own"},
			wantSecret: "own", wantDigest: drbd.DefaultCramHMACAlg,
		},
		"definition digest": {
			rdProps:    map[string]string{drbd.CramHMACAlgProp: "sha256"},
			wantSecret: "generated", wantDigest: "sha256",
		},
		"group secret and digest": {
			rgProps: map[string]string{drbd.SharedSecretProp: "group", drbd.CramHMACAlgProp: "sha256"},
		},
		"controller digest": {
			ctrlProps:  map[string]string{drbd.CramHMACAlgProp: "sha512"},
			wantSecret: "generated",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg", Props: tc.rgProps}); err != nil {
				t.Fatalf("seed group: %v", err)
			}

			if tc.ctrlProps != nil {
				if err := st.ControllerProps().Set(ctx, tc.ctrlProps); err != nil {
					t.Fatalf("seed controller: %v", err)
				}
			}

			rd := &apiv1.ResourceDefinition{Name: "pvc-a", ResourceGroupName: "rg", Props: tc.rdProps}

			if err := store.SeedPeerAuthentication(ctx, st, rd, ""); err != nil {
				t.Fatalf("SeedPeerAuthentication: %v", err)
			}

			secret := rd.Props[drbd.SharedSecretProp]
			if tc.wantSecret == "generated" {
				if len(secret) != 20 {
					t.Errorf("secret of length %d, want a generated one", len(secret))
				}
			} else if secret != tc.wantSecret {
				t.Errorf("secret %q, want %q", secret, tc.wantSecret)
			}

			if got := rd.Props[drbd.CramHMACAlgProp]; got != tc.wantDigest {
				t.Errorf("digest %q, want %q", got, tc.wantDigest)
			}
		})
	}
}

// The satellite renders a DrbdOptions key only in its canonical spelling,
// so a secret or digest under another spelling does not stop the stamp:
// it would never reach the .res.
func TestSeedPeerAuthenticationIgnoresAnOffSpelling(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	rd := &apiv1.ResourceDefinition{Name: "pvc-a", Props: map[string]string{
		"drbdoptions/net/shared-secret": "off-spelling",
		"drbdoptions/net/cram-hmac-alg": "sha256",
	}}

	if err := store.SeedPeerAuthentication(t.Context(), st, rd, ""); err != nil {
		t.Fatalf("SeedPeerAuthentication: %v", err)
	}

	if len(rd.Props[drbd.SharedSecretProp]) != 20 || rd.Props[drbd.CramHMACAlgProp] != drbd.DefaultCramHMACAlg {
		t.Errorf("the canonical secret and digest were not stamped next to an off-spelling")
	}
}
