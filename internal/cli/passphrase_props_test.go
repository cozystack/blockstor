// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/passphrase"
)

// The CLI writes the CRs itself, so it is a door REST's guard does not cover.
// It refuses the same write: the master passphrase never goes into a
// property, under any of its keys or spellings, on any noun.
func TestSetPropertyRefusesThePlaintextPassphrase(t *testing.T) {
	t.Parallel()

	for name, argv := range map[string][]string{
		"controller canonical": {"c", "sp", passphrase.PropKeyCanonical, "prop-value"},
		"controller case":      {"c", "sp", "drbdoptions/ENCRYPTPASSPHRASE", "prop-value"},
		"definition alias":     {"rd", "sp", "pvc-x", passphrase.PropKeyLegacy, "prop-value"},
		"definition wire key":  {"rd", "sp", "pvc-x", passphrase.WirePropKey, "prop-value"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			app, backend, errBuf := newKubeAppWithStore(t)

			err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-x"})
			if err != nil {
				t.Fatalf("seed: %v", err)
			}

			if got := app.Run(t.Context(), argv); got == 0 {
				t.Fatalf("exit 0: %v stored the passphrase in a property", argv)
			}

			if !strings.Contains(errBuf.String(), "encryption create-passphrase") {
				t.Errorf("the refusal does not point at `encryption create-passphrase`: %s", errBuf.String())
			}

			ctrl, err := backend.ControllerProps().Get(t.Context())
			if err != nil {
				t.Fatalf("read controller props: %v", err)
			}

			rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-x")
			if err != nil {
				t.Fatalf("read the definition: %v", err)
			}

			for _, props := range []map[string]string{ctrl, rd.Props} {
				for key := range props {
					if passphrase.IsPlaintextPropKey(key) {
						t.Errorf("the refused write stored %q", key)
					}
				}
			}
		})
	}
}

// Deleting a passphrase property, put there with kubectl, stays allowed:
// that is how an operator clears one on the way to the Secret.
func TestSetPropertyDeletesThePlaintextPassphrase(t *testing.T) {
	t.Parallel()

	app, backend, errBuf := newKubeAppWithStore(t)

	err := backend.ControllerProps().PatchProps(t.Context(), func(props map[string]string) error {
		props[passphrase.PropKeyCanonical] = "prop-value"

		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if got := app.Run(t.Context(), []string{"c", "sp", passphrase.PropKeyCanonical, ""}); got != 0 {
		t.Fatalf("delete through an empty value: exit %d (stderr: %s)", got, errBuf.String())
	}

	ctrl, err := backend.ControllerProps().Get(t.Context())
	if err != nil {
		t.Fatalf("read controller props: %v", err)
	}

	if _, still := ctrl[passphrase.PropKeyCanonical]; still {
		t.Errorf("the property survived its delete: %v", ctrl)
	}
}
