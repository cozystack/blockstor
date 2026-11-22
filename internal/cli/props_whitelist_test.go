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

package cli_test

import (
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// typoKey is the key the alpha.14 report set on a definition with
// `rd sp`: a typo of AutoSnapshot/Keep that no DRBD section knows, which
// the satellites rendered into a .res drbdadm refused on every node.
const typoKey = "DrbdOptions/AutoSnapshot/Keep"

// TestSetPropertyRefusesAnUnwhitelistedDrbdOption: this CLI writes the
// CRDs directly, so the whitelist REST applies has to hold here too.
func TestSetPropertyRefusesAnUnwhitelistedDrbdOption(t *testing.T) {
	t.Parallel()

	for name, argv := range map[string][]string{
		"definition":         {"rd", "sp", "pvc-x", typoKey, "2"},
		"controller":         {"c", "sp", typoKey, "2"},
		"definition value":   {"rd", "sp", "pvc-x", "DrbdOptions/Resource/quorum-minimum-redundancy", "bogus"},
		"definition net key": {"rd", "sp", "pvc-x", "DrbdOptions/Net/protcol", "C"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			app, backend, errBuf := newKubeAppWithStore(t)

			err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-x"})
			if err != nil {
				t.Fatalf("seed: %v", err)
			}

			if got := app.Run(t.Context(), argv); got == 0 {
				t.Fatalf("exit 0: %v was stored", argv)
			}

			if !strings.Contains(errBuf.String(), argv[len(argv)-2]) {
				t.Errorf("the refusal does not name the key: %s", errBuf.String())
			}

			ctrl, err := backend.ControllerProps().Get(t.Context())
			if err != nil {
				t.Fatalf("read controller props: %v", err)
			}

			rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-x")
			if err != nil {
				t.Fatalf("read the definition: %v", err)
			}

			if len(ctrl) != 0 || len(rd.Props) != 0 {
				t.Errorf("the refused write stored props: controller %v, definition %v", ctrl, rd.Props)
			}
		})
	}
}

// TestSetPropertyStillDeletesAnUnwhitelistedKey: a key stored before the
// check is cleaned by deleting it, which is not judged.
func TestSetPropertyStillDeletesAnUnwhitelistedKey(t *testing.T) {
	t.Parallel()

	app, backend, errBuf := newKubeAppWithStore(t)

	err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "pvc-x", Props: map[string]string{typoKey: "2"},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if got := app.Run(t.Context(), []string{"rd", "dp", "pvc-x", typoKey}); got != 0 {
		t.Fatalf("delete-property exit %d (stderr: %s)", got, errBuf.String())
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("read the definition: %v", err)
	}

	if _, kept := rd.Props[typoKey]; kept {
		t.Errorf("the key survived its delete: %v", rd.Props)
	}
}

// TestDrbdOptionsRefusesAValueTheRuleRefuses: drbd-options names a known
// knob, but its value is the operator's and is judged like set-property's.
func TestDrbdOptionsRefusesAValueTheRuleRefuses(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDefinition)

	if got := app.Run(t.Context(), []string{"rd", "drbd-options", "--max-buffers", "8", "pvc-x"}); got == 0 {
		t.Fatalf("exit 0: max-buffers 8 was stored")
	}

	if !strings.Contains(errBuf.String(), "(32 - 131072)") {
		t.Errorf("the refusal does not say what the rule accepts: %s", errBuf.String())
	}

	def, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	if _, stored := def.Props["DrbdOptions/Net/max-buffers"]; stored {
		t.Errorf("the refused value was stored: %v", def.Props)
	}
}

// TestSetPropertyAcceptsBlockstorsOwnKeys: the tiebreaker switch and the
// node port and minor ranges are blockstor's own keys, documented as the
// way to set them, so the whitelist must not refuse them.
func TestSetPropertyAcceptsBlockstorsOwnKeys(t *testing.T) {
	t.Parallel()

	app, backend, errBuf := newKubeAppWithStore(t)

	err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-x"})
	if err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	err = backend.Nodes().Create(t.Context(), &apiv1.Node{Name: "node-1"})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}

	for _, argv := range [][]string{
		{"rd", "sp", "pvc-x", "DrbdOptions/AutoAddQuorumTiebreaker", "false"},
		{"n", "sp", "node-1", "DrbdOptions/TcpPortRange", "21000-21999"},
		{"n", "sp", "node-1", "DrbdOptions/MinorNrRange", "21000-29999"},
	} {
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Errorf("%v: exit %d (stderr: %s)", argv, got, errBuf.String())
		}
	}

	node, err := backend.Nodes().Get(t.Context(), "node-1")
	if err != nil {
		t.Fatalf("read the node: %v", err)
	}

	if node.Props["DrbdOptions/TcpPortRange"] != "21000-21999" || node.Props["DrbdOptions/MinorNrRange"] != "21000-29999" {
		t.Errorf("node ranges not stored: %v", node.Props)
	}
}

// TestPropertyWritesStoreBooleansAsLinstorDoes: LINSTOR stores an accepted
// Yes as yes. Stored as given, the satellite leaves it out of the .res,
// since drbdadm takes yes and no only.
func TestPropertyWritesStoreBooleansAsLinstorDoes(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDefinition)

	for _, argv := range [][]string{
		{"rd", "sp", "pvc-x", "DrbdOptions/Net/load-balance-paths", "Yes"},
		{"rd", "drbd-options", "--tcp-cork", "NO", "pvc-x"},
	} {
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v: exit %d (stderr: %s)", argv, got, errBuf.String())
		}
	}

	def, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	for key, want := range map[string]string{
		"DrbdOptions/Net/load-balance-paths": "yes",
		"DrbdOptions/Net/tcp-cork":           "no",
	} {
		if def.Props[key] != want {
			t.Errorf("%s: stored %q, want %q", key, def.Props[key], want)
		}
	}
}
