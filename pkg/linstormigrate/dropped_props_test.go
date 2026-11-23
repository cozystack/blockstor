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

package linstormigrate

import (
	"strings"
	"testing"
)

// TestDroppedPropsAreReported: a property no blockstor object carries is
// named in the report, by instance and key, without its value. Props of
// an object that did not convert stay out of it: that object's own line
// already says it was skipped.
func TestDroppedPropsAreReported(t *testing.T) {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-GONE", ResourceDspName: "pvc-gone", ResourceFlags: resourceFlagDelete, UUID: "rd-gone"},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/NODE_CONNS/NODE-A/NODE-B", PropKey: "DrbdOptions/Net/protocol", PropValue: "A"},
			{PropsInstance: "/VLM_GRPS/SC/0", PropKey: "DrbdOptions/Disk/al-extents", PropValue: "6007"},
			{PropsInstance: "/RSC_DFNS/PVC-GONE", PropKey: "Aux/skipped", PropValue: "x"},
			{PropsInstance: "/RSC_DFNS/PVC-GONE", PropKey: "DrbdOptions/EncryptPassphrase", PropValue: "do-not-print-me"},
		},
	}

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	for _, want := range []string{
		"LINSTOR props /NODE_CONNS/NODE-A/NODE-B: not migrated, no blockstor object carries them: DrbdOptions/Net/protocol",
		"LINSTOR props /VLM_GRPS/SC/0: not migrated, no blockstor object carries them: DrbdOptions/Disk/al-extents",
	} {
		if !hasWarning(res, want) {
			t.Errorf("missing %q; warnings: %v", want, res.Warnings)
		}
	}

	if hasWarning(res, "Aux/skipped") {
		t.Errorf("props of a skipped definition were listed on top of its skip; warnings: %v", res.Warnings)
	}

	for _, w := range res.Warnings {
		if strings.Contains(w, "do-not-print-me") {
			t.Errorf("a passphrase value reached the report: %q", w)
		}
	}
}

// TestWithheldPassphrasePropsAreNamed: a property NewPropsIndex keeps out
// because it would carry a passphrase is named, key only, so it does not
// vanish from the report either.
func TestWithheldPassphrasePropsAreNamed(t *testing.T) {
	dump := &Dump{
		PropsContainers: []PropsContainerRow{
			{PropsInstance: PropsInstanceController, PropKey: "DrbdOptions/EncryptPassphrase", PropValue: "do-not-print-me"},
		},
	}

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if !hasWarning(res, "LINSTOR props /CTRL: not migrated, would carry a passphrase in plaintext: DrbdOptions/EncryptPassphrase") {
		t.Errorf("withheld passphrase key was not named; warnings: %v", res.Warnings)
	}

	for _, w := range res.Warnings {
		if strings.Contains(w, "do-not-print-me") {
			t.Errorf("a passphrase value reached the report: %q", w)
		}
	}
}

// TestNodeLevelDRBDOptionsAreReported: LINSTOR resolves DRBD options on
// the node, between the group and the satellite bag. blockstor has no
// such rung, so the key reaches the Node and never a .res; the report
// has to say so.
func TestNodeLevelDRBDOptionsAreReported(t *testing.T) {
	res := convertFixture(t)

	if !hasWarning(res, "node node-b: DrbdOptions/Net/sndbuf-size carried onto the object, but blockstor does not resolve DRBD options at this level") {
		t.Errorf("node-level DRBD option not reported; warnings: %v", res.Warnings)
	}
}
