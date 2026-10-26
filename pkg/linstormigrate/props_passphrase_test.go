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

import "testing"

// The generated YAML is applied with kubectl, past the REST guard, so a
// passphrase LINSTOR kept in a property never makes it into the index the
// converter copies properties from, under any of its keys.
func TestPropsIndexLeavesPassphrasePropertiesOut(t *testing.T) {
	t.Parallel()

	idx := NewPropsIndex([]PropsContainerRow{
		{PropsInstance: PropsInstanceController, PropKey: "DrbdOptions/EncryptPassphrase", PropValue: "ctrl-secret"},
		{PropsInstance: PropsInstanceController, PropKey: "DrbdOptions/Net/max-buffers", PropValue: "8000"},
		{PropsInstance: "/RSC_DFNS/PVC-A", PropKey: "DrbdOptions/Encryption/passphrase", PropValue: "rd-secret"},
		{PropsInstance: "/VLM_DFNS/PVC-A/0", PropKey: "DrbdOptions/Encrypt/Passphrase", PropValue: "vd-secret"},
		{PropsInstance: "/RSCS/NODE-A/PVC-A", PropKey: "LuksPassphrase", PropValue: "rsc-secret"},
	})

	if _, ok := idx.Controller["DrbdOptions/EncryptPassphrase"]; ok || idx.Controller["DrbdOptions/Net/max-buffers"] != "8000" {
		t.Errorf("controller props = %v, want the passphrase left out and the rest kept", idx.Controller)
	}

	for name, bag := range map[string]map[string]string{
		"definition": idx.ResourceDefinition("PVC-A"),
		"volume":     idx.VolumeDefinition("PVC-A", "0"),
		"replica":    idx.Resource("NODE-A", "PVC-A"),
	} {
		if len(bag) != 0 {
			t.Errorf("%s props = %v, want the passphrase left out", name, bag)
		}
	}
}
