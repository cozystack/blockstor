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

// LINSTOR keeps a volume definition flagged DELETE until every satellite
// confirmed deleting its volume, and its VolumeDefinition DELETE bit is
// 1<<0. The migration carries that delete in flight under a deleteID made
// from the definition's UUID, so blockstor's satellites finish it.
func TestAVolumeDefinitionMarkedDeleteIsCarried(t *testing.T) {
	t.Parallel()

	dump := stackDump(`["DRBD","STORAGE"]`)
	dump.VolumeDefinitions[0].VlmFlags = 1
	dump.VolumeDefinitions = append(dump.VolumeDefinitions, VolumeDefinitionRow{ResourceName: "PVC-S", VlmNr: 1, VlmSize: 1048576, UUID: "vd-s1"})

	res := convertTwoReplicas(t, dump)
	if len(res.ResourceDefinitions) != 1 {
		t.Fatalf("converted %d definitions, want 1; warnings %v", len(res.ResourceDefinitions), res.Warnings)
	}

	vds := res.ResourceDefinitions[0].Spec.VolumeDefinitions
	assertDeletedThenLive(t, vds)

	if vds[0].DeleteID != "linstor-vd-s" {
		t.Errorf("deleteID = %q, want one made from the LINSTOR UUID", vds[0].DeleteID)
	}

	if !hasWarning(res, "PVC-S/0: marked DELETE in the source cluster — carried") &&
		!hasWarning(res, "pvc-s/0: marked DELETE in the source cluster — carried") {
		t.Errorf("no warning about the carried delete: %v", res.Warnings)
	}
}

// Bit 1<<1 of a volume definition is ENCRYPTED, not DELETE: such a volume
// converts as a live one.
func TestAnEncryptedVolumeDefinitionIsNotTakenForDeleted(t *testing.T) {
	t.Parallel()

	dump := stackDump(`["DRBD","STORAGE"]`)
	dump.VolumeDefinitions[0].VlmFlags = 1 << 1

	res := convertTwoReplicas(t, dump)
	if len(res.ResourceDefinitions) != 1 {
		t.Fatalf("converted %d definitions, want 1; warnings %v", len(res.ResourceDefinitions), res.Warnings)
	}

	vds := res.ResourceDefinitions[0].Spec.VolumeDefinitions
	if len(vds) != 1 || vds[0].Deleting() {
		t.Errorf("volumes %+v, want volume 0 live", vds)
	}
}
