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

// TestVolumeDefinitionDiskOptionsAreNotReportedAsUnrendered: the satellite
// renders a volume definition's DrbdOptions/Disk/* inside its volume block
// and reads auto-rs-discard-granularity there, so the report must not tell
// the operator they are lost. Any other DRBD option on the volume
// definition still never reaches a .res and is still reported.
func TestVolumeDefinitionDiskOptionsAreNotReportedAsUnrendered(t *testing.T) {
	t.Parallel()

	res := convertTwoReplicas(t, withProps(stackDump(`["DRBD","STORAGE"]`),
		PropsContainerRow{PropsInstance: "/VLM_DFNS/PVC-S/0", PropKey: "DrbdOptions/Disk/block-size", PropValue: "4096"},
		PropsContainerRow{PropsInstance: "/VLM_DFNS/PVC-S/0", PropKey: "DrbdOptions/Disk/rs-discard-granularity", PropValue: "65536"},
		PropsContainerRow{PropsInstance: "/VLM_DFNS/PVC-S/0", PropKey: "DrbdOptions/auto-rs-discard-granularity", PropValue: "false"},
		PropsContainerRow{PropsInstance: "/VLM_DFNS/PVC-S/0", PropKey: "DrbdOptions/Net/sndbuf-size", PropValue: "0"},
	))

	if len(res.ResourceDefinitions) != 1 {
		t.Fatalf("converted %d definitions, want 1; warnings %v", len(res.ResourceDefinitions), res.Warnings)
	}

	const line = "volume definition pvc-s/0: DrbdOptions/Net/sndbuf-size carried onto the object, but blockstor does not resolve DRBD options at this level"
	if !hasWarning(res, line) {
		t.Errorf("the net option on the volume definition is not reported; warnings %v", res.Warnings)
	}

	for _, w := range res.Warnings {
		if strings.Contains(w, "block-size") || strings.Contains(w, "rs-discard-granularity") {
			t.Errorf("a volume definition option the satellite renders is reported as lost: %q", w)
		}
	}

	props := res.ResourceDefinitions[0].Spec.VolumeDefinitions[0].Props
	if props["DrbdOptions/Disk/block-size"] != "4096" {
		t.Errorf("block-size was not carried onto the volume definition: %v", props)
	}
}
