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

package k8s

import (
	"testing"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestDRBDOptionUnsetRevertsTyped is the C3 `--unset-<opt>` regression.
// `linstor rd drbd-options --unset-protocol <rd>` sends
// delete_props:["DrbdOptions/Net/protocol"]. The store mutate closure
// runs in wire vocabulary (typed DRBDOptions re-flattened into a Props
// bag), deletes the key, then re-transcodes back into the CRD. After
// the round-trip the typed Net.Protocol field must be cleared so the
// resource reverts to the inherited / DRBD-default value rather than
// keeping the previously-pinned override.
func TestDRBDOptionUnsetRevertsTyped(t *testing.T) {
	// Start from a CRD that pinned protocol=A in its typed slot, as
	// `rd drbd-options --protocol A` would have persisted it.
	crd := &crdv1alpha1.ResourceDefinition{
		Spec: crdv1alpha1.ResourceDefinitionSpec{
			DRBDOptions: &crdv1alpha1.DRBDOptions{
				Net: &crdv1alpha1.DRBDNetOptions{
					Protocol:   "A",
					MaxBuffers: int32PtrK8s(8192),
				},
			},
		},
	}

	// Surface as wire shape (the vocabulary the REST/CLI delete runs in).
	wire := crdToWireRD(crd)

	if wire.Props["DrbdOptions/Net/protocol"] != "A" {
		t.Fatalf("precondition: expected protocol=A in flattened wire props, got %q",
			wire.Props["DrbdOptions/Net/protocol"])
	}

	// `--unset-protocol` → delete the flattened key (mirrors the
	// `delete(rd.Props, k)` in PatchResourceDefinitionSpec's closure).
	delete(wire.Props, "DrbdOptions/Net/protocol")

	// Re-transcode back into the CRD spec.
	spec := wireToCRDRDSpec(&wire)

	if spec.DRBDOptions == nil || spec.DRBDOptions.Net == nil {
		t.Fatalf("max-buffers override should survive the unset, but typed Net is nil")
	}

	if spec.DRBDOptions.Net.Protocol != "" {
		t.Fatalf("unset-protocol must clear the typed Net.Protocol (revert to inherited): got %q",
			spec.DRBDOptions.Net.Protocol)
	}

	if spec.DRBDOptions.Net.MaxBuffers == nil || *spec.DRBDOptions.Net.MaxBuffers != 8192 {
		t.Fatalf("unsetting protocol must not disturb the sibling max-buffers override: got %v",
			spec.DRBDOptions.Net.MaxBuffers)
	}
}

func int32PtrK8s(v int32) *int32 { return &v }
