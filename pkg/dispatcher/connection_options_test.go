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

package dispatcher_test

import (
	"maps"
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// The DRBD props of a resource connection reach the satellite on that
// connection: joined onto the entry carrying the pair's paths, or on an
// entry of their own. Props outside DrbdOptions stay behind.
func TestConnectionDRBDPropsReachTheirConnection(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				"Cozystack/ResourceConnectionPaths/n1/N2":                                          `[{"name":"path1","node_a_address":"10.1.1.5","node_b_address":"10.1.1.6"}]`,
				apiv1.ResourceConnectionPropKey("n2", "n1", "DrbdOptions/Net/allow-two-primaries"): "yes",
				apiv1.ResourceConnectionPropKey("n1", "n3", "DrbdOptions/Net/protocol"):            "C",
				apiv1.ResourceConnectionPropKey("n1", "n3", "Aux/note"):                            "kept off the wire",
			},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 1024}},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "vm-disk", NodeName: "n1"},
	}

	conns := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil).GetConnections()
	if len(conns) != 2 {
		t.Fatalf("connections: %+v, want two", conns)
	}

	withPaths, withoutPaths := conns[0], conns[1]
	if withPaths.NodeB != "N2" {
		withPaths, withoutPaths = withoutPaths, withPaths
	}

	if len(withPaths.Paths) != 1 ||
		!maps.Equal(withPaths.DrbdOptions, map[string]string{"DrbdOptions/Net/allow-two-primaries": "yes"}) {
		t.Errorf("n1-n2: %+v", withPaths)
	}

	if withoutPaths.NodeA != "n1" || withoutPaths.NodeB != "n3" || len(withoutPaths.Paths) != 0 ||
		!maps.Equal(withoutPaths.DrbdOptions, map[string]string{"DrbdOptions/Net/protocol": "C"}) {
		t.Errorf("n1-n3: %+v", withoutPaths)
	}
}
