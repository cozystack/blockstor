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

package satellite

import (
	"strings"
	"testing"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// A connection with paths names its nodes as the request spelled them,
// Node-1 and Node-2, while the hosts carry the replicas' CR names, node-1 and
// node-2. The paths still reach the .res file.
func TestAPathStoredInTheRegisteredSpellingIsRendered(t *testing.T) {
	t.Parallel()

	dr := &intent.DesiredResource{
		Name:     "pvc-path",
		NodeName: "node-1",
		Volumes:  []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "lvm1"}},
		Peers:    []intent.DesiredPeer{{Name: "node-2"}},
		DrbdOptions: map[string]string{
			"port":                "7000",
			"node-id":             "0",
			"address":             "10.0.0.1",
			"minor":               "1000",
			"peer.node-2.address": "10.0.0.2",
			"peer.node-2.node-id": "1",
			"peer.node-2.port":    "7000",
		},
		Connections: []intent.DesiredConnection{{
			NodeA: "Node-1",
			NodeB: "Node-2",
			Paths: []intent.DesiredConnectionPath{{Name: "path1", AddressA: "192.0.2.1", AddressB: "192.0.2.2"}},
		}},
	}

	body, err := buildResFile(dr, "node-1", "10.0.0.1", map[int32]string{0: "/dev/lvm1/pvc-path_00000"}, nil)
	if err != nil {
		t.Fatalf("buildResFile: %v", err)
	}

	if !strings.Contains(body, "path {") || !strings.Contains(body, "192.0.2.2") {
		t.Errorf("the path stored for Node-1 and Node-2 is not rendered:\n%s", body)
	}
}
