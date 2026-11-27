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

// The DRBD props a live migration sets on a resource connection reach the
// .res file as that connection's net section; a non-net prop of the
// connection does not leak into it.
func TestConnectionDRBDOptionsRenderAsTheConnectionNet(t *testing.T) {
	t.Parallel()

	dr := &intent.DesiredResource{
		Name:     "vm-disk",
		NodeName: "node-a",
		Volumes:  []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "lvm1"}},
		Peers:    []intent.DesiredPeer{{Name: "node-b"}},
		DrbdOptions: map[string]string{
			"port":                     "7000",
			"node-id":                  "0",
			"address":                  "10.0.0.1",
			"minor":                    "1000",
			"peer.node-b.address":      "10.0.0.2",
			"peer.node-b.node-id":      "1",
			"peer.node-b.port":         "7000",
			"DrbdOptions/Net/protocol": "A",
		},
		Connections: []intent.DesiredConnection{{
			NodeA: "node-a",
			NodeB: "node-b",
			DrbdOptions: map[string]string{
				"DrbdOptions/Net/allow-two-primaries": "yes",
				"DrbdOptions/Net/protocol":            "C",
				"DrbdOptions/PeerDevice/c-max-rate":   "100M",
			},
		}},
	}

	body, err := BuildResFile(dr, "node-a", "10.0.0.1")
	if err != nil {
		t.Fatalf("BuildResFile: %v", err)
	}

	want := "    net {\n      allow-two-primaries yes;\n      protocol C;\n    }\n  }\n"
	if !strings.Contains(body, want) {
		t.Errorf("connection net section missing\nwant:\n%s\nin:\n%s", want, body)
	}

	if strings.Contains(body, "c-max-rate") {
		t.Errorf("a peer-device prop of the connection was rendered:\n%s", body)
	}
}
