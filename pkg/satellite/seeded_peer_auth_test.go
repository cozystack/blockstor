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
	"maps"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/store"
)

// A definition blockstor created renders the authentication LINSTOR puts
// in every .res: `cram-hmac-alg sha1;` and the definition's secret,
// quoted, since a base64 secret routinely carries '+', '/' or '='.
func TestASeededDefinitionRendersPeerAuthentication(t *testing.T) {
	t.Parallel()

	rd := &apiv1.ResourceDefinition{Name: "pvc-new"}

	err := store.SeedPeerAuthentication(t.Context(), store.NewInMemory(), rd, "")
	if err != nil {
		t.Fatalf("SeedPeerAuthentication: %v", err)
	}

	opts := map[string]string{
		"port":                "7000",
		"node-id":             "0",
		"address":             "10.0.0.1",
		"minor":               "1000",
		"peer.node-2.address": "10.0.0.2",
		"peer.node-2.node-id": "1",
		"peer.node-2.port":    "7000",
	}
	maps.Copy(opts, rd.Props)

	dr := &intent.DesiredResource{
		Name:        rd.Name,
		NodeName:    "node-1",
		Volumes:     []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "lvm1"}},
		Peers:       []intent.DesiredPeer{{Name: "node-2"}},
		DrbdOptions: opts,
	}

	body, err := buildResFile(dr, "node-1", "10.0.0.1", map[int32]string{0: "/dev/lvm1/pvc-new_00000"}, nil, nil)
	if err != nil {
		t.Fatalf("buildResFile: %v", err)
	}

	if !strings.Contains(body, "    cram-hmac-alg sha1;\n") {
		t.Errorf("no cram-hmac-alg sha1 in the net section")
	}

	if !strings.Contains(body, "    shared-secret \""+rd.Props[drbd.SharedSecretProp]+"\";\n") {
		t.Errorf("the definition's secret is not rendered quoted in the net section")
	}
}
