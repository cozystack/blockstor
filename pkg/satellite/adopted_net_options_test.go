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

// A resource adopted from LINSTOR arrives with its definition's secret and
// LINSTOR's automatic verify algorithm, and the satellite-level timing
// knobs through ControllerConfig. The rendered net section has to carry
// all of them, plus the digest that makes the secret count; otherwise
// `drbdadm adjust` takes them off a live connection.
func TestAnAdoptedResourceKeepsItsNetOptions(t *testing.T) {
	t.Parallel()

	dr := &intent.DesiredResource{
		Name:     "pvc-adopted",
		NodeName: "node-1",
		Volumes:  []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "lvm1"}},
		Peers:    []intent.DesiredPeer{{Name: "node-2"}},
		DrbdOptions: map[string]string{
			"port":                          "7000",
			"node-id":                       "0",
			"address":                       "10.0.0.1",
			"minor":                         "1000",
			"peer.node-2.address":           "10.0.0.2",
			"peer.node-2.node-id":           "1",
			"peer.node-2.port":              "7000",
			"DrbdOptions/Net/shared-secret": "s3cret",
			"DrbdOptions/Net/ping-int":      "12",
			"DrbdOptions/auto-verify-alg":   "crct10dif",
		},
	}

	body, err := buildResFile(dr, "node-1", "10.0.0.1", map[int32]string{0: "/dev/lvm1/pvc-adopted_00000"}, nil)
	if err != nil {
		t.Fatalf("buildResFile: %v", err)
	}

	for _, want := range []string{
		"    cram-hmac-alg sha1;\n",
		"    ping-int 12;\n",
		"    shared-secret \"s3cret\";\n",
		"    verify-alg crct10dif;\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}

	if strings.Contains(body, "auto-verify-alg") {
		t.Errorf("the controller-only key reached the .res:\n%s", body)
	}
}
