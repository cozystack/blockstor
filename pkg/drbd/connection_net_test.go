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

package drbd_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// A live migration opens one connection to two Primaries: its net options
// render inside that connection's block and nowhere else, so the other
// peers of the resource keep the resource-wide protocol.
func TestConnectionNetOptionsRenderOnTheirConnectionOnly(t *testing.T) {
	t.Parallel()

	got, err := drbd.Build(drbd.Resource{
		Name: "vm-disk",
		Net:  drbd.Net{ProtocolC: true, Options: map[string]string{"protocol": "A"}},
		Hosts: []drbd.Host{
			{NodeName: "node-a", Address: "10.0.0.1", Port: 7000, NodeID: 0, IsLocal: true},
			{NodeName: "node-b", Address: "10.0.0.2", Port: 7000, NodeID: 1, Diskless: true},
			{NodeName: "node-c", Address: "10.0.0.3", Port: 7000, NodeID: 2},
		},
		Volumes: []drbd.Volume{{Number: 0, Device: "/dev/drbd1000", Disk: "/dev/vg/vm-disk_00000", Minor: 1000}},
		// Stored in the other order and spelling: matched all the same.
		Connections: []drbd.ResourceConnection{{
			NodeA: "Node-B",
			NodeB: "node-a",
			Net:   map[string]string{"allow-two-primaries": "yes", "protocol": "C"},
		}},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	wantAB := "  connection {\n" +
		"    host node-a address 10.0.0.1:7000;\n" +
		"    host node-b address 10.0.0.2:7000;\n" +
		"    net {\n" +
		"      allow-two-primaries yes;\n" +
		"      protocol C;\n" +
		"    }\n" +
		"  }\n"
	if !strings.Contains(got, wantAB) {
		t.Errorf("a-b connection block missing\nwant:\n%s\nin:\n%s", wantAB, got)
	}

	if strings.Count(got, "allow-two-primaries") != 1 {
		t.Errorf("allow-two-primaries rendered outside the a-b connection:\n%s", got)
	}

	wantAC := "  connection {\n" +
		"    host node-a address 10.0.0.1:7000;\n" +
		"    host node-c address 10.0.0.3:7000;\n" +
		"  }\n"
	if !strings.Contains(got, wantAC) {
		t.Errorf("a-c connection block changed\nwant:\n%s\nin:\n%s", wantAC, got)
	}
}

func TestConnectionNetOptionValuesAreChecked(t *testing.T) {
	t.Parallel()

	_, err := drbd.Build(drbd.Resource{
		Name: "vm-disk",
		Hosts: []drbd.Host{
			{NodeName: "node-a", Address: "10.0.0.1", Port: 7000, IsLocal: true},
			{NodeName: "node-b", Address: "10.0.0.2", Port: 7000, NodeID: 1},
		},
		Connections: []drbd.ResourceConnection{{
			NodeA: "node-a",
			NodeB: "node-b",
			Net:   map[string]string{"protocol": "C\n}"},
		}},
	})
	if !errors.Is(err, drbd.ErrUnrepresentableValue) {
		t.Fatalf("Build: %v, want ErrUnrepresentableValue", err)
	}
}
