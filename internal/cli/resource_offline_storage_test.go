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

package cli_test

import (
	"context"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedOfflineStorageOnly is a STORAGE-only definition with a replica
// on a node that went down. The replicas carry what the store projects
// for them, the default DRBD chain with the last reported peer links,
// and only the definition names the real stack.
func seedOfflineStorageOnly(ctx context.Context, backend store.Store) {
	_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1", ConnectionStatus: apiv1.NodeTypeOnline})
	_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-2", ConnectionStatus: apiv1.NodeTypeOffline})
	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-local", LayerStack: []string{apiv1.LayerKindStorage},
	})

	for _, node := range []struct{ name, peer string }{{"node-1", "node-2"}, {"node-2", "node-1"}} {
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-local", NodeName: node.name,
			State:   apiv1.ResourceState{DrbdState: "UpToDate"},
			Volumes: []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "UpToDate"}}},
			LayerObject: &apiv1.ResourceLayer{
				Type: apiv1.LayerKindDRBD,
				Drbd: &apiv1.DrbdResourceLayer{
					Connections: map[string]apiv1.DrbdConnection{node.peer: {Connected: true, Message: "Connected"}},
				},
			},
		})
	}
}

// A STORAGE-only replica on an OFFLINE node keeps reading Created with
// Conns Ok, as linstor-client shows it against LINSTOR: the client
// answers a volume whose top layer is not DRBD with Created before it
// looks at any state, and a replica with no DRBD layer has no peers to
// lose. Forgetting the dead satellite's report must not turn it into
// the Unknown a DRBD replica reads there.
func TestResourceListOfflineNodeStorageOnlyReadsCreated(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedOfflineStorageOnly)

	if got := app.Run(t.Context(), []string{"r", "l"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	for _, node := range []string{"node-1", "node-2"} {
		row := rowFor(t, out.String(), node)

		cells := strings.Split(row, "|")
		if len(cells) < 7 {
			t.Fatalf("row for %s has too few cells: %q", node, row)
		}

		if conns := strings.TrimSpace(cells[5]); conns != "Ok" {
			t.Errorf("%s: Conns = %q, want Ok:\n%s", node, conns, out.String())
		}

		if state := strings.TrimSpace(cells[6]); state != "Created" {
			t.Errorf("%s: State = %q, want Created:\n%s", node, state, out.String())
		}
	}
}

// `--faulty` lists only what the table flags, and a Created STORAGE
// replica is not flagged, on an OFFLINE node or not.
func TestResourceListFaultyOmitsOfflineStorageOnly(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedOfflineStorageOnly)

	if got := app.Run(t.Context(), []string{"r", "l", "--faulty"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if strings.Contains(out.String(), "pvc-local") {
		t.Errorf("--faulty lists a STORAGE-only replica:\n%s", out.String())
	}
}
