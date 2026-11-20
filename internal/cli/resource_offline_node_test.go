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

// seedOfflinePeer is a two-replica definition whose second node went
// down: the store still holds that satellite's last report, a
// connected UpToDate Secondary, as it does after a hard reset.
func seedOfflinePeer(ctx context.Context, backend store.Store) {
	_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1", ConnectionStatus: apiv1.NodeTypeOnline})
	_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-2", ConnectionStatus: apiv1.NodeTypeOffline})
	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})

	for _, node := range []struct{ name, peer string }{{"node-1", "node-2"}, {"node-2", "node-1"}} {
		unused := false
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: node.name,
			State:   apiv1.ResourceState{InUse: &unused, DrbdState: "UpToDate"},
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

// rowFor returns the table line that names the node.
func rowFor(t *testing.T, table, node string) string {
	t.Helper()

	for line := range strings.SplitSeq(table, "\n") {
		if strings.Contains(line, node) {
			return line
		}
	}

	t.Fatalf("no row for %s:\n%s", node, table)

	return ""
}

// A replica on an OFFLINE node reads Unknown with blank Usage and
// Conns, the way linstor-client shows it against LINSTOR, instead of
// the UpToDate the dead satellite last reported. The replica on the
// node that is up keeps its own state.
func TestResourceListOfflineNodeReadsUnknown(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedOfflinePeer)

	if got := app.Run(t.Context(), []string{"r", "l"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	offline := rowFor(t, out.String(), "node-2")
	if !strings.Contains(offline, "Unknown") {
		t.Errorf("replica on the OFFLINE node does not read Unknown: %q", offline)
	}

	for _, stale := range []string{"UpToDate", "Unused", "Ok"} {
		if strings.Contains(offline, stale) {
			t.Errorf("replica on the OFFLINE node still shows the stale %q: %q", stale, offline)
		}
	}

	online := rowFor(t, out.String(), "node-1")
	if !strings.Contains(online, "UpToDate") || !strings.Contains(online, "Unused") {
		t.Errorf("replica on the ONLINE node lost its state: %q", online)
	}
}

// The store keeps the dead satellite's report: only the answer changes,
// so the replica reads its state again the moment the node is back.
func TestResourceListOfflineNodeKeepsStoredState(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedOfflinePeer)

	if got := app.Run(t.Context(), []string{"r", "l"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	stored := getResource(t, app, "node-2")
	if len(stored.Volumes) == 0 || stored.Volumes[0].State.DiskState != "UpToDate" {
		t.Errorf("listing rewrote the stored report: %+v", stored.Volumes)
	}
}
