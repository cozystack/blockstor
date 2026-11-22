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

package rest

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedRestoreWithOnlineNodes is seedRestoreSourceWithNeverReadySnapshot
// with both snapshot nodes ONLINE, so the deploy wait awaits a replica
// restored onto them instead of warning it unreachable.
func seedRestoreWithOnlineNodes(t *testing.T, st store.Store, srcRD, snapName string) {
	t.Helper()

	seedRestoreSourceWithNeverReadySnapshot(t, st, srcRD, snapName)

	for _, n := range []string{"n1", "n2"} {
		seedReadyWaitNode(t, st, n, apiv1.NodeTypeOnline)
	}
}

func restoreOnto(t *testing.T, base, srcRD, snapName, toResource, node string) ([]apiv1.APICallRc, time.Duration) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"to_resource":   toResource,
		"snapshot_name": snapName,
		"node_names":    []string{node},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return postForRcs(t, base+"/v1/resource-definitions/"+srcRD+"/snapshot-restore-resource", body, http.StatusCreated)
}

// TestSnapshotRestoreAnswersOnceTheRestoredReplicaIsBuilt pins
// LINSTOR v1.33.1's CtrlSnapshotRestoreApiCallHandler, which answers a
// restore after ctrlRscCrtApiHelper.deployResources, the wait a resource
// create has. linstor-csi restores a volume onto the node its consumer
// was scheduled to and autoplaces the rest after that answer, and the
// autoplace waits only for the replicas it placed itself. Answered at
// once, the restore left the one replica ControllerPublishVolume attaches
// unawaited: the attach read no device path and backed off, and the
// replica came up only after the autoplaced peers had joined it.
func TestSnapshotRestoreAnswersOnceTheRestoredReplicaIsBuilt(t *testing.T) {
	st := store.NewInMemory()
	seedRestoreWithOnlineNodes(t, st, "src-built", "snap-built")

	base := startDeployWaitServer(t, st, 10*time.Second, 10*time.Second)
	standIn := deviceStandIn(t.Context(), st, "dst-built", 1, []string{"n1"}, 300*time.Millisecond)

	rcs, _ := restoreOnto(t, base, "src-built", "snap-built", "dst-built", "n1")

	requireDevicePaths(t, st, "dst-built", []string{"n1"})

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for a restored replica that was built: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestSnapshotRestoreWarnsWhenReplicaIsNotBuiltInTime pins the bound
// the restore shares with a create: a restored replica that is never
// built does not hold the answer past it, the restore stands, and the
// node gets LINSTOR's not-ready warning.
func TestSnapshotRestoreWarnsWhenReplicaIsNotBuiltInTime(t *testing.T) {
	st := store.NewInMemory()
	seedRestoreWithOnlineNodes(t, st, "src-nobuild", "snap-nobuild")

	base := startDeployWaitServer(t, st, 500*time.Millisecond, 10*time.Second)

	rcs, elapsed := restoreOnto(t, base, "src-nobuild", "snap-nobuild", "dst-nobuild", "n1")

	if elapsed < 500*time.Millisecond || elapsed > 5*time.Second {
		t.Errorf("restore took %s, want the 500ms deploy bound", elapsed)
	}

	replicas, err := st.Resources().ListByDefinition(t.Context(), "dst-nobuild")
	if err != nil || len(replicas) != 1 {
		t.Fatalf("restore did not stand: %d replicas, err %v", len(replicas), err)
	}

	if w := warningsFor(rcs); len(w) != 1 || w["n1"] != maskWarn {
		t.Errorf("want one not-ready warning, for n1, got %v", w)
	}
}

// TestSnapshotRestoreReplayWaitsForTheRestoredReplica pins the replay
// of a finished restore to the same wait as its first answer. A client
// that gave up during the build wait finds its replicas already
// written, so the retry is answered as a finished restore; answered at
// once, it handed linstor-csi the replica whose device path the attach
// reads before the satellite had built it, the gap the first answer's
// wait closes.
func TestSnapshotRestoreReplayWaitsForTheRestoredReplica(t *testing.T) {
	st := store.NewInMemory()
	seedRestoreWithOnlineNodes(t, st, "src-replay", "snap-replay")

	base := startDeployWaitServer(t, st, 500*time.Millisecond, 10*time.Second)

	restoreOnto(t, base, "src-replay", "snap-replay", "dst-replay", "n1")

	standIn := deviceStandIn(t.Context(), st, "dst-replay", 1, []string{"n1"}, 300*time.Millisecond)

	rcs, _ := restoreOnto(t, base, "src-replay", "snap-replay", "dst-replay", "n1")
	requireReplayAnswer(t, rcs)

	requireDevicePaths(t, st, "dst-replay", []string{"n1"})

	if w := warningsFor(rcs); len(w) != 0 {
		t.Errorf("unexpected warnings for a replayed restore whose replica was built: %v", w)
	}

	if err := <-standIn; err != nil {
		t.Fatalf("satellite stand-in: %v", err)
	}
}

// TestSnapshotRestoreReplayWarnsWhenReplicaIsNotBuiltInTime pins the
// bound on the replay: a replica still unbuilt gets LINSTOR's not-ready
// warning instead of a bare success.
func TestSnapshotRestoreReplayWarnsWhenReplicaIsNotBuiltInTime(t *testing.T) {
	st := store.NewInMemory()
	seedRestoreWithOnlineNodes(t, st, "src-replay-nb", "snap-replay-nb")

	base := startDeployWaitServer(t, st, 500*time.Millisecond, 10*time.Second)

	restoreOnto(t, base, "src-replay-nb", "snap-replay-nb", "dst-replay-nb", "n1")

	rcs, elapsed := restoreOnto(t, base, "src-replay-nb", "snap-replay-nb", "dst-replay-nb", "n1")
	requireReplayAnswer(t, rcs)

	if elapsed < 500*time.Millisecond || elapsed > 5*time.Second {
		t.Errorf("replay took %s, want the 500ms deploy bound", elapsed)
	}

	if w := warningsFor(rcs); len(w) != 1 || w["n1"] != maskWarn {
		t.Errorf("want one not-ready warning on the replay, for n1, got %v", w)
	}
}

// requireReplayAnswer fails unless rcs is the answer of a replayed restore,
// so the replay tests cannot pass through the first-run path.
func requireReplayAnswer(t *testing.T, rcs []apiv1.APICallRc) {
	t.Helper()

	if len(rcs) == 0 || !strings.HasPrefix(rcs[0].Message, "snapshot restore completed on retry") {
		t.Fatalf("want the replay answer of a finished restore, got %v", rcs)
	}
}
