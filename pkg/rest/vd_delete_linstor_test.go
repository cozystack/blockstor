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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LINBIT/golinstor/clonestatus"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/validate"
)

// fakeSatellites stands in for the satellites of the named nodes: whenever a
// volume definition of rd is flagged DELETE, each of them confirms deleting
// its volume under the definition's deleteID, as the satellite does once its
// storage is gone. A node left out never confirms, like a satellite that is
// down.
func fakeSatellites(t *testing.T, st store.Store, rd string, nodes ...string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	t.Cleanup(func() {
		cancel()
		<-done
	})

	go func() {
		defer close(done)

		for ctx.Err() == nil {
			confirmFlagged(ctx, st, rd, nodes)

			select {
			case <-ctx.Done():
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
}

func confirmFlagged(ctx context.Context, st store.Store, rd string, nodes []string) {
	vds, err := st.VolumeDefinitions().List(ctx, rd)
	if err != nil {
		return
	}

	for _, vd := range vds {
		if !vd.Deleting() {
			continue
		}

		for _, node := range nodes {
			res, err := st.Resources().Get(ctx, rd, node)
			if err != nil || res.ConfirmedVolumeDelete(vd.VolumeNumber, vd.DeleteID) {
				continue
			}

			res.DeletedVolumes = append(res.DeletedVolumes, apiv1.DeletedVolume{VolumeNumber: vd.VolumeNumber, DeleteID: vd.DeleteID})
			_ = st.Resources().Update(ctx, &res)
		}
	}
}

// seedVDWithReplicas creates rd with volume 0 and a Secondary replica on
// each node.
func seedVDWithReplicas(t *testing.T, st store.Store, rd string, nodes ...string) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rd, &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	for _, node := range nodes {
		err := st.Resources().Create(ctx, &apiv1.Resource{Name: rd, NodeName: node, Volumes: []apiv1.Volume{{VolumeNumber: 0}}})
		if err != nil {
			t.Fatalf("seed Resource %s/%s: %v", rd, node, err)
		}
	}
}

func deleteVD(t *testing.T, base, rd string, vn int32) (int, []apiv1.APICallRc) {
	t.Helper()

	resp := httpDelete(t, fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d", base, rd, vn))
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	return resp.StatusCode, rcs
}

func messages(rcs []apiv1.APICallRc) []string {
	out := make([]string, 0, len(rcs))
	for _, rc := range rcs {
		out = append(out, rc.Message)
	}

	return out
}

// A definition whose replicas all confirm goes in the same call, and the
// answer reads like LINSTOR's: marked for deletion, one line per node that
// deleted its volume, then deleted.
func TestVDDeleteRemovesOnceEveryReplicaConfirmed(t *testing.T) {
	t.Parallel()

	const rd = "pvc-vdd-confirmed"

	st := store.NewInMemory()
	seedVDWithReplicas(t, st, rd, "node-a", "node-b")
	fakeSatellites(t, st, rd, "node-a", "node-b")

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rcs := deleteVD(t, base, rd, 0)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", code, messages(rcs))
	}

	want := []string{
		"Volume definition with number '0' of resource definition '" + rd + "' marked for deletion.",
		"Deleted volume 0 of '" + rd + "' on 'node-a'",
		"Deleted volume 0 of '" + rd + "' on 'node-b'",
		"Volume definition with number '0' of resource definition '" + rd + "' deleted.",
	}
	if !slices.Equal(messages(rcs), want) {
		t.Errorf("answer = %q\nwant     %q", messages(rcs), want)
	}

	for _, rc := range rcs {
		if rc.RetCode != maskInfo || rc.ObjRefs[objRefRscDfn] != rd || rc.ObjRefs[objRefVlmNr] != "0" {
			t.Errorf("entry %q: ret_code %#x obj_refs %v, want info with RscDfn and VlmNr", rc.Message, rc.RetCode, rc.ObjRefs)
		}
	}

	if _, err := st.VolumeDefinitions().Get(t.Context(), rd, 0); err == nil {
		t.Error("volume definition still there after every replica confirmed")
	}
}

// A definition without replicas has nobody to wait for.
func TestVDDeleteWithoutReplicasGoesAtOnce(t *testing.T) {
	t.Parallel()

	const rd = "pvc-vdd-empty"

	st := store.NewInMemory()
	seedVDWithReplicas(t, st, rd)

	base, stop := startServerWithStore(t, st)
	defer stop()

	start := time.Now()

	code, rcs := deleteVD(t, base, rd, 0)
	if code != http.StatusOK || len(rcs) != 2 || !strings.HasSuffix(rcs[1].Message, " deleted.") {
		t.Fatalf("status %d answer %q, want marked + deleted", code, messages(rcs))
	}

	if elapsed := time.Since(start); elapsed > cacheConvergeBudget/2 {
		t.Errorf("delete took %s with nothing to wait for", elapsed)
	}
}

// A replica whose satellite never confirms holds the definition: it stays,
// flagged DELETE, and the answer names the node it waits on and the ones
// that are done, instead of saying deleted. Nothing removes the definition
// early to make the answer look finished: that would leave the waiting
// node's volume on disk with nothing that ever deletes it.
func TestVDDeleteWaitsForASilentReplica(t *testing.T) {
	t.Parallel()

	const rd = "pvc-vdd-silent"

	st := store.NewInMemory()
	seedVDWithReplicas(t, st, rd, "node-a", "node-down")
	fakeSatellites(t, st, rd, "node-a")

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rcs := deleteVD(t, base, rd, 0)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", code, messages(rcs))
	}

	last := rcs[len(rcs)-1]
	if last.RetCode != warnDeleteInProgress || !strings.Contains(last.Message, "node-down") ||
		strings.Contains(last.Message, "node-a") {
		t.Errorf("last entry = %#x %q, want the in-progress warning naming node-down only", last.RetCode, last.Message)
	}

	if !slices.Contains(messages(rcs), "Deleted volume 0 of '"+rd+"' on 'node-a'") {
		t.Errorf("answer %q does not report node-a's confirmation", messages(rcs))
	}

	vd, err := st.VolumeDefinitions().Get(t.Context(), rd, 0)
	if err != nil {
		t.Fatalf("volume definition removed while a replica had not confirmed: %v", err)
	}

	if !vd.Deleting() || vd.DeleteID == "" {
		t.Errorf("volume definition = %+v, want flagged DELETE with a deleteID", vd)
	}
}

// A confirmation left over from an earlier delete of a volume with the same
// number does not stand for this one.
func TestVDDeleteIgnoresAnotherDeletesConfirmation(t *testing.T) {
	t.Parallel()

	const rd = "pvc-vdd-stale"

	st := store.NewInMemory()
	seedVDWithReplicas(t, st, rd, "node-a")

	res, err := st.Resources().Get(t.Context(), rd, "node-a")
	if err != nil {
		t.Fatal(err)
	}

	res.DeletedVolumes = []apiv1.DeletedVolume{{VolumeNumber: 0, DeleteID: "an-earlier-delete"}}
	if err := st.Resources().Update(t.Context(), &res); err != nil {
		t.Fatal(err)
	}

	progress, err := store.FinishVolumeDefinitionDelete(t.Context(), st, rd, 0)
	if err == nil {
		t.Fatalf("finish on a definition nobody flagged = %+v, want an error", progress)
	}

	if _, _, err := store.MarkVolumeDefinitionDeleted(t.Context(), st, rd, 0); err != nil {
		t.Fatal(err)
	}

	progress, err = store.FinishVolumeDefinitionDelete(t.Context(), st, rd, 0)
	if err != nil {
		t.Fatal(err)
	}

	if progress.Removed || !slices.Equal(progress.Pending, []string{"node-a"}) {
		t.Errorf("progress = %+v, want node-a pending: its confirmation is for another delete", progress)
	}
}

// A repeated `vd d` joins the delete in flight: the deleteID the replicas
// confirm under stays the same.
func TestVDDeleteRepeatedKeepsTheDeleteID(t *testing.T) {
	t.Parallel()

	const rd = "pvc-vdd-repeat"

	st := store.NewInMemory()
	seedVDWithReplicas(t, st, rd, "node-a")

	first, marked, err := store.MarkVolumeDefinitionDeleted(t.Context(), st, rd, 0)
	if err != nil || !marked {
		t.Fatalf("first mark: %+v %v %v", first, marked, err)
	}

	second, marked, err := store.MarkVolumeDefinitionDeleted(t.Context(), st, rd, 0)
	if err != nil || marked {
		t.Fatalf("second mark: %+v %v %v, want no new mark", second, marked, err)
	}

	if second.DeleteID != first.DeleteID {
		t.Errorf("deleteID changed from %q to %q", first.DeleteID, second.DeleteID)
	}
}

// A modify of a definition being deleted does not take the delete back.
func TestVDModifyKeepsTheDeleteFlag(t *testing.T) {
	t.Parallel()

	const rd = "pvc-vdd-modify"

	st := store.NewInMemory()
	seedVDWithReplicas(t, st, rd, "node-a")

	flagged, _, err := store.MarkVolumeDefinitionDeleted(t.Context(), st, rd, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = st.VolumeDefinitions().Update(t.Context(), rd, &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	got, err := st.VolumeDefinitions().Get(t.Context(), rd, 0)
	if err != nil {
		t.Fatal(err)
	}

	if !got.Deleting() || got.DeleteID != flagged.DeleteID {
		t.Errorf("after modify: %+v, want DELETE and deleteID %q kept", got, flagged.DeleteID)
	}
}

// The refusals leave the definition as it was: not flagged, so no
// satellite deletes anything.
func TestVDDeleteRefusalsLeaveTheDefinitionUnflagged(t *testing.T) {
	t.Parallel()

	t.Run("in use", func(t *testing.T) {
		t.Parallel()

		const rd = "pvc-vdd-inuse"

		st := store.NewInMemory()
		seedVDWithReplicas(t, st, rd)

		err := st.Resources().Create(t.Context(), &apiv1.Resource{Name: rd, NodeName: "node-p", State: apiv1.ResourceState{InUse: boolPtr(true)}})
		if err != nil {
			t.Fatal(err)
		}

		assertRefusedUnflagged(t, st, rd, apiCallRcFailInUse)
	})

	t.Run("ZFS delete strategy", func(t *testing.T) {
		t.Parallel()

		const rd = "pvc-vdd-zfs"

		st := store.NewInMemory()
		seedVDWithReplicas(t, st, rd, "node-a")

		err := st.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), rd, func(def *apiv1.ResourceDefinition) error {
			def.Props = map[string]string{validate.ZFSDeleteStrategyProp: string(validate.ZFSDeleteStrategyDelete)}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{ResourceName: rd, Name: "snap1", Nodes: []string{"node-a"}}); err != nil {
			t.Fatal(err)
		}

		assertRefusedUnflagged(t, st, rd, apiCallRcFailExistsSnapshot)
	})
}

func assertRefusedUnflagged(t *testing.T, st store.Store, rd string, code int64) {
	t.Helper()

	base, stop := startServerWithStore(t, st)
	defer stop()

	status, rcs := deleteVD(t, base, rd, 0)
	if status != http.StatusConflict || len(rcs) != 1 || rcs[0].RetCode != apiCallRcError|code {
		t.Fatalf("status %d answer %+v, want 409 with code %d", status, rcs, code)
	}

	vd, err := st.VolumeDefinitions().Get(t.Context(), rd, 0)
	if err != nil {
		t.Fatalf("volume definition gone after a refused delete: %v", err)
	}

	if vd.Deleting() {
		t.Error("a refused delete flagged the volume definition DELETE")
	}
}

// A snapshot or clone taken while a volume definition is flagged DELETE
// leaves that volume out: the replicas that already deleted it would fail
// the snapshot, and the flag can stay for as long as a replica is offline.
// A clone of the definition still reads as complete without it.
func TestSnapshotLeavesOutAVolumeBeingDeleted(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	st := store.NewInMemory()
	seedVDWithReplicas(t, st, "pvc-snapdel", "node-a")

	if err := st.VolumeDefinitions().Create(ctx, "pvc-snapdel", &apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("seed VD 1: %v", err)
	}

	if _, _, err := store.MarkVolumeDefinitionDeleted(ctx, st, "pvc-snapdel", 1); err != nil {
		t.Fatalf("flag VD 1: %v", err)
	}

	snap := apiv1.Snapshot{Name: "s1", ResourceName: "pvc-snapdel"}
	if err := (&Server{Store: st}).hydrateSnapshotFromRD(ctx, &snap, "pvc-snapdel"); err != nil {
		t.Fatalf("hydrateSnapshotFromRD: %v", err)
	}

	got := make([]int32, 0, len(snap.VolumeDefinitions))
	for _, vd := range snap.VolumeDefinitions {
		got = append(got, vd.VolumeNumber)
	}

	if !slices.Equal(got, []int32{0}) {
		t.Errorf("snapshot volumes %v, want only volume 0", got)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-snapdel-clone"}); err != nil {
		t.Fatalf("seed clone RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-snapdel-clone", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("seed clone VD: %v", err)
	}

	if status := computeCloneStatus(ctx, st, "pvc-snapdel", "pvc-snapdel-clone"); status != clonestatus.Complete {
		t.Errorf("clone status %v, want complete without the flagged volume", status)
	}
}
