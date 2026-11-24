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
	"strings"
	"testing"
	"time"

	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

const (
	growRD      = "pvc-grow-ack"
	growOldKib  = int64(1024 * 1024)
	growNewKib  = int64(2 * 1024 * 1024)
	growPending = 600 * time.Millisecond
	growDone    = 5 * time.Second
)

// seedGrow builds a definition at growOldKib with diskful replicas on
// the given nodes, each reporting applied, plus two replicas a grow does
// not wait for: a diskless one still carrying the size it applied while
// it was diskful, and a diskful one whose satellite never reported.
func seedGrow(t *testing.T, applied map[string]int64) store.Store {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: growRD})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	err = st.VolumeDefinitions().Create(ctx, growRD, &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: growOldKib})
	if err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	for node, kib := range applied {
		err = st.Resources().Create(ctx, &apiv1.Resource{
			Name: growRD, NodeName: node,
			Volumes: []apiv1.Volume{{VolumeNumber: 0, AppliedSizeKib: kib}},
		})
		if err != nil {
			t.Fatalf("seed replica %s: %v", node, err)
		}
	}

	err = st.Resources().Create(ctx, &apiv1.Resource{
		Name: growRD, NodeName: "diskless", Flags: []string{apiv1.ResourceFlagDiskless},
		Volumes: []apiv1.Volume{{VolumeNumber: 0, AppliedSizeKib: growOldKib}},
	})
	if err != nil {
		t.Fatalf("seed diskless replica: %v", err)
	}

	err = st.Resources().Create(ctx, &apiv1.Resource{
		Name: growRD, NodeName: "unreported", Volumes: []apiv1.Volume{{VolumeNumber: 0}},
	})
	if err != nil {
		t.Fatalf("seed unreported replica: %v", err)
	}

	nodes := make([]string, 0, len(applied)+2)
	nodes = append(nodes, "diskless", "unreported")

	for node := range applied {
		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		err = st.Nodes().Create(ctx, &apiv1.Node{Name: node, Type: "SATELLITE", ConnectionStatus: apiv1.NodeTypeOnline})
		if err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}
	}

	return st
}

// setFailure records that the satellite on node failed its last pass
// while applying kib.
func setFailure(t *testing.T, st store.Store, node string, kib int64, message string) {
	t.Helper()

	rsc, err := st.Resources().Get(t.Context(), growRD, node)
	if err != nil {
		t.Fatalf("get replica %s: %v", node, err)
	}

	rsc.ApplyFailure = &apiv1.ApplyFailure{Message: message, SizesKib: map[int32]int64{0: kib}}

	err = st.Resources().Update(t.Context(), &rsc)
	if err != nil {
		t.Fatalf("update replica %s: %v", node, err)
	}
}

// setApplied records that the satellite on node finished applying kib.
func setApplied(t *testing.T, st store.Store, node string, kib int64) {
	t.Helper()

	rsc, err := st.Resources().Get(t.Context(), growRD, node)
	if err != nil {
		t.Fatalf("get replica %s: %v", node, err)
	}

	rsc.Volumes = []apiv1.Volume{{VolumeNumber: 0, AppliedSizeKib: kib}}

	err = st.Resources().Update(t.Context(), &rsc)
	if err != nil {
		t.Fatalf("update replica %s: %v", node, err)
	}
}

// growAnswer is what a resize request was answered with.
type growAnswer struct {
	status int
	rcs    []apiv1.APICallRc
}

// putGrow sends linstor-csi's ControllerExpandVolume body and reports
// the answer on the returned channel.
func putGrow(t *testing.T, base string, kib int64) <-chan growAnswer {
	t.Helper()

	body, err := json.Marshal(map[string]int64{"size_kib": kib})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	done := make(chan growAnswer, 1)
	url := fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/0", base, growRD)

	go func() {
		resp := httpPut(t, url, body)
		defer resp.Body.Close()

		answer := growAnswer{status: resp.StatusCode}
		_ = json.NewDecoder(resp.Body).Decode(&answer.rcs)
		done <- answer
	}()

	return done
}

func expectPending(t *testing.T, done <-chan growAnswer, why string) {
	t.Helper()

	select {
	case answer := <-done:
		t.Fatalf("the resize was answered (%d) while %s", answer.status, why)
	case <-time.After(growPending):
	}
}

func expectAnswered(t *testing.T, done <-chan growAnswer) []apiv1.APICallRc {
	t.Helper()

	return expectAnsweredWith(t, done, http.StatusOK)
}

func expectAnsweredWith(t *testing.T, done <-chan growAnswer, status int) []apiv1.APICallRc {
	t.Helper()

	select {
	case answer := <-done:
		if answer.status != status {
			t.Fatalf("status %d, want %d", answer.status, status)
		}

		return answer.rcs
	case <-time.After(growDone):
		t.Fatal("the resize was not answered")
	}

	return nil
}

// TestVDGrowAnsweredOnlyAfterEveryReplicaGrew is the online-expansion
// regression: linstor-csi's ControllerExpandVolume returns when the PUT
// does and kubelet runs resize2fs next, so a PUT answered while one
// replica still had the old device size left the filesystem at the old
// size. LINSTOR answers after its satellites resized the backing volume
// and then DRBD.
func TestVDGrowAnsweredOnlyAfterEveryReplicaGrew(t *testing.T) {
	st := seedGrow(t, map[string]int64{"a": growOldKib, "b": growOldKib, "c": growOldKib})

	base, stop := startServerWithStore(t, st)
	defer stop()

	done := putGrow(t, base, growNewKib)

	expectPending(t, done, "no replica had grown")

	setApplied(t, st, "a", growNewKib)
	setApplied(t, st, "b", growNewKib)

	expectPending(t, done, "one replica was still at the old size")

	setApplied(t, st, "c", growNewKib)

	expectAnswered(t, done)
}

// TestVDGrowRetryOfSameSizeWaits covers linstor-csi retrying after the
// external resizer's timeout cut the first PUT short: the definition
// already holds the new size, and the retry must still not be answered
// before the replicas have grown.
func TestVDGrowRetryOfSameSizeWaits(t *testing.T) {
	st := seedGrow(t, map[string]int64{"a": growNewKib, "b": growOldKib})

	err := st.VolumeDefinitions().PatchVolumeDefinitionSpec(t.Context(), growRD, 0, func(vd *apiv1.VolumeDefinition) error {
		vd.SizeKib = growNewKib

		return nil
	})
	if err != nil {
		t.Fatalf("grow VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	done := putGrow(t, base, growNewKib)

	expectPending(t, done, "a replica was still at the old size")

	setApplied(t, st, "b", growNewKib)

	expectAnswered(t, done)
}

// TestVDGetReportsAppliedSizeWhileGrowing pins the read side of the
// same retry: linstor-csi skips the PUT when the definition already
// has the requested size, so until the replicas have grown the
// definition reports the size they applied.
func TestVDGetReportsAppliedSizeWhileGrowing(t *testing.T) {
	st := seedGrow(t, map[string]int64{"a": growNewKib, "b": growOldKib})

	err := st.VolumeDefinitions().PatchVolumeDefinitionSpec(t.Context(), growRD, 0, func(vd *apiv1.VolumeDefinition) error {
		vd.SizeKib = growNewKib

		return nil
	})
	if err != nil {
		t.Fatalf("grow VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	getSizes := func() (int64, int64) {
		t.Helper()

		var one apiv1.VolumeDefinition

		resp := httpGet(t, fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/0", base, growRD))
		decodeErr := json.NewDecoder(resp.Body).Decode(&one)
		_ = resp.Body.Close()

		if decodeErr != nil {
			t.Fatalf("decode VD: %v", decodeErr)
		}

		var list []apiv1.VolumeDefinition

		resp = httpGet(t, fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions", base, growRD))
		decodeErr = json.NewDecoder(resp.Body).Decode(&list)
		_ = resp.Body.Close()

		if decodeErr != nil || len(list) != 1 {
			t.Fatalf("decode VD list: %v (%d entries)", decodeErr, len(list))
		}

		return one.SizeKib, list[0].SizeKib
	}

	one, listed := getSizes()
	if one != growOldKib || listed != growOldKib {
		t.Fatalf("while a replica is still at the old size: GET %d, list %d, want %d", one, listed, growOldKib)
	}

	setApplied(t, st, "b", growNewKib)

	one, listed = getSizes()
	if one != growNewKib || listed != growNewKib {
		t.Fatalf("after every replica grew: GET %d, list %d, want %d", one, listed, growNewKib)
	}
}

// TestVDGrowSkipsOfflineReplica: as in LINSTOR, a satellite that is not
// connected catches up when it reconnects; the resize does not wait for
// it and names it in a WARN_NOT_CONNECTED entry.
func TestVDGrowSkipsOfflineReplica(t *testing.T) {
	st := seedGrow(t, map[string]int64{"a": growOldKib, "down": growOldKib})
	ctx := t.Context()

	err := st.Nodes().SetConnectionStatus(ctx, "down", apiv1.NodeTypeOffline)
	if err != nil {
		t.Fatalf("mark node offline: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	done := putGrow(t, base, growNewKib)

	setApplied(t, st, "a", growNewKib)

	rcs := expectAnswered(t, done)

	found := false

	for _, rc := range rcs {
		if rc.RetCode == warnNotConnected && rc.ObjRefs[objRefNode] == "down" {
			found = true
		}
	}

	if !found {
		t.Fatalf("no WARN_NOT_CONNECTED entry for the offline replica: %+v", rcs)
	}
}

// TestVDGrowCallerGivingUpEndsTheWait: the wait has no deadline of its
// own, so a caller that stops waiting must end it.
func TestVDGrowCallerGivingUpEndsTheWait(t *testing.T) {
	st := seedGrow(t, map[string]int64{"a": growOldKib})

	srv := &Server{Store: st}

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	result := make(chan growOutcome, 1)

	go func() {
		_, outcome := srv.waitForVolumeGrown(ctx, growRD, 0, growNewKib)
		result <- outcome
	}()

	select {
	case outcome := <-result:
		if outcome != outcomeCancelled {
			t.Fatalf("outcome %d while the replica was at the old size, want cancelled", outcome)
		}
	case <-time.After(growDone):
		t.Fatal("the wait outlived its caller")
	}
}

// TestVDGrowServerShutdownIsNotSuccess: the request context is the
// server's, so a controller that stops while a grow waits ends the wait
// with the connection still open. An empty 200 then reached golinstor,
// which does not decode a PUT's body, so linstor-csi reported the
// expansion done and kubelet grew the filesystem on the old device.
func TestVDGrowServerShutdownIsNotSuccess(t *testing.T) {
	st := seedGrow(t, map[string]int64{"a": growOldKib})

	base, stop := startServerWithStore(t, st)

	result := make(chan error, 1)

	go func() {
		result <- newClient(t, base).ResourceDefinitions.ModifyVolumeDefinition(
			t.Context(), growRD, 0, lapi.VolumeDefinitionModify{SizeKib: uint64(growNewKib)})
	}()

	time.Sleep(growPending)
	stop()

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("golinstor reported the grow done although the server stopped before the replica grew")
		}
	case <-time.After(growDone):
		t.Fatal("the resize was not answered when the server stopped")
	}
}

// TestVDGrowEndsWithSatelliteError: a replica whose satellite fails its
// pass at the new size is not going to grow until someone fixes it, and
// LINSTOR answers with that satellite's error. A failure left from a pass
// at the old size is not this grow's and does not end the wait.
func TestVDGrowEndsWithSatelliteError(t *testing.T) {
	st := seedGrow(t, map[string]int64{"a": growOldKib, "b": growOldKib})
	setFailure(t, st, "b", growOldKib, "an earlier failure")

	base, stop := startServerWithStore(t, st)
	defer stop()

	done := putGrow(t, base, growNewKib)

	expectPending(t, done, "the only failure was from a pass at the old size")

	setApplied(t, st, "a", growNewKib)
	setFailure(t, st, "b", growNewKib, "lvextend: insufficient free space")

	rcs := expectAnsweredWith(t, done, http.StatusInternalServerError)

	found := false

	for _, rc := range rcs {
		if rc.RetCode == apiv1.APICallRcMaskError && rc.ObjRefs[objRefNode] == "b" &&
			strings.Contains(rc.Cause, "insufficient free space") {
			found = true
		}
	}

	if !found {
		t.Fatalf("no error entry carrying the satellite's failure: %+v", rcs)
	}
}

// TestVDGrowSkipsReplicaNotOnline: only a satellite the heartbeat
// reports ONLINE is waited for. An evicted node, one in any other state
// and one that cannot be read are reported as not connected, as LINSTOR
// does, instead of holding the resize until the caller gives up.
func TestVDGrowSkipsReplicaNotOnline(t *testing.T) {
	st := seedGrow(t, map[string]int64{
		"a": growOldKib, "evicted": growOldKib, "connecting": growOldKib, "unknown": growOldKib, "missing": growOldKib,
	})
	ctx := t.Context()

	err := st.Nodes().SetConnectionStatus(ctx, "connecting", apiv1.NodeTypeConnecting)
	if err != nil {
		t.Fatalf("mark node connecting: %v", err)
	}

	err = st.Nodes().SetConnectionStatus(ctx, "unknown", "")
	if err != nil {
		t.Fatalf("clear node status: %v", err)
	}

	evicted, err := st.Nodes().Get(ctx, "evicted")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	evicted.Flags = []string{apiv1.NodeFlagEvicted}

	err = st.Nodes().Update(ctx, &evicted)
	if err != nil {
		t.Fatalf("evict node: %v", err)
	}

	err = st.Nodes().Delete(ctx, "missing")
	if err != nil {
		t.Fatalf("drop node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	done := putGrow(t, base, growNewKib)

	setApplied(t, st, "a", growNewKib)

	rcs := expectAnswered(t, done)

	warned := map[string]bool{}

	for _, rc := range rcs {
		if rc.RetCode == warnNotConnected {
			warned[rc.ObjRefs[objRefNode]] = true
		}
	}

	for _, node := range []string{"evicted", "connecting", "unknown", "missing"} {
		if !warned[node] {
			t.Errorf("no WARN_NOT_CONNECTED entry for %s: %+v", node, rcs)
		}
	}
}

// setConnection records what the satellite on node reports about its
// DRBD connection to peer.
func setConnection(t *testing.T, st store.Store, node, peer string, connected bool) {
	t.Helper()

	rsc, err := st.Resources().Get(t.Context(), growRD, node)
	if err != nil {
		t.Fatalf("get replica %s: %v", node, err)
	}

	if rsc.LayerObject == nil {
		rsc.LayerObject = &apiv1.ResourceLayer{Type: apiv1.LayerKindDRBD}
	}

	if rsc.LayerObject.Drbd == nil {
		rsc.LayerObject.Drbd = &apiv1.DrbdResourceLayer{}
	}

	if rsc.LayerObject.Drbd.Connections == nil {
		rsc.LayerObject.Drbd.Connections = map[string]apiv1.DrbdConnection{}
	}

	rsc.LayerObject.Drbd.Connections[peer] = apiv1.DrbdConnection{Connected: connected}

	err = st.Resources().Update(t.Context(), &rsc)
	if err != nil {
		t.Fatalf("update replica %s: %v", node, err)
	}
}

// TestVDGrowSkipsReplicaCutOffFromDiskfulPeer: while a diskful peer is
// not connected, DRBD 9 caps a grow at the last agreed size
// (drbd_new_dev_size without --assume-peer-has-space), so a replica cut
// off from one never reports the new size until the link is back.
// LINSTOR answers that resize; waiting for such a replica held the PUT
// and kept `vd l` at the old size for as long as the partition lasted.
// A down connection to a diskless peer does not cap the grow. b reports
// its peer as "A": DRBD names a connection the way the peer's node was
// spelled when it was created, which need not match the node's key.
func TestVDGrowSkipsReplicaCutOffFromDiskfulPeer(t *testing.T) {
	st := seedGrow(t, map[string]int64{"a": growOldKib, "b": growOldKib, "c": growOldKib})
	setConnection(t, st, "a", "b", false)
	setConnection(t, st, "b", "A", false)
	setConnection(t, st, "c", "diskless", false)

	base, stop := startServerWithStore(t, st)
	defer stop()

	done := putGrow(t, base, growNewKib)

	expectPending(t, done, "a replica with every diskful peer connected was still at the old size")

	setApplied(t, st, "c", growNewKib)

	rcs := expectAnswered(t, done)

	warned := map[string]bool{}

	for _, rc := range rcs {
		if rc.RetCode == warnNotConnected {
			warned[rc.ObjRefs[objRefNode]] = true
		}
	}

	if !warned["a"] || !warned["b"] || warned["c"] {
		t.Fatalf("want WARN_NOT_CONNECTED for a and b only: %+v", rcs)
	}

	var vd apiv1.VolumeDefinition

	resp := httpGet(t, fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/0", base, growRD))
	decodeErr := json.NewDecoder(resp.Body).Decode(&vd)
	_ = resp.Body.Close()

	if decodeErr != nil {
		t.Fatalf("decode VD: %v", decodeErr)
	}

	if vd.SizeKib != growNewKib {
		t.Fatalf("vd size %d while the cut-off replicas wait for their peer, want %d", vd.SizeKib, growNewKib)
	}
}
