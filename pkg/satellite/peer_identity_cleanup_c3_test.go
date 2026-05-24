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
	"context"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

func i32(v int32) *int32 { return &v }

// statusJSONForPeer renders a minimal `drbdsetup status -j` body with
// one peer connection at the given node-id (incl. 0) so the Show probe
// resolves a kernel slot for that peer.
func statusJSONForPeer(rd, peer string, nodeID int32) string {
	return `[{"name":"` + rd + `","connections":[{"peer-node-id":` +
		itoa(nodeID) + `,"name":"` + peer + `","connection-state":"Connecting","peer_devices":[]}]}]`
}

func itoa(v int32) string {
	// tiny local helper to avoid importing strconv just for one call
	if v == 0 {
		return "0"
	}

	neg := v < 0
	if neg {
		v = -v
	}

	var b [12]byte

	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}

	if neg {
		i--
		b[i] = '-'
	}

	return string(b[i:])
}

// TestEvictByUIDMismatchFiresForKernelSlotNodeIDZero is the Bug 342 C3
// manifestation-1 regression pin: a re-incarnated peer whose kernel
// slot sits at node-id 0 (the exact re-allocated-worker case) MUST be
// evicted. The pre-C3 `slot.NodeID != 0` guard skipped a valid id-0
// kernel slot, and the downstream `nodeID == 0` test deferred eviction
// forever — the stall behind the wedge.
func TestEvictByUIDMismatchFiresForKernelSlotNodeIDZero(t *testing.T) {
	const rd = "pvc-c3-evict"

	fx := storage.NewFakeExec()
	// Kernel reports peer w1 occupying slot node-id 0.
	fx.Expect("drbdsetup status -j "+rd, storage.FakeResponse{
		Stdout: []byte(statusJSONForPeer(rd, "w1", 0)),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "self",
	})

	// Desired peer w1 carries a NEW UID; the satellite's last-applied
	// baseline has the OLD one → UID mismatch.
	desired := []intent.DesiredPeer{
		{Name: "w1", NodeID: i32(0), ResourceUID: "new-uid"},
	}
	applied := map[string]string{"w1": "old-uid"}
	devices := map[int32]string{0: "/dev/pool/vol0"}

	cleaned, err := rec.EvictPeersByUIDMismatch(context.Background(), rd, desired, applied, nil, devices)
	if err != nil {
		t.Fatalf("EvictPeersByUIDMismatch: %v", err)
	}

	if cleaned["w1"] != "new-uid" {
		t.Fatalf("eviction did not fire for id-0 kernel slot: cleaned=%v", cleaned)
	}

	cmds := fx.CommandLines()
	if !containsCmd(cmds, "drbdadm del-peer w1:"+rd) {
		t.Errorf("del-peer not issued; cmds=%v", cmds)
	}

	if !containsForgetPeer(cmds, rd, 0) {
		t.Errorf("forget-peer --node-id 0 not issued; cmds=%v", cmds)
	}
}

// TestEvictByUIDMismatchFiresForK8sNodeIDZero pins the same fire
// behaviour when the kernel slot is absent but the dispatcher resolved
// a K8s-side allocated id 0 (peer.NodeID = &0). Pre-C3 this deferred
// forever via `nodeID == 0`.
func TestEvictByUIDMismatchFiresForK8sNodeIDZero(t *testing.T) {
	const rd = "pvc-c3-k8s0"

	fx := storage.NewFakeExec()
	// No kernel slot for w1 (Show returns empty array).
	fx.Expect("drbdsetup status -j "+rd, storage.FakeResponse{
		Stdout: []byte(`[{"name":"` + rd + `","connections":[]}]`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "self",
	})

	desired := []intent.DesiredPeer{
		{Name: "w1", NodeID: i32(0), ResourceUID: "new-uid"},
	}
	applied := map[string]string{"w1": "old-uid"}
	devices := map[int32]string{0: "/dev/pool/vol0"}

	cleaned, err := rec.EvictPeersByUIDMismatch(context.Background(), rd, desired, applied, nil, devices)
	if err != nil {
		t.Fatalf("EvictPeersByUIDMismatch: %v", err)
	}

	if cleaned["w1"] != "new-uid" {
		t.Fatalf("eviction did not fire for K8s-allocated id 0: cleaned=%v", cleaned)
	}

	if !containsForgetPeer(fx.CommandLines(), rd, 0) {
		t.Errorf("forget-peer --node-id 0 not issued; cmds=%v", fx.CommandLines())
	}
}

// TestEvictByUIDMismatchDefersWhenTrulyUnresolved pins that eviction is
// DEFERRED only when the peer is genuinely unresolved: nil K8s id AND
// no kernel slot. This is the legitimately-skip case the pre-C3 code
// conflated with id 0.
func TestEvictByUIDMismatchDefersWhenTrulyUnresolved(t *testing.T) {
	const rd = "pvc-c3-defer"

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status -j "+rd, storage.FakeResponse{
		Stdout: []byte(`[{"name":"` + rd + `","connections":[]}]`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "self",
	})

	desired := []intent.DesiredPeer{
		{Name: "w1", NodeID: nil, ResourceUID: "new-uid"},
	}
	applied := map[string]string{"w1": "old-uid"}
	devices := map[int32]string{0: "/dev/pool/vol0"}

	cleaned, err := rec.EvictPeersByUIDMismatch(context.Background(), rd, desired, applied, nil, devices)
	if err != nil {
		t.Fatalf("EvictPeersByUIDMismatch: %v", err)
	}

	if len(cleaned) != 0 {
		t.Fatalf("eviction should DEFER for truly-unresolved peer, got cleaned=%v", cleaned)
	}

	for _, c := range fx.CommandLines() {
		if strings.HasPrefix(c, "drbdadm del-peer") {
			t.Errorf("del-peer must NOT run for truly-unresolved peer; cmds=%v", fx.CommandLines())
		}
	}
}

func containsCmd(cmds []string, want string) bool {
	for _, c := range cmds {
		if c == want {
			return true
		}
	}

	return false
}

func containsForgetPeer(cmds []string, rd string, nodeID int32) bool {
	want := "forget-peer " + itoa(nodeID)
	for _, c := range cmds {
		if strings.Contains(c, "drbdmeta") && strings.Contains(c, rd) && strings.HasSuffix(c, want) {
			return true
		}
	}

	return false
}
