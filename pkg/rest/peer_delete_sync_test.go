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
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedRDWithReplicas seeds a ResourceDefinition + the requested
// per-node Resources into the in-memory store. Used by the
// waitForPeerDeletionAcks unit tests so each test starts from a
// known-shape RD without re-running autoplace.
func seedRDWithReplicas(t *testing.T, st store.Store, rdName string, nodes []string) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range nodes {
		if err := st.Nodes().Create(t.Context(), &apiv1.Node{
			Name:             n,
			Type:             apiv1.NodeTypeSatellite,
			ConnectionStatus: apiv1.NodeTypeOnline,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.Resources().Create(t.Context(), &apiv1.Resource{
			Name:     rdName,
			NodeName: n,
		}); err != nil {
			t.Fatalf("seed resource %s.%s: %v", rdName, n, err)
		}
	}
}

// stampSiblingAck patches the peer-forget ACK annotation onto
// sibling `sibNode` so waitForPeerDeletionAcks observes the ACK
// as if the satellite FSM had run.
func stampSiblingAck(t *testing.T, st store.Store, rdName, sibNode, doomedNode string) {
	t.Helper()

	key := peerForgetAckAnnotationPrefix + doomedNode

	err := st.Resources().PatchResourceSpec(t.Context(), rdName, sibNode, func(live *apiv1.Resource) error {
		if live.Annotations == nil {
			live.Annotations = map[string]string{}
		}

		live.Annotations[key] = time.Now().UTC().Format(time.RFC3339Nano)

		return nil
	})
	if err != nil {
		t.Fatalf("stamp sibling ACK on %s: %v", sibNode, err)
	}
}

// TestWaitForPeerDeletionAcksHappyPath: once every online sibling
// stamps the ACK annotation, the helper returns promptly (well
// under the 15s timeout). Mirrors the production happy path where
// every satellite reconcile fires sub-second.
func TestWaitForPeerDeletionAcksHappyPath(t *testing.T) {
	defer restorePeerDeleteAckTimeout(t)
	peerDeleteAckTimeout = 2 * time.Second
	peerDeletePollInterval = 25 * time.Millisecond

	st := store.NewInMemory()
	seedRDWithReplicas(t, st, "pvc-happy", []string{"n1", "n2", "n3"})

	// Pre-stamp ACKs on every online sibling (n2 + n3); n1 is the
	// doomed node and never gets queried.
	stampSiblingAck(t, st, "pvc-happy", "n2", "n1")
	stampSiblingAck(t, st, "pvc-happy", "n3", "n1")

	s := &Server{Store: st}
	start := time.Now()
	s.waitForPeerDeletionAcks(t.Context(), "pvc-happy", "n1")
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("happy path took %s; want < 500ms (ACKs pre-stamped)", elapsed)
	}
}

// TestWaitForPeerDeletionAcksTimeout: no sibling stamps an ACK —
// the helper MUST return after the configured timeout without
// blocking forever. The REST handler then proceeds with the
// physical Delete (pre-v10 fallback).
func TestWaitForPeerDeletionAcksTimeout(t *testing.T) {
	defer restorePeerDeleteAckTimeout(t)
	peerDeleteAckTimeout = 150 * time.Millisecond
	peerDeletePollInterval = 25 * time.Millisecond

	st := store.NewInMemory()
	seedRDWithReplicas(t, st, "pvc-timeout", []string{"n1", "n2"})

	s := &Server{Store: st}
	start := time.Now()
	s.waitForPeerDeletionAcks(t.Context(), "pvc-timeout", "n1")
	elapsed := time.Since(start)

	if elapsed < peerDeleteAckTimeout {
		t.Errorf("timeout path took %s; want >= %s", elapsed, peerDeleteAckTimeout)
	}

	if elapsed > 2*peerDeleteAckTimeout+200*time.Millisecond {
		t.Errorf("timeout path took %s; want roughly %s", elapsed, peerDeleteAckTimeout)
	}
}

// TestWaitForPeerDeletionAcksSkipsOffline: an OFFLINE sibling is
// unreachable; the waiter MUST treat it as ACK-satisfied so the
// caller doesn't block on a node that can never write the
// annotation. n2 is OFFLINE; n3 is online and stamps an ACK;
// total time must be sub-second (online ACK lands fast, OFFLINE
// skipped).
func TestWaitForPeerDeletionAcksSkipsOffline(t *testing.T) {
	defer restorePeerDeleteAckTimeout(t)
	peerDeleteAckTimeout = 2 * time.Second
	peerDeletePollInterval = 25 * time.Millisecond

	st := store.NewInMemory()
	seedRDWithReplicas(t, st, "pvc-offline", []string{"n1", "n2", "n3"})

	// Mark n2 OFFLINE; n3 stays online and ACKs.
	if err := st.Nodes().Update(t.Context(), &apiv1.Node{
		Name:             "n2",
		Type:             apiv1.NodeTypeSatellite,
		ConnectionStatus: apiv1.NodeTypeOffline,
	}); err != nil {
		t.Fatalf("mark n2 offline: %v", err)
	}

	stampSiblingAck(t, st, "pvc-offline", "n3", "n1")

	s := &Server{Store: st}
	start := time.Now()
	s.waitForPeerDeletionAcks(t.Context(), "pvc-offline", "n1")
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("OFFLINE-skip path took %s; want < 500ms (n3 ACKed, n2 skipped as OFFLINE)", elapsed)
	}
}

// TestWaitForPeerDeletionAcksAllOffline: when every sibling is
// OFFLINE the waiter returns immediately — there's no reachable
// satellite that could ever ACK, so blocking would be wasted time.
func TestWaitForPeerDeletionAcksAllOffline(t *testing.T) {
	defer restorePeerDeleteAckTimeout(t)
	peerDeleteAckTimeout = 5 * time.Second

	st := store.NewInMemory()
	seedRDWithReplicas(t, st, "pvc-all-offline", []string{"n1", "n2", "n3"})

	for _, name := range []string{"n2", "n3"} {
		if err := st.Nodes().Update(t.Context(), &apiv1.Node{
			Name:             name,
			Type:             apiv1.NodeTypeSatellite,
			ConnectionStatus: apiv1.NodeTypeOffline,
		}); err != nil {
			t.Fatalf("mark %s offline: %v", name, err)
		}
	}

	s := &Server{Store: st}
	start := time.Now()
	s.waitForPeerDeletionAcks(t.Context(), "pvc-all-offline", "n1")
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("all-offline path took %s; want < 100ms (no online sibling to wait for)", elapsed)
	}
}

// restorePeerDeleteAckTimeout snapshots the default tunables on
// entry and restores them when the test exits. Lets each test
// override independently without leaking shorter timeouts into
// concurrently-running peers.
func restorePeerDeleteAckTimeout(t *testing.T) {
	t.Helper()

	origTimeout := peerDeleteAckTimeout
	origInterval := peerDeletePollInterval

	t.Cleanup(func() {
		peerDeleteAckTimeout = origTimeout
		peerDeletePollInterval = origInterval
	})
}
