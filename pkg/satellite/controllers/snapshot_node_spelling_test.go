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

package controllers

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// A snapshot's node list carries the registered spelling, Node-1, and the
// satellite started as node-1 takes it, both in the event filter and in the
// reconcile.
func TestSatelliteTakesASnapshotListingItsNodeInAnotherSpelling(t *testing.T) {
	t.Parallel()

	pred := snapshotNodePredicate("node-1")

	for spelling, want := range map[string]bool{"Node-1": true, "node-1": true, "node-2": false} {
		snap := &blockstoriov1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "rd.snap"},
			Spec:       blockstoriov1alpha1.SnapshotSpec{Nodes: []string{spelling}},
		}

		if got := pred.Create(event.CreateEvent{Object: snap}); got != want {
			t.Errorf("predicate for Nodes [%s] = %v, want %v", spelling, got, want)
		}

		if got := listsNode(snap.Spec.Nodes, "node-1"); got != want {
			t.Errorf("listsNode([%s], node-1) = %v, want %v", spelling, got, want)
		}
	}
}

// The fetcher reads a peer's address from the Node CR, named from the
// folded name, while the snapshot lists the peer as Node-2.
func TestFetcherResolvesAPeerListedInTheRegisteredSpelling(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	peer := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-2"},
		Spec: blockstoriov1alpha1.NodeSpec{
			NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{{Name: "default", Address: "192.0.2.2"}},
		},
	}

	f := &SnapshotFetcher{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(peer).Build(), NodeName: "node-1"}

	addr, err := f.resolvePeerAddr(t.Context(), "Node-2")
	if err != nil {
		t.Fatalf("resolvePeerAddr(Node-2): %v", err)
	}

	if addr == "" {
		t.Error("resolvePeerAddr(Node-2) gave no address")
	}
}
