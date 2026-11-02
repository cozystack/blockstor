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

package dispatcher_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

const peerEndpoint = "192.0.2.2"

func registeredNode(registered string, spec blockstoriov1alpha1.NodeSpec) blockstoriov1alpha1.Node {
	node := blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name(registered)}, Spec: spec}
	k8s.SetOriginalName(&node.ObjectMeta, registered)

	return node
}

func replicaOn(node string, id int32) blockstoriov1alpha1.Resource {
	return blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1." + k8s.Name(node)},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               node,
			StoragePool:            "data",
		},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &id},
	}
}

// A peer resolves to its node's address whichever spelling the replica
// stores: the node's CR name, which the store writes, or the registered
// spelling a replica written before carries.
func TestPeerAddressResolvesUnderEverySpellingOfItsNode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		registered string
		stored     string
	}{
		{name: "lowercase", registered: "node-2", stored: "node-2"},
		{name: "mixed case, CR name stored", registered: "Node-2", stored: k8s.Name("Node-2")},
		{name: "mixed case, registered spelling stored", registered: "Node-2", stored: "Node-2"},
		{name: "outside RFC 1123, CR name stored", registered: "my_node", stored: k8s.Name("my_node")},
		{name: "outside RFC 1123, registered spelling stored", registered: "my_node", stored: "my_node"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target := replicaOn("node-1", 0)
			peer := replicaOn(tc.stored, 1)
			nodes := []blockstoriov1alpha1.Node{
				registeredNode("node-1", blockstoriov1alpha1.NodeSpec{SatelliteEndpoint: "192.0.2.1"}),
				registeredNode(tc.registered, blockstoriov1alpha1.NodeSpec{SatelliteEndpoint: peerEndpoint}),
			}

			got := dispatcher.BuildDesired(&target, []blockstoriov1alpha1.Resource{peer}, nodes, nil, nil, nil)

			key := "peer." + tc.stored + ".address"
			if addr := got.DrbdOptions[key]; addr != peerEndpoint {
				t.Errorf("%s = %q, want %s", key, addr, peerEndpoint)
			}
		})
	}
}

// A case variant of a name outside RFC 1123 hashes to another CR name, so
// My_Node is another node than my_node, the way the store keeps them: a
// replica stored as My_Node is not given my_node's address.
func TestACaseVariantOutsideRFC1123IsAnotherNode(t *testing.T) {
	t.Parallel()

	target := replicaOn("node-1", 0)
	peer := replicaOn("My_Node", 1)
	nodes := []blockstoriov1alpha1.Node{
		registeredNode("node-1", blockstoriov1alpha1.NodeSpec{SatelliteEndpoint: "192.0.2.1"}),
		registeredNode("my_node", blockstoriov1alpha1.NodeSpec{SatelliteEndpoint: peerEndpoint}),
	}

	got := dispatcher.BuildDesired(&target, []blockstoriov1alpha1.Resource{peer}, nodes, nil, nil, nil)
	if addr := got.DrbdOptions["peer.My_Node.address"]; addr == peerEndpoint {
		t.Errorf("peer.My_Node.address = %s, the address of my_node, another node", addr)
	}
}

// A PrefNic on the peer's pool, and a NetInterface it names on the node,
// are found when the replica stores the node's CR name and the pool its
// registered spelling.
func TestPeerPrefNicResolvesUnderTheCRNameOfItsNode(t *testing.T) {
	t.Parallel()

	const replication = "192.0.2.22"

	target := replicaOn("node-1", 0)
	peer := replicaOn(k8s.Name("Node-2"), 1)
	nodes := []blockstoriov1alpha1.Node{
		registeredNode("node-1", blockstoriov1alpha1.NodeSpec{SatelliteEndpoint: "192.0.2.1"}),
		registeredNode("Node-2", blockstoriov1alpha1.NodeSpec{
			SatelliteEndpoint: peerEndpoint,
			NetInterfaces:     []blockstoriov1alpha1.NodeNetInterface{{Name: "repl", Address: replication}},
		}),
	}
	pools := []blockstoriov1alpha1.StoragePool{{
		ObjectMeta: metav1.ObjectMeta{Name: "data.node-2"},
		Spec: blockstoriov1alpha1.StoragePoolSpec{
			NodeName: "Node-2",
			PoolName: "data",
			Props:    map[string]string{"PrefNic": "repl"},
		},
	}}

	got := dispatcher.BuildDesired(&target, []blockstoriov1alpha1.Resource{peer}, nodes, pools, nil, nil)

	if addr := got.DrbdOptions["peer.node-2.address"]; addr != replication {
		t.Errorf("peer.node-2.address = %q, want the PrefNic address %s", addr, replication)
	}
}
