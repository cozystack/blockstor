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
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

var errInjectedParentRead = errors.New("injected parent read failure")

func crScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

// A definition delete under another spelling never passes its snapshot check
// on a short listing. Whichever read of the definition fails, the delete
// either refuses or deletes nothing that still has a snapshot.
func TestAFailedParentReadNeverLetsADeletePassItsSnapshots(t *testing.T) {
	t.Parallel()

	for fail := int32(1); fail <= 8; fail++ {
		rd := crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Pvc-Reg")}}
		k8s.SetOriginalName(&rd.ObjectMeta, "Pvc-Reg")

		base := fake.NewClientBuilder().WithScheme(crScheme(t)).WithObjects(&rd, &crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-reg.snap-1"},
			Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "Pvc-Reg", SnapshotName: "snap-1"},
		}).Build()

		var gets atomic.Int32

		cli := interceptor.NewClient(base, interceptor.Funcs{
			Get: func(ctx context.Context, c ctrlclient.WithWatch, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
				if _, ok := obj.(*crdv1alpha1.ResourceDefinition); ok && gets.Add(1) == fail {
					return errInjectedParentRead
				}

				return c.Get(ctx, key, obj, opts...)
			},
		})

		addr, stop := startServerWithStore(t, k8s.New(cli))
		resp := httpDelete(t, addr+"/v1/resource-definitions/PVC-REG")
		_ = resp.Body.Close()

		stop()

		var snap crdv1alpha1.Snapshot

		snapErr := base.Get(t.Context(), types.NamespacedName{Name: "pvc-reg.snap-1"}, &snap)

		var left crdv1alpha1.ResourceDefinition

		rdErr := base.Get(t.Context(), types.NamespacedName{Name: rd.Name}, &left)

		if snapErr == nil && (rdErr != nil || left.DeletionTimestamp != nil) {
			t.Errorf("read %d failed: DELETE answered %d and took the definition away from its snapshot", fail, resp.StatusCode)
		}

		if resp.StatusCode == http.StatusOK && snapErr == nil {
			t.Errorf("read %d failed: DELETE answered 200 over a definition with a snapshot", fail)
		}
	}
}

// My_Node and my_node are outside RFC 1123: they hash to two CR names and are
// two nodes. A snapshot targeting both is not successful on one entry.
func TestSnapshotSuccessKeepsTwoNodesOfOneLowercaseApart(t *testing.T) {
	t.Parallel()

	diskful := map[string]struct{}{nodeKey("My_Node"): {}, nodeKey("my_node"): {}}
	snap := &apiv1.Snapshot{
		Nodes:     []string{"My_Node", "my_node"},
		Snapshots: []apiv1.SnapshotPerNode{{NodeName: "my_node", CreateTimestamp: 1}},
	}

	if isSnapshotSuccessful(snap, diskful) {
		t.Error("a snapshot reported on my_node alone reads as successful on My_Node too")
	}
}

// An entry reported under the node's CR name matches the node as registered.
func TestSnapshotSuccessMatchesAnEntryUnderTheCRName(t *testing.T) {
	t.Parallel()

	diskful := map[string]struct{}{nodeKey("My_Node"): {}}
	snap := &apiv1.Snapshot{
		Nodes:     []string{"My_Node"},
		Snapshots: []apiv1.SnapshotPerNode{{NodeName: k8s.Name("My_Node"), CreateTimestamp: 1}},
	}

	if !isSnapshotSuccessful(snap, diskful) {
		t.Error("an entry under My_Node's CR name does not count for My_Node")
	}
}

// A restore takes the pool of the source's replica on the node it lands on,
// and My_Node's replica is not my_node's.
func TestRestorePoolKeepsTwoNodesOfOneLowercaseApart(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(crScheme(t)).Build()
	st := k8s.New(cli)
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-two"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	for node, pool := range map[string]string{"My_Node": "pool-upper", "my_node": "pool-lower"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node}); err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-two", NodeName: node, Props: map[string]string{"StorPoolName": pool},
		}); err != nil {
			t.Fatalf("seed replica on %s: %v", node, err)
		}
	}

	byNode, _ := storPoolsByNodeFromSourceRD(ctx, st, "pvc-two")
	if got := byNode[nodeKey("My_Node")]; got != "pool-upper" {
		t.Errorf("pool for My_Node = %q, want pool-upper", got)
	}

	if got := byNode[nodeKey("my_node")]; got != "pool-lower" {
		t.Errorf("pool for my_node = %q, want pool-lower", got)
	}
}

// A replica created over HTTP shows its parents as the store resolved them,
// whatever reserved annotations the request carried.
func TestAnHTTPCreateCannotForgeTheShownParentNames(t *testing.T) {
	t.Parallel()

	st := k8s.New(fake.NewClientBuilder().WithScheme(crScheme(t)).Build())
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "lower-rd"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "lower-node"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	addr, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceCreate{Resource: apiv1.Resource{
		NodeName: "lower-node",
		Flags:    []string{apiv1.ResourceFlagDiskless},
		Annotations: map[string]string{
			k8s.AnnotationLinstorRDName:   "wrong-rd",
			k8s.AnnotationLinstorNodeName: "wrong-node",
		},
	}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, addr+"/v1/resource-definitions/lower-rd/resources", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create answered %d", resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, "lower-rd", "lower-node")
	if err != nil || got.Name != "lower-rd" || got.NodeName != "lower-node" {
		t.Errorf("replica = %s/%s, %v; want lower-rd/lower-node", got.Name, got.NodeName, err)
	}
}
