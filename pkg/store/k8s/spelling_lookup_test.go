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

package k8s_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

var errParentRead = errors.New("injected parent read failure")

func spellingScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

func registeredRD(name string) *crdv1alpha1.ResourceDefinition {
	rd := &crdv1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name(name)}}
	k8s.SetOriginalName(&rd.ObjectMeta, name)

	return rd
}

func registeredNode(name string) *crdv1alpha1.Node {
	node := &crdv1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name(name)}}
	k8s.SetOriginalName(&node.ObjectMeta, name)

	return node
}

// failParentGets fails the first Get of a definition or node, the way an
// API server times out once; every later Get answers.
func failParentGets(base ctrlclient.WithWatch) ctrlclient.WithWatch {
	var failed atomic.Bool

	return interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c ctrlclient.WithWatch, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
			switch obj.(type) {
			case *crdv1alpha1.ResourceDefinition, *crdv1alpha1.Node:
				if failed.CompareAndSwap(false, true) {
					return errParentRead
				}
			}

			return c.Get(ctx, key, obj, opts...)
		},
	})
}

// A listing whose parent cannot be read fails, rather than coming back short
// of the rows stored under the registered spelling: a delete would read the
// short answer as "no snapshots" and leave them behind.
func TestAListingWhoseParentCannotBeReadFails(t *testing.T) {
	t.Parallel()

	newStore := func() *k8s.Store {
		return k8s.New(failParentGets(fake.NewClientBuilder().WithScheme(spellingScheme(t)).WithObjects(
			registeredRD("Pvc-Reg"), registeredNode("Node-Reg"),
			&crdv1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-reg.node-reg"},
				Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Reg", NodeName: "Node-Reg"},
			},
			&crdv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-reg.snap-1"},
				Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "Pvc-Reg", SnapshotName: "snap-1"},
			},
		).Build()))
	}

	ctx := context.Background()

	if snaps, err := newStore().Snapshots().ListByDefinitionUncached(ctx, "PVC-REG"); !errors.Is(err, errParentRead) {
		t.Errorf("uncached snapshots of PVC-REG = %v, %v; want the read failure", snaps, err)
	}

	if snaps, err := newStore().Snapshots().ListByDefinition(ctx, "PVC-REG"); !errors.Is(err, errParentRead) {
		t.Errorf("snapshots of PVC-REG = %v, %v; want the read failure", snaps, err)
	}

	if replicas, err := newStore().Resources().ListByDefinition(ctx, "PVC-REG"); !errors.Is(err, errParentRead) {
		t.Errorf("replicas of PVC-REG = %v, %v; want the read failure", replicas, err)
	}

	if replicas, err := newStore().Resources().ListByNode(ctx, "NODE-REG"); !errors.Is(err, errParentRead) {
		t.Errorf("replicas on NODE-REG = %v, %v; want the read failure", replicas, err)
	}
}

// A write whose parent cannot be read fails, rather than storing a name the
// readers will not find.
func TestAWriteWhoseParentCannotBeReadFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	newStore := func() *k8s.Store {
		return k8s.New(failParentGets(fake.NewClientBuilder().WithScheme(spellingScheme(t)).WithObjects(
			registeredRD("Pvc-W"), registeredNode("Node-W"),
		).Build()))
	}

	if err := newStore().Resources().Create(ctx, &apiv1.Resource{Name: "pvc-w", NodeName: "node-w"}); !errors.Is(err, errParentRead) {
		t.Errorf("replica create = %v; want the read failure", err)
	}

	if err := newStore().Snapshots().Create(ctx, &apiv1.Snapshot{Name: "snap-1", ResourceName: "pvc-w"}); !errors.Is(err, errParentRead) {
		t.Errorf("snapshot create = %v; want the read failure", err)
	}
}

// My_Res and my_res are outside RFC 1123, so they hash to two CR names and
// are two definitions. A listing of one does not return the other's rows,
// including rows the other stores under its own lowercase spelling.
func TestAListingAsksNoOtherDefinitionsSpelling(t *testing.T) {
	t.Parallel()

	st := k8s.New(fake.NewClientBuilder().WithScheme(spellingScheme(t)).WithObjects(
		registeredRD("My_Res"), registeredRD("my_res"), registeredNode("node-a"),
		&crdv1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "lower-replica"},
			Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "my_res", NodeName: "node-a"},
		},
		&crdv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "lower-snap"},
			Spec:       crdv1alpha1.SnapshotSpec{ResourceDefinitionName: "my_res", SnapshotName: "snap-1"},
		},
	).Build())

	ctx := context.Background()

	if replicas, err := st.Resources().ListByDefinition(ctx, "My_Res"); err != nil || len(replicas) != 0 {
		t.Errorf("replicas of My_Res = %v, %v; want none, the row is my_res's", replicas, err)
	}

	if snaps, err := st.Snapshots().ListByDefinition(ctx, "My_Res"); err != nil || len(snaps) != 0 {
		t.Errorf("snapshots of My_Res = %v, %v; want none, the row is my_res's", snaps, err)
	}

	if replicas, err := st.Resources().ListByDefinition(ctx, "my_res"); err != nil || len(replicas) != 1 {
		t.Errorf("replicas of my_res = %v, %v; want its own row", replicas, err)
	}

	if snaps, err := st.Snapshots().ListByDefinition(ctx, "my_res"); err != nil || len(snaps) != 1 {
		t.Errorf("snapshots of my_res = %v, %v; want its own row", snaps, err)
	}
}

// The names a child shows for its parents are the store's to derive. A
// caller writing the reserved annotations, on create or on update, does not
// replace them.
func TestACallerCannotForgeTheShownParentNames(t *testing.T) {
	t.Parallel()

	forged := map[string]string{
		k8s.AnnotationLinstorRDName:   "wrong-rd",
		k8s.AnnotationLinstorNodeName: "wrong-node",
		"user/kept":                   "yes",
	}

	st := k8s.New(fake.NewClientBuilder().WithScheme(spellingScheme(t)).WithObjects(
		registeredRD("lower-rd"), registeredNode("lower-node"),
	).Build())

	ctx := context.Background()

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "lower-rd", NodeName: "lower-node", Annotations: forged}); err != nil {
		t.Fatalf("replica create: %v", err)
	}

	got, err := st.Resources().Get(ctx, "lower-rd", "lower-node")
	if err != nil || got.Name != "lower-rd" || got.NodeName != "lower-node" || got.Annotations["user/kept"] != "yes" {
		t.Errorf("replica after create = %+v, %v; want lower-rd/lower-node with the caller's own annotation", got, err)
	}

	got.Annotations = forged
	if err := st.Resources().Update(ctx, &got); err != nil {
		t.Fatalf("replica update: %v", err)
	}

	got, err = st.Resources().Get(ctx, "lower-rd", "lower-node")
	if err != nil || got.Name != "lower-rd" || got.NodeName != "lower-node" {
		t.Errorf("replica after update = %+v, %v; want lower-rd/lower-node", got, err)
	}

	if err := st.Resources().PatchResourceSpec(ctx, "lower-rd", "lower-node", func(r *apiv1.Resource) error {
		r.Annotations = forged

		return nil
	}); err != nil {
		t.Fatalf("replica patch: %v", err)
	}

	got, err = st.Resources().Get(ctx, "lower-rd", "lower-node")
	if err != nil || got.Name != "lower-rd" || got.NodeName != "lower-node" {
		t.Errorf("replica after patch = %+v, %v; want lower-rd/lower-node", got, err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "snap-1", ResourceName: "lower-rd", Annotations: forged}); err != nil {
		t.Fatalf("snapshot create: %v", err)
	}

	snap, err := st.Snapshots().Get(ctx, "lower-rd", "snap-1")
	if err != nil || snap.ResourceName != "lower-rd" {
		t.Errorf("snapshot after create = %+v, %v; want it under lower-rd", snap, err)
	}

	snap.Annotations = forged
	if err := st.Snapshots().Update(ctx, &snap); err != nil {
		t.Fatalf("snapshot update: %v", err)
	}

	snap, err = st.Snapshots().Get(ctx, "lower-rd", "snap-1")
	if err != nil || snap.ResourceName != "lower-rd" {
		t.Errorf("snapshot after update = %+v, %v; want it under lower-rd", snap, err)
	}
}
