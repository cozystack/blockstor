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
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

var errPastTheDefinition = errors.New("reached the peer read after the definition Get")

// storedMixedCaseReplica registers a definition as Pvc-Mix and a node as
// Node-1, creates a replica of it through the store in yet another
// spelling, and returns the client and the replica as stored.
func storedMixedCaseReplica(t *testing.T, funcs interceptor.Funcs) (client.Client, *blockstoriov1alpha1.Resource) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	rd := blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Pvc-Mix")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "Pvc-Mix")

	node := blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Node-1")}}
	k8s.SetOriginalName(&node.ObjectMeta, "Node-1")

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rd, &node).Build()

	ctx := context.Background()

	if err := k8s.New(base).Resources().Create(ctx, &apiv1.Resource{Name: "PVC-MIX", NodeName: "NODE-1"}); err != nil {
		t.Fatalf("create the replica through the store: %v", err)
	}

	var list blockstoriov1alpha1.ResourceList
	if err := base.List(ctx, &list); err != nil || len(list.Items) != 1 {
		t.Fatalf("list the stored replica = %v, %v", list.Items, err)
	}

	res := list.Items[0]
	res.Finalizers = append(res.Finalizers, SatelliteResourceFinalizer)

	if err := base.Update(ctx, &res); err != nil {
		t.Fatalf("stamp the satellite finalizer: %v", err)
	}

	return interceptor.NewClient(base, funcs), &res
}

// The satellite applies a replica of a mixed-case definition: its reconcile
// finds the definition by the stored name and goes on to read the peers.
// With the registered spelling in spec the Get answered NotFound and the
// reconcile returned without applying or requeueing.
func TestSatelliteAppliesAReplicaOfAMixedCaseDefinition(t *testing.T) {
	t.Parallel()

	cli, res := storedMixedCaseReplica(t, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*blockstoriov1alpha1.ResourceList); ok {
				return errPastTheDefinition
			}

			return c.List(ctx, list, opts...)
		},
	})

	r := &ResourceReconciler{Client: cli, Config: Config{NodeName: "node-1"}}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: res.Name}})
	if !errors.Is(err, errPastTheDefinition) {
		t.Fatalf("reconcile = %v, want it past the definition Get into the peer read", err)
	}
}

// The DRBD resource and its volumes are named after the stored definition
// name, so a mixed-case definition keeps the lowercase names on disk that a
// replica written before has, pvc-mix and pvc-mix_00000.
func TestMixedCaseReplicaKeepsLowercaseNamesOnDisk(t *testing.T) {
	t.Parallel()

	_, res := storedMixedCaseReplica(t, interceptor.Funcs{})

	desired := dispatcher.BuildDesired(res, nil, nil, nil, nil, nil)
	if desired.GetName() != "pvc-mix" || desired.GetNodeName() != "node-1" {
		t.Errorf("desired resource %q on %q, want pvc-mix on node-1", desired.GetName(), desired.GetNodeName())
	}
}
