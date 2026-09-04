//go:build integration

// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// node delete asks whether one node is still referenced, and it used to answer
// by listing every Resource in the cluster and filtering client-side. The CRD
// declares spec.nodeName as a selectable field so the API server can answer
// it, and that only works if the field is actually declared — a selector on an
// undeclared field is REJECTED, not silently ignored, so this asserts against
// a running API server rather than against the generated YAML.
//
// The loudness is the point. The alternative, a label selector, would return
// a partial-but-correct subset for replicas applied by hand, which is the
// shape that once hid diskful replicas from the clone handler.
func TestResourceNodeFieldSelectorIsServedByTheAPIServer(t *testing.T) {
	stack := harness.StartStack(t)
	ctx := context.Background()

	seed := []struct{ rd, node string }{
		{"pvc-a", "node-1"},
		{"pvc-b", "node-1"},
		{"pvc-c", "node-2"},
	}

	for _, s := range seed {
		res := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: s.rd + "." + s.node},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: s.rd,
				NodeName:               s.node,
			},
		}

		if err := stack.Env.Client.Create(ctx, res); err != nil {
			t.Fatalf("seed %s on %s: %v", s.rd, s.node, err)
		}
	}

	var got blockstoriov1alpha1.ResourceList

	err := stack.Env.Client.List(ctx, &got, client.MatchingFields{"spec.nodeName": "node-1"})
	if err != nil {
		t.Fatalf("the API server refused a selector on spec.nodeName, so the CRD does "+
			"not declare it and every node-scoped read falls back to listing the "+
			"whole cluster: %v", err)
	}

	if len(got.Items) != 2 {
		t.Fatalf("selector returned %d replicas, want the 2 on node-1", len(got.Items))
	}

	for i := range got.Items {
		if got.Items[i].Spec.NodeName != "node-1" {
			t.Errorf("selector returned a replica on %s", got.Items[i].Spec.NodeName)
		}
	}
}

// The store falls back to an exhaustive read when the selector is refused,
// which is what a cluster running an older CRD does. That branch is otherwise
// only reachable on such a cluster, so it is exercised here by taking the
// selectable fields off the live CRD and asking the store the same question.
func TestListByNodeFallsBackWhenTheSelectorIsRefused(t *testing.T) {
	stack := harness.StartStack(t)
	ctx := context.Background()

	for _, s := range []struct{ rd, node string }{
		{"fb-a", "node-1"},
		{"fb-b", "node-1"},
		{"fb-c", "node-2"},
	} {
		res := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: s.rd + "." + s.node},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: s.rd,
				NodeName:               s.node,
			},
		}

		if err := stack.Env.Client.Create(ctx, res); err != nil {
			t.Fatalf("seed %s on %s: %v", s.rd, s.node, err)
		}
	}

	stripSelectableFields(t, ctx, stack)

	st := k8sstore.New(stack.Env.Client)

	got, err := st.Resources().ListByNode(ctx, "node-1")
	if err != nil {
		t.Fatalf("the fallback did not answer: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("fallback returned %d replicas, want the 2 on node-1", len(got))
	}

	for i := range got {
		if got[i].NodeName != "node-1" {
			t.Errorf("fallback returned a replica on %s", got[i].NodeName)
		}
	}
}

// stripSelectableFields removes the selectable fields from the live Resource
// CRD and waits until the API server actually refuses a selector on them.
func stripSelectableFields(t *testing.T, ctx context.Context, stack *harness.Stack) {
	t.Helper()

	crds := crdClient(t, stack)

	const crdName = "resources.blockstor.cozystack.io"

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var crd apiextensionsv1.CustomResourceDefinition

		if err := crds.Get(ctx, types.NamespacedName{Name: crdName}, &crd); err != nil {
			return err
		}

		for i := range crd.Spec.Versions {
			crd.Spec.Versions[i].SelectableFields = nil
		}

		return crds.Update(ctx, &crd)
	})
	if err != nil {
		t.Fatalf("strip the selectable fields: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		var probe blockstoriov1alpha1.ResourceList

		if stack.Env.Client.List(ctx, &probe, client.MatchingFields{"spec.nodeName": "node-1"}) != nil {
			return
		}

		if time.Now().After(deadline) {
			t.Fatal("the API server never stopped serving the selector")
		}

		time.Sleep(200 * time.Millisecond)
	}
}
